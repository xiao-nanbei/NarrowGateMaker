#!/usr/bin/env python3
"""Train and gate the SELL repair-vs-trend-through skip family.

The randomized behavior policy acts once per short inventory campaign on the
first baseline-eligible exposure-increasing SELL quote. It either preserves
the baseline quote or skips that single quote cycle with exact 50/50
propensity. A chronological doubly robust learner may select skip only in a
high baseline trend-through-risk region and in honest leaves where reward,
campaign cost, and competing-risk effects agree.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeRegressor

from research.families.f09_campaign_action_uplift.audit.buy_conditional_widen_cate import (
    _bootstrap_by_day,
    _directory_identity,
    _file_identity,
    _honest_day_split,
    _numeric,
    _write_immutable_json,
)
from models.audit.evidence_split import load_evidence_panel
from models.audit.experiment_manifest import git_workspace_identity
from research.families.f09_campaign_action_uplift.audit.local_action_uplift import validate_action_panel
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import OPEConfig, make_day_folds
from models.replay_policies import SELL_ADD_SKIP_ACTIONS

ROOT = Path(__file__).resolve().parents[4]
FAMILY_ID = "sell_add_repair_trend_skip_causal_v4_v1"
SCHEMA_VERSION = "sell_repair_trend_skip_cate.v1"
CONTROL_ACTION = "baseline"
CANDIDATE_ACTION = "skip_one_add_cycle"
SUPPORTED_ACTIONS = SELL_ADD_SKIP_ACTIONS
COMPETING_HORIZON_S = 1_800.0
TAIL_THRESHOLD_USDC = -5.0

FEATURES = (
    "short_inventory_ratio",
    "campaign_pnl_so_far",
    "campaign_loss_so_far",
    "campaign_mae_abs_so_far",
    "campaign_max_abs_qty_so_far",
    "campaign_log_age_s",
    "campaign_add_count_so_far",
    "shock_adverse_flow_imbalance_1s",
    "shock_adverse_flow_imbalance_5s",
    "shock_adverse_flow_imbalance_since_fill",
    "shock_log1p_adverse_qty_to_depth_5s",
    "shock_log1p_adverse_qty_to_depth_since_fill",
    "shock_adverse_move_bps",
    "shock_log1p_time_to_extreme_ms",
    "refill_depletion_ratio",
    "refill_recovery_ratio",
    "refill_log1p_current_vs_start_ratio",
    "refill_log1p_half_life_ms",
    "refill_half_life_observed",
    "recovery_current_adverse_bps",
    "recovery_from_extreme_bps",
    "recovery_price_ratio",
    "recovery_microprice_current_adverse_bps",
    "recovery_microprice_ratio",
    "quote_distance_bps",
    "quote_delta_to_bbo_ticks",
    "trend_through_distance_bps",
    "exact_l2_spread_bps",
    "queue_log1p_ahead",
    "microprice_shift_bps",
    "toxicity",
    "markout_ema",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_log1p_near_depth",
    "decision_hours_to_day_end",
)

OUTCOMES = (
    "reward",
    "campaign_cost_avoidance",
    "negative_terminal_mtm",
    "development_q10_shortfall",
    "repair_first_30m",
    "trend_through_avoidance_30m",
    "competing_risk_utility_30m",
    "intervention_fill",
)

MODEL_SPEC = {
    "nuisance": {
        "model": "action_specific_ridge",
        "alpha": 10.0,
        "min_train_days": 50,
        "test_days": 10,
        "embargo_days": 1,
        "min_action_rows": 50,
    },
    "policy": {
        "model": "depth_2_honest_competing_risk_tree",
        "max_depth": 2,
        "min_structure_leaf_rows": 40,
        "min_estimation_leaf_rows": 30,
        "min_estimation_action_rows": 10,
        "min_estimation_leaf_days": 4,
        "min_train_days": 20,
        "test_days": 10,
        "embargo_days": 1,
        "baseline_trend_risk_quantile": 0.70,
    },
    "bootstrap": {"cluster": "UTC_day", "trials": 2_000, "seed": 20260718},
}

GATE_SPEC = {
    "primary": "reward uplift p2.5 > 0",
    "co_primary": "competing-risk utility uplift p2.5 > 0",
    "campaign": "campaign-cost avoidance uplift p2.5 >= 0",
    "trend": "trend-through avoidance uplift p2.5 >= 0",
    "downside": (
        "negative terminal MTM and development-q10 protection p2.5 >= 0"
    ),
    "repair": "repair-first point estimate >= 0",
    "support": (
        "skip rate in [0.03, 0.40], policy ESS >= 100, "
        "at least 10 evaluated days"
    ),
}


def freeze_family_spec(
    *,
    evidence_split_manifest: Path,
    config_path: Path,
    model_dir: Path,
    p3_artifact: Path,
    queue_artifact: Path,
    latency_tape: Path,
    output: Path,
) -> dict[str, Any]:
    split_path = evidence_split_manifest.expanduser().resolve()
    split = json.loads(split_path.read_text(encoding="utf-8"))
    if str(split.get("family_id")) != FAMILY_ID:
        raise ValueError("evidence split family_id does not match the trainer")
    family = split.get("action_family") or {}
    if list(family.get("actions") or ()) != list(SUPPORTED_ACTIONS):
        raise ValueError(
            "family must register only baseline and skip_one_add_cycle"
        )
    if family.get("behavior_probabilities") != {
        CONTROL_ACTION: 0.5,
        CANDIDATE_ACTION: 0.5,
    }:
        raise ValueError("family requires exact 50/50 behavior propensity")
    if family.get("sides") != ["SELL"]:
        raise ValueError("family must be SELL-only")

    payload = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "status": "frozen_before_outcome_replay",
        "surface": "SELL exposure-increasing add",
        "actions": list(SUPPORTED_ACTIONS),
        "behavior_probabilities": {
            CONTROL_ACTION: 0.5,
            CANDIDATE_ACTION: 0.5,
        },
        "buy_action": "baseline",
        "external_reference": False,
        "intervention": (
            "skip exactly one otherwise baseline-eligible SELL add quote cycle; "
            "the next cycle returns to baseline"
        ),
        "competing_risk": {
            "repair": "campaign reaches flat before quote-level trend-through",
            "trend_through": (
                "execution trade price reaches baseline SELL quote plus one tick "
                "before repair"
            ),
            "horizon_s": COMPETING_HORIZON_S,
            "right_censoring": (
                "decisions require at least 1800 seconds to UTC day end"
            ),
        },
        "invariants": {
            "size_modified": False,
            "reducing_side_modified": False,
            "inventory_limit_modified": False,
        },
        "features": list(FEATURES),
        "outcomes": list(OUTCOMES),
        "model_spec": MODEL_SPEC,
        "gate_spec": GATE_SPEC,
        "evidence_split": _file_identity(split_path),
        "config": _file_identity(config_path),
        "model_dir": _directory_identity(model_dir),
        "p3_artifact": _file_identity(p3_artifact),
        "queue_artifact": _file_identity(queue_artifact),
        "latency_tape": _file_identity(latency_tape),
        "workspace": git_workspace_identity(ROOT),
    }
    _write_immutable_json(output, payload)
    return payload


def _load_panel(
    path: Path,
    *,
    expected_panel: str,
    family_spec: dict[str, Any],
) -> pd.DataFrame:
    panel_path = path.expanduser().resolve()
    frame = pd.read_csv(panel_path)
    validate_action_panel(frame, actions=SUPPORTED_ACTIONS)
    frame["day"] = frame["day"].astype(str).str.slice(0, 10)
    frame["side"] = frame["side"].astype(str).str.upper()
    frame["inventory_role"] = frame["inventory_role"].astype(str).str.lower()
    frame["action"] = frame["action"].astype(str).str.lower()
    frame = frame[
        frame["side"].eq("SELL") & frame["inventory_role"].eq("add")
    ].copy()
    if frame.empty:
        raise ValueError("panel contains no SELL exposure-increasing add rows")
    if set(frame["action"]) != set(SUPPORTED_ACTIONS):
        raise ValueError("panel lacks baseline/skip overlap")
    for action in SUPPORTED_ACTIONS:
        values = pd.to_numeric(
            frame[f"behavior_prob_{action}"], errors="coerce"
        )
        if not np.allclose(values, 0.5, atol=1e-12, rtol=0.0):
            raise ValueError(f"{action} behavior probability is not 0.5")
    if pd.to_numeric(
        frame["external_reference_used"], errors="coerce"
    ).ne(0).any():
        raise ValueError("external reference is forbidden in this family")
    if (
        pd.to_numeric(frame["remaining_day_s"], errors="coerce")
        < COMPETING_HORIZON_S
    ).any():
        raise ValueError("panel violates the frozen competing-risk follow-up")
    events = set(frame["competing_event"].astype(str))
    if events - {"repair", "trend_through", "censored"}:
        raise ValueError(f"panel has invalid competing events: {events}")

    metadata_path = Path(
        str(panel_path).replace(".action_panel.csv", ".metadata.json")
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if str(metadata.get("panel_role")) != expected_panel:
        raise ValueError("panel metadata role differs from requested evidence")
    if str(metadata.get("action_family")) != "sell_add_skip":
        raise ValueError("panel was not generated by the SELL add-skip replay")
    split_identity = metadata.get("evidence_split") or {}
    if str(split_identity.get("manifest_sha256")) != str(
        family_spec["evidence_split"]["sha256"]
    ):
        raise ValueError("panel does not belong to the frozen evidence split")
    expected_days, _ = load_evidence_panel(
        Path(family_spec["evidence_split"]["path"]),
        expected_panel,
        allow_sealed_holdout=expected_panel == "sealed_holdout",
    )
    if sorted(frame["day"].unique()) != sorted(expected_days):
        raise ValueError(f"{expected_panel} days differ from the frozen split")
    return _derive_features(frame)


def _derive_features(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["short_inventory_ratio"] = np.maximum(
        0.0, -_numeric(output, "inventory_ratio")
    )
    output["campaign_loss_so_far"] = np.maximum(
        0.0, -_numeric(output, "campaign_pnl_so_far")
    )
    output["campaign_mae_abs_so_far"] = np.maximum(
        0.0, -_numeric(output, "campaign_mae_so_far")
    )
    output["campaign_log_age_s"] = np.log1p(
        np.maximum(0.0, _numeric(output, "campaign_age_s"))
    )
    output["queue_log1p_ahead"] = np.log1p(
        np.maximum(0.0, _numeric(output, "baseline_estimated_queue_ahead"))
    )
    output["l2_log1p_near_depth"] = np.log1p(
        np.maximum(0.0, _numeric(output, "l2_near_depth_total"))
    )
    decision_ms = _numeric(output, "decision_ts_ms")
    utc = pd.to_datetime(decision_ms, unit="ms", utc=True)
    seconds = (
        utc.dt.hour * 3_600
        + utc.dt.minute * 60
        + utc.dt.second
        + utc.dt.microsecond / 1_000_000.0
    )
    output["decision_hours_to_day_end"] = (
        86_400.0 - seconds
    ) / 3_600.0
    for feature in FEATURES:
        output[feature] = _numeric(output, feature)
    return output


def _add_outcomes(
    frame: pd.DataFrame,
    *,
    development_q10: float,
) -> pd.DataFrame:
    output = frame.copy()
    terminal = _numeric(output, "terminal_campaign_pnl")
    event = output["competing_event"].astype(str)
    event_time = _numeric(output, "competing_event_time_s")
    within_horizon = event_time <= COMPETING_HORIZON_S
    repair = event.eq("repair") & within_horizon
    trend = event.eq("trend_through") & within_horizon
    time_weight = np.maximum(
        0.0, 1.0 - event_time / COMPETING_HORIZON_S
    )
    output["target_reward"] = _numeric(output, "reward")
    output["target_campaign_cost_avoidance"] = -_numeric(
        output, "campaign_cost"
    )
    output["target_negative_terminal_mtm"] = np.minimum(terminal, 0.0)
    output["target_development_q10_shortfall"] = -np.maximum(
        float(development_q10) - terminal, 0.0
    )
    output["target_repair_first_30m"] = repair.astype(float)
    output["target_trend_through_avoidance_30m"] = -trend.astype(float)
    output["target_competing_risk_utility_30m"] = np.where(
        repair,
        time_weight,
        np.where(trend, -time_weight, 0.0),
    )
    output["target_intervention_fill"] = (
        _numeric(output, "intervention_fill_count") > 0
    ).astype(float)
    return output


def _nuisance_model() -> Pipeline:
    return Pipeline(
        [
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            (
                "model",
                Ridge(
                    alpha=float(MODEL_SPEC["nuisance"]["alpha"]),
                    solver="lsqr",
                ),
            ),
        ]
    )


def _fit_predict_mu(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    target: str,
) -> tuple[np.ndarray, np.ndarray, dict[str, int]]:
    predictions: dict[str, np.ndarray] = {}
    support: dict[str, int] = {}
    for action in SUPPORTED_ACTIONS:
        subset = train[train["action"].eq(action)]
        support[action] = int(len(subset))
        if len(subset) < int(MODEL_SPEC["nuisance"]["min_action_rows"]):
            raise ValueError(
                f"nuisance support for {action} is below the frozen gate"
            )
        model = _nuisance_model()
        model.fit(
            subset.loc[:, FEATURES],
            _numeric(subset, f"target_{target}"),
        )
        values = np.asarray(
            model.predict(test.loc[:, FEATURES]), dtype=float
        )
        if not np.isfinite(values).all():
            raise FloatingPointError(
                f"non-finite nuisance prediction for {target}:{action}"
            )
        predictions[action] = values
    return (
        predictions[CONTROL_ACTION],
        predictions[CANDIDATE_ACTION],
        support,
    )


def _dr_tau(
    frame: pd.DataFrame,
    *,
    target: str,
    mu0: np.ndarray,
    mu1: np.ndarray,
) -> np.ndarray:
    treated = frame["action"].eq(CANDIDATE_ACTION).to_numpy(dtype=float)
    reward = _numeric(frame, f"target_{target}").to_numpy(dtype=float)
    p1 = _numeric(
        frame, f"behavior_prob_{CANDIDATE_ACTION}"
    ).to_numpy(dtype=float)
    p0 = _numeric(
        frame, f"behavior_prob_{CONTROL_ACTION}"
    ).to_numpy(dtype=float)
    return (
        mu1
        - mu0
        + treated / p1 * (reward - mu1)
        - (1.0 - treated) / p0 * (reward - mu0)
    )


def _cross_fitted_pseudo_panel(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    config = OPEConfig(
        min_train_days=int(MODEL_SPEC["nuisance"]["min_train_days"]),
        test_days=int(MODEL_SPEC["nuisance"]["test_days"]),
        embargo_days=int(MODEL_SPEC["nuisance"]["embargo_days"]),
    )
    rows: list[pd.DataFrame] = []
    fold_rows: list[dict[str, Any]] = []
    for fold in make_day_folds(frame["day"].tolist(), config):
        train = frame[frame["day"].isin(fold.train_days)]
        test = frame[frame["day"].isin(fold.test_days)].copy()
        if test.empty:
            continue
        metadata: dict[str, Any] = {
            "fold": int(fold.fold),
            "train_days": list(fold.train_days),
            "test_days": list(fold.test_days),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
        }
        for target in OUTCOMES:
            mu0, mu1, support = _fit_predict_mu(
                train, test, target=target
            )
            test[f"mu0_{target}"] = mu0
            test[f"mu1_{target}"] = mu1
            test[f"dr_tau_{target}"] = _dr_tau(
                test, target=target, mu0=mu0, mu1=mu1
            )
            metadata[f"support_{target}"] = support
        test["nuisance_fold"] = int(fold.fold)
        rows.append(test)
        fold_rows.append(metadata)
    if not rows:
        raise ValueError("no chronological nuisance folds were evaluable")
    return pd.concat(rows, ignore_index=True), pd.DataFrame(fold_rows)


def _fit_honest_tree(
    frame: pd.DataFrame,
) -> tuple[
    DecisionTreeRegressor,
    dict[int, dict[str, Any]],
    float,
]:
    structure_days, estimation_days = _honest_day_split(
        frame["day"].tolist()
    )
    structure = frame[frame["day"].isin(structure_days)]
    estimation = frame[frame["day"].isin(estimation_days)]
    tree = DecisionTreeRegressor(
        max_depth=int(MODEL_SPEC["policy"]["max_depth"]),
        min_samples_leaf=int(
            MODEL_SPEC["policy"]["min_structure_leaf_rows"]
        ),
        random_state=int(MODEL_SPEC["bootstrap"]["seed"]),
    )
    tree.fit(
        structure.loc[:, FEATURES],
        _numeric(structure, "dr_tau_competing_risk_utility_30m"),
    )
    risk = np.clip(
        -_numeric(
            estimation, "mu0_trend_through_avoidance_30m"
        ).to_numpy(dtype=float),
        0.0,
        1.0,
    )
    risk_threshold = float(
        np.quantile(
            risk,
            float(MODEL_SPEC["policy"]["baseline_trend_risk_quantile"]),
        )
    )
    estimation_leaf = tree.apply(estimation.loc[:, FEATURES])
    leaf_values: dict[int, dict[str, Any]] = {}
    for leaf in sorted(set(int(value) for value in estimation_leaf)):
        subset = estimation[estimation_leaf == leaf]
        action_counts = subset["action"].value_counts().to_dict()
        supported = bool(
            len(subset)
            >= int(MODEL_SPEC["policy"]["min_estimation_leaf_rows"])
            and subset["day"].nunique()
            >= int(MODEL_SPEC["policy"]["min_estimation_leaf_days"])
            and all(
                int(action_counts.get(action, 0))
                >= int(MODEL_SPEC["policy"]["min_estimation_action_rows"])
                for action in SUPPORTED_ACTIONS
            )
        )
        effects = {
            target: (
                float(_numeric(subset, f"dr_tau_{target}").mean())
                if supported
                else 0.0
            )
            for target in OUTCOMES
        }
        skip_eligible = bool(
            supported
            and effects["reward"] > 0.0
            and effects["campaign_cost_avoidance"] >= 0.0
            and effects["trend_through_avoidance_30m"] >= 0.0
            and effects["competing_risk_utility_30m"] > 0.0
        )
        leaf_values[leaf] = {
            "effects": effects,
            "supported": supported,
            "skip_eligible": skip_eligible,
            "rows": int(len(subset)),
            "days": int(subset["day"].nunique()),
            "action_rows": {
                action: int(action_counts.get(action, 0))
                for action in SUPPORTED_ACTIONS
            },
        }
    return tree, leaf_values, risk_threshold


def _tree_actions(
    tree: DecisionTreeRegressor,
    leaf_values: dict[int, dict[str, Any]],
    risk_threshold: float,
    frame: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    leaves = tree.apply(frame.loc[:, FEATURES]).astype(int)
    effects = np.asarray(
        [
            float(
                leaf_values.get(int(leaf), {})
                .get("effects", {})
                .get("reward", 0.0)
            )
            for leaf in leaves
        ]
    )
    leaf_eligible = np.asarray(
        [
            bool(
                leaf_values.get(int(leaf), {}).get(
                    "skip_eligible", False
                )
            )
            for leaf in leaves
        ]
    )
    baseline_trend_risk = np.clip(
        -_numeric(
            frame, "mu0_trend_through_avoidance_30m"
        ).to_numpy(dtype=float),
        0.0,
        1.0,
    )
    skip = leaf_eligible & (baseline_trend_risk >= risk_threshold)
    return skip, effects, baseline_trend_risk


def _policy_oof(
    pseudo: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    config = OPEConfig(
        min_train_days=int(MODEL_SPEC["policy"]["min_train_days"]),
        test_days=int(MODEL_SPEC["policy"]["test_days"]),
        embargo_days=int(MODEL_SPEC["policy"]["embargo_days"]),
    )
    rows: list[pd.DataFrame] = []
    fold_rows: list[dict[str, Any]] = []
    for fold in make_day_folds(pseudo["day"].tolist(), config):
        train = pseudo[pseudo["day"].isin(fold.train_days)]
        test = pseudo[pseudo["day"].isin(fold.test_days)].copy()
        if test.empty:
            continue
        tree, leaves, risk_threshold = _fit_honest_tree(train)
        skip, effects, baseline_risk = _tree_actions(
            tree, leaves, risk_threshold, test
        )
        test["policy_skip"] = skip.astype(int)
        test["policy_effect"] = effects
        test["policy_baseline_trend_risk"] = baseline_risk
        test["policy_action"] = np.where(
            skip, CANDIDATE_ACTION, CONTROL_ACTION
        )
        for target in OUTCOMES:
            test[f"policy_uplift_{target}"] = (
                skip.astype(float) * _numeric(test, f"dr_tau_{target}")
            )
        test["policy_fold"] = int(fold.fold)
        rows.append(test)
        fold_rows.append(
            {
                "fold": int(fold.fold),
                "train_days": list(fold.train_days),
                "test_days": list(fold.test_days),
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
                "candidate_rows": int(skip.sum()),
                "candidate_rate": float(skip.mean()),
                "baseline_trend_risk_threshold": risk_threshold,
                "supported_leaves": int(
                    sum(bool(row["supported"]) for row in leaves.values())
                ),
                "eligible_leaves": int(
                    sum(
                        bool(row["skip_eligible"])
                        for row in leaves.values()
                    )
                ),
            }
        )
    if not rows:
        raise ValueError("no chronological policy folds were evaluable")
    return pd.concat(rows, ignore_index=True), pd.DataFrame(fold_rows)


def _summarize_policy(frame: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for idx, target in enumerate(OUTCOMES):
        column = f"policy_uplift_{target}"
        daily = frame.groupby("day", sort=True)[column].mean()
        summary[target] = {
            "uplift": float(_numeric(frame, column).mean()),
            "interval": _bootstrap_by_day(
                frame,
                column,
                trials=int(MODEL_SPEC["bootstrap"]["trials"]),
                seed=int(MODEL_SPEC["bootstrap"]["seed"]) + idx,
            ),
            "daily_positive_days": int((daily > 0.0).sum()),
            "daily_negative_days": int((daily < 0.0).sum()),
            "daily_positive_rate": float((daily > 0.0).mean()),
        }
    selected = frame["policy_action"].astype(str)
    logged = frame["action"].astype(str)
    selected_propensity = np.where(
        selected.eq(CANDIDATE_ACTION),
        _numeric(frame, f"behavior_prob_{CANDIDATE_ACTION}"),
        _numeric(frame, f"behavior_prob_{CONTROL_ACTION}"),
    )
    weights = np.where(
        selected.eq(logged), 1.0 / selected_propensity, 0.0
    )
    denominator = float(np.square(weights).sum())
    ess = (
        float(weights.sum() ** 2 / denominator)
        if denominator > 0.0
        else 0.0
    )
    summary["support"] = {
        "rows": int(len(frame)),
        "days": int(frame["day"].nunique()),
        "candidate_rows": int(selected.eq(CANDIDATE_ACTION).sum()),
        "candidate_rate": float(selected.eq(CANDIDATE_ACTION).mean()),
        "logged_action_rows": {
            action: int(logged.eq(action).sum())
            for action in SUPPORTED_ACTIONS
        },
        "min_behavior_propensity": float(selected_propensity.min()),
        "policy_ess": ess,
    }
    terminal = _numeric(frame, "terminal_campaign_pnl")
    summary["diagnostic_terminal_le_minus_5"] = {
        "all_logged_events": int(
            (terminal <= TAIL_THRESHOLD_USDC).sum()
        ),
        "baseline_logged_events": int(
            (
                (terminal <= TAIL_THRESHOLD_USDC)
                & logged.eq(CONTROL_ACTION)
            ).sum()
        ),
        "skip_logged_events": int(
            (
                (terminal <= TAIL_THRESHOLD_USDC)
                & logged.eq(CANDIDATE_ACTION)
            ).sum()
        ),
    }
    summary["competing_events"] = {
        str(key): int(value)
        for key, value in frame["competing_event"].value_counts().items()
    }
    return summary


def _promotion_gate(summary: dict[str, Any]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for target in ("reward", "competing_risk_utility_30m"):
        if summary[target]["interval"]["p025"] <= 0.0:
            failures.append(f"{target}_lower_bound_not_positive")
    for target in (
        "campaign_cost_avoidance",
        "trend_through_avoidance_30m",
        "negative_terminal_mtm",
        "development_q10_shortfall",
    ):
        if summary[target]["interval"]["p025"] < 0.0:
            failures.append(f"{target}_lower_bound_negative")
    if summary["repair_first_30m"]["uplift"] < 0.0:
        failures.append("repair_first_30m_point_estimate_negative")
    support = summary["support"]
    if not 0.03 <= float(support["candidate_rate"]) <= 0.40:
        failures.append("candidate_rate_outside_frozen_budget")
    if float(support["policy_ess"]) < 100.0:
        failures.append("policy_ess_below_100")
    if int(support["days"]) < 10:
        failures.append("evaluated_days_below_10")
    return not failures, failures


def _fixed_panel_pseudo(
    development: pd.DataFrame,
    panel: pd.DataFrame,
) -> pd.DataFrame:
    output = panel.copy()
    for target in OUTCOMES:
        mu0, mu1, _ = _fit_predict_mu(
            development, output, target=target
        )
        output[f"mu0_{target}"] = mu0
        output[f"mu1_{target}"] = mu1
        output[f"dr_tau_{target}"] = _dr_tau(
            output, target=target, mu0=mu0, mu1=mu1
        )
    return output


def _evaluate_fixed_panel(
    development: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    tree: DecisionTreeRegressor,
    leaves: dict[int, dict[str, Any]],
    risk_threshold: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    pseudo = _fixed_panel_pseudo(development, panel)
    skip, effects, baseline_risk = _tree_actions(
        tree, leaves, risk_threshold, pseudo
    )
    pseudo["policy_skip"] = skip.astype(int)
    pseudo["policy_effect"] = effects
    pseudo["policy_baseline_trend_risk"] = baseline_risk
    pseudo["policy_action"] = np.where(
        skip, CANDIDATE_ACTION, CONTROL_ACTION
    )
    for target in OUTCOMES:
        pseudo[f"policy_uplift_{target}"] = (
            skip.astype(float) * _numeric(pseudo, f"dr_tau_{target}")
        )
    return pseudo, _summarize_policy(pseudo)


def _tree_artifact(
    tree: DecisionTreeRegressor,
    leaves: dict[int, dict[str, Any]],
    *,
    risk_threshold: float,
    status: str,
) -> dict[str, Any]:
    state = tree.tree_
    return {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "status": status,
        "runtime_policy_enabled": False,
        "features": list(FEATURES),
        "control_action": CONTROL_ACTION,
        "candidate_action": CANDIDATE_ACTION,
        "baseline_trend_risk_threshold": risk_threshold,
        "tree": {
            "max_depth": int(tree.get_depth()),
            "node_count": int(state.node_count),
            "children_left": state.children_left.astype(int).tolist(),
            "children_right": state.children_right.astype(int).tolist(),
            "feature_index": state.feature.astype(int).tolist(),
            "threshold": state.threshold.astype(float).tolist(),
        },
        "leaf_effects": {str(key): value for key, value in leaves.items()},
    }


def _render_report(result: dict[str, Any]) -> str:
    development = result["development"]
    lines = [
        "# SELL Repair vs Trend-Through Skip Causal v4 v1",
        "",
        f"- Family: `{FAMILY_ID}`",
        f"- Status: **{result['status']}**",
        f"- Development gate passed: `{result['development_gate_passed']}`",
        f"- Validation accessed: `{result['validation_accessed']}`",
        f"- Sealed holdout accessed: `{result['sealed_holdout_accessed']}`",
        f"- Development failures: `{result['development_failures']}`",
        "",
        "## Development OOF",
        "",
        "| outcome | uplift | p2.5 | p97.5 | positive days |",
        "|---|---:|---:|---:|---:|",
    ]
    for target in OUTCOMES:
        row = development[target]
        lines.append(
            f"| {target} | {row['uplift']:+.6f} | "
            f"{row['interval']['p025']:+.6f} | "
            f"{row['interval']['p975']:+.6f} | "
            f"{row['daily_positive_days']}/{row['interval']['days']} |"
        )
    support = development["support"]
    lines.extend(
        [
            "",
            f"- Skip rate: `{support['candidate_rate']:.4f}`",
            f"- Policy ESS: `{support['policy_ess']:.1f}`",
            f"- Competing events: `{development['competing_events']}`",
            "",
            "> Validation and sealed holdout are opened only after the "
            "preceding gate passes. Failure closes this exact SELL skip family.",
            "",
        ]
    )
    return "\n".join(lines)


def evaluate_family(
    *,
    family_spec_path: Path,
    development_panel_path: Path,
    output_prefix: Path,
    validation_panel_path: Path | None = None,
    sealed_holdout_panel_path: Path | None = None,
    allow_sealed_holdout: bool = False,
) -> dict[str, Any]:
    spec_path = family_spec_path.expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if str(spec.get("family_id")) != FAMILY_ID:
        raise ValueError("family spec does not match this trainer")
    output = output_prefix.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    development_raw = _load_panel(
        development_panel_path,
        expected_panel="development",
        family_spec=spec,
    )
    development_q10 = float(
        _numeric(
            development_raw, "terminal_campaign_pnl"
        ).quantile(0.10)
    )
    development = _add_outcomes(
        development_raw, development_q10=development_q10
    )
    nuisance_oof, nuisance_folds = _cross_fitted_pseudo_panel(development)
    policy_oof, policy_folds = _policy_oof(nuisance_oof)
    development_summary = _summarize_policy(policy_oof)
    development_passed, development_failures = _promotion_gate(
        development_summary
    )
    final_tree, final_leaves, final_risk_threshold = _fit_honest_tree(
        nuisance_oof
    )
    status = (
        "development_passed_validation_locked"
        if development_passed
        else "development_failed_family_closed"
    )
    validation_accessed = False
    holdout_accessed = False
    validation_summary: dict[str, Any] | None = None
    holdout_summary: dict[str, Any] | None = None
    validation_failures: list[str] = []

    if development_passed and validation_panel_path is not None:
        validation_accessed = True
        validation = _add_outcomes(
            _load_panel(
                validation_panel_path,
                expected_panel="validation",
                family_spec=spec,
            ),
            development_q10=development_q10,
        )
        validation_rows, validation_summary = _evaluate_fixed_panel(
            development,
            validation,
            tree=final_tree,
            leaves=final_leaves,
            risk_threshold=final_risk_threshold,
        )
        validation_passed, validation_failures = _promotion_gate(
            validation_summary
        )
        validation_rows.to_csv(
            output.with_suffix(".validation_dr_rows.csv"), index=False
        )
        status = (
            "validation_passed_holdout_locked"
            if validation_passed
            else "validation_failed_family_closed"
        )
        if (
            validation_passed
            and sealed_holdout_panel_path is not None
            and allow_sealed_holdout
        ):
            holdout_accessed = True
            holdout = _add_outcomes(
                _load_panel(
                    sealed_holdout_panel_path,
                    expected_panel="sealed_holdout",
                    family_spec=spec,
                ),
                development_q10=development_q10,
            )
            holdout_rows, holdout_summary = _evaluate_fixed_panel(
                development,
                holdout,
                tree=final_tree,
                leaves=final_leaves,
                risk_threshold=final_risk_threshold,
            )
            holdout_rows.to_csv(
                output.with_suffix(".sealed_holdout_dr_rows.csv"),
                index=False,
            )
            holdout_passed, _ = _promotion_gate(holdout_summary)
            status = (
                "sealed_holdout_passed_shadow_candidate"
                if holdout_passed
                else "sealed_holdout_failed_family_closed"
            )

    nuisance_oof.to_csv(output.with_suffix(".nuisance_oof.csv"), index=False)
    policy_oof.to_csv(output.with_suffix(".policy_oof.csv"), index=False)
    nuisance_folds.to_csv(
        output.with_suffix(".nuisance_folds.csv"), index=False
    )
    policy_folds.to_csv(
        output.with_suffix(".policy_folds.csv"), index=False
    )
    artifact = _tree_artifact(
        final_tree,
        final_leaves,
        risk_threshold=final_risk_threshold,
        status=status,
    )
    artifact_path = output.with_suffix(".artifact.json")
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    result = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "status": status,
        "family_spec": _file_identity(spec_path),
        "development_panel": _file_identity(development_panel_path),
        "development_q10_terminal_campaign_pnl": development_q10,
        "development_gate_passed": development_passed,
        "development_failures": development_failures,
        "development": development_summary,
        "validation_accessed": validation_accessed,
        "validation_failures": validation_failures,
        "validation": validation_summary,
        "sealed_holdout_accessed": holdout_accessed,
        "sealed_holdout": holdout_summary,
        "artifact": _file_identity(artifact_path),
        "runtime_policy_enabled": False,
    }
    output.with_suffix(".summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output.with_suffix(".report.md").write_text(
        _render_report(result), encoding="utf-8"
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--evidence-split-manifest", type=Path, required=True)
    freeze.add_argument("--config", type=Path, required=True)
    freeze.add_argument("--model-dir", type=Path, required=True)
    freeze.add_argument("--p3-artifact", type=Path, required=True)
    freeze.add_argument("--queue-artifact", type=Path, required=True)
    freeze.add_argument("--latency-tape", type=Path, required=True)
    freeze.add_argument("--output", type=Path, required=True)

    evaluate = subparsers.add_parser("evaluate")
    evaluate.add_argument("--family-spec", type=Path, required=True)
    evaluate.add_argument("--development-panel", type=Path, required=True)
    evaluate.add_argument("--validation-panel", type=Path)
    evaluate.add_argument("--sealed-holdout-panel", type=Path)
    evaluate.add_argument("--allow-sealed-holdout", action="store_true")
    evaluate.add_argument("--output-prefix", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "freeze":
        payload = freeze_family_spec(
            evidence_split_manifest=args.evidence_split_manifest,
            config_path=args.config,
            model_dir=args.model_dir,
            p3_artifact=args.p3_artifact,
            queue_artifact=args.queue_artifact,
            latency_tape=args.latency_tape,
            output=args.output,
        )
        print(
            json.dumps(
                {
                    "family_id": payload["family_id"],
                    "status": payload["status"],
                    "output": str(args.output.expanduser().resolve()),
                },
                indent=2,
            )
        )
        return
    result = evaluate_family(
        family_spec_path=args.family_spec,
        development_panel_path=args.development_panel,
        validation_panel_path=args.validation_panel,
        sealed_holdout_panel_path=args.sealed_holdout_panel,
        allow_sealed_holdout=bool(args.allow_sealed_holdout),
        output_prefix=args.output_prefix,
    )
    print(
        json.dumps(
            {
                "family_id": result["family_id"],
                "status": result["status"],
                "development_gate_passed": result[
                    "development_gate_passed"
                ],
                "validation_accessed": result["validation_accessed"],
                "sealed_holdout_accessed": result[
                    "sealed_holdout_accessed"
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
