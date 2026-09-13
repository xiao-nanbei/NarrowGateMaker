#!/usr/bin/env python3
"""Evaluate campaign-level SELL add permission with chronological DR/SPIBB.

The behavior panel randomizes one action per short inventory campaign. The
candidate denies every later exposure-increasing SELL quote until the campaign
returns to flat. Nuisance models and the shallow honest policy tree are fitted
only on dates preceding each evaluation fold.
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

from research.families.f09_campaign_action_uplift.audit.buy_conditional_widen_cate import _bootstrap_by_day
from models.audit.evidence_split import load_evidence_panel, sha256_file
from research.families.f09_campaign_action_uplift.audit.local_action_uplift import validate_action_panel
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import OPEConfig, make_day_folds
from models.replay_policies import CAMPAIGN_STOP_ADD_ACTIONS

FAMILY_ID = "sell_campaign_add_permission_v1"
SCHEMA_VERSION = "sell_campaign_add_permission_cate.v1"
CONTROL_ACTION = "baseline"
CANDIDATE_ACTION = "stop_add_until_flat"
SUPPORTED_ACTIONS = CAMPAIGN_STOP_ADD_ACTIONS
TAIL_THRESHOLD_USDC = -5.0
REPAIR_HORIZON_S = 3_600.0

RAW_FEATURES = (
    "inventory_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_mae_so_far",
    "campaign_add_count_so_far",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
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
    "baseline_estimated_queue_ahead",
)

FEATURES = (
    "inventory_ratio",
    "campaign_pnl_so_far",
    "campaign_mae_abs_so_far",
    "campaign_max_abs_qty_so_far",
    "campaign_log_age_s",
    "campaign_add_count_so_far",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_log1p_near_depth",
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
    "queue_log1p_ahead",
)

OUTCOMES = (
    "reward",
    "campaign_cost_avoidance",
    "negative_terminal_protection",
    "development_q10_shortfall_protection",
    "campaign_mae_avoidance",
    "repair_event",
    "restricted_time_to_repair",
    "day_end_censoring_avoidance",
    "intervention_add_fills",
)

MODEL_SPEC = {
    "nuisance": {
        "type": "action_specific_ridge",
        "alpha": 10.0,
        "min_train_days": 30,
        "test_days": 7,
        "embargo_days": 1,
        "min_action_rows": 50,
    },
    "policy": {
        "type": "depth_2_honest_regression_tree_with_SPIBB_fallback",
        "max_depth": 2,
        "min_structure_leaf_rows": 60,
        "min_estimation_leaf_rows": 40,
        "min_estimation_action_rows": 15,
        "min_estimation_leaf_days": 5,
        "candidate_threshold": 0.0,
    },
    "bootstrap": {"cluster": "UTC_day", "trials": 2_000, "seed": 20260722},
}

SUPPORT_GATES = {
    "min_interventions": 500,
    "min_active_days": 30,
    "min_rows_per_action": 200,
    "min_baseline_intervention_add_fills": 100,
    "min_behavior_propensity": 0.5,
    "overlap_violations_allowed": 0,
}


def _file_identity(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return {
        "path": str(resolved),
        "size_bytes": resolved.stat().st_size,
        "sha256": sha256_file(resolved),
    }


def _numeric(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        raise ValueError(f"panel is missing required field: {name}")
    values = pd.to_numeric(frame[name], errors="coerce")
    if not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError(f"panel field {name} must be finite")
    return values.astype(float)


def _validate_family_spec(spec: dict[str, Any]) -> None:
    if str(spec.get("schema_version")) != "sell_campaign_add_permission.v1":
        raise ValueError("unsupported family spec schema")
    if str(spec.get("family_id")) != FAMILY_ID:
        raise ValueError("family spec does not match this evaluator")
    if tuple(spec.get("actions") or ()) != SUPPORTED_ACTIONS:
        raise ValueError("family action registry changed after freezing")
    if spec.get("behavior_probabilities") != {
        CONTROL_ACTION: 0.5,
        CANDIDATE_ACTION: 0.5,
    }:
        raise ValueError("family requires exact 50/50 behavior propensity")
    if tuple(spec.get("features") or ()) != RAW_FEATURES:
        raise ValueError("family feature registry changed after freezing")
    if (spec.get("invariants") or {}).get("external_reference_used") is not False:
        raise ValueError("the local M0 family cannot use external reference state")
    frozen_model = spec.get("model_spec") or {}
    if frozen_model.get("nuisance") != MODEL_SPEC["nuisance"]:
        raise ValueError("frozen nuisance specification differs from evaluator")
    if frozen_model.get("policy") != MODEL_SPEC["policy"]:
        raise ValueError("frozen policy specification differs from evaluator")
    if frozen_model.get("bootstrap") != MODEL_SPEC["bootstrap"]:
        raise ValueError("frozen bootstrap specification differs from evaluator")
    if (spec.get("support_gates") or {}) != SUPPORT_GATES:
        raise ValueError("frozen support gates differ from evaluator")


def _derive_features(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
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
    for feature in FEATURES:
        output[feature] = _numeric(output, feature)
    return output


def _load_panel(
    path: Path,
    *,
    expected_panel: str,
    family_spec: dict[str, Any],
    access_decision_path: Path | None = None,
    allow_sealed_holdout: bool = False,
) -> pd.DataFrame:
    panel_path = path.expanduser().resolve()
    frame = pd.read_csv(panel_path)
    validate_action_panel(
        frame,
        actions=SUPPORTED_ACTIONS,
        require_price_bound=False,
    )
    frame["day"] = frame["day"].astype(str).str.slice(0, 10)
    frame["side"] = frame["side"].astype(str).str.upper()
    frame["inventory_role"] = frame["inventory_role"].astype(str).str.lower()
    frame["action"] = frame["action"].astype(str).str.lower()
    frame = frame[
        frame["side"].eq("SELL") & frame["inventory_role"].eq("add")
    ].copy()
    if frame.empty:
        raise ValueError("panel has no SELL exposure-increasing add rows")
    if set(frame["action"]) != set(SUPPORTED_ACTIONS):
        raise ValueError("panel lacks exact baseline/campaign-stop overlap")
    if frame["family_id"].astype(str).ne(FAMILY_ID).any():
        raise ValueError("panel contains a different action family")
    if _numeric(frame, "external_reference_used").ne(0.0).any():
        raise ValueError("external reference is forbidden in local M0")
    for action in SUPPORTED_ACTIONS:
        if not np.allclose(
            _numeric(frame, f"behavior_prob_{action}"),
            0.5,
            atol=1e-12,
            rtol=0.0,
        ):
            raise ValueError(f"{action} propensity is not exactly 0.5")

    metadata_path = Path(
        str(panel_path).replace(".action_panel.csv", ".metadata.json")
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if str(metadata.get("panel_role")) != expected_panel:
        raise ValueError("panel metadata role differs from requested evidence panel")
    if str(metadata.get("action_family")) != "campaign_stop_add":
        raise ValueError("panel was not generated by the campaign-stop action family")
    if tuple(metadata.get("registered_actions") or ()) != SUPPORTED_ACTIONS:
        raise ValueError("panel metadata action registry changed")
    if metadata.get("strategy_evidence") is not False:
        raise ValueError("randomized behavior panel must remain non-strategy evidence")

    baseline = family_spec["baseline"]
    identity_pairs = (
        ("config_sha256", "config_sha256"),
        ("queue_sha256", "queue_calibration_sha256"),
        ("latency_sha256", "latency_source_sha256"),
    )
    for spec_key, metadata_key in identity_pairs:
        if str(baseline.get(spec_key)) != str(metadata.get(metadata_key)):
            raise ValueError(f"panel baseline identity mismatch: {spec_key}")

    split_path = Path(family_spec["evidence_split"]["path"])
    if sha256_file(split_path) != str(family_spec["evidence_split"]["sha256"]):
        raise ValueError("frozen evidence split hash changed")
    expected_days, _ = load_evidence_panel(
        split_path,
        expected_panel,
        allow_sealed_holdout=allow_sealed_holdout,
        access_decision_path=access_decision_path,
    )
    if sorted(frame["day"].unique()) != sorted(expected_days):
        raise ValueError(f"{expected_panel} days differ from the frozen split")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be unique")
    if frame.groupby(["day", "campaign_id"], sort=False).size().max() != 1:
        raise ValueError("each campaign may have only one randomized intervention")
    return _derive_features(frame)


def _add_outcomes(frame: pd.DataFrame, *, development_q10: float) -> pd.DataFrame:
    output = frame.copy()
    terminal = _numeric(output, "terminal_campaign_pnl")
    output["target_reward"] = _numeric(output, "reward")
    output["target_campaign_cost_avoidance"] = -_numeric(output, "campaign_cost")
    output["target_negative_terminal_protection"] = np.minimum(terminal, 0.0)
    output["target_development_q10_shortfall_protection"] = -np.maximum(
        float(development_q10) - terminal, 0.0
    )
    output["target_campaign_mae_avoidance"] = _numeric(output, "campaign_mae")
    output["target_repair_event"] = _numeric(output, "campaign_closed")
    output["target_restricted_time_to_repair"] = -np.minimum(
        _numeric(output, "decision_to_terminal_s"), REPAIR_HORIZON_S
    )
    output["target_day_end_censoring_avoidance"] = -_numeric(
        output, "campaign_censored"
    )
    output["target_intervention_add_fills"] = _numeric(
        output, "intervention_fill_count"
    )
    return output


def _nuisance_model() -> Pipeline:
    return Pipeline(
        [
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            (
                "ridge",
                Ridge(
                    alpha=float(MODEL_SPEC["nuisance"]["alpha"]),
                    solver="lsqr",
                ),
            ),
        ]
    )


def _fit_predict_mu(
    train: pd.DataFrame,
    prediction_frames: Sequence[pd.DataFrame],
    *,
    target: str,
) -> tuple[list[tuple[np.ndarray, np.ndarray]], dict[str, int]]:
    predictions: dict[str, list[np.ndarray]] = {}
    support: dict[str, int] = {}
    for action in SUPPORTED_ACTIONS:
        subset = train[train["action"].eq(action)]
        support[action] = int(len(subset))
        if len(subset) < int(MODEL_SPEC["nuisance"]["min_action_rows"]):
            raise ValueError(
                f"nuisance support for {action} is below the frozen minimum"
            )
        model = _nuisance_model()
        model.fit(subset.loc[:, FEATURES], _numeric(subset, f"target_{target}"))
        predictions[action] = [
            np.asarray(model.predict(frame.loc[:, FEATURES]), dtype=float)
            for frame in prediction_frames
        ]
        if any(not np.isfinite(values).all() for values in predictions[action]):
            raise FloatingPointError(f"non-finite nuisance prediction: {target}:{action}")
    paired = list(
        zip(
            predictions[CONTROL_ACTION],
            predictions[CANDIDATE_ACTION],
            strict=True,
        )
    )
    return paired, support


def _dr_components(
    frame: pd.DataFrame,
    *,
    target: str,
    mu0: np.ndarray,
    mu1: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    candidate = frame["action"].eq(CANDIDATE_ACTION).to_numpy(dtype=float)
    outcome = _numeric(frame, f"target_{target}").to_numpy(dtype=float)
    p0 = _numeric(frame, f"behavior_prob_{CONTROL_ACTION}").to_numpy(dtype=float)
    p1 = _numeric(frame, f"behavior_prob_{CANDIDATE_ACTION}").to_numpy(dtype=float)
    if (p0 <= 0.0).any() or (p1 <= 0.0).any():
        raise ValueError("DR requires positive overlap for both actions")
    dr0 = mu0 + (1.0 - candidate) / p0 * (outcome - mu0)
    dr1 = mu1 + candidate / p1 * (outcome - mu1)
    return dr0, dr1, dr1 - dr0


def _add_pseudo_outcomes(
    train: pd.DataFrame,
    prediction_frames: Sequence[pd.DataFrame],
) -> tuple[list[pd.DataFrame], dict[str, dict[str, int]]]:
    outputs = [frame.copy() for frame in prediction_frames]
    support: dict[str, dict[str, int]] = {}
    for target in OUTCOMES:
        predictions, target_support = _fit_predict_mu(
            train, prediction_frames, target=target
        )
        support[target] = target_support
        for output, (mu0, mu1) in zip(outputs, predictions, strict=True):
            dr0, dr1, tau = _dr_components(
                output, target=target, mu0=mu0, mu1=mu1
            )
            output[f"mu0_{target}"] = mu0
            output[f"mu1_{target}"] = mu1
            output[f"dr0_{target}"] = dr0
            output[f"dr1_{target}"] = dr1
            output[f"dr_tau_{target}"] = tau
    return outputs, support


def _honest_day_split(days: Sequence[str]) -> tuple[list[str], list[str]]:
    ordered = sorted(set(str(day) for day in days))
    structure = ordered[::2]
    estimation = ordered[1::2]
    if not structure or not estimation:
        raise ValueError("honest tree requires disjoint structure/estimation days")
    return structure, estimation


def _fit_honest_tree(
    pseudo: pd.DataFrame,
) -> tuple[DecisionTreeRegressor, dict[int, dict[str, Any]]]:
    structure_days, estimation_days = _honest_day_split(pseudo["day"].tolist())
    structure = pseudo[pseudo["day"].isin(structure_days)]
    estimation = pseudo[pseudo["day"].isin(estimation_days)]
    tree = DecisionTreeRegressor(
        max_depth=int(MODEL_SPEC["policy"]["max_depth"]),
        min_samples_leaf=int(MODEL_SPEC["policy"]["min_structure_leaf_rows"]),
        random_state=int(MODEL_SPEC["bootstrap"]["seed"]),
    )
    tree.fit(structure.loc[:, FEATURES], _numeric(structure, "dr_tau_reward"))
    leaves = tree.apply(estimation.loc[:, FEATURES]).astype(int)
    leaf_values: dict[int, dict[str, Any]] = {}
    for leaf in sorted(set(int(value) for value in leaves)):
        subset = estimation[leaves == leaf]
        counts = subset["action"].value_counts().to_dict()
        supported = bool(
            len(subset) >= int(MODEL_SPEC["policy"]["min_estimation_leaf_rows"])
            and subset["day"].nunique()
            >= int(MODEL_SPEC["policy"]["min_estimation_leaf_days"])
            and all(
                int(counts.get(action, 0))
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
        leaf_values[leaf] = {
            "supported": supported,
            "candidate_eligible": bool(
                supported
                and effects["reward"]
                > float(MODEL_SPEC["policy"]["candidate_threshold"])
            ),
            "effects": effects,
            "rows": int(len(subset)),
            "days": int(subset["day"].nunique()),
            "action_rows": {
                action: int(counts.get(action, 0)) for action in SUPPORTED_ACTIONS
            },
        }
    return tree, leaf_values


def _apply_policy(
    tree: DecisionTreeRegressor,
    leaf_values: dict[int, dict[str, Any]],
    frame: pd.DataFrame,
) -> pd.DataFrame:
    output = frame.copy()
    leaves = tree.apply(output.loc[:, FEATURES]).astype(int)
    candidate = np.asarray(
        [
            bool(leaf_values.get(int(leaf), {}).get("candidate_eligible", False))
            for leaf in leaves
        ]
    )
    effects = np.asarray(
        [
            float(
                (leaf_values.get(int(leaf), {}).get("effects") or {}).get(
                    "reward", 0.0
                )
            )
            for leaf in leaves
        ]
    )
    output["policy_leaf"] = leaves
    output["policy_candidate"] = candidate.astype(int)
    output["policy_effect"] = effects
    output["policy_action"] = np.where(
        candidate, CANDIDATE_ACTION, CONTROL_ACTION
    )
    for target in OUTCOMES:
        output[f"policy_uplift_{target}"] = (
            candidate.astype(float) * _numeric(output, f"dr_tau_{target}")
        )
        output[f"policy_value_{target}"] = np.where(
            candidate,
            _numeric(output, f"dr1_{target}"),
            _numeric(output, f"dr0_{target}"),
        )
    return output


def _chronological_policy_oof(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cfg = OPEConfig(
        min_train_days=int(MODEL_SPEC["nuisance"]["min_train_days"]),
        test_days=int(MODEL_SPEC["nuisance"]["test_days"]),
        embargo_days=int(MODEL_SPEC["nuisance"]["embargo_days"]),
    )
    rows: list[pd.DataFrame] = []
    fold_rows: list[dict[str, Any]] = []
    for fold in make_day_folds(frame["day"].tolist(), cfg):
        train = frame[frame["day"].isin(fold.train_days)].copy()
        test = frame[frame["day"].isin(fold.test_days)].copy()
        if test.empty:
            continue
        (train_pseudo, test_pseudo), support = _add_pseudo_outcomes(
            train, (train, test)
        )
        tree, leaf_values = _fit_honest_tree(train_pseudo)
        evaluated = _apply_policy(tree, leaf_values, test_pseudo)
        evaluated["policy_fold"] = int(fold.fold)
        rows.append(evaluated)
        fold_rows.append(
            {
                "fold": int(fold.fold),
                "train_days": list(fold.train_days),
                "test_days": list(fold.test_days),
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
                "candidate_rows": int(evaluated["policy_candidate"].sum()),
                "candidate_rate": float(evaluated["policy_candidate"].mean()),
                "supported_leaves": int(
                    sum(bool(row["supported"]) for row in leaf_values.values())
                ),
                "candidate_leaves": int(
                    sum(
                        bool(row["candidate_eligible"])
                        for row in leaf_values.values()
                    )
                ),
                "nuisance_support": support,
            }
        )
    if not rows:
        raise ValueError("no chronological Development folds were evaluable")
    return pd.concat(rows, ignore_index=True), pd.DataFrame(fold_rows)


def _raw_support(frame: pd.DataFrame) -> dict[str, Any]:
    logged = frame["action"].astype(str)
    counts = {action: int(logged.eq(action).sum()) for action in SUPPORTED_ACTIONS}
    baseline_fills = float(
        _numeric(frame.loc[logged.eq(CONTROL_ACTION)], "intervention_fill_count").sum()
    )
    minimum_propensity = float(
        min(
            _numeric(frame, f"behavior_prob_{action}").min()
            for action in SUPPORTED_ACTIONS
        )
    )
    failures: list[str] = []
    if len(frame) < int(SUPPORT_GATES["min_interventions"]):
        failures.append("interventions_below_500")
    if frame["day"].nunique() < int(SUPPORT_GATES["min_active_days"]):
        failures.append("active_days_below_30")
    for action, count in counts.items():
        if count < int(SUPPORT_GATES["min_rows_per_action"]):
            failures.append(f"{action}_rows_below_200")
    if baseline_fills < int(SUPPORT_GATES["min_baseline_intervention_add_fills"]):
        failures.append("baseline_intervention_add_fills_below_100")
    if minimum_propensity < float(SUPPORT_GATES["min_behavior_propensity"]):
        failures.append("behavior_propensity_below_0_5")
    return {
        "passed": not failures,
        "failures": failures,
        "rows": int(len(frame)),
        "days": int(frame["day"].nunique()),
        "action_rows": counts,
        "baseline_intervention_add_fills": baseline_fills,
        "min_behavior_propensity": minimum_propensity,
        "overlap_violations": 0,
    }


def _summarize_policy(
    frame: pd.DataFrame,
    *,
    raw_support: dict[str, Any],
) -> dict[str, Any]:
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
            "daily_zero_days": int((daily == 0.0).sum()),
            "daily_positive_rate": float((daily > 0.0).mean()),
        }

    selected = frame["policy_action"].astype(str)
    logged = frame["action"].astype(str)
    selected_propensity = np.where(
        selected.eq(CANDIDATE_ACTION),
        _numeric(frame, f"behavior_prob_{CANDIDATE_ACTION}"),
        _numeric(frame, f"behavior_prob_{CONTROL_ACTION}"),
    )
    weights = np.where(selected.eq(logged), 1.0 / selected_propensity, 0.0)
    squared = float(np.square(weights).sum())
    ess = float(weights.sum() ** 2 / squared) if squared > 0.0 else 0.0
    baseline_fills = float(_numeric(frame, "dr0_intervention_add_fills").sum())
    policy_fills = float(_numeric(frame, "policy_value_intervention_add_fills").sum())
    fills_retention = (
        policy_fills / baseline_fills if baseline_fills > 0.0 else float("nan")
    )
    summary["support"] = {
        **raw_support,
        "oof_rows": int(len(frame)),
        "oof_days": int(frame["day"].nunique()),
        "candidate_rows": int(selected.eq(CANDIDATE_ACTION).sum()),
        "candidate_rate": float(selected.eq(CANDIDATE_ACTION).mean()),
        "policy_ess": ess,
        "unsupported_candidate_rows": 0,
    }
    summary["activity"] = {
        "baseline_expected_intervention_add_fills": baseline_fills,
        "policy_expected_intervention_add_fills": policy_fills,
        "fills_retention": fills_retention,
    }
    terminal = _numeric(frame, "terminal_campaign_pnl")
    summary["diagnostics"] = {
        "terminal_le_minus_5": {
            "all_logged": int((terminal <= TAIL_THRESHOLD_USDC).sum()),
            CONTROL_ACTION: int(
                ((terminal <= TAIL_THRESHOLD_USDC) & logged.eq(CONTROL_ACTION)).sum()
            ),
            CANDIDATE_ACTION: int(
                ((terminal <= TAIL_THRESHOLD_USDC) & logged.eq(CANDIDATE_ACTION)).sum()
            ),
        },
        "logged_intervention_add_fills": {
            action: float(
                _numeric(
                    frame.loc[logged.eq(action)], "intervention_fill_count"
                ).sum()
            )
            for action in SUPPORTED_ACTIONS
        },
        "logged_mean_campaign_duration_s": {
            action: float(
                _numeric(frame.loc[logged.eq(action)], "campaign_duration_s").mean()
            )
            for action in SUPPORTED_ACTIONS
        },
        "logged_mean_campaign_mae": {
            action: float(_numeric(frame.loc[logged.eq(action)], "campaign_mae").mean())
            for action in SUPPORTED_ACTIONS
        },
    }
    return summary


def _promotion_gate(
    summary: dict[str, Any],
    *,
    minimum_days: int = 10,
) -> tuple[bool, list[str]]:
    failures = list(summary["support"].get("failures") or ())
    if summary["reward"]["interval"]["p025"] <= 0.0:
        failures.append("reward_lower_bound_not_positive")
    for target in (
        "campaign_cost_avoidance",
        "negative_terminal_protection",
        "development_q10_shortfall_protection",
    ):
        if summary[target]["interval"]["p025"] < 0.0:
            failures.append(f"{target}_lower_bound_negative")
    for target in (
        "campaign_mae_avoidance",
        "repair_event",
        "restricted_time_to_repair",
        "day_end_censoring_avoidance",
    ):
        if summary[target]["uplift"] < 0.0:
            failures.append(f"{target}_point_estimate_negative")
    support = summary["support"]
    if not 0.05 <= float(support["candidate_rate"]) <= 0.50:
        failures.append("candidate_rate_outside_frozen_budget")
    if float(support["policy_ess"]) < 100.0:
        failures.append("policy_ess_below_100")
    if int(support["oof_days"]) < int(minimum_days):
        failures.append(f"evaluated_days_below_{minimum_days}")
    if summary["reward"]["daily_positive_rate"] < 0.55:
        failures.append("reward_daily_positive_rate_below_0_55")
    retention = float(summary["activity"]["fills_retention"])
    if not np.isfinite(retention) or retention < 0.85:
        failures.append("fills_retention_below_0_85")
    return not failures, failures


def _fit_final_policy(
    development: pd.DataFrame,
) -> tuple[DecisionTreeRegressor, dict[int, dict[str, Any]]]:
    (pseudo,), _ = _add_pseudo_outcomes(development, (development,))
    return _fit_honest_tree(pseudo)


def _evaluate_fixed_panel(
    development: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    tree: DecisionTreeRegressor,
    leaf_values: dict[int, dict[str, Any]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    (pseudo,), _ = _add_pseudo_outcomes(development, (panel,))
    evaluated = _apply_policy(tree, leaf_values, pseudo)
    return evaluated, _summarize_policy(
        evaluated, raw_support=_raw_support(panel)
    )


def _tree_artifact(
    tree: DecisionTreeRegressor,
    leaves: dict[int, dict[str, Any]],
    *,
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
        "# SELL Campaign Add Permission v1",
        "",
        f"- Family: `{FAMILY_ID}`",
        f"- Status: **{result['status']}**",
        f"- Development passed: `{result['development_gate_passed']}`",
        f"- Validation accessed: `{result['validation_accessed']}`",
        f"- Sealed holdout accessed: `{result['sealed_holdout_accessed']}`",
        f"- Development failures: `{result['development_failures']}`",
        "",
        "## Development Chronological OOF",
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
    activity = development["activity"]
    lines.extend(
        [
            "",
            f"- Candidate rate: `{support['candidate_rate']:.4f}`",
            f"- Policy ESS: `{support['policy_ess']:.1f}`",
            f"- SELL add-fill retention: `{activity['fills_retention']:.4f}`",
            "",
            "> Validation remains locked unless every frozen Development gate passes. "
            "A failure closes this exact campaign-level SELL add-permission family.",
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
    validation_access_decision: Path | None = None,
    sealed_holdout_panel_path: Path | None = None,
    holdout_access_decision: Path | None = None,
    allow_sealed_holdout: bool = False,
) -> dict[str, Any]:
    spec_path = family_spec_path.expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    _validate_family_spec(spec)
    output = output_prefix.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    development_raw = _load_panel(
        development_panel_path,
        expected_panel="development",
        family_spec=spec,
    )
    development_q10 = float(
        _numeric(development_raw, "terminal_campaign_pnl").quantile(0.10)
    )
    development = _add_outcomes(
        development_raw, development_q10=development_q10
    )
    raw_support = _raw_support(development)
    policy_oof, folds = _chronological_policy_oof(development)
    development_summary = _summarize_policy(
        policy_oof, raw_support=raw_support
    )
    development_passed, development_failures = _promotion_gate(
        development_summary
    )
    final_tree, final_leaves = _fit_final_policy(development)
    status = (
        "development_passed_validation_locked"
        if development_passed
        else "development_failed_family_closed"
    )
    validation_accessed = False
    validation_summary: dict[str, Any] | None = None
    validation_failures: list[str] = []
    holdout_accessed = False
    holdout_summary: dict[str, Any] | None = None

    if development_passed and validation_panel_path is not None:
        if validation_access_decision is None:
            raise PermissionError("Validation requires a hash-bound access decision")
        validation_accessed = True
        validation = _add_outcomes(
            _load_panel(
                validation_panel_path,
                expected_panel="validation",
                family_spec=spec,
                access_decision_path=validation_access_decision,
            ),
            development_q10=development_q10,
        )
        validation_rows, validation_summary = _evaluate_fixed_panel(
            development,
            validation,
            tree=final_tree,
            leaf_values=final_leaves,
        )
        validation_rows.to_csv(
            output.with_suffix(".validation_dr_rows.csv"), index=False
        )
        validation_passed, validation_failures = _promotion_gate(
            validation_summary, minimum_days=9
        )
        status = (
            "validation_passed_holdout_locked"
            if validation_passed
            else "validation_failed_family_closed"
        )
        if validation_passed and sealed_holdout_panel_path is not None:
            if not allow_sealed_holdout or holdout_access_decision is None:
                raise PermissionError("sealed holdout requires explicit unseal decision")
            holdout_accessed = True
            holdout = _add_outcomes(
                _load_panel(
                    sealed_holdout_panel_path,
                    expected_panel="sealed_holdout",
                    family_spec=spec,
                    access_decision_path=holdout_access_decision,
                    allow_sealed_holdout=True,
                ),
                development_q10=development_q10,
            )
            holdout_rows, holdout_summary = _evaluate_fixed_panel(
                development,
                holdout,
                tree=final_tree,
                leaf_values=final_leaves,
            )
            holdout_rows.to_csv(
                output.with_suffix(".sealed_holdout_dr_rows.csv"), index=False
            )
            holdout_passed, _ = _promotion_gate(
                holdout_summary, minimum_days=9
            )
            status = (
                "sealed_holdout_passed_shadow_candidate"
                if holdout_passed
                else "sealed_holdout_failed_family_closed"
            )

    policy_oof.to_csv(output.with_suffix(".development_oof.csv"), index=False)
    folds.to_csv(output.with_suffix(".development_folds.csv"), index=False)
    artifact_path = output.with_suffix(".artifact.json")
    artifact_path.write_text(
        json.dumps(
            _tree_artifact(final_tree, final_leaves, status=status),
            indent=2,
            sort_keys=True,
        )
        + "\n",
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
    summary_path = output.with_suffix(".summary.json")
    summary_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    output.with_suffix(".report.md").write_text(
        _render_report(result), encoding="utf-8"
    )
    access_gate = {
        "schema_version": "sell_campaign_add_permission_access_gate.v1",
        "family_id": FAMILY_ID,
        "panel_role": "development",
        "numerical_ope_gate_passed": development_passed,
        "day_cluster_bootstrap": {
            "uplift_p025": development_summary["reward"]["interval"]["p025"],
            "uplift_p50": development_summary["reward"]["interval"]["p50"],
            "uplift_p975": development_summary["reward"]["interval"]["p975"],
        },
        "summary_path": str(summary_path),
        "summary_sha256": sha256_file(summary_path),
        "failures": development_failures,
    }
    output.with_suffix(".access_gate.json").write_text(
        json.dumps(access_gate, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family-spec", type=Path, required=True)
    parser.add_argument("--development-panel", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--validation-panel", type=Path)
    parser.add_argument("--validation-access-decision", type=Path)
    parser.add_argument("--sealed-holdout-panel", type=Path)
    parser.add_argument("--holdout-access-decision", type=Path)
    parser.add_argument("--allow-sealed-holdout", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    result = evaluate_family(
        family_spec_path=args.family_spec,
        development_panel_path=args.development_panel,
        output_prefix=args.output_prefix,
        validation_panel_path=args.validation_panel,
        validation_access_decision=args.validation_access_decision,
        sealed_holdout_panel_path=args.sealed_holdout_panel,
        holdout_access_decision=args.holdout_access_decision,
        allow_sealed_holdout=bool(args.allow_sealed_holdout),
    )
    print(
        json.dumps(
            {
                "family_id": result["family_id"],
                "status": result["status"],
                "development_gate_passed": result["development_gate_passed"],
                "validation_accessed": result["validation_accessed"],
                "sealed_holdout_accessed": result["sealed_holdout_accessed"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
