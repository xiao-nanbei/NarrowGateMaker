#!/usr/bin/env python3
"""Development-only ordered placement fill surface and transport audit.

The ordered model is conditional on all three paired placement actions having
activated.  Its LightGBM Poisson hazard is monotone in same-side BBO distance,
contains no free action dummy, and uses a shared positive role calibrator.  GTX
activation and pending-cancel fill remain separate nuisance processes.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from scipy.stats import beta as beta_distribution

from data_paths import data_root
from models.audit.content_addressed_cache import file_sha256
from models.audit.experiment_manifest import git_workspace_identity, write_code_checkpoint
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.evaluate_empirical_pending_nuisance import (
    ACTIONS,
    _action_curve_gate,
    _daily_action_metrics,
    _pending_predictive_interval,
)
from research.families.f06_placement_fill_cif.audit.evaluate_request_state_race import (
    _bootstrap_mean,
    _constant_rates,
    _known_targets,
    _phase_frame,
    _predict_horizon,
    _role_scales,
)
from research.families.f06_placement_fill_cif.audit.evaluate_request_state_race import (
    _load_days as _legacy_load_days,
)
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    ROOT,
    STATIC_MODEL_FEATURES,
)
from research.families.f06_placement_fill_cif.audit.request_state_race import (
    CauseSpecificRateModel,
    _encoded_base,
    empirical_bin_edges,
)
from research.families.f06_placement_fill_cif.audit.risk_set_expansion import (
    EVENT_CENSOR,
    EVENT_FILL,
    expand_competing_risk_intervals_native,
)
from research.governance.paths import verify_path_identity

DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = FAMILY_DOCS / "ordered_common_support_fill_surface_v1_spec_20260728.json"
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "ordered_common_support_fill_surface_v1_development_20260728"
)
SCHEMA_VERSION = "ordered_common_support_fill_surface.development.v1"
ROLES = ("opener", "add", "reducing")
SIDES = ("BUY", "SELL")


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _identity(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    stat = resolved.stat()
    return {
        "path": str(resolved),
        "size_bytes": int(stat.st_size),
        "sha256": file_sha256(resolved),
    }


def _require_identity(path: Path, expected: str, label: str) -> None:
    try:
        verify_path_identity(path, expected)
    except (FileNotFoundError, RuntimeError) as exc:
        raise RuntimeError(f"{label} identity check failed: {exc}") from exc


def _load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != "ordered_common_support_fill_surface_spec.v1":
        raise RuntimeError("unsupported ordered common-support specification")
    for label, identity in spec["source_identity"].items():
        _require_identity(
            Path(str(identity["path"])).expanduser().resolve(),
            str(identity["sha256"]),
            label,
        )
    implementation = spec["implementation"]
    _require_identity(
        ROOT / str(implementation["path"]),
        str(implementation["sha256"]),
        "ordered implementation",
    )
    return spec


def _load_days(index: pd.DataFrame, days: Sequence[str]) -> pd.DataFrame:
    frame = _legacy_load_days(index, days)
    selected = index.loc[index["day"].astype(str).isin(set(days))]
    additions: list[pd.DataFrame] = []
    for row in selected.sort_values("day").itertuples(index=False):
        path = Path(str(row.payload_path)).expanduser().resolve()
        additions.append(
            pd.read_parquet(
                path,
                columns=[
                    "action_lifecycle_id",
                    "activation_status",
                    "request_mid",
                    "pending_cancel_fill_qty",
                ],
            )
        )
    status = pd.concat(additions, ignore_index=True)
    return frame.merge(
        status,
        on="action_lifecycle_id",
        how="left",
        validate="one_to_one",
    )


def common_support_mask(frame: pd.DataFrame) -> np.ndarray:
    """Return rows belonging to a complete, all-active paired cohort."""

    work = frame.loc[:, ["cohort_id", "action", "activation_status"]].copy()
    work["_active"] = work["activation_status"].astype(str).eq("active")
    summary = work.groupby("cohort_id", observed=True).agg(
        rows=("action", "size"),
        actions=("action", "nunique"),
        active=("_active", "sum"),
    )
    supported = summary.index[
        (summary["rows"] == len(ACTIONS))
        & (summary["actions"] == len(ACTIONS))
        & (summary["active"] == len(ACTIONS))
    ]
    return frame["cohort_id"].isin(supported).to_numpy()


def assert_paired_feature_contract(frame: pd.DataFrame) -> None:
    """Fail when an action-varying feature can bypass the distance constraint."""

    ordered = frame.assign(
        _action_rank=frame["action"].map(
            {"closer_1tick": 0, "current": 1, "farther_1tick": 2}
        )
    ).sort_values(["cohort_id", "_action_rank"])
    if ordered["_action_rank"].isna().any() or len(ordered) % len(ACTIONS):
        raise RuntimeError("paired action rows are incomplete")
    cohorts = ordered["cohort_id"].to_numpy().reshape(-1, len(ACTIONS))
    if not bool(np.all(cohorts == cohorts[:, :1])):
        raise RuntimeError("paired action rows are not contiguous triples")
    distance = pd.to_numeric(ordered["distance_ticks"], errors="coerce").to_numpy()
    distance = distance.reshape(-1, len(ACTIONS))
    if not bool(
        np.all(np.isfinite(distance))
        and np.all(distance[:, 0] <= distance[:, 1])
        and np.all(distance[:, 1] <= distance[:, 2])
    ):
        raise RuntimeError("paired action distance is not closer <= current <= farther")
    for name in STATIC_MODEL_FEATURES:
        if name == "distance_ticks":
            continue
        values = pd.to_numeric(ordered[name], errors="coerce").fillna(0.0).to_numpy()
        values = values.reshape(-1, len(ACTIONS))
        if not bool(np.allclose(values, values[:, :1], rtol=0.0, atol=0.0)):
            raise RuntimeError(f"action-varying feature bypasses monotonicity: {name}")


def fit_ordered_pre_request_model(
    frame: pd.DataFrame,
    *,
    maximum_bins: int,
    random_seed: int,
    model_config: Mapping[str, Any],
) -> CauseSpecificRateModel:
    """Fit one side-specific distance-monotone pre-request fill hazard."""

    pre = _phase_frame(frame.loc[common_support_mask(frame)].copy(), "pre_request")
    if pre.empty:
        raise RuntimeError("ordered model has no common-support pre-request rows")
    assert_paired_feature_contract(pre)
    duration = pd.to_numeric(pre["pre_request_exposure_ms"], errors="coerce").to_numpy(float)
    event_kind = np.where(
        pre["pre_request_first_fill"].to_numpy(np.int8) != 0,
        EVENT_FILL,
        EVENT_CENSOR,
    ).astype(np.uint8)
    edges = empirical_bin_edges(duration, maximum_bins=maximum_bins)
    expanded = expand_competing_risk_intervals_native(duration, event_kind, edges)
    base, base_columns = _encoded_base(
        pre,
        numeric_features=STATIC_MODEL_FEATURES,
        categorical_features=("inventory_role",),
    )
    row_index = expanded["row_index"].to_numpy(np.int64)
    features = base.iloc[row_index].reset_index(drop=True)
    width_ms = (
        expanded["interval_end_ms"].to_numpy(float)
        - expanded["interval_start_ms"].to_numpy(float)
    )
    features["risk_elapsed_log1p"] = np.log1p(
        expanded["interval_start_ms"].to_numpy(float)
    )
    features["risk_interval_width_log1p"] = np.log1p(width_ms)
    encoded_columns = tuple(features.columns)
    constraints = [-1 if name == "distance_ticks" else 0 for name in encoded_columns]
    if constraints.count(-1) != 1:
        raise RuntimeError("ordered model must constrain exactly one distance feature")
    exposure_seconds = np.maximum(
        1e-6,
        width_ms
        * expanded["exposure_fraction"].to_numpy(float)
        / 1_000.0,
    )
    target = expanded["fill_target"].to_numpy(float)
    if int(target.sum()) < int(model_config["minimum_fill_events"]):
        raise RuntimeError("ordered model has insufficient fill events")
    model = LGBMRegressor(
        objective="poisson",
        n_estimators=int(model_config["n_estimators"]),
        learning_rate=float(model_config["learning_rate"]),
        num_leaves=int(model_config["num_leaves"]),
        max_depth=int(model_config["max_depth"]),
        min_child_samples=int(model_config["min_child_samples"]),
        reg_lambda=float(model_config["reg_lambda"]),
        monotone_constraints=constraints,
        monotone_constraints_method="advanced",
        random_state=int(random_seed),
        deterministic=True,
        force_col_wise=True,
        verbosity=-1,
        n_jobs=1,
    )
    # Three paired actions form one dependent cohort contribution.
    model.fit(
        features,
        target / exposure_seconds,
        sample_weight=exposure_seconds / float(len(ACTIONS)),
    )
    return CauseSpecificRateModel(
        bin_edges_ms=edges,
        numeric_features=STATIC_MODEL_FEATURES,
        categorical_features=("inventory_role",),
        encoded_columns=encoded_columns,
        fill_model=model,
        ack_model=None,
    )


@dataclass(frozen=True)
class BetaRate:
    alpha: float
    beta: float
    events: int
    trials: int

    @property
    def mean(self) -> float:
        return float(self.alpha / (self.alpha + self.beta))


def _beta_rates(
    frame: pd.DataFrame,
    target: np.ndarray,
    group_columns: Sequence[str],
) -> dict[tuple[str, ...], BetaRate]:
    work = frame.loc[:, list(group_columns)].copy()
    work["_target"] = np.asarray(target, dtype=np.int8)
    output: dict[tuple[str, ...], BetaRate] = {}
    for keys, group in work.groupby(list(group_columns), observed=True):
        normalized = keys if isinstance(keys, tuple) else (keys,)
        events = int(group["_target"].sum())
        trials = int(len(group))
        output[tuple(str(value) for value in normalized)] = BetaRate(
            alpha=events + 0.5,
            beta=trials - events + 0.5,
            events=events,
            trials=trials,
        )
    return output


def _cohort_activation(frame: pd.DataFrame) -> pd.DataFrame:
    active = frame["activation_status"].astype(str).eq("active")
    work = frame.loc[:, ["cohort_id", "day", "side", "inventory_role", "action"]].copy()
    work["activation_target"] = active.to_numpy(np.int8)
    work["all_three_target"] = (
        work.groupby("cohort_id", observed=True)["activation_target"].transform("sum")
        == len(ACTIONS)
    ).astype(np.int8)
    return work


def _activation_predictions(train: pd.DataFrame, test: pd.DataFrame, fold: int) -> pd.DataFrame:
    train_rows = _cohort_activation(train)
    test_rows = _cohort_activation(test)
    activation = _beta_rates(
        train_rows,
        train_rows["activation_target"].to_numpy(np.int8),
        ("side", "inventory_role", "action"),
    )
    support_train = train_rows.loc[train_rows["action"].astype(str).eq("current")]
    support = _beta_rates(
        support_train,
        support_train["all_three_target"].to_numpy(np.int8),
        ("side", "inventory_role"),
    )
    output = test_rows.copy()
    output["fold"] = int(fold)
    output["activation_probability"] = [
        activation[(str(row.side), str(row.inventory_role), str(row.action))].mean
        for row in output.itertuples(index=False)
    ]
    output["support_probability"] = [
        support[(str(row.side), str(row.inventory_role))].mean
        for row in output.itertuples(index=False)
    ]
    return output


def _daily_activation_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in frame.groupby(
        ["day", "side", "inventory_role", "action"], observed=True
    ):
        day, side, role, action = keys
        active = group["activation_target"].to_numpy(float)
        support = group["all_three_target"].to_numpy(float)
        rows.append(
            {
                "day": str(day),
                "side": str(side),
                "inventory_role": str(role),
                "action": str(action),
                "rows": int(len(group)),
                "active": int(active.sum()),
                "non_common_active": int(((active == 1) & (support == 0)).sum()),
                "activation_bias": float(
                    np.mean(group["activation_probability"].to_numpy(float) - active)
                ),
                "support_bias": float(
                    np.mean(group["support_probability"].to_numpy(float) - support)
                ),
                "activation_brier": float(
                    np.mean((group["activation_probability"].to_numpy(float) - active) ** 2)
                ),
                "support_brier": float(
                    np.mean((group["support_probability"].to_numpy(float) - support) ** 2)
                ),
            }
        )
    return pd.DataFrame(rows)


def _cluster_ratio_interval(
    frame: pd.DataFrame,
    numerator: str,
    denominator: str,
    *,
    samples: int,
    probability: float,
    seed: int,
) -> dict[str, float]:
    daily = frame.groupby("day", observed=True)[[numerator, denominator]].sum()
    if daily.empty or float(daily[denominator].sum()) <= 0.0:
        return {"mean": math.nan, "lower": math.nan, "upper": math.nan}
    rng = np.random.default_rng(int(seed))
    values = daily.to_numpy(float)
    selected = rng.integers(0, len(values), size=(int(samples), len(values)))
    draws = values[selected].sum(axis=1)
    ratios = np.divide(
        draws[:, 0],
        draws[:, 1],
        out=np.zeros(int(samples), dtype=float),
        where=draws[:, 1] > 0.0,
    )
    tail = (1.0 - float(probability)) / 2.0
    return {
        "mean": float(daily[numerator].sum() / daily[denominator].sum()),
        "lower": float(np.quantile(ratios, tail)),
        "upper": float(np.quantile(ratios, 1.0 - tail)),
    }


def _activation_gate(daily: pd.DataFrame, config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, (keys, group) in enumerate(
        daily.groupby(["side", "inventory_role", "action"], observed=True)
    ):
        side, role, action = keys
        activation = _bootstrap_mean(
            group["activation_bias"].to_numpy(float),
            samples=int(config["bootstrap_samples"]),
            seed=int(config["bootstrap_seed"]) + index * 3,
        )
        support = _bootstrap_mean(
            group["support_bias"].to_numpy(float),
            samples=int(config["bootstrap_samples"]),
            seed=int(config["bootstrap_seed"]) + index * 3 + 1,
        )
        transport = _cluster_ratio_interval(
            group,
            "non_common_active",
            "active",
            samples=int(config["bootstrap_samples"]),
            probability=float(config["transport_interval_probability"]),
            seed=int(config["bootstrap_seed"]) + index * 3 + 2,
        )
        rows.append(
            {
                "side": str(side),
                "inventory_role": str(role),
                "action": str(action),
                "days": int(group["day"].nunique()),
                "rows": int(group["rows"].sum()),
                "active": int(group["active"].sum()),
                "non_common_active": int(group["non_common_active"].sum()),
                "activation_calibration": activation,
                "support_calibration": support,
                "transport_probability_bound": transport,
                "activation_calibration_pass": bool(
                    activation["lower"] <= 0.0 <= activation["upper"]
                ),
                "support_calibration_pass": bool(
                    support["lower"] <= 0.0 <= support["upper"]
                ),
                "transport_bound_pass": bool(
                    transport["upper"]
                    <= float(config["maximum_transport_probability_bound"])
                ),
            }
        )
    return rows


def _fit_pending_cells(
    frame: pd.DataFrame,
    horizon_ms: int,
    config: Mapping[str, Any],
) -> tuple[
    dict[str, float],
    dict[tuple[str, str, str], BetaRate],
]:
    pending = _phase_frame(frame, "pending")
    known, target, _ = _known_targets(pending, "pending", int(horizon_ms))
    work = pending.loc[known].copy()
    work["_target"] = target[known]
    thresholds: dict[str, float] = {}
    cells: dict[tuple[str, str, str], BetaRate] = {}
    parent_equivalent = float(config["side_action_parent_equivalent_trials"])
    child_equivalent = float(config["freshness_child_equivalent_trials"])
    for side in SIDES:
        side_rows = work.loc[work["side"].astype(str).eq(side)].copy()
        age = pd.to_numeric(side_rows["request_book_age_ms"], errors="coerce")
        finite = age[np.isfinite(age)]
        if finite.empty:
            raise RuntimeError(f"{side} pending rows have no finite request book age")
        threshold = float(np.quantile(finite, float(config["fresh_book_quantile"])))
        thresholds[side] = threshold
        side_rows["_freshness"] = np.where(age <= threshold, "fresh", "other")
        side_events = int(side_rows["_target"].sum())
        side_trials = int(len(side_rows))
        side_mean = (side_events + 0.5) / (side_trials + 1.0)
        for action in ACTIONS:
            action_rows = side_rows.loc[side_rows["action"].astype(str).eq(action)]
            action_events = int(action_rows["_target"].sum())
            action_trials = int(len(action_rows))
            parent = BetaRate(
                alpha=parent_equivalent * side_mean + action_events,
                beta=parent_equivalent * (1.0 - side_mean)
                + action_trials
                - action_events,
                events=action_events,
                trials=action_trials,
            )
            for freshness in ("fresh", "other"):
                child = action_rows.loc[
                    action_rows["_freshness"].astype(str).eq(freshness)
                ]
                events = int(child["_target"].sum())
                trials = int(len(child))
                cells[(side, action, freshness)] = BetaRate(
                    alpha=child_equivalent * parent.mean + events,
                    beta=child_equivalent * (1.0 - parent.mean) + trials - events,
                    events=events,
                    trials=trials,
                )
    return thresholds, cells


def _pending_fold_metrics(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    fold: int,
    horizons_ms: Sequence[int],
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    artifact: dict[str, Any] = {}
    posterior_probability = float(config["posterior_probability"])
    tail = (1.0 - posterior_probability) / 2.0
    stress_bps = float(config["stress_fill_value_bps"])
    for horizon in horizons_ms:
        thresholds, cells = _fit_pending_cells(train, int(horizon), config)
        artifact[str(horizon)] = {
            "fresh_book_threshold_ms": thresholds,
            "cells": {
                "|".join(key): {
                    "alpha": value.alpha,
                    "beta": value.beta,
                    "events": value.events,
                    "trials": value.trials,
                    "mean": value.mean,
                }
                for key, value in cells.items()
            },
        }
        pending = _phase_frame(test, "pending")
        known, target, _ = _known_targets(pending, "pending", int(horizon))
        selected = pending.loc[known].copy()
        selected["_target"] = target[known]
        selected["_freshness"] = [
            "fresh"
            if float(age) <= thresholds[str(side)]
            else "other"
            for side, age in zip(
                selected["side"].astype(str),
                pd.to_numeric(selected["request_book_age_ms"], errors="coerce").fillna(
                    math.inf
                ),
                strict=True,
            )
        ]
        cell_keys = list(
            zip(
                selected["side"].astype(str),
                selected["action"].astype(str),
                selected["_freshness"].astype(str),
                strict=True,
            )
        )
        selected["_probability"] = [cells[key].mean for key in cell_keys]
        selected["_alpha"] = [cells[key].alpha for key in cell_keys]
        selected["_beta"] = [cells[key].beta for key in cell_keys]
        selected["_posterior_upper"] = beta_distribution.ppf(
            1.0 - tail,
            selected["_alpha"].to_numpy(float),
            selected["_beta"].to_numpy(float),
        )
        remaining = pd.to_numeric(
            selected["request_remaining_qty"], errors="coerce"
        ).fillna(0.0)
        mid = pd.to_numeric(selected["request_mid"], errors="coerce").fillna(0.0)
        selected["_ev_uncertainty_usdc"] = (
            np.maximum(
                0.0,
                selected["_posterior_upper"].to_numpy(float)
                - selected["_probability"].to_numpy(float),
            )
            * remaining.to_numpy(float)
            * mid.to_numpy(float)
            * stress_bps
            / 10_000.0
        )
        for keys, group in selected.groupby(
            ["day", "side", "action", "_freshness"], observed=True
        ):
            day, side, action, freshness = keys
            rows.append(
                {
                    "fold": int(fold),
                    "day": str(day),
                    "side": str(side),
                    "action": str(action),
                    "freshness": str(freshness),
                    "horizon_ms": int(horizon),
                    "rows": int(len(group)),
                    "events": int(group["_target"].sum()),
                    "predicted": float(group["_probability"].sum()),
                    "posterior_alpha": float(group["_alpha"].iloc[0]),
                    "posterior_beta": float(group["_beta"].iloc[0]),
                    "maximum_ev_uncertainty_usdc": float(
                        group["_ev_uncertainty_usdc"].max()
                    ),
                }
            )
    return rows, artifact


def _pending_gate(
    daily: pd.DataFrame,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, (keys, group) in enumerate(
        daily.groupby(["side", "action", "freshness", "horizon_ms"], observed=True)
    ):
        side, action, freshness, horizon = keys
        posterior_groups = (
            group.groupby(
                ["fold", "posterior_alpha", "posterior_beta"], observed=True
            )
            .agg(rows=("rows", "sum"))
            .reset_index()
        )
        lower, upper = _pending_predictive_interval(
            posterior_groups,
            probability=float(config["posterior_probability"]),
            samples=int(config["posterior_predictive_samples"]),
            seed=int(config["posterior_predictive_seed"]) + index,
        )
        events = int(group["events"].sum())
        max_ev = float(group["maximum_ev_uncertainty_usdc"].max())
        rows.append(
            {
                "side": str(side),
                "action": str(action),
                "freshness": str(freshness),
                "horizon_ms": int(horizon),
                "days": int(group["day"].nunique()),
                "rows": int(group["rows"].sum()),
                "events": events,
                "predicted": float(group["predicted"].sum()),
                "posterior_predictive_lower": lower,
                "posterior_predictive_upper": upper,
                "posterior_predictive_pass": bool(lower <= events <= upper),
                "maximum_ev_uncertainty_usdc": max_ev,
                "economic_bound_pass": bool(
                    max_ev <= float(config["maximum_ev_uncertainty_usdc"])
                ),
            }
        )
    return rows


def _monotonicity_rows(predictions: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for keys, group in predictions.groupby(
        ["fold", "side", "inventory_role", "horizon_ms"], observed=True
    ):
        fold, side, role, horizon = keys
        pivot = group.pivot(
            index="cohort_id", columns="action", values="fill_probability"
        ).dropna(subset=list(ACTIONS))
        violation = (
            (pivot["closer_1tick"] + 1e-12 < pivot["current"])
            | (pivot["current"] + 1e-12 < pivot["farther_1tick"])
        )
        delta = pivot["closer_1tick"] - pivot["farther_1tick"]
        rows.append(
            {
                "fold": int(fold),
                "side": str(side),
                "inventory_role": str(role),
                "horizon_ms": int(horizon),
                "cohorts": int(len(pivot)),
                "violations": int(violation.sum()),
                "violation_rate": float(violation.mean()),
                "nonzero_delta_fraction": float((delta > 1e-12).mean()),
                "delta_median": float(delta.median()),
                "delta_p90": float(delta.quantile(0.9)),
            }
        )
    return rows


def _unconstrained_daily_regret(
    ordered: pd.DataFrame,
    unconstrained_path: Path,
) -> pd.DataFrame:
    unconstrained = pd.read_parquet(
        unconstrained_path,
        columns=[
            "action_lifecycle_id",
            "phase",
            "horizon_ms",
            "fill_probability",
        ],
    ).rename(columns={"fill_probability": "unconstrained_probability"})
    selected = ordered.merge(
        unconstrained.loc[unconstrained["phase"].astype(str).eq("pre_request")],
        on=["action_lifecycle_id", "phase", "horizon_ms"],
        how="inner",
        validate="one_to_one",
    )
    if len(selected) != len(ordered):
        raise RuntimeError("unconstrained diagnostic does not cover ordered OOF rows")
    selected["loss_regret"] = (
        (selected["fill_target"] - selected["fill_probability"]) ** 2
        - (selected["fill_target"] - selected["unconstrained_probability"]) ** 2
    )
    daily = (
        selected.groupby(
            ["day", "side", "inventory_role", "action"], observed=True
        )["loss_regret"]
        .mean()
        .reset_index()
    )
    return daily


def _unconstrained_gate(
    daily: pd.DataFrame,
    *,
    bootstrap_samples: int,
    bootstrap_seed: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, (keys, group) in enumerate(
        daily.groupby(["side", "inventory_role", "action"], observed=True)
    ):
        interval = _bootstrap_mean(
            group["loss_regret"].to_numpy(float),
            samples=int(bootstrap_samples),
            seed=int(bootstrap_seed) + index,
        )
        rows.append(
            {
                "side": str(keys[0]),
                "inventory_role": str(keys[1]),
                "action": str(keys[2]),
                "days": int(group["day"].nunique()),
                "ordered_minus_unconstrained_brier": interval,
                "significantly_worse": bool(interval["lower"] > 0.0),
            }
        )
    return rows


def run(spec_path: Path, output_dir: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    index_identity = spec["source_identity"]["request_state_index"]
    index = pd.read_csv(index_identity["path"], dtype={"day": str})
    days = list(spec["panels"]["development_days"])
    if set(index["day"].astype(str)) != set(days):
        raise RuntimeError("request-state index differs from frozen Development panel")
    fold_source = json.loads(
        Path(spec["source_identity"]["unconstrained_oof_report"]["path"]).read_text()
    )
    folds = fold_source["folds"]
    output_dir.mkdir(parents=True, exist_ok=True)
    write_code_checkpoint(
        output_dir / "code_checkpoint",
        repo_root=ROOT,
        code_identity=git_workspace_identity(ROOT),
    )
    horizons = [int(value) for value in spec["report_horizons_ms"]]
    fold_records: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    activation_parts: list[pd.DataFrame] = []
    monotonicity: list[dict[str, Any]] = []
    unconstrained_daily_parts: list[pd.DataFrame] = []
    pending_daily_rows: list[dict[str, Any]] = []
    pending_artifacts: list[dict[str, Any]] = []
    for fold_record in folds:
        fold = int(fold_record["fold"])
        base = _load_days(index, fold_record["base_days"])
        calibration = _load_days(index, fold_record["calibration_days"])
        test = _load_days(index, fold_record["test_days"])
        models: dict[str, CauseSpecificRateModel] = {}
        for side in SIDES:
            models[side] = fit_ordered_pre_request_model(
                base.loc[base["side"].astype(str).eq(side)].copy(),
                maximum_bins=int(spec["model"]["maximum_bins"]),
                random_seed=int(spec["model"]["random_seed"]) + fold,
                model_config=spec["model"],
            )
        fold_predictions: list[pd.DataFrame] = []
        calibration_identity: dict[str, Any] = {}
        for side in SIDES:
            base_side = base.loc[
                base["side"].astype(str).eq(side) & common_support_mask(base)
            ]
            calibration_side = calibration.loc[
                calibration["side"].astype(str).eq(side)
                & common_support_mask(calibration)
            ]
            test_side = test.loc[
                test["side"].astype(str).eq(side) & common_support_mask(test)
            ]
            base_pre = _phase_frame(base_side, "pre_request")
            calibration_pre = _phase_frame(calibration_side, "pre_request")
            test_pre = _phase_frame(test_side, "pre_request")
            calibrators = _role_scales(
                calibration_pre,
                "pre_request",
                models[side],
                horizons,
                minimum_events=int(spec["calibration"]["minimum_events_per_role"]),
                log_scale_bound=float(spec["calibration"]["absolute_log_scale_bound"]),
            )
            calibration_identity[side] = calibrators
            baseline_rates = _constant_rates(base_pre, "pre_request")
            for horizon in horizons:
                fold_predictions.append(
                    _predict_horizon(
                        test_pre,
                        "pre_request",
                        models[side],
                        calibrators,
                        baseline_rates,
                        horizon,
                        fold,
                    )
                )
        predictions = pd.concat(fold_predictions, ignore_index=True)
        fold_path = output_dir / f"fold_{fold:02d}_ordered_oof.parquet"
        predictions.to_parquet(fold_path, index=False, compression="zstd")
        daily_parts.append(_daily_action_metrics(predictions))
        monotonicity.extend(_monotonicity_rows(predictions))
        activation = _activation_predictions(base, test, fold)
        activation_path = output_dir / f"fold_{fold:02d}_activation_oof.parquet"
        activation.to_parquet(activation_path, index=False, compression="zstd")
        activation_parts.append(_daily_activation_metrics(activation))
        unconstrained_daily_parts.append(
            _unconstrained_daily_regret(
                predictions, Path(fold_record["output"]["path"])
            )
        )
        pending_rows, pending_artifact = _pending_fold_metrics(
            base,
            test,
            fold=fold,
            horizons_ms=spec["pending_nuisance"]["report_horizons_ms"],
            config=spec["pending_nuisance"],
        )
        pending_daily_rows.extend(pending_rows)
        pending_artifacts.append({"fold": fold, **pending_artifact})
        fold_records.append(
            {
                "fold": fold,
                "base_days": list(fold_record["base_days"]),
                "calibration_days": list(fold_record["calibration_days"]),
                "test_days": list(fold_record["test_days"]),
                "ordered_oof": _identity(fold_path),
                "activation_oof": _identity(activation_path),
                "calibrators": calibration_identity,
            }
        )
        del base, calibration, test, models, predictions, fold_predictions, activation
        gc.collect()
    daily = pd.concat(daily_parts, ignore_index=True)
    daily_path = output_dir / "action_curve_daily_metrics.parquet"
    daily.to_parquet(daily_path, index=False, compression="zstd")
    activation_daily = pd.concat(activation_parts, ignore_index=True)
    activation_daily_path = output_dir / "activation_transport_daily_metrics.parquet"
    activation_daily.to_parquet(
        activation_daily_path, index=False, compression="zstd"
    )
    pending_daily = pd.DataFrame(pending_daily_rows)
    pending_daily_path = output_dir / "pending_nuisance_daily_metrics.parquet"
    pending_daily.to_parquet(pending_daily_path, index=False, compression="zstd")
    _atomic_json(
        {"folds": pending_artifacts}, output_dir / "pending_nuisance_artifact.json"
    )
    curves = _action_curve_gate(daily, config=spec["curve_gate"])
    activation_gate = _activation_gate(
        activation_daily, spec["activation_transport_gate"]
    )
    pending_gate = _pending_gate(pending_daily, spec["pending_nuisance"])
    unconstrained_rows = _unconstrained_gate(
        pd.concat(unconstrained_daily_parts, ignore_index=True),
        bootstrap_samples=int(spec["diagnostic"]["bootstrap_samples"]),
        bootstrap_seed=int(spec["diagnostic"]["bootstrap_seed"]),
    )
    monotonicity_valid = bool(
        monotonicity and all(row["violations"] == 0 for row in monotonicity)
    )
    prediction_supported = bool(
        monotonicity_valid
        and all(row["curve_pass"] for row in curves if row["required_component"])
    )
    transport_supported = bool(
        activation_gate
        and all(
            row["activation_calibration_pass"]
            and row["support_calibration_pass"]
            and row["transport_bound_pass"]
            for row in activation_gate
        )
    )
    unconstrained_supported = bool(
        unconstrained_rows
        and not any(row["significantly_worse"] for row in unconstrained_rows)
    )
    pending_supported = bool(
        pending_gate
        and all(
            row["posterior_predictive_pass"] and row["economic_bound_pass"]
            for row in pending_gate
        )
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "status": "development_complete",
        "spec": _identity(spec_path),
        "folds": fold_records,
        "report_horizons_ms": horizons,
        "action_curves": curves,
        "monotonicity_contract": monotonicity,
        "activation_transport": activation_gate,
        "pending_nuisance_bound": pending_gate,
        "unconstrained_diagnostic": unconstrained_rows,
        "artifacts": {
            "action_curve_daily_metrics": _identity(daily_path),
            "activation_transport_daily_metrics": _identity(activation_daily_path),
            "pending_nuisance_daily_metrics": _identity(pending_daily_path),
            "pending_nuisance_artifact": _identity(
                output_dir / "pending_nuisance_artifact.json"
            ),
        },
        "gates": {
            "monotonicity_contract_valid": monotonicity_valid,
            "prediction_supported": prediction_supported,
            "transport_supported": transport_supported,
            "pending_nuisance_bound_supported": pending_supported,
            "constrained_not_significantly_worse": unconstrained_supported,
            "economic_resolution_supported": False,
            "action_uplift_supported": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
        "validation_read": False,
        "sealed_holdout_read": False,
        "decision": (
            "prediction_building_blocks_supported_pending_bound_required"
            if prediction_supported
            and transport_supported
            and pending_supported
            and unconstrained_supported
            else "close_ordered_prediction_family_on_development"
        ),
    }
    report_path = output_dir / "report.json"
    _atomic_json(report, report_path)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = run(args.spec, args.output)
    print(json.dumps(report["gates"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
