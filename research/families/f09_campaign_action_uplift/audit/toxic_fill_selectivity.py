#!/usr/bin/env python3
"""Nonlinear selectivity metrics for actions that deliberately remove fills.

Raw fill retention is not sufficient for a defensive maker action.  A policy
may rationally trade less when the removed fills are disproportionately toxic.
This module keeps the intuitive reduction-leverage ratio as a diagnostic, but
uses two numerically stable quantities for inference:

``reduction_surplus = toxic_reduction - fill_reduction``

and the log ratio between baseline and candidate toxic-fill shares.  The
bounded nonlinear score is ``tanh(log_ratio / log(2))``; zero means that toxic
fills and all fills changed proportionally, while positive values mean that
toxic fills were removed faster than activity.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "toxic_fill_selectivity.v1"
BOOTSTRAP_SCHEMA_VERSION = "toxic_fill_selectivity_bootstrap.v1"
RANDOMIZED_SCHEMA_VERSION = "randomized_toxic_fill_selectivity.v1"


@dataclass(frozen=True)
class ToxicFillSelectivity:
    baseline_fill_rate: float
    candidate_fill_rate: float
    baseline_toxic_fill_rate: float
    candidate_toxic_fill_rate: float
    baseline_toxic_share: float
    candidate_toxic_share: float
    fills_retention: float
    toxic_fills_retention: float
    fill_reduction: float
    toxic_fill_reduction: float
    toxic_reduction_surplus: float
    toxic_reduction_leverage: float | None
    toxic_selectivity_log_ratio: float
    nonlinear_selectivity_score: float
    probability_projection_applied: bool

    def to_payload(self) -> dict[str, Any]:
        return {"schema_version": SCHEMA_VERSION, **asdict(self)}


def _project_rates(
    fill_rate: float,
    toxic_fill_rate: float,
) -> tuple[float, float, bool]:
    fill = float(fill_rate)
    toxic = float(toxic_fill_rate)
    if not math.isfinite(fill) or not math.isfinite(toxic):
        raise ValueError("fill and toxic-fill rates must be finite")
    projected_fill = min(1.0, max(0.0, fill))
    projected_toxic = min(projected_fill, max(0.0, toxic))
    projected = not (
        math.isclose(fill, projected_fill, abs_tol=1e-15)
        and math.isclose(toxic, projected_toxic, abs_tol=1e-15)
    )
    return projected_fill, projected_toxic, projected


def toxic_fill_selectivity(
    *,
    baseline_fill_rate: float,
    candidate_fill_rate: float,
    baseline_toxic_fill_rate: float,
    candidate_toxic_fill_rate: float,
    epsilon: float = 1e-9,
) -> ToxicFillSelectivity:
    """Compute activity and toxicity changes on a common decision denominator."""

    if not math.isfinite(float(epsilon)) or float(epsilon) <= 0.0:
        raise ValueError("epsilon must be finite and positive")
    baseline_fill, baseline_toxic, projected_baseline = _project_rates(
        baseline_fill_rate,
        baseline_toxic_fill_rate,
    )
    candidate_fill, candidate_toxic, projected_candidate = _project_rates(
        candidate_fill_rate,
        candidate_toxic_fill_rate,
    )
    eps = float(epsilon)
    fills_retention = candidate_fill / max(baseline_fill, eps)
    toxic_retention = candidate_toxic / max(baseline_toxic, eps)
    fill_reduction = 1.0 - fills_retention
    toxic_reduction = 1.0 - toxic_retention
    reduction_surplus = toxic_reduction - fill_reduction

    leverage: float | None
    if fill_reduction > eps:
        leverage = toxic_reduction / fill_reduction
    elif toxic_reduction > eps:
        leverage = None
    else:
        leverage = None

    baseline_share = baseline_toxic / max(baseline_fill, eps)
    candidate_share = candidate_toxic / max(candidate_fill, eps)
    log_ratio = math.log(
        (baseline_share + eps) / (candidate_share + eps)
    )
    nonlinear_score = math.tanh(log_ratio / math.log(2.0))
    return ToxicFillSelectivity(
        baseline_fill_rate=baseline_fill,
        candidate_fill_rate=candidate_fill,
        baseline_toxic_fill_rate=baseline_toxic,
        candidate_toxic_fill_rate=candidate_toxic,
        baseline_toxic_share=baseline_share,
        candidate_toxic_share=candidate_share,
        fills_retention=fills_retention,
        toxic_fills_retention=toxic_retention,
        fill_reduction=fill_reduction,
        toxic_fill_reduction=toxic_reduction,
        toxic_reduction_surplus=reduction_surplus,
        toxic_reduction_leverage=leverage,
        toxic_selectivity_log_ratio=log_ratio,
        nonlinear_selectivity_score=nonlinear_score,
        probability_projection_applied=(projected_baseline or projected_candidate),
    )


def _aligned_dr_values(
    frame: pd.DataFrame,
    *,
    value_column: str,
) -> pd.DataFrame:
    required = {"day", "decision_id", value_column}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"selectivity OPE rows missing columns: {missing}")
    if frame.duplicated(["day", "decision_id"]).any():
        raise ValueError("selectivity OPE rows contain duplicate decision keys")
    output = frame[["day", "decision_id", value_column]].copy()
    output[value_column] = pd.to_numeric(output[value_column], errors="coerce")
    if output[value_column].isna().any():
        raise ValueError(f"{value_column} contains non-finite values")
    return output


def paired_dr_selectivity(
    *,
    candidate_fill_rows: pd.DataFrame,
    baseline_fill_rows: pd.DataFrame,
    candidate_toxic_rows: pd.DataFrame,
    baseline_toxic_rows: pd.DataFrame,
    value_column: str = "ope_dr_value",
    bootstrap_trials: int = 20_000,
    random_seed: int = 20260722,
) -> dict[str, Any]:
    """Estimate selectivity and day-clustered intervals from aligned DR rows."""

    sources = {
        "candidate_fill": candidate_fill_rows,
        "baseline_fill": baseline_fill_rows,
        "candidate_toxic": candidate_toxic_rows,
        "baseline_toxic": baseline_toxic_rows,
    }
    aligned: pd.DataFrame | None = None
    for name, source in sources.items():
        values = _aligned_dr_values(source, value_column=value_column).rename(
            columns={value_column: name}
        )
        aligned = (
            values
            if aligned is None
            else aligned.merge(
                values,
                on=["day", "decision_id"],
                how="inner",
                validate="one_to_one",
            )
        )
    assert aligned is not None
    expected_rows = {len(frame) for frame in sources.values()}
    if len(expected_rows) != 1 or len(aligned) != next(iter(expected_rows)):
        raise ValueError("fill and toxic-fill OPE rows do not share one denominator")

    def summarize(frame: pd.DataFrame) -> ToxicFillSelectivity:
        return toxic_fill_selectivity(
            baseline_fill_rate=float(frame["baseline_fill"].mean()),
            candidate_fill_rate=float(frame["candidate_fill"].mean()),
            baseline_toxic_fill_rate=float(frame["baseline_toxic"].mean()),
            candidate_toxic_fill_rate=float(frame["candidate_toxic"].mean()),
        )

    point = summarize(aligned)
    metrics = (
        "fills_retention",
        "toxic_fills_retention",
        "fill_reduction",
        "toxic_fill_reduction",
        "toxic_reduction_surplus",
        "toxic_selectivity_log_ratio",
        "nonlinear_selectivity_score",
    )
    samples = {name: np.empty(int(bootstrap_trials), dtype=float) for name in metrics}
    days = tuple(sorted(aligned["day"].astype(str).unique()))
    day_sums = (
        aligned.assign(day=aligned["day"].astype(str))
        .groupby("day", sort=True)[
            [
                "candidate_fill",
                "baseline_fill",
                "candidate_toxic",
                "baseline_toxic",
            ]
        ]
        .agg(["sum", "count"])
    )

    def summarize_day_indices(indices: np.ndarray) -> ToxicFillSelectivity:
        selected = day_sums.iloc[indices]
        rates: dict[str, float] = {}
        for name in (
            "candidate_fill",
            "baseline_fill",
            "candidate_toxic",
            "baseline_toxic",
        ):
            rates[name] = float(
                selected[(name, "sum")].sum()
                / max(float(selected[(name, "count")].sum()), 1.0)
            )
        return toxic_fill_selectivity(
            baseline_fill_rate=rates["baseline_fill"],
            candidate_fill_rate=rates["candidate_fill"],
            baseline_toxic_fill_rate=rates["baseline_toxic"],
            candidate_toxic_fill_rate=rates["candidate_toxic"],
        )

    rng = np.random.default_rng(int(random_seed))
    for index in range(int(bootstrap_trials)):
        chosen = rng.integers(0, len(days), size=len(days))
        value = summarize_day_indices(chosen)
        for name in metrics:
            samples[name][index] = float(getattr(value, name))

    intervals = {
        name: {
            "p025": float(np.quantile(values, 0.025)),
            "p50": float(np.quantile(values, 0.50)),
            "p975": float(np.quantile(values, 0.975)),
        }
        for name, values in samples.items()
    }
    daily_rows: list[dict[str, Any]] = []
    for day_index, day in enumerate(days):
        value = summarize_day_indices(np.asarray([day_index], dtype=int))
        daily_rows.append(
            {
                "day": day,
                "toxic_reduction_surplus": value.toxic_reduction_surplus,
                "toxic_selectivity_log_ratio": value.toxic_selectivity_log_ratio,
                "nonlinear_selectivity_score": value.nonlinear_selectivity_score,
            }
        )
    daily = pd.DataFrame(daily_rows)
    return {
        "schema_version": BOOTSTRAP_SCHEMA_VERSION,
        "estimand": "candidate_action_minus_baseline_action_on_common_decision_denominator",
        "rows": int(len(aligned)),
        "days": int(len(days)),
        "point": point.to_payload(),
        "day_cluster_bootstrap": {
            "trials": int(bootstrap_trials),
            "random_seed": int(random_seed),
            "intervals": intervals,
        },
        "daily": {
            "toxic_selectivity_positive_days": int(
                (daily["toxic_selectivity_log_ratio"] > 0.0).sum()
            ),
            "toxic_selectivity_negative_days": int(
                (daily["toxic_selectivity_log_ratio"] < 0.0).sum()
            ),
            "toxic_selectivity_positive_rate": float(
                (daily["toxic_selectivity_log_ratio"] > 0.0).mean()
            ),
        },
        "gate_inputs": {
            "toxic_reduction_surplus_lower_bound": intervals[
                "toxic_reduction_surplus"
            ]["p025"],
            "toxic_selectivity_log_ratio_lower_bound": intervals[
                "toxic_selectivity_log_ratio"
            ]["p025"],
        },
    }


def randomized_panel_selectivity(
    panel: pd.DataFrame,
    *,
    candidate_action: str,
    baseline_action: str,
    bootstrap_trials: int = 20_000,
    random_seed: int = 20260722,
) -> dict[str, Any]:
    """Estimate logged K1/K0 toxic-fill selectivity from randomized actions."""

    required = {
        "day",
        "decision_id",
        "action",
        "behavior_propensity",
        "intervention_fill_count",
    }
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ValueError(f"randomized selectivity panel missing columns: {missing}")
    if panel.duplicated(["day", "decision_id"]).any():
        raise ValueError("randomized selectivity panel has duplicate decisions")
    scoped = panel[
        panel["action"].astype(str).isin(
            {str(candidate_action), str(baseline_action)}
        )
    ].copy()
    if set(scoped["action"].astype(str)) != {
        str(candidate_action),
        str(baseline_action),
    }:
        raise ValueError("both randomized actions require observed support")
    propensity = pd.to_numeric(
        scoped["behavior_propensity"], errors="coerce"
    )
    if propensity.isna().any() or (propensity <= 0.0).any():
        raise ValueError("randomized selectivity has invalid propensities")
    scoped["filled"] = (
        pd.to_numeric(scoped["intervention_fill_count"], errors="coerce")
        .fillna(0.0)
        .gt(0.0)
        .astype(float)
    )
    markout = pd.to_numeric(
        scoped.get(
            "fill_value_markout_bps",
            pd.Series(math.nan, index=scoped.index),
        ),
        errors="coerce",
    )
    threshold = pd.to_numeric(
        scoped.get(
            "fill_value_threshold_bps",
            pd.Series(0.0, index=scoped.index),
        ),
        errors="coerce",
    ).fillna(0.0)
    censored = pd.to_numeric(
        scoped.get(
            "fill_value_horizon_censored",
            pd.Series(0.0, index=scoped.index),
        ),
        errors="coerce",
    ).fillna(1.0).astype(bool)
    scoped["toxic"] = (
        scoped["filled"].astype(bool)
        & (censored | markout.isna() | markout.lt(threshold))
    ).astype(float)

    grouped = (
        scoped.assign(day=scoped["day"].astype(str))
        .groupby(["day", "action"], sort=True)[["filled", "toxic"]]
        .agg(["sum", "count"])
    )
    days = tuple(sorted(scoped["day"].astype(str).unique()))

    def summarize(indices: np.ndarray) -> ToxicFillSelectivity:
        selected_days = [days[int(index)] for index in indices]

        def rate(action: str, outcome: str) -> float:
            rows = grouped.loc[
                [
                    (day, action)
                    for day in selected_days
                    if (day, action) in grouped.index
                ]
            ]
            return float(
                rows[(outcome, "sum")].sum()
                / max(float(rows[(outcome, "count")].sum()), 1.0)
            )

        return toxic_fill_selectivity(
            baseline_fill_rate=rate(str(baseline_action), "filled"),
            candidate_fill_rate=rate(str(candidate_action), "filled"),
            baseline_toxic_fill_rate=rate(str(baseline_action), "toxic"),
            candidate_toxic_fill_rate=rate(str(candidate_action), "toxic"),
        )

    point = summarize(np.arange(len(days), dtype=int))
    metrics = (
        "fills_retention",
        "toxic_fills_retention",
        "fill_reduction",
        "toxic_fill_reduction",
        "toxic_reduction_surplus",
        "toxic_selectivity_log_ratio",
        "nonlinear_selectivity_score",
    )
    samples = {
        name: np.empty(int(bootstrap_trials), dtype=float) for name in metrics
    }
    rng = np.random.default_rng(int(random_seed))
    for trial in range(int(bootstrap_trials)):
        value = summarize(
            rng.integers(0, len(days), size=len(days))
        )
        for name in metrics:
            samples[name][trial] = float(getattr(value, name))
    intervals = {
        name: {
            "p025": float(np.quantile(values, 0.025)),
            "p50": float(np.quantile(values, 0.50)),
            "p975": float(np.quantile(values, 0.975)),
        }
        for name, values in samples.items()
    }
    daily_values = [
        summarize(np.asarray([index], dtype=int))
        for index in range(len(days))
    ]
    return {
        "schema_version": RANDOMIZED_SCHEMA_VERSION,
        "estimand": "logged randomized candidate versus baseline selectivity",
        "candidate_action": str(candidate_action),
        "baseline_action": str(baseline_action),
        "rows": int(len(scoped)),
        "days": int(len(days)),
        "point": point.to_payload(),
        "day_cluster_bootstrap": {
            "trials": int(bootstrap_trials),
            "random_seed": int(random_seed),
            "intervals": intervals,
        },
        "daily": {
            "toxic_selectivity_positive_days": int(
                sum(
                    value.toxic_selectivity_log_ratio > 0.0
                    for value in daily_values
                )
            ),
            "toxic_selectivity_positive_rate": float(
                np.mean(
                    [
                        value.toxic_selectivity_log_ratio > 0.0
                        for value in daily_values
                    ]
                )
            ),
        },
        "gate_inputs": {
            "toxic_reduction_surplus_lower_bound": intervals[
                "toxic_reduction_surplus"
            ]["p025"],
            "toxic_selectivity_log_ratio_lower_bound": intervals[
                "toxic_selectivity_log_ratio"
            ]["p025"],
        },
    }


def selectivity_metric_from_summary(
    summary: Mapping[str, Any],
    name: str,
) -> dict[str, float]:
    """Convert one bootstrap selectivity metric to scorecard evidence."""

    point = summary.get("point") or {}
    intervals = ((summary.get("day_cluster_bootstrap") or {}).get("intervals") or {})
    interval = intervals.get(name) or {}
    return {
        "estimate": float(point[name]),
        "lower_bound": float(interval["p025"]),
        "upper_bound": float(interval["p975"]),
        "daily_positive_rate": float(
            (summary.get("daily") or {}).get(
                "toxic_selectivity_positive_rate",
                math.nan,
            )
        ),
    }
