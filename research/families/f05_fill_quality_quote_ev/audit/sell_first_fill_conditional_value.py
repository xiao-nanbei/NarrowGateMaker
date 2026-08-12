"""Development-only SELL first-fill conditional value feasibility."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from research.families.f10_live_replay_attribution.audit import (
    first_opener_decision_to_terminal_contract as lifecycle_contract,
)

REPORT_SCHEMA_VERSION = "sell_first_fill_conditional_value.report.v2"
PRIMARY_PANEL = "grade_a_primary"
SENSITIVITY_PANEL = "grade_b_sensitivity"
SIDES = ("SELL", "BUY")


@dataclass(frozen=True)
class ConditionalValueEvaluation:
    oof_predictions: pd.DataFrame
    report: dict[str, Any]


def _ridge(alpha: float) -> Pipeline:
    return Pipeline(
        [
            ("scale", StandardScaler()),
            ("model", Ridge(alpha=float(alpha), fit_intercept=True)),
        ]
    )


def _fit_sha256(
    model: Pipeline,
    *,
    features: Sequence[str],
    train_days: Sequence[str],
    risk_threshold: float,
) -> str:
    ridge = model.named_steps["model"]
    payload = {
        "features": list(features),
        "train_days": list(train_days),
        "coef": np.asarray(ridge.coef_, dtype=float).tolist(),
        "intercept": float(ridge.intercept_),
        "risk_threshold": float(risk_threshold),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _outer_folds(
    days: Sequence[str],
    *,
    minimum_train_days: int,
    embargo_calendar_days: int,
    maximum_test_block_days: int,
) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    ordered = tuple(sorted(dict.fromkeys(str(day) for day in days)))
    folds: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    cursor = int(minimum_train_days)
    while cursor < len(ordered):
        test_days = ordered[cursor : cursor + int(maximum_test_block_days)]
        cutoff = pd.Timestamp(test_days[0]) - pd.Timedelta(
            days=int(embargo_calendar_days)
        )
        train_days = tuple(
            day for day in ordered[:cursor] if pd.Timestamp(day) < cutoff
        )
        if len(train_days) >= int(minimum_train_days):
            folds.append((train_days, test_days))
        cursor += int(maximum_test_block_days)
    return folds


def prepare_native_trace(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    require_complete_development: bool = True,
) -> pd.DataFrame:
    """Validate exact lifecycle rows and attach the frozen panel identity."""

    output = lifecycle_contract.validate_native_trace(frame, spec)
    panels = spec["panels"]
    grade_a = tuple(panels["development_primary_grade_a_days"])
    grade_b = tuple(panels["development_sensitivity_grade_b_days"])
    panel_by_day = {
        **{str(day): PRIMARY_PANEL for day in grade_a},
        **{str(day): SENSITIVITY_PANEL for day in grade_b},
    }
    observed = set(output["day"].astype(str))
    if require_complete_development and observed != set(panel_by_day):
        raise ValueError(
            "SELL first-fill trace lacks the exact frozen Development denominator"
        )
    output = output.copy()
    output["day"] = output["day"].astype(str)
    output["analysis_panel"] = output["day"].map(panel_by_day)
    if output["analysis_panel"].isna().any():
        raise ValueError("SELL first-fill trace read outside frozen Development")

    features = tuple(spec["decision_visible_features"]["model_features"])
    queue = pd.to_numeric(output["queue_ahead_btc"], errors="coerce")
    queue_available = pd.to_numeric(
        output["queue_ahead_available"], errors="coerce"
    )
    finite_queue = np.isfinite(queue.to_numpy(dtype=float))
    if queue_available.isna().any() or not np.array_equal(
        queue_available.to_numpy(dtype=np.uint8), finite_queue.astype(np.uint8)
    ):
        raise ValueError("SELL first-fill queue availability identity drifted")
    output["queue_ahead_btc"] = queue.fillna(0.0)
    output["queue_ahead_available"] = queue_available.astype(float)
    for column in features + (lifecycle_contract.PRIMARY_ESTIMAND,):
        values = pd.to_numeric(output[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
            raise ValueError(f"SELL first-fill trace has invalid {column}")
        output[column] = values.astype(float)
    minimum_unique = int(
        spec["decision_visible_features"]["minimum_unique_values_per_panel_side"]
    )
    for panel in (PRIMARY_PANEL, SENSITIVITY_PANEL):
        for side in SIDES:
            cell = output.loc[
                output["analysis_panel"].eq(panel)
                & output["side"].astype(str).str.upper().eq(side)
            ]
            if cell.empty:
                raise ValueError(f"SELL first-fill has no {panel} {side} support")
            for feature in features:
                if cell[feature].nunique(dropna=False) < minimum_unique:
                    raise ValueError(
                        f"SELL first-fill feature is degenerate for "
                        f"{panel} {side}: {feature}"
                    )
    return output.sort_values(
        ["day", "decision_ts_ms", "campaign_id"], kind="stable"
    ).reset_index(drop=True)


def _score_folds(
    *,
    train_cell: pd.DataFrame,
    test_cell: pd.DataFrame,
    folds: Sequence[tuple[tuple[str, ...], tuple[str, ...]]],
    spec: Mapping[str, Any],
    panel: str,
    side: str,
) -> pd.DataFrame:
    target_column = lifecycle_contract.PRIMARY_ESTIMAND
    features = list(spec["decision_visible_features"]["model_features"])
    chronology = spec["chronological_evaluation"]
    alpha = float(chronology["ridge_alpha"])
    high_risk_quantile = float(chronology["high_risk_quantile"])
    outputs: list[pd.DataFrame] = []
    for fold_number, (train_days, test_days) in enumerate(folds):
        train = train_cell.loc[train_cell["day"].isin(train_days)]
        test = test_cell.loc[test_cell["day"].isin(test_days)]
        if train.empty or test.empty:
            continue
        baseline_prediction = float(train[target_column].mean())
        model = _ridge(alpha)
        model.fit(train[features], train[target_column])
        train_prediction = np.asarray(model.predict(train[features]), dtype=float)
        risk_threshold = float(np.quantile(train_prediction, high_risk_quantile))
        fit_sha256 = _fit_sha256(
            model,
            features=features,
            train_days=train_days,
            risk_threshold=risk_threshold,
        )
        scored = test[
            [
                "day",
                "quality_grade",
                "campaign_id",
                "decision_id",
                "order_id",
                "side",
                target_column,
            ]
        ].copy()
        scored["analysis_panel"] = panel
        scored["training_panel"] = PRIMARY_PANEL
        scored["outer_fold"] = fold_number
        scored["outer_train_day_count"] = len(train_days)
        scored["outer_train_max_day"] = max(train_days)
        scored["outer_fit_sha256"] = fit_sha256
        scored["prediction_intercept_usdc"] = baseline_prediction
        scored["prediction_local_usdc"] = np.asarray(
            model.predict(test[features]), dtype=float
        )
        scored["high_risk_threshold_usdc"] = risk_threshold
        scored["high_risk"] = scored["prediction_local_usdc"].le(risk_threshold)
        scored["squared_error_improvement_usdc2"] = (
            scored[target_column] - scored["prediction_intercept_usdc"]
        ).pow(2) - (
            scored[target_column] - scored["prediction_local_usdc"]
        ).pow(2)
        outputs.append(scored)
    if not outputs:
        raise ValueError(f"SELL first-fill produced no OOF rows for {panel} {side}")
    output = pd.concat(outputs, ignore_index=True)
    if output.duplicated(["day", "campaign_id"]).any():
        raise ValueError("SELL first-fill produced duplicate campaign OOF rows")
    return output


def _fit_grade_a_side(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    side: str,
) -> pd.DataFrame:
    cell = frame.loc[
        frame["analysis_panel"].eq(PRIMARY_PANEL)
        & frame["side"].astype(str).str.upper().eq(side)
    ].copy()
    chronology = spec["chronological_evaluation"]
    folds = _outer_folds(
        sorted(cell["day"].unique()),
        minimum_train_days=int(chronology["minimum_train_days"]),
        embargo_calendar_days=int(chronology["embargo_calendar_days"]),
        maximum_test_block_days=int(chronology["maximum_test_block_days"]),
    )
    return _score_folds(
        train_cell=cell,
        test_cell=cell,
        folds=folds,
        spec=spec,
        panel=PRIMARY_PANEL,
        side=side,
    )


def _fit_grade_b_transport_side(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    side: str,
) -> pd.DataFrame:
    grade_a = frame.loc[
        frame["analysis_panel"].eq(PRIMARY_PANEL)
        & frame["side"].astype(str).str.upper().eq(side)
    ].copy()
    grade_b = frame.loc[
        frame["analysis_panel"].eq(SENSITIVITY_PANEL)
        & frame["side"].astype(str).str.upper().eq(side)
    ].copy()
    chronology = spec["chronological_evaluation"]
    minimum_train_days = int(chronology["minimum_train_days"])
    embargo_days = int(chronology["embargo_calendar_days"])
    block_size = int(chronology["grade_b_transport_test_block_days"])
    ordered_test_days = tuple(sorted(grade_b["day"].unique()))
    folds: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
    for cursor in range(0, len(ordered_test_days), block_size):
        test_days = ordered_test_days[cursor : cursor + block_size]
        cutoff = pd.Timestamp(test_days[0]) - pd.Timedelta(days=embargo_days)
        train_days = tuple(
            day
            for day in sorted(grade_a["day"].unique())
            if pd.Timestamp(day) < cutoff
        )
        if len(train_days) >= minimum_train_days:
            folds.append((train_days, test_days))
    return _score_folds(
        train_cell=grade_a,
        test_cell=grade_b,
        folds=folds,
        spec=spec,
        panel=SENSITIVITY_PANEL,
        side=side,
    )


def _point_metrics(frame: pd.DataFrame) -> dict[str, float]:
    target = lifecycle_contract.PRIMARY_ESTIMAND
    high_risk = frame.loc[frame["high_risk"]]
    complement = frame.loc[~frame["high_risk"]]
    day_column = (
        "_bootstrap_day_cluster"
        if "_bootstrap_day_cluster" in frame.columns
        else "day"
    )
    high_risk_daily = high_risk.groupby(day_column, sort=False)[target].mean()
    return {
        "unconditional_mean_value_usdc": float(frame[target].mean()),
        "local_mse_improvement_usdc2": float(
            frame["squared_error_improvement_usdc2"].mean()
        ),
        "high_risk_mean_value_usdc": (
            float(high_risk[target].mean()) if not high_risk.empty else math.nan
        ),
        "high_risk_value_gap_usdc": (
            float(high_risk[target].mean() - complement[target].mean())
            if not high_risk.empty and not complement.empty
            else math.nan
        ),
        "high_risk_daily_negative_fraction": (
            float((high_risk_daily < 0.0).mean())
            if not high_risk_daily.empty
            else math.nan
        ),
    }


def _cluster_bootstrap(
    frame: pd.DataFrame,
    *,
    samples: int,
    seed: int,
    confidence: float,
    simultaneous_family_size: int,
) -> dict[str, dict[str, float]]:
    days = np.asarray(sorted(frame["day"].unique()), dtype=object)
    if len(days) < 2:
        raise ValueError("SELL first-fill bootstrap requires at least two days")
    rows_by_day = {
        str(day): frame.loc[frame["day"].eq(day)].reset_index(drop=True)
        for day in days
    }
    rng = np.random.default_rng(seed)
    draws = {name: [] for name in _point_metrics(frame)}
    for _ in range(int(samples)):
        chunks: list[pd.DataFrame] = []
        for draw_position, day in enumerate(
            rng.choice(days, size=len(days), replace=True)
        ):
            source = rows_by_day[str(day)]
            positions = rng.integers(0, len(source), size=len(source))
            sampled = source.iloc[positions].copy()
            sampled["_bootstrap_day_cluster"] = (
                f"{draw_position}:{day}"
            )
            chunks.append(sampled)
        metrics = _point_metrics(pd.concat(chunks, ignore_index=True))
        for name, value in metrics.items():
            if np.isfinite(value):
                draws[name].append(float(value))
    tail = (1.0 - float(confidence)) / (2.0 * int(simultaneous_family_size))
    intervals: dict[str, dict[str, float]] = {}
    for name, values in draws.items():
        if len(values) < 0.95 * int(samples):
            raise ValueError(f"SELL first-fill bootstrap support failed for {name}")
        array = np.asarray(values, dtype=float)
        intervals[name] = {
            "lcb": float(np.quantile(array, tail)),
            "median": float(np.quantile(array, 0.5)),
            "ucb": float(np.quantile(array, 1.0 - tail)),
        }
    return intervals


def _summarize(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
    *,
    panel: str,
    side: str,
    seed_offset: int,
) -> dict[str, Any]:
    inference = spec["inference"]
    high_risk = frame.loc[frame["high_risk"]]
    minimum_rows = int(inference["minimum_high_risk_rows"])
    minimum_days = int(inference["minimum_high_risk_days"])
    minimum_oof_days = int(
        inference[
            "minimum_grade_a_oof_days"
            if panel == PRIMARY_PANEL
            else "minimum_grade_b_transport_oof_days"
        ]
    )
    observed_oof_days = int(frame["day"].nunique())
    expected_key = (
        "expected_grade_a_scored_oof_days"
        if panel == PRIMARY_PANEL
        else "expected_grade_b_transport_oof_days"
    )
    expected_oof_days = tuple(chronology_day for chronology_day in (
        spec["chronological_evaluation"][expected_key]
    ))
    actual_oof_days = tuple(sorted(frame["day"].astype(str).unique()))
    if actual_oof_days != expected_oof_days:
        raise ValueError(
            f"SELL first-fill OOF day identity drifted for {panel} {side}: "
            f"{actual_oof_days} != {expected_oof_days}"
        )
    if observed_oof_days < minimum_oof_days:
        raise ValueError(
            f"SELL first-fill OOF day support failed for {panel} {side}: "
            f"{observed_oof_days} < {minimum_oof_days}"
        )
    if len(high_risk) < minimum_rows or high_risk["day"].nunique() < minimum_days:
        raise ValueError(f"SELL first-fill high-risk support failed for {panel} {side}")
    point = _point_metrics(frame)
    intervals = _cluster_bootstrap(
        frame,
        samples=int(inference["bootstrap_samples"]),
        seed=int(inference["bootstrap_seed"]) + int(seed_offset),
        confidence=float(inference["familywise_confidence"]),
        simultaneous_family_size=int(
            inference["simultaneous_metric_family_size"]
        ),
    )
    daily_negative_fraction = float(
        point["high_risk_daily_negative_fraction"]
    )
    threshold = float(inference["daily_negative_direction_threshold"])
    minimum_gap = float(inference["minimum_selective_value_gap_usdc"])
    candidate_rate = float(frame["high_risk"].mean())
    candidate_rate_min = float(inference["candidate_rate_min"])
    candidate_rate_max = float(inference["candidate_rate_max"])
    prediction_supported = bool(
        intervals["local_mse_improvement_usdc2"]["lcb"] > 0.0
        and intervals["high_risk_mean_value_usdc"]["ucb"] < 0.0
        and intervals["high_risk_value_gap_usdc"]["ucb"] < -minimum_gap
        and intervals["high_risk_daily_negative_fraction"]["lcb"]
        >= threshold
        and candidate_rate_min <= candidate_rate <= candidate_rate_max
        and (
            panel != PRIMARY_PANEL
            or intervals["unconditional_mean_value_usdc"]["ucb"] < 0.0
        )
    )
    return {
        "panel": panel,
        "side": side,
        "rows": int(len(frame)),
        "days": observed_oof_days,
        "scored_oof_days": list(actual_oof_days),
        "minimum_oof_days": minimum_oof_days,
        "outer_folds": int(frame["outer_fold"].nunique()),
        "point_metrics": point,
        "simultaneous_intervals": intervals,
        "high_risk_rows": int(len(high_risk)),
        "high_risk_days": int(high_risk["day"].nunique()),
        "high_risk_daily_negative_fraction": daily_negative_fraction,
        "high_risk_daily_negative_fraction_lcb": intervals[
            "high_risk_daily_negative_fraction"
        ]["lcb"],
        "daily_negative_threshold": threshold,
        "minimum_selective_value_gap_usdc": minimum_gap,
        "high_risk_candidate_rate": candidate_rate,
        "candidate_rate_bounds": [candidate_rate_min, candidate_rate_max],
        "prediction_supported": prediction_supported,
    }


def evaluate_prepared_trace(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
) -> ConditionalValueEvaluation:
    """Fit Grade-A OOF and rolling past-Grade-A transport evidence."""

    summaries: dict[str, dict[str, Any]] = {
        PRIMARY_PANEL: {},
        SENSITIVITY_PANEL: {},
    }
    predictions: list[pd.DataFrame] = []
    seed_offset = 0
    for panel in (PRIMARY_PANEL, SENSITIVITY_PANEL):
        for side in SIDES:
            oof = (
                _fit_grade_a_side(frame, spec, side=side)
                if panel == PRIMARY_PANEL
                else _fit_grade_b_transport_side(frame, spec, side=side)
            )
            predictions.append(oof)
            summaries[panel][side] = _summarize(
                oof,
                spec,
                panel=panel,
                side=side,
                seed_offset=seed_offset,
            )
            seed_offset += 10_003
    grade_a_sell = summaries[PRIMARY_PANEL]["SELL"]
    grade_b_sell = summaries[SENSITIVITY_PANEL]["SELL"]
    direction_not_reversed = bool(
        grade_a_sell["point_metrics"]["high_risk_mean_value_usdc"] < 0.0
        and grade_b_sell["point_metrics"]["high_risk_mean_value_usdc"] < 0.0
    )
    prediction_supported = bool(
        grade_a_sell["prediction_supported"]
        and grade_b_sell["prediction_supported"]
        and direction_not_reversed
    )
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": spec["identity"],
        "status": (
            "development_prediction_supported"
            if prediction_supported
            else "close_local_prediction_branch_on_development"
        ),
        "target": {
            "column": lifecycle_contract.PRIMARY_ESTIMAND,
            "unit": "USDC_per_first_opener_fill_decision",
            "fill_conditioned_observational": True,
            "operational_quote_value": False,
        },
        "panels": summaries,
        "grade_b_transport_mode": "rolling_past_grade_a_refit_transport",
        "grade_b_sell_direction_not_reversed": direction_not_reversed,
        "buy_role": "descriptive_comparator_not_a_falsification_gate",
        "uncertainty_scope": (
            "day_then_campaign_clustered_OOF_performance_conditional_on_"
            "the_frozen_fold_fits_and_train_derived_risk_thresholds"
        ),
        "prediction_supported": prediction_supported,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    return ConditionalValueEvaluation(
        oof_predictions=pd.concat(predictions, ignore_index=True),
        report=report,
    )


def evaluate_native_trace(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
) -> ConditionalValueEvaluation:
    return evaluate_prepared_trace(prepare_native_trace(frame, spec), spec)
