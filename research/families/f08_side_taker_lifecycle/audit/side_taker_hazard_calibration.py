#!/usr/bin/env python3
"""Audit pooled and maker-side cause-specific hazards chronologically.

This is a closed diagnostic for ``side_taker_hazard_m0_v1``.  It does not
choose keep/cancel/widen/rearm actions.  The pooled model has a penalized
maker-side coefficient and shared feature slopes.  The split model fits
independent BUY and SELL normalizers, intercepts, and slopes.  That comparison
is not a clean nested test of side-by-feature slope heterogeneity.

The frozen labels also mix market-path outcomes, baseline-policy cancellation,
and post-fill campaign transitions.  Historical individual trades are
exchange-time observations.  Consequently this report is diagnostic evidence
only: no predictive result emitted here can authorize an action experiment or
a live policy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from research.families.f07_active_order_continuation.audit.queue_value_competing_risk import (
    ALL_HAZARDS,
    CAUSES,
    QUEUE_RECOVERY_CAUSE,
    CauseHazardModel,
    FeatureNormalizer,
    _fit_hazard,
    fit_feature_normalizer,
)
from research.families.f07_active_order_continuation.audit.queue_value_competing_risk import (
    DEFAULT_FEATURES as LOCAL_FEATURES,
)
from research.governance.historical_reproduction import (
    add_historical_reproduction_argument,
    require_historical_reproduction,
)

SCHEMA_VERSION = "side_taker_hazard_calibration.audit.v2"
FAMILY_ID = "side_taker_hazard_m0_v1"
POOLED_SIDE_FEATURE = "maker_side_buy"
PRIMARY_CAUSES = ("favorable_fill", "adverse_fill")

TAKER_FEATURES = (
    "net_counterparty_pressure_100ms",
    "counterparty_taker_quote_100ms",
    "away_taker_quote_100ms",
    "counterparty_taker_max_run_100ms",
    "counterparty_taker_sweep_bps_100ms",
    "counterparty_taker_burst_ratio_100ms",
    "maker_adverse_trade_move_bps_100ms",
    "net_counterparty_pressure_500ms",
    "counterparty_taker_quote_500ms",
    "away_taker_quote_500ms",
    "counterparty_taker_max_run_500ms",
    "counterparty_taker_sweep_bps_500ms",
    "counterparty_taker_burst_ratio_500ms",
    "maker_adverse_trade_move_bps_500ms",
    "counterparty_taker_current_run",
)
DEFAULT_FEATURES = (*LOCAL_FEATURES, *TAKER_FEATURES)


@dataclass(frozen=True)
class ChronologicalFold:
    fold: int
    train_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    test_days: tuple[str, ...]


@dataclass(frozen=True)
class HazardSet:
    normalizer: FeatureNormalizer
    hazards: Mapping[str, CauseHazardModel]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_chronological_folds(
    days: Sequence[str],
    *,
    min_train_days: int = 8,
    embargo_days: int = 1,
    test_days: int = 2,
) -> tuple[ChronologicalFold, ...]:
    ordered = tuple(sorted({str(day) for day in days}))
    if min_train_days < 2 or embargo_days < 1 or test_days < 1:
        raise ValueError("chronological fold sizes are invalid")
    first_test = int(min_train_days) + int(embargo_days)
    if len(ordered) < first_test + int(test_days):
        raise ValueError("not enough days for chronological hazard calibration")
    folds: list[ChronologicalFold] = []
    for test_start in range(first_test, len(ordered), int(test_days)):
        test = ordered[test_start : test_start + int(test_days)]
        if not test:
            continue
        train_end = test_start - int(embargo_days)
        train = ordered[:train_end]
        embargo = ordered[train_end:test_start]
        if not (train and embargo and max(train) < min(embargo) < min(test)):
            raise ValueError("chronological fold ordering is invalid")
        folds.append(
            ChronologicalFold(
                fold=len(folds),
                train_days=train,
                embargo_days=embargo,
                test_days=test,
            )
        )
    return tuple(folds)


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "day",
        "side",
        "decision_id",
        "decision_ts_ns",
        "taker_feature_ready_ts_ns",
        "taker_feature_available",
        "taker_policy_eligible",
        "event_time_ms",
        "first_event",
        "exchange_book_refill_count",
        *DEFAULT_FEATURES,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"side-taker hazard panel missing columns: {missing}")
    output = frame.copy()
    output["day"] = output["day"].astype(str)
    output["side"] = output["side"].astype(str).str.upper()
    if set(output["side"]) - {"BUY", "SELL"}:
        raise ValueError("maker side must be BUY or SELL")
    ready = pd.to_numeric(output["taker_feature_ready_ts_ns"], errors="coerce")
    decision = pd.to_numeric(output["decision_ts_ns"], errors="coerce")
    if ready.isna().any() or ready.gt(decision).any():
        raise ValueError("taker features are missing or contain future observations")
    if not pd.to_numeric(
        output["taker_feature_available"], errors="coerce"
    ).fillna(0.0).eq(1.0).all():
        raise ValueError("all hazard rows must have a completed taker state")
    allowed_events = {*CAUSES, "censored"}
    unknown_events = sorted(set(output["first_event"].astype(str)) - allowed_events)
    if unknown_events:
        raise ValueError(f"unknown competing-risk events: {unknown_events}")
    exposure = pd.to_numeric(output["event_time_ms"], errors="coerce")
    if exposure.isna().any() or exposure.lt(0.0).any():
        raise ValueError("event_time_ms must be finite and non-negative")
    output[POOLED_SIDE_FEATURE] = output["side"].eq("BUY").astype(float)
    return output


def _optional_numeric(
    frame: pd.DataFrame,
    column: str,
    *,
    default: float = 0.0,
) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


def audit_label_lineage(frame: pd.DataFrame) -> dict[str, Any]:
    """Describe the actual event sources without upgrading their semantics."""

    events = frame["first_event"].astype(str)
    jump = events.eq("adverse_price_jump")
    decision_ts = _optional_numeric(frame, "decision_ts_ns")
    first_event_ts = _optional_numeric(frame, "first_event_ts_ns")
    if "first_event_ts_ns" not in frame.columns:
        first_event_ts = decision_ts + (
            _optional_numeric(frame, "event_time_ms") * 1_000_000.0
        )
    legacy_jump_ts = _optional_numeric(frame, "adverse_price_jump_ts_ns")
    native_jump_ts = _optional_numeric(frame, "future_mid_first_hit_ts_ns")
    legacy_matches = jump & legacy_jump_ts.gt(0.0) & first_event_ts.eq(legacy_jump_ts)
    native_available = jump & native_jump_ts.gt(0.0)
    native_matches = native_available & first_event_ts.eq(native_jump_ts)
    zero_exposure = jump & _optional_numeric(frame, "event_time_ms").le(0.0)
    ambiguous = (
        frame.get(
            "label_censor_reason",
            pd.Series("", index=frame.index, dtype=object),
        )
        .astype(str)
        .eq("same_ms_competing_event_ambiguous")
    )
    declared_identities = (
        sorted(frame["label_identity"].dropna().astype(str).unique())
        if "label_identity" in frame.columns
        else []
    )
    return {
        "declared_label_identities": declared_identities,
        "inferred_label_identity": (
            "exact_order_id_mixed_market_policy_campaign_first_event.v2"
        ),
        "rows": int(len(frame)),
        "jump_first_events": int(jump.sum()),
        "jump_first_event_timestamp_source": "adverse_price_jump_ts_ns",
        "legacy_jump_timestamp_matches": int(legacy_matches.sum()),
        "native_first_hit_available_rows": int(native_jump_ts.gt(0.0).sum()),
        "native_first_hit_available_on_jump_rows": int(native_available.sum()),
        "native_first_hit_timestamp_matches": int(native_matches.sum()),
        "native_first_hit_timestamp_mismatches": int((jump & ~native_matches).sum()),
        "zero_ms_jump_first_events": int(zero_exposure.sum()),
        "zero_ms_exposure_clamped_to_ms_during_fit": 1,
        "same_ms_ambiguous_censors": int(ambiguous.sum()),
        "native_first_hit_used_in_competing_risk": False,
        "cause_roles": {
            "favorable_fill": "market_path_outcome_conditional_on_baseline_order",
            "adverse_fill": "market_path_outcome_conditional_on_baseline_order",
            "adverse_price_jump": "market_path_state_transition",
            "cancel": "baseline_policy_action_or_censor",
            "campaign_repair": "post_fill_campaign_transition",
            "queue_recovery": "recurrent_state_count_not_terminal_competing_cause",
        },
        "action_independent_competing_risk_estimand": False,
        "transportable_to_keep_cancel_policy": False,
    }


def _counts(frame: pd.DataFrame, cause: str) -> np.ndarray:
    if cause == QUEUE_RECOVERY_CAUSE:
        return (
            pd.to_numeric(frame["exchange_book_refill_count"], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
            .to_numpy(dtype=float)
        )
    return frame["first_event"].astype(str).eq(cause).astype(float).to_numpy()


def _fit_hazard_set(
    frame: pd.DataFrame,
    *,
    feature_names: Sequence[str],
    alpha: float,
) -> HazardSet:
    normalizer = fit_feature_normalizer(frame, feature_names=feature_names)
    x = normalizer.transform_frame(frame)
    exposure_s = np.maximum(
        pd.to_numeric(frame["event_time_ms"], errors="coerce").to_numpy(dtype=float)
        / 1_000.0,
        0.001,
    )
    hazards = {
        cause: _fit_hazard(
            cause=cause,
            x=x,
            exposure_s=exposure_s,
            counts=_counts(frame, cause),
            alpha=float(alpha),
        )
        for cause in ALL_HAZARDS
    }
    return HazardSet(normalizer=normalizer, hazards=hazards)


def _predict_hazard_set(
    model: HazardSet,
    frame: pd.DataFrame,
) -> dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]]:
    x = model.normalizer.transform_frame(frame)
    exposure_s = np.maximum(
        pd.to_numeric(frame["event_time_ms"], errors="coerce").to_numpy(dtype=float)
        / 1_000.0,
        0.001,
    )
    result: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
    for cause in ALL_HAZARDS:
        rate = model.hazards[cause].predict_rates(x)
        expected = rate * exposure_s
        probability = -np.expm1(-np.clip(expected, 0.0, 50.0))
        result[cause] = (rate, expected, probability)
    return result


def _fixed_horizon_probabilities(
    prediction: Mapping[str, tuple[np.ndarray, np.ndarray, np.ndarray]],
    *,
    horizon_s: float,
) -> dict[str, np.ndarray]:
    if not math.isfinite(float(horizon_s)) or float(horizon_s) <= 0.0:
        raise ValueError("evaluation horizon must be finite and positive")
    total_rate = np.zeros_like(prediction[CAUSES[0]][0], dtype=float)
    for cause in CAUSES:
        total_rate += prediction[cause][0]
    any_event = -np.expm1(-np.clip(total_rate * float(horizon_s), 0.0, 50.0))
    probabilities: dict[str, np.ndarray] = {}
    for cause in CAUSES:
        probabilities[cause] = np.divide(
            prediction[cause][0],
            total_rate,
            out=np.zeros_like(total_rate),
            where=total_rate > 0.0,
        ) * any_event
    queue_rate = prediction[QUEUE_RECOVERY_CAUSE][0]
    probabilities[QUEUE_RECOVERY_CAUSE] = -np.expm1(
        -np.clip(queue_rate * float(horizon_s), 0.0, 50.0)
    )
    return probabilities


def _clip_probability(values: np.ndarray) -> np.ndarray:
    return np.clip(np.asarray(values, dtype=float), 1e-9, 1.0 - 1e-9)


def _balanced_log_loss(y: np.ndarray, probability: np.ndarray) -> float:
    target = np.asarray(y, dtype=float) > 0.0
    probability = _clip_probability(probability)
    if not target.any() or target.all():
        return math.nan
    positive = float(-np.log(probability[target]).mean())
    negative = float(-np.log1p(-probability[~target]).mean())
    return 0.5 * (positive + negative)


def _calibration_line(
    y: np.ndarray,
    probability: np.ndarray,
) -> tuple[float, float]:
    target = (np.asarray(y, dtype=float) > 0.0).astype(int)
    if int(target.sum()) < 10 or int((1 - target).sum()) < 10:
        return math.nan, math.nan
    probability = _clip_probability(probability)
    logit = np.log(probability / (1.0 - probability)).reshape(-1, 1)
    model = LogisticRegression(C=1_000_000.0, solver="lbfgs", max_iter=1_000)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            module=r"sklearn\..*",
        )
        model.fit(logit, target)
    return float(model.intercept_[0]), float(model.coef_[0, 0])


def _day_bootstrap_interval(
    values: np.ndarray,
    *,
    seed: int,
    draws: int = 5_000,
) -> tuple[float, float]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size < 2:
        return math.nan, math.nan
    rng = np.random.default_rng(int(seed))
    samples = rng.choice(finite, size=(int(draws), finite.size), replace=True)
    means = samples.mean(axis=1)
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def _binary_metrics(y: np.ndarray, probability: np.ndarray) -> dict[str, Any]:
    target = (np.asarray(y, dtype=float) > 0.0).astype(float)
    probability = _clip_probability(probability)
    log_loss = float(
        np.mean(-(target * np.log(probability) + (1.0 - target) * np.log1p(-probability)))
    )
    intercept, slope = _calibration_line(target, probability)
    auc = (
        float(roc_auc_score(target, probability))
        if np.unique(target).size == 2
        else math.nan
    )
    return {
        "rows": int(len(target)),
        "events": int(target.sum()),
        "predicted_events": float(probability.sum()),
        "observed_to_expected": float(target.sum() / max(probability.sum(), 1e-12)),
        "brier": float(np.mean((probability - target) ** 2)),
        "log_loss": log_loss,
        "balanced_log_loss": _balanced_log_loss(target, probability),
        "roc_auc": auc,
        "calibration_intercept": intercept,
        "calibration_slope": slope,
    }


def _daily_balanced_delta(
    frame: pd.DataFrame,
    *,
    cause: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day, daily in frame.groupby("day", sort=True):
        y = pd.to_numeric(daily[f"observed_{cause}"], errors="coerce").to_numpy()
        pooled = _balanced_log_loss(y, daily[f"pooled_probability_{cause}"].to_numpy())
        split = _balanced_log_loss(y, daily[f"split_probability_{cause}"].to_numpy())
        if math.isfinite(pooled) and math.isfinite(split):
            rows.append(
                {
                    "day": str(day),
                    "pooled": pooled,
                    "split": split,
                    "delta": split - pooled,
                }
            )
    return pd.DataFrame(rows)


def _complete_primary_daily_effect(
    primary_daily: Mapping[str, pd.Series],
) -> pd.Series:
    frame = pd.DataFrame(primary_daily).reindex(columns=list(PRIMARY_CAUSES))
    return frame.dropna().mean(axis=1)


def _summarize_predictions(
    predictions: pd.DataFrame,
    *,
    folds: Sequence[ChronologicalFold],
    minimum_primary_events: int,
    evaluation_horizon_ms: int,
    minimum_primary_auc: float,
    calibration_slope_min: float,
    calibration_slope_max: float,
    panel_policy_eligible: bool,
    label_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "comparison": {
            "pooled": "shared slopes plus penalized maker-side coefficient",
            "split": "independent BUY and SELL normalizers, intercepts, and slopes",
            "clean_nested_side_slope_test": False,
            "limitations": [
                "pooled maker-side coefficient is regularized",
                "pooled and split models use different fitted normalizers",
                "split models have separate intercepts and regularization geometry",
                "loss deltas cannot isolate side-by-feature slope heterogeneity",
                "AUC values are point estimates without refit uncertainty",
                "rare-event PR-AUC and within-day candidate-budget precision are absent",
            ],
        },
        "folds": [asdict(fold) for fold in folds],
        "oof_rows": int(len(predictions)),
        "oof_days": sorted(predictions["day"].astype(str).unique()),
        "minimum_primary_events": int(minimum_primary_events),
        "evaluation_horizon_ms": int(evaluation_horizon_ms),
        "minimum_primary_auc": float(minimum_primary_auc),
        "calibration_slope_range": [
            float(calibration_slope_min),
            float(calibration_slope_max),
        ],
        "panel_policy_eligible": bool(panel_policy_eligible),
        "label_lineage": dict(label_lineage),
        "estimand_valid_for_side_slope_heterogeneity": False,
        "estimand_valid_for_action_uplift": False,
        "evidence_role": (
            "development_internal_chronological_diagnostic_not_confirmatory"
        ),
        "sides": {},
    }
    any_raw_predictive_pass = False
    for side in ("BUY", "SELL"):
        side_frame = predictions[predictions["side"].eq(side)].copy()
        side_report: dict[str, Any] = {"rows": int(len(side_frame)), "causes": {}}
        primary_daily: dict[str, pd.Series] = {}
        primary_support = True
        primary_model_valid = True
        for cause in ALL_HAZARDS:
            observed = pd.to_numeric(
                side_frame[f"observed_{cause}"], errors="coerce"
            ).fillna(0.0).to_numpy(dtype=float)
            cause_report: dict[str, Any] = {
                "evaluation": (
                    "variable_exposure_occurrence"
                    if cause == QUEUE_RECOVERY_CAUSE
                    else f"first_event_within_{int(evaluation_horizon_ms)}ms"
                ),
                "pooled": _binary_metrics(
                    observed,
                    side_frame[f"pooled_probability_{cause}"].to_numpy(dtype=float),
                ),
                "split": _binary_metrics(
                    observed,
                    side_frame[f"split_probability_{cause}"].to_numpy(dtype=float),
                ),
            }
            daily = _daily_balanced_delta(side_frame, cause=cause)
            seed_material = f"{FAMILY_ID}:{side}:{cause}".encode()
            seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:8], "big")
            lower, upper = _day_bootstrap_interval(
                daily["delta"].to_numpy(dtype=float) if not daily.empty else np.asarray([]),
                seed=seed,
            )
            point = float(daily["delta"].mean()) if not daily.empty else math.nan
            cause_report["split_minus_pooled"] = {
                "balanced_log_loss": (
                    cause_report["split"]["balanced_log_loss"]
                    - cause_report["pooled"]["balanced_log_loss"]
                ),
                "daily_balanced_log_loss": point,
                "daily_ci95": [lower, upper],
                "daily_split_better_rate": (
                    float(daily["delta"].lt(0.0).mean()) if not daily.empty else math.nan
                ),
                "daily_effect_days": int(len(daily)),
            }
            side_report["causes"][cause] = cause_report
            if cause in PRIMARY_CAUSES:
                primary_support = primary_support and int((observed > 0.0).sum()) >= int(
                    minimum_primary_events
                )
                split_metrics = cause_report["split"]
                split_auc = float(split_metrics["roc_auc"])
                split_slope = float(split_metrics["calibration_slope"])
                split_oe = float(split_metrics["observed_to_expected"])
                primary_model_valid = bool(
                    primary_model_valid
                    and math.isfinite(split_auc)
                    and split_auc >= float(minimum_primary_auc)
                    and math.isfinite(split_slope)
                    and float(calibration_slope_min)
                    <= split_slope
                    <= float(calibration_slope_max)
                    and math.isfinite(split_oe)
                    and 0.50 <= split_oe <= 2.0
                )
                if not daily.empty:
                    primary_daily[cause] = daily.set_index("day")["delta"]

        primary_effect = _complete_primary_daily_effect(primary_daily)
        seed = int.from_bytes(
            hashlib.sha256(f"{FAMILY_ID}:{side}:primary".encode()).digest()[:8],
            "big",
        )
        lower, upper = _day_bootstrap_interval(
            primary_effect.to_numpy(dtype=float),
            seed=seed,
        )
        point = float(primary_effect.mean()) if len(primary_effect) else math.nan
        raw_predictive_criteria_passed = bool(
            primary_support
            and primary_model_valid
            and len(primary_effect) >= 4
            and math.isfinite(point)
            and math.isfinite(upper)
            and point < 0.0
            and upper < 0.0
        )
        side_report["primary_composite"] = {
            "causes": list(PRIMARY_CAUSES),
            "support_passed": bool(primary_support),
            "model_validity_passed": bool(primary_model_valid),
            "daily_effect_days": int(len(primary_effect)),
            "split_minus_pooled_balanced_log_loss": point,
            "daily_ci95": [lower, upper],
            "daily_split_better_rate": (
                float(primary_effect.lt(0.0).mean())
                if len(primary_effect)
                else math.nan
            ),
            "raw_predictive_criteria_passed": raw_predictive_criteria_passed,
            "predictive_split_gate_passed": False,
            "estimand_valid": False,
            "followup_randomized_experiment_registration_eligible": False,
            "action_family_allowed": False,
            "decision": "closed_invalid_estimand_diagnostic_only",
        }
        any_raw_predictive_pass = (
            any_raw_predictive_pass or raw_predictive_criteria_passed
        )
        summary["sides"][side] = side_report
    summary["raw_predictive_criteria_any_side"] = bool(any_raw_predictive_pass)
    summary["predictive_split_gate_any_side"] = False
    summary["followup_randomized_experiment_registration_eligible"] = False
    summary["new_action_family_created"] = False
    summary["interpretation"] = (
        "Closed diagnostic only. Split-minus-pooled loss mixes side base-rate, "
        "normalization, intercept, and slope effects; labels mix market, policy, "
        "and campaign processes. It establishes neither side-slope heterogeneity "
        "nor action uplift."
    )
    return summary


def run_chronological_calibration(
    frame: pd.DataFrame,
    *,
    min_train_days: int = 8,
    embargo_days: int = 1,
    test_days: int = 2,
    alpha: float = 0.25,
    minimum_primary_events: int = 20,
    evaluation_horizon_ms: int = 1_000,
    minimum_primary_auc: float = 0.55,
    calibration_slope_min: float = 0.25,
    calibration_slope_max: float = 2.0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    prepared = _prepare_frame(frame)
    folds = make_chronological_folds(
        prepared["day"].unique(),
        min_train_days=min_train_days,
        embargo_days=embargo_days,
        test_days=test_days,
    )
    pooled_features = (*DEFAULT_FEATURES, POOLED_SIDE_FEATURE)
    evaluation_horizon_s = float(evaluation_horizon_ms) / 1_000.0
    prediction_parts: list[pd.DataFrame] = []
    for fold in folds:
        train = prepared[prepared["day"].isin(fold.train_days)].copy()
        test = prepared[prepared["day"].isin(fold.test_days)].copy()
        pooled_model = _fit_hazard_set(
            train,
            feature_names=pooled_features,
            alpha=alpha,
        )
        pooled_prediction = _predict_hazard_set(pooled_model, test)
        pooled_fixed_probability = _fixed_horizon_probabilities(
            pooled_prediction,
            horizon_s=evaluation_horizon_s,
        )
        fold_output = test[
            ["day", "side", "decision_id", "first_event", "event_time_ms"]
        ].copy()
        fold_output["fold"] = int(fold.fold)
        for cause in ALL_HAZARDS:
            observed = _counts(test, cause)
            if cause != QUEUE_RECOVERY_CAUSE:
                observed = observed * (
                    pd.to_numeric(test["event_time_ms"], errors="coerce")
                    .le(float(evaluation_horizon_ms))
                    .to_numpy(dtype=float)
                )
            fold_output[f"observed_{cause}"] = observed
            rate, expected, variable_probability = pooled_prediction[cause]
            fold_output[f"pooled_rate_{cause}"] = rate
            fold_output[f"pooled_expected_{cause}"] = expected
            fold_output[f"pooled_probability_{cause}"] = (
                variable_probability
                if cause == QUEUE_RECOVERY_CAUSE
                else pooled_fixed_probability[cause]
            )

        for side in ("BUY", "SELL"):
            train_side = train[train["side"].eq(side)].copy()
            test_index = test.index[test["side"].eq(side)]
            if train_side.empty or len(test_index) == 0:
                raise ValueError(f"{side} fold {fold.fold} has no fit/test rows")
            split_model = _fit_hazard_set(
                train_side,
                feature_names=DEFAULT_FEATURES,
                alpha=alpha,
            )
            split_prediction = _predict_hazard_set(split_model, test.loc[test_index])
            split_fixed_probability = _fixed_horizon_probabilities(
                split_prediction,
                horizon_s=evaluation_horizon_s,
            )
            output_mask = fold_output["side"].eq(side).to_numpy()
            for cause in ALL_HAZARDS:
                rate, expected, probability = split_prediction[cause]
                fold_output.loc[output_mask, f"split_rate_{cause}"] = rate
                fold_output.loc[output_mask, f"split_expected_{cause}"] = expected
                fold_output.loc[output_mask, f"split_probability_{cause}"] = (
                    probability
                    if cause == QUEUE_RECOVERY_CAUSE
                    else split_fixed_probability[cause]
                )
        prediction_parts.append(fold_output)

    predictions = pd.concat(prediction_parts, ignore_index=True)
    split_columns = [
        column for column in predictions.columns if column.startswith("split_")
    ]
    if predictions[split_columns].isna().any().any():
        raise ValueError("side-specific OOF predictions contain missing values")
    policy_values = pd.to_numeric(
        prepared["taker_policy_eligible"], errors="coerce"
    ).fillna(0.0)
    panel_policy_eligible = bool(policy_values.eq(1.0).all())
    summary = _summarize_predictions(
        predictions,
        folds=folds,
        minimum_primary_events=minimum_primary_events,
        evaluation_horizon_ms=evaluation_horizon_ms,
        minimum_primary_auc=minimum_primary_auc,
        calibration_slope_min=calibration_slope_min,
        calibration_slope_max=calibration_slope_max,
        panel_policy_eligible=panel_policy_eligible,
        label_lineage=audit_label_lineage(prepared),
    )
    summary["fit"] = {
        "alpha": float(alpha),
        "features": list(DEFAULT_FEATURES),
        "pooled_side_feature": POOLED_SIDE_FEATURE,
        "min_train_days": int(min_train_days),
        "embargo_days": int(embargo_days),
        "test_days": int(test_days),
        "evaluation_horizon_ms": int(evaluation_horizon_ms),
    }
    return predictions, summary


def build_dataset_manifest(
    frame: pd.DataFrame,
    *,
    input_path: Path,
) -> pd.DataFrame:
    """Build the day-level CSV identity consumed by experiment_manifest."""

    panel_sha256 = _sha256_file(input_path)
    rows: list[dict[str, Any]] = []
    for day, daily in frame.groupby(frame["day"].astype(str), sort=True):
        events = daily["first_event"].astype(str)
        sides = daily["side"].astype(str).str.upper()
        decision_ts = pd.to_numeric(daily["decision_ts_ns"], errors="coerce")
        row: dict[str, Any] = {
            "day": str(day),
            "rows": int(len(daily)),
            "buy_rows": int(sides.eq("BUY").sum()),
            "sell_rows": int(sides.eq("SELL").sum()),
            "first_decision_ts_ns": int(decision_ts.min()),
            "last_decision_ts_ns": int(decision_ts.max()),
            "source_panel_sha256": panel_sha256,
        }
        for event in (*CAUSES, "censored"):
            row[f"event_{event}_rows"] = int(events.eq(event).sum())
        rows.append(row)
    return pd.DataFrame(rows)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_historical_reproduction_argument(parser)
    parser.add_argument("--input-panel", type=Path, required=True)
    parser.add_argument("--output-predictions", type=Path, required=True)
    parser.add_argument("--output-summary", type=Path, required=True)
    parser.add_argument("--output-dataset-manifest", type=Path, required=True)
    parser.add_argument("--min-train-days", type=int, default=8)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--test-days", type=int, default=2)
    parser.add_argument("--alpha", type=float, default=0.25)
    parser.add_argument("--minimum-primary-events", type=int, default=20)
    parser.add_argument("--evaluation-horizon-ms", type=int, default=1_000)
    parser.add_argument("--minimum-primary-auc", type=float, default=0.55)
    parser.add_argument("--calibration-slope-min", type=float, default=0.25)
    parser.add_argument("--calibration-slope-max", type=float, default=2.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    require_historical_reproduction(
        runner_id="f08.side_taker_hazard_calibration",
        enabled=bool(args.historical_reproduction),
        spec_path=None,
    )
    input_path = args.input_panel.expanduser().resolve()
    output_path = args.output_predictions.expanduser().resolve()
    summary_path = args.output_summary.expanduser().resolve()
    dataset_manifest_path = args.output_dataset_manifest.expanduser().resolve()
    frame = pd.read_parquet(input_path)
    predictions, summary = run_chronological_calibration(
        frame,
        min_train_days=args.min_train_days,
        embargo_days=args.embargo_days,
        test_days=args.test_days,
        alpha=args.alpha,
        minimum_primary_events=args.minimum_primary_events,
        evaluation_horizon_ms=args.evaluation_horizon_ms,
        minimum_primary_auc=args.minimum_primary_auc,
        calibration_slope_min=args.calibration_slope_min,
        calibration_slope_max=args.calibration_slope_max,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_parquet(output_path, index=False)
    dataset_manifest = build_dataset_manifest(frame, input_path=input_path)
    dataset_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    dataset_manifest.to_csv(dataset_manifest_path, index=False)
    builder_path = Path(__file__).resolve()
    summary["input_panel"] = {
        "path": str(input_path),
        "sha256": _sha256_file(input_path),
        "rows": int(len(frame)),
        "days": sorted(frame["day"].astype(str).unique()),
    }
    summary["builder"] = {
        "path": str(builder_path),
        "sha256": _sha256_file(builder_path),
    }
    summary["output_predictions"] = {
        "path": str(output_path),
        "sha256": _sha256_file(output_path),
        "rows": int(len(predictions)),
    }
    summary["dataset_manifest"] = {
        "path": str(dataset_manifest_path),
        "sha256": _sha256_file(dataset_manifest_path),
        "rows": int(len(dataset_manifest)),
        "days": dataset_manifest["day"].astype(str).tolist(),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "predictions": str(output_path),
                "summary": str(summary_path),
                "dataset_manifest": str(dataset_manifest_path),
                "predictive_split_gate_any_side": summary[
                    "predictive_split_gate_any_side"
                ],
                "new_action_family_created": False,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
