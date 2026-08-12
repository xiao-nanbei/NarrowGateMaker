#!/usr/bin/env python3
"""Side-specific cause hazards and model-based keep/cancel-reenter value.

This module defines the state model for
``queue_value_net_hazard_keep_cancel_v2``.  It is deliberately separate from
the consumed v1 joint-tail state.  Five mutually exclusive first-event causes
are fitted as exposure-weighted Poisson hazards, while exact-level refill is a
sixth transition hazard used to value re-entry:

* favorable fill;
* adverse fill;
* baseline cancel;
* adverse one-tick price jump;
* inventory-campaign repair;
* queue recovery/refill.

The model-based value difference chooses only the randomized action
denominator.  It is not action-uplift evidence; K0/K1 value still has to be
identified by a new randomized replay and DR evaluation.
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
from sklearn.linear_model import PoissonRegressor

from research.families.f07_active_order_continuation.audit.queue_value_models import (
    QueueReactiveHawkesArtifact,
)
from research.governance.historical_reproduction import (
    add_historical_reproduction_argument,
    require_historical_reproduction,
)

SCHEMA_VERSION = "queue_value_competing_risk_bundle.v1"
SIDE_SCHEMA_VERSION = "queue_value_competing_risk_side.v1"
NORMALIZER_SCHEMA_VERSION = "queue_value_feature_normalizer.v1"
HAZARD_SCHEMA_VERSION = "cause_specific_poisson_hazard.v1"
STATE_SCHEMA_VERSION = "queue_value_net_state.v1"

CAUSES = (
    "favorable_fill",
    "adverse_fill",
    "cancel",
    "adverse_price_jump",
    "campaign_repair",
)
QUEUE_RECOVERY_CAUSE = "queue_recovery"
ALL_HAZARDS = (*CAUSES, QUEUE_RECOVERY_CAUSE)

DEFAULT_FEATURES = (
    "spread_ticks",
    "book_imbalance",
    "queue_fraction_left",
    "quote_distance_ticks",
    "order_age_ms",
    "campaign_age_s",
    "inventory_ratio",
    "campaign_pnl_so_far",
    "campaign_mae_so_far",
    "campaign_add_count_so_far",
    "microprice_shift_bps",
    "l2_book_cancel_ratio",
    "l2_book_refresh_ratio",
    "l2_quote_flip_rate",
    "toxicity",
    "markout_ema",
)

FORBIDDEN_FEATURE_TOKENS = (
    "future_",
    "terminal_",
    "reward",
    "fill_value",
    "first_event",
    "event_",
    "exchange_book_",
    "simulator_",
)


def _canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_feature_names(features: Sequence[str]) -> None:
    invalid = sorted(
        name
        for name in features
        if any(token in str(name) for token in FORBIDDEN_FEATURE_TOKENS)
    )
    if invalid:
        raise ValueError(f"competing-risk features are not decision-time safe: {invalid}")


@dataclass(frozen=True)
class FeatureNormalizer:
    schema_version: str
    feature_names: tuple[str, ...]
    medians: tuple[float, ...]
    lower_bounds: tuple[float, ...]
    upper_bounds: tuple[float, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]

    def transform_mapping(self, features: Mapping[str, Any]) -> np.ndarray:
        raw = np.asarray(
            [float(features.get(name, math.nan)) for name in self.feature_names],
            dtype=float,
        )
        medians = np.asarray(self.medians, dtype=float)
        raw = np.where(np.isfinite(raw), raw, medians)
        raw = np.clip(
            raw,
            np.asarray(self.lower_bounds, dtype=float),
            np.asarray(self.upper_bounds, dtype=float),
        )
        return (raw - np.asarray(self.means, dtype=float)) / np.asarray(
            self.scales,
            dtype=float,
        )

    def transform_frame(self, frame: pd.DataFrame) -> np.ndarray:
        missing = sorted(set(self.feature_names) - set(frame.columns))
        if missing:
            raise ValueError(f"competing-risk panel missing features: {missing}")
        values = (
            frame.loc[:, self.feature_names]
            .apply(pd.to_numeric, errors="coerce")
            .to_numpy(dtype=float)
        )
        medians = np.asarray(self.medians, dtype=float)
        values = np.where(np.isfinite(values), values, medians[None, :])
        values = np.clip(
            values,
            np.asarray(self.lower_bounds, dtype=float)[None, :],
            np.asarray(self.upper_bounds, dtype=float)[None, :],
        )
        return (values - np.asarray(self.means, dtype=float)[None, :]) / np.asarray(
            self.scales,
            dtype=float,
        )[None, :]

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> FeatureNormalizer:
        if payload.get("schema_version") != NORMALIZER_SCHEMA_VERSION:
            raise ValueError("unsupported competing-risk normalizer schema")
        return cls(
            schema_version=str(payload["schema_version"]),
            feature_names=tuple(str(value) for value in payload["feature_names"]),
            medians=tuple(float(value) for value in payload["medians"]),
            lower_bounds=tuple(float(value) for value in payload["lower_bounds"]),
            upper_bounds=tuple(float(value) for value in payload["upper_bounds"]),
            means=tuple(float(value) for value in payload["means"]),
            scales=tuple(float(value) for value in payload["scales"]),
        )


def fit_feature_normalizer(
    frame: pd.DataFrame,
    *,
    feature_names: Sequence[str] = DEFAULT_FEATURES,
) -> FeatureNormalizer:
    names = tuple(str(name) for name in feature_names)
    _validate_feature_names(names)
    missing = sorted(set(names) - set(frame.columns))
    if missing:
        raise ValueError(f"competing-risk fit panel missing features: {missing}")
    numeric = frame.loc[:, names].apply(pd.to_numeric, errors="coerce")
    medians = numeric.median(axis=0).fillna(0.0)
    filled = numeric.fillna(medians)
    lower = filled.quantile(0.005)
    upper = filled.quantile(0.995)
    clipped = filled.clip(lower, upper, axis=1)
    means = clipped.mean(axis=0)
    scales = clipped.std(axis=0).replace(0.0, 1.0).fillna(1.0)
    return FeatureNormalizer(
        schema_version=NORMALIZER_SCHEMA_VERSION,
        feature_names=names,
        medians=tuple(float(medians[name]) for name in names),
        lower_bounds=tuple(float(lower[name]) for name in names),
        upper_bounds=tuple(float(upper[name]) for name in names),
        means=tuple(float(means[name]) for name in names),
        scales=tuple(float(scales[name]) for name in names),
    )


@dataclass(frozen=True)
class CauseHazardModel:
    schema_version: str
    cause: str
    intercept: float
    coefficients: tuple[float, ...]
    alpha: float
    fit_rows: int
    observed_events: float
    exposure_s: float
    constant_rate_per_s: float
    fit_mode: str = "regularized_poisson"

    def predict_rate(self, normalized_features: np.ndarray) -> float:
        linear = float(self.intercept) + float(
            np.dot(
                np.asarray(self.coefficients, dtype=float),
                np.asarray(normalized_features, dtype=float),
            )
        )
        return math.exp(min(20.0, max(-30.0, linear)))

    def predict_rates(self, normalized_features: np.ndarray) -> np.ndarray:
        linear = float(self.intercept) + np.asarray(normalized_features).dot(
            np.asarray(self.coefficients, dtype=float)
        )
        return np.exp(np.clip(linear, -30.0, 20.0))

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> CauseHazardModel:
        if payload.get("schema_version") != HAZARD_SCHEMA_VERSION:
            raise ValueError("unsupported cause-specific hazard schema")
        return cls(
            schema_version=str(payload["schema_version"]),
            cause=str(payload["cause"]),
            intercept=float(payload["intercept"]),
            coefficients=tuple(float(value) for value in payload["coefficients"]),
            alpha=float(payload["alpha"]),
            fit_rows=int(payload["fit_rows"]),
            observed_events=float(payload["observed_events"]),
            exposure_s=float(payload["exposure_s"]),
            constant_rate_per_s=float(payload["constant_rate_per_s"]),
            fit_mode=str(payload.get("fit_mode", "regularized_poisson")),
        )


@dataclass(frozen=True)
class QueueValueAmplitudes:
    favorable_fill_bps: float
    adverse_fill_bps: float
    adverse_price_jump_bps: float
    fresh_order_option_bps: float
    cancel_ack_horizon_s: float
    recovery_horizon_s: float


@dataclass(frozen=True)
class QueueValueNetConfig:
    decision_horizon_s: float
    entry_advantage_bps: float
    exit_advantage_bps: float
    calibration_candidate_rate: float
    target_candidate_rate: float
    minimum_candidate_rate: float
    maximum_candidate_rate: float


@dataclass(frozen=True)
class CompetingRiskPrediction:
    intensities_per_s: dict[str, float]
    cause_probabilities: dict[str, float]
    queue_recovery_probability: float
    keep_value_bps: float
    cancel_reenter_value_bps: float
    cancel_advantage_bps: float
    queue_reset_option_cost_bps: float


@dataclass(frozen=True)
class QueueValueNetState:
    schema_version: str
    active: bool
    reason: str
    maker_expected_ticks: float
    adverse_probability: float
    favorable_probability: float
    adverse_flow_intensity: float
    cancel_intensity: float
    refill_intensity: float
    adverse_to_refill_ratio: float
    queue_state_key: str
    microprice_state_key: str
    cause_intensities_per_s: dict[str, float]
    cause_probabilities: dict[str, float]
    queue_recovery_probability: float
    keep_value_bps: float
    cancel_reenter_value_bps: float
    cancel_advantage_bps: float
    queue_reset_option_cost_bps: float


@dataclass(frozen=True)
class CompetingRiskSideArtifact:
    schema_version: str
    side: str
    normalizer: FeatureNormalizer
    hazards: dict[str, CauseHazardModel]
    amplitudes: QueueValueAmplitudes
    state_config: QueueValueNetConfig
    runtime_queue_artifact: QueueReactiveHawkesArtifact
    calibration: dict[str, Any]

    def predict(self, features: Mapping[str, Any]) -> CompetingRiskPrediction:
        x = self.normalizer.transform_mapping(features)
        intensities = {
            cause: self.hazards[cause].predict_rate(x) for cause in ALL_HAZARDS
        }
        competing_total = max(
            1e-12,
            sum(float(intensities[cause]) for cause in CAUSES),
        )
        horizon = float(self.state_config.decision_horizon_s)
        any_event = 1.0 - math.exp(-competing_total * horizon)
        probabilities = {
            cause: float(intensities[cause]) / competing_total * any_event
            for cause in CAUSES
        }
        recovery_rate = (
            float(intensities[QUEUE_RECOVERY_CAUSE])
            + float(intensities["campaign_repair"])
        )
        recovery_probability = 1.0 - math.exp(
            -recovery_rate * float(self.amplitudes.recovery_horizon_s)
        )
        queue_progress = min(
            1.0,
            max(0.0, 1.0 - float(features.get("queue_fraction_left", 1.0))),
        )
        favorable_option = max(
            0.0,
            probabilities["favorable_fill"]
            * float(self.amplitudes.favorable_fill_bps),
        )
        queue_reset_cost = favorable_option * queue_progress
        keep_value = (
            probabilities["favorable_fill"]
            * float(self.amplitudes.favorable_fill_bps)
            + probabilities["adverse_fill"]
            * float(self.amplitudes.adverse_fill_bps)
            - probabilities["adverse_price_jump"]
            * float(self.amplitudes.adverse_price_jump_bps)
        )
        pre_ack_total = competing_total
        pre_ack_any = 1.0 - math.exp(
            -pre_ack_total * float(self.amplitudes.cancel_ack_horizon_s)
        )
        pre_ack_adverse = (
            float(intensities["adverse_fill"]) / pre_ack_total * pre_ack_any
        )
        cancel_value = (
            recovery_probability * float(self.amplitudes.fresh_order_option_bps)
            - pre_ack_adverse * abs(float(self.amplitudes.adverse_fill_bps))
            - queue_reset_cost
        )
        return CompetingRiskPrediction(
            intensities_per_s=intensities,
            cause_probabilities=probabilities,
            queue_recovery_probability=recovery_probability,
            keep_value_bps=keep_value,
            cancel_reenter_value_bps=cancel_value,
            cancel_advantage_bps=cancel_value - keep_value,
            queue_reset_option_cost_bps=queue_reset_cost,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "side": self.side,
            "normalizer": self.normalizer.to_payload(),
            "hazards": {
                cause: model.to_payload()
                for cause, model in sorted(self.hazards.items())
            },
            "amplitudes": asdict(self.amplitudes),
            "state_config": asdict(self.state_config),
            "runtime_queue_artifact": self.runtime_queue_artifact.to_payload(),
            "calibration": self.calibration,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> CompetingRiskSideArtifact:
        if payload.get("schema_version") != SIDE_SCHEMA_VERSION:
            raise ValueError("unsupported competing-risk side schema")
        hazards = {
            str(cause): CauseHazardModel.from_payload(model)
            for cause, model in (payload.get("hazards") or {}).items()
        }
        if set(hazards) != set(ALL_HAZARDS):
            raise ValueError("competing-risk side artifact has incomplete hazards")
        return cls(
            schema_version=str(payload["schema_version"]),
            side=str(payload["side"]).upper(),
            normalizer=FeatureNormalizer.from_payload(payload["normalizer"]),
            hazards=hazards,
            amplitudes=QueueValueAmplitudes(**payload["amplitudes"]),
            state_config=QueueValueNetConfig(**payload["state_config"]),
            runtime_queue_artifact=QueueReactiveHawkesArtifact.from_payload(
                payload["runtime_queue_artifact"]
            ),
            calibration=dict(payload.get("calibration") or {}),
        )


class QueueValueNetEvaluator:
    def __init__(self, artifact: CompetingRiskSideArtifact) -> None:
        self.artifact = artifact

    @property
    def input_scope(self) -> str:
        return "local_only"

    def evaluate(
        self,
        *,
        side: str,
        features: Mapping[str, Any],
        excitation: Mapping[str, float] | None = None,
        was_active: bool = False,
    ) -> QueueValueNetState:
        del excitation
        normalized_side = str(side).upper()
        if normalized_side != self.artifact.side:
            raise ValueError(
                f"competing-risk evaluator side mismatch: {normalized_side}"
            )
        prediction = self.artifact.predict(features)
        cfg = self.artifact.state_config
        threshold = (
            float(cfg.exit_advantage_bps)
            if was_active
            else float(cfg.entry_advantage_bps)
        )
        active = prediction.cancel_advantage_bps > threshold
        reason = (
            "net_value_hysteresis_hold"
            if was_active and active
            else "net_value_exit"
            if was_active
            else "net_value_entry"
            if active
            else "net_value_entry_not_met"
        )
        probabilities = prediction.cause_probabilities
        intensities = prediction.intensities_per_s
        adverse_probability = (
            probabilities["adverse_fill"]
            + probabilities["adverse_price_jump"]
        )
        favorable_probability = (
            probabilities["favorable_fill"]
            + probabilities["campaign_repair"]
        )
        adverse_intensity = (
            intensities["adverse_fill"]
            + intensities["adverse_price_jump"]
        )
        refill_intensity = intensities[QUEUE_RECOVERY_CAUSE]
        ratio = (
            adverse_intensity + intensities["cancel"]
        ) / max(refill_intensity, 1e-12)
        return QueueValueNetState(
            schema_version=STATE_SCHEMA_VERSION,
            active=bool(active),
            reason=reason,
            maker_expected_ticks=float(prediction.keep_value_bps),
            adverse_probability=float(adverse_probability),
            favorable_probability=float(favorable_probability),
            adverse_flow_intensity=float(adverse_intensity),
            cancel_intensity=float(intensities["cancel"]),
            refill_intensity=float(refill_intensity),
            adverse_to_refill_ratio=float(ratio),
            queue_state_key=f"competing-risk:{self.artifact.side}",
            microprice_state_key="net-order-value-bps",
            cause_intensities_per_s=dict(intensities),
            cause_probabilities=dict(probabilities),
            queue_recovery_probability=float(
                prediction.queue_recovery_probability
            ),
            keep_value_bps=float(prediction.keep_value_bps),
            cancel_reenter_value_bps=float(
                prediction.cancel_reenter_value_bps
            ),
            cancel_advantage_bps=float(prediction.cancel_advantage_bps),
            queue_reset_option_cost_bps=float(
                prediction.queue_reset_option_cost_bps
            ),
        )


@dataclass(frozen=True)
class CompetingRiskBundle:
    schema_version: str
    family_id: str
    bundle_id: str
    input_scope: str
    fit_days: tuple[str, ...]
    internal_embargo_days: tuple[str, ...]
    calibration_days: tuple[str, ...]
    source_panel_path: str
    source_panel_sha256: str
    base_queue_bundle_path: str
    base_queue_bundle_sha256: str
    evidence_split_path: str
    evidence_split_sha256: str
    score_profile_contract: dict[str, Any]
    sides: dict[str, CompetingRiskSideArtifact]
    calibration_passed: bool

    def side_artifact(self, side: str) -> CompetingRiskSideArtifact:
        normalized = str(side).upper()
        try:
            return self.sides[normalized]
        except KeyError as exc:
            raise ValueError(f"competing-risk bundle has no {normalized} side") from exc

    def evaluator(self, side: str) -> QueueValueNetEvaluator:
        return QueueValueNetEvaluator(self.side_artifact(side))

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "family_id": self.family_id,
            "bundle_id": self.bundle_id,
            "input_scope": self.input_scope,
            "fit_days": list(self.fit_days),
            "internal_embargo_days": list(self.internal_embargo_days),
            "calibration_days": list(self.calibration_days),
            "source_panel_path": self.source_panel_path,
            "source_panel_sha256": self.source_panel_sha256,
            "base_queue_bundle_path": self.base_queue_bundle_path,
            "base_queue_bundle_sha256": self.base_queue_bundle_sha256,
            "evidence_split_path": self.evidence_split_path,
            "evidence_split_sha256": self.evidence_split_sha256,
            "score_profile_contract": self.score_profile_contract,
            "sides": {
                side: artifact.to_payload()
                for side, artifact in sorted(self.sides.items())
            },
            "calibration_passed": self.calibration_passed,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: Path) -> CompetingRiskBundle:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported competing-risk bundle schema")
        sides = {
            str(side).upper(): CompetingRiskSideArtifact.from_payload(artifact)
            for side, artifact in payload["sides"].items()
        }
        if set(sides) != {"BUY", "SELL"}:
            raise ValueError("competing-risk bundle requires BUY and SELL")
        return cls(
            schema_version=str(payload["schema_version"]),
            family_id=str(payload["family_id"]),
            bundle_id=str(payload["bundle_id"]),
            input_scope=str(payload["input_scope"]),
            fit_days=tuple(str(day) for day in payload["fit_days"]),
            internal_embargo_days=tuple(
                str(day) for day in payload["internal_embargo_days"]
            ),
            calibration_days=tuple(str(day) for day in payload["calibration_days"]),
            source_panel_path=str(payload["source_panel_path"]),
            source_panel_sha256=str(payload["source_panel_sha256"]),
            base_queue_bundle_path=str(payload["base_queue_bundle_path"]),
            base_queue_bundle_sha256=str(payload["base_queue_bundle_sha256"]),
            evidence_split_path=str(payload["evidence_split_path"]),
            evidence_split_sha256=str(payload["evidence_split_sha256"]),
            score_profile_contract=dict(payload["score_profile_contract"]),
            sides=sides,
            calibration_passed=bool(payload["calibration_passed"]),
        )


def _fit_hazard(
    *,
    cause: str,
    x: np.ndarray,
    exposure_s: np.ndarray,
    counts: np.ndarray,
    alpha: float,
) -> CauseHazardModel:
    if len(x) != len(exposure_s) or len(x) != len(counts):
        raise ValueError("hazard fit arrays have different lengths")
    target_rate = np.asarray(counts, dtype=float) / np.maximum(
        np.asarray(exposure_s, dtype=float),
        1e-6,
    )
    total_exposure = float(np.sum(exposure_s))
    observed = float(np.sum(counts))
    fitted_model: PoissonRegressor | None = None
    fitted_alpha = float(alpha)
    for candidate_alpha in (
        float(alpha),
        max(1.0, float(alpha) * 10.0),
        max(10.0, float(alpha) * 100.0),
    ):
        model = PoissonRegressor(
            alpha=candidate_alpha,
            fit_intercept=True,
            max_iter=1_000,
            tol=1e-9,
        )
        # L-BFGS may transiently probe overflowing log-link parameters before
        # returning to the regularized finite optimum.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=RuntimeWarning,
                module=r"sklearn\..*",
            )
            model.fit(x, target_rate, sample_weight=exposure_s)
        if (
            math.isfinite(float(model.intercept_))
            and np.isfinite(np.asarray(model.coef_, dtype=float)).all()
        ):
            fitted_model = model
            fitted_alpha = candidate_alpha
            break
    constant_rate = observed / max(total_exposure, 1e-12)
    if fitted_model is None:
        intercept = math.log(max(constant_rate, 1e-12))
        coefficients = tuple(0.0 for _ in range(x.shape[1]))
        fit_mode = "constant_rate_fallback"
    else:
        intercept = float(fitted_model.intercept_)
        coefficients = tuple(float(value) for value in fitted_model.coef_)
        fit_mode = (
            "regularized_poisson"
            if math.isclose(fitted_alpha, float(alpha))
            else "regularized_poisson_retry"
        )
    return CauseHazardModel(
        schema_version=HAZARD_SCHEMA_VERSION,
        cause=str(cause),
        intercept=intercept,
        coefficients=coefficients,
        alpha=fitted_alpha,
        fit_rows=int(len(x)),
        observed_events=observed,
        exposure_s=total_exposure,
        constant_rate_per_s=constant_rate,
        fit_mode=fit_mode,
    )


def _winsorized_mean(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        raise ValueError("cannot estimate value amplitude from an empty sample")
    lower, upper = numeric.quantile([0.05, 0.95])
    return float(numeric.clip(lower, upper).mean())


def _calibrate_hazards(
    artifact: CompetingRiskSideArtifact,
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    x = artifact.normalizer.transform_frame(frame)
    exposure_s = np.maximum(
        pd.to_numeric(frame["event_time_ms"], errors="coerce")
        .fillna(1.0)
        .to_numpy(dtype=float)
        / 1_000.0,
        0.001,
    )
    output = frame[["day", "decision_id", "side"]].copy()
    report: dict[str, Any] = {}
    passed = True
    for cause in ALL_HAZARDS:
        model = artifact.hazards[cause]
        rate = model.predict_rates(x)
        if cause == QUEUE_RECOVERY_CAUSE:
            counts = (
                pd.to_numeric(
                    frame["exchange_book_refill_count"], errors="coerce"
                )
                .fillna(0.0)
                .to_numpy(dtype=float)
            )
        else:
            counts = (frame["first_event"].astype(str) == cause).astype(float).to_numpy()
        occurrence = counts > 0.0
        probability = 1.0 - np.exp(-rate * exposure_s)
        null_probability = 1.0 - np.exp(
            -float(model.constant_rate_per_s) * exposure_s
        )
        brier = float(np.mean((probability - occurrence.astype(float)) ** 2))
        null_brier = float(
            np.mean((null_probability - occurrence.astype(float)) ** 2)
        )
        expected = float(np.sum(rate * exposure_s))
        observed = float(np.sum(counts))
        oe = observed / max(expected, 1e-12)
        cause_passed = bool(
            math.isfinite(brier)
            and math.isfinite(oe)
            and 0.50 <= oe <= 2.0
            and brier <= null_brier + 0.01
        )
        report[cause] = {
            "observed": observed,
            "expected": expected,
            "observed_to_expected": oe,
            "occurrence_brier": brier,
            "constant_occurrence_brier": null_brier,
            "passed": cause_passed,
        }
        passed = passed and cause_passed
        output[f"hazard_{cause}_per_s"] = rate
        output[f"probability_{cause}"] = probability

    states = [artifact.predict(row) for row in frame.to_dict("records")]
    output["keep_value_bps"] = [state.keep_value_bps for state in states]
    output["cancel_reenter_value_bps"] = [
        state.cancel_reenter_value_bps for state in states
    ]
    output["cancel_advantage_bps"] = [
        state.cancel_advantage_bps for state in states
    ]
    output["queue_recovery_probability"] = [
        state.queue_recovery_probability for state in states
    ]
    report["passed"] = bool(passed)
    return output, report


def fit_competing_risk_bundle(
    frame: pd.DataFrame,
    *,
    base_queue_bundle_payload: Mapping[str, Any],
    fit_days: Sequence[str],
    internal_embargo_days: Sequence[str],
    calibration_days: Sequence[str],
    source_panel_path: Path,
    base_queue_bundle_path: Path,
    evidence_split_path: Path,
    score_profile_contract: Mapping[str, Any],
    family_id: str = "queue_value_net_hazard_keep_cancel_v2",
    target_candidate_rate: float = 0.15,
    minimum_candidate_rate: float = 0.05,
    maximum_candidate_rate: float = 0.30,
    decision_horizon_s: float = 1.0,
    cancel_ack_horizon_s: float = 0.50,
    recovery_horizon_s: float = 5.0,
    alpha: float = 0.10,
) -> tuple[CompetingRiskBundle, pd.DataFrame, dict[str, Any]]:
    """Fit the side artifacts and freeze value thresholds without action outcomes."""

    fit_days = tuple(str(day) for day in fit_days)
    internal_embargo_days = tuple(str(day) for day in internal_embargo_days)
    calibration_days = tuple(str(day) for day in calibration_days)
    if not (
        fit_days
        and internal_embargo_days
        and calibration_days
        and max(fit_days) < min(internal_embargo_days)
        and max(internal_embargo_days) < min(calibration_days)
    ):
        raise ValueError("competing-risk fit/embargo/calibration split is invalid")
    if not 0.05 <= float(target_candidate_rate) <= 0.30:
        raise ValueError("target candidate rate must be in [0.05, 0.30]")
    if not (
        0.0 < minimum_candidate_rate <= target_candidate_rate <= maximum_candidate_rate < 1.0
    ):
        raise ValueError("candidate-rate budget is invalid")
    required = {
        "day",
        "side",
        "decision_id",
        "event_time_ms",
        "first_event",
        "fill_value_markout_bps",
        "order_price",
        "price_jump_ticks",
        "exchange_book_refill_count",
        *DEFAULT_FEATURES,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"competing-risk source panel missing columns: {missing}")
    if set(base_queue_bundle_payload.get("sides") or {}) != {"BUY", "SELL"}:
        raise ValueError("base queue bundle must contain BUY and SELL")

    side_artifacts: dict[str, CompetingRiskSideArtifact] = {}
    predictions: list[pd.DataFrame] = []
    calibration_report: dict[str, Any] = {
        "family_id": family_id,
        "fit_days": list(fit_days),
        "internal_embargo_days": list(internal_embargo_days),
        "calibration_days": list(calibration_days),
        "target_candidate_rate": float(target_candidate_rate),
        "sides": {},
    }
    for side in ("BUY", "SELL"):
        fit = frame[
            frame["day"].astype(str).isin(fit_days)
            & frame["side"].astype(str).str.upper().eq(side)
        ].copy()
        calibration = frame[
            frame["day"].astype(str).isin(calibration_days)
            & frame["side"].astype(str).str.upper().eq(side)
        ].copy()
        if fit.empty or calibration.empty:
            raise ValueError(f"{side} competing-risk fit/calibration rows are empty")
        normalizer = fit_feature_normalizer(fit)
        x = normalizer.transform_frame(fit)
        exposure_s = np.maximum(
            pd.to_numeric(fit["event_time_ms"], errors="coerce")
            .fillna(1.0)
            .to_numpy(dtype=float)
            / 1_000.0,
            0.001,
        )
        hazards: dict[str, CauseHazardModel] = {}
        for cause in CAUSES:
            counts = (
                fit["first_event"].astype(str).eq(cause).astype(float).to_numpy()
            )
            hazards[cause] = _fit_hazard(
                cause=cause,
                x=x,
                exposure_s=exposure_s,
                counts=counts,
                alpha=alpha,
            )
        hazards[QUEUE_RECOVERY_CAUSE] = _fit_hazard(
            cause=QUEUE_RECOVERY_CAUSE,
            x=x,
            exposure_s=exposure_s,
            counts=(
                pd.to_numeric(
                    fit["exchange_book_refill_count"], errors="coerce"
                )
                .fillna(0.0)
                .to_numpy(dtype=float)
            ),
            alpha=alpha,
        )

        markout = pd.to_numeric(
            fit["fill_value_markout_bps"], errors="coerce"
        )
        favorable = _winsorized_mean(markout[markout >= 0.0])
        adverse = _winsorized_mean(markout[markout < 0.0])
        jump_bps = (
            pd.to_numeric(fit["price_jump_ticks"], errors="coerce")
            .fillna(1.0)
            .clip(lower=0.0)
            * 0.1
            / pd.to_numeric(fit["order_price"], errors="coerce")
            .replace(0.0, np.nan)
            .fillna(np.inf)
            * 10_000.0
        )
        jump_penalty = max(1e-6, _winsorized_mean(jump_bps))
        runtime_queue = QueueReactiveHawkesArtifact.from_payload(
            base_queue_bundle_payload["sides"][side]["queue_artifact"]
        )
        provisional = CompetingRiskSideArtifact(
            schema_version=SIDE_SCHEMA_VERSION,
            side=side,
            normalizer=normalizer,
            hazards=hazards,
            amplitudes=QueueValueAmplitudes(
                favorable_fill_bps=max(0.0, favorable),
                adverse_fill_bps=min(0.0, adverse),
                adverse_price_jump_bps=jump_penalty,
                fresh_order_option_bps=0.0,
                cancel_ack_horizon_s=float(cancel_ack_horizon_s),
                recovery_horizon_s=float(recovery_horizon_s),
            ),
            state_config=QueueValueNetConfig(
                decision_horizon_s=float(decision_horizon_s),
                entry_advantage_bps=math.inf,
                exit_advantage_bps=math.inf,
                calibration_candidate_rate=0.0,
                target_candidate_rate=float(target_candidate_rate),
                minimum_candidate_rate=float(minimum_candidate_rate),
                maximum_candidate_rate=float(maximum_candidate_rate),
            ),
            runtime_queue_artifact=runtime_queue,
            calibration={},
        )
        raw_predictions = [
            provisional.predict(row) for row in calibration.to_dict("records")
        ]
        advantages = np.asarray(
            [prediction.cancel_advantage_bps for prediction in raw_predictions],
            dtype=float,
        )
        entry_threshold = float(
            np.quantile(advantages, 1.0 - float(target_candidate_rate))
        )
        exit_threshold = float(np.quantile(advantages, 0.50))
        candidate_rate = float((advantages > entry_threshold).mean())
        config = QueueValueNetConfig(
            decision_horizon_s=float(decision_horizon_s),
            entry_advantage_bps=entry_threshold,
            exit_advantage_bps=min(entry_threshold - 1e-12, exit_threshold),
            calibration_candidate_rate=candidate_rate,
            target_candidate_rate=float(target_candidate_rate),
            minimum_candidate_rate=float(minimum_candidate_rate),
            maximum_candidate_rate=float(maximum_candidate_rate),
        )
        artifact = CompetingRiskSideArtifact(
            schema_version=SIDE_SCHEMA_VERSION,
            side=side,
            normalizer=normalizer,
            hazards=hazards,
            amplitudes=provisional.amplitudes,
            state_config=config,
            runtime_queue_artifact=runtime_queue,
            calibration={},
        )
        prediction_frame, report = _calibrate_hazards(artifact, calibration)
        report.update(
            {
                "candidate_rate": candidate_rate,
                "candidate_rate_passed": bool(
                    minimum_candidate_rate
                    <= candidate_rate
                    <= maximum_candidate_rate
                ),
                "entry_advantage_bps": config.entry_advantage_bps,
                "exit_advantage_bps": config.exit_advantage_bps,
                "amplitudes": asdict(artifact.amplitudes),
                "selection_rule": (
                    "outcome-blind top 15% calibration V_cancel_reenter-minus-V_keep; "
                    "exit when advantage falls below the calibration median"
                ),
            }
        )
        report["passed"] = bool(
            report["passed"] and report["candidate_rate_passed"]
        )
        artifact = CompetingRiskSideArtifact(
            schema_version=artifact.schema_version,
            side=artifact.side,
            normalizer=artifact.normalizer,
            hazards=artifact.hazards,
            amplitudes=artifact.amplitudes,
            state_config=artifact.state_config,
            runtime_queue_artifact=artifact.runtime_queue_artifact,
            calibration=report,
        )
        prediction_frame["state_entry"] = (
            prediction_frame["cancel_advantage_bps"]
            > float(config.entry_advantage_bps)
        ).astype(int)
        side_artifacts[side] = artifact
        predictions.append(prediction_frame)
        calibration_report["sides"][side] = report

    calibration_passed = all(
        bool(artifact.calibration.get("passed"))
        for artifact in side_artifacts.values()
    )
    identity = {
        "family_id": family_id,
        "fit_days": fit_days,
        "internal_embargo_days": internal_embargo_days,
        "calibration_days": calibration_days,
        "source_panel_sha256": _sha256_file(source_panel_path),
        "base_queue_bundle_sha256": _sha256_file(base_queue_bundle_path),
        "evidence_split_sha256": _sha256_file(evidence_split_path),
        "score_profile_contract": dict(score_profile_contract),
        "side_thresholds": {
            side: asdict(artifact.state_config)
            for side, artifact in sorted(side_artifacts.items())
        },
        "side_model_sha256": {
            side: _canonical_sha256(artifact.to_payload())
            for side, artifact in sorted(side_artifacts.items())
        },
    }
    bundle = CompetingRiskBundle(
        schema_version=SCHEMA_VERSION,
        family_id=family_id,
        bundle_id=f"queue-value-net-{_canonical_sha256(identity)[:16]}",
        input_scope="local_only",
        fit_days=fit_days,
        internal_embargo_days=internal_embargo_days,
        calibration_days=calibration_days,
        source_panel_path=str(source_panel_path.resolve()),
        source_panel_sha256=identity["source_panel_sha256"],
        base_queue_bundle_path=str(base_queue_bundle_path.resolve()),
        base_queue_bundle_sha256=identity["base_queue_bundle_sha256"],
        evidence_split_path=str(evidence_split_path.resolve()),
        evidence_split_sha256=identity["evidence_split_sha256"],
        score_profile_contract=dict(score_profile_contract),
        sides=side_artifacts,
        calibration_passed=bool(calibration_passed),
    )
    calibration_report["calibration_passed"] = bool(calibration_passed)
    calibration_report["bundle_id"] = bundle.bundle_id
    return bundle, pd.concat(predictions, ignore_index=True), calibration_report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_historical_reproduction_argument(parser)
    parser.add_argument("--source-panel", type=Path, required=True)
    parser.add_argument("--base-queue-bundle", type=Path, required=True)
    parser.add_argument("--evidence-split", type=Path, required=True)
    parser.add_argument("--score-profile-contract", type=Path, required=True)
    parser.add_argument("--output-bundle", type=Path, required=True)
    parser.add_argument("--output-predictions", type=Path, required=True)
    parser.add_argument("--output-report", type=Path, required=True)
    parser.add_argument("--target-candidate-rate", type=float, default=0.15)
    parser.add_argument("--minimum-candidate-rate", type=float, default=0.05)
    parser.add_argument("--maximum-candidate-rate", type=float, default=0.30)
    parser.add_argument("--decision-horizon-s", type=float, default=1.0)
    parser.add_argument("--cancel-ack-horizon-s", type=float, default=0.50)
    parser.add_argument("--recovery-horizon-s", type=float, default=5.0)
    parser.add_argument("--alpha", type=float, default=0.10)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    require_historical_reproduction(
        runner_id="f07.queue_value_competing_risk",
        enabled=bool(args.historical_reproduction),
        spec_path=None,
    )
    source_panel = args.source_panel.expanduser().resolve()
    base_bundle_path = args.base_queue_bundle.expanduser().resolve()
    evidence_split = args.evidence_split.expanduser().resolve()
    score_profile_path = args.score_profile_contract.expanduser().resolve()
    frame = pd.read_parquet(source_panel)
    base_payload = json.loads(base_bundle_path.read_text(encoding="utf-8"))
    score_profile = json.loads(score_profile_path.read_text(encoding="utf-8"))
    bundle, predictions, report = fit_competing_risk_bundle(
        frame,
        base_queue_bundle_payload=base_payload,
        fit_days=base_payload["fit_days"],
        internal_embargo_days=base_payload["internal_embargo_days"],
        calibration_days=base_payload["calibration_days"],
        source_panel_path=source_panel,
        base_queue_bundle_path=base_bundle_path,
        evidence_split_path=evidence_split,
        score_profile_contract=score_profile,
        target_candidate_rate=float(args.target_candidate_rate),
        minimum_candidate_rate=float(args.minimum_candidate_rate),
        maximum_candidate_rate=float(args.maximum_candidate_rate),
        decision_horizon_s=float(args.decision_horizon_s),
        cancel_ack_horizon_s=float(args.cancel_ack_horizon_s),
        recovery_horizon_s=float(args.recovery_horizon_s),
        alpha=float(args.alpha),
    )
    output_bundle = args.output_bundle.expanduser().resolve()
    output_predictions = args.output_predictions.expanduser().resolve()
    output_report = args.output_report.expanduser().resolve()
    bundle.save(output_bundle)
    output_predictions.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_parquet(output_predictions, index=False)
    output_report.parent.mkdir(parents=True, exist_ok=True)
    output_report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "bundle": str(output_bundle),
                "bundle_id": bundle.bundle_id,
                "calibration_passed": bundle.calibration_passed,
                "predictions": str(output_predictions),
                "report": str(output_report),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
