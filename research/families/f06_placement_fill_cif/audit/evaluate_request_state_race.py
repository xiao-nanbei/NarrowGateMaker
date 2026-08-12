#!/usr/bin/env python3
"""Fit and evaluate the Development-only request-state placement race."""

from __future__ import annotations

import argparse
import gc
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from data_paths import data_root
from models.audit.content_addressed_cache import canonical_sha256, file_sha256
from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.direct_fill_cif import make_expanding_folds
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    ROOT,
    STATIC_MODEL_FEATURES,
)
from research.families.f06_placement_fill_cif.audit.request_state_race import (
    REQUEST_CATEGORICAL_FEATURES,
    REQUEST_SHARED_NUMERIC_FEATURES,
    CauseSpecificRateModel,
    _encoded_base,
    fit_three_phase_by_side,
    pending_event_kind,
)
from research.families.f06_placement_fill_cif.audit.risk_set_expansion import (
    EVENT_ACK,
    EVENT_CENSOR,
    EVENT_FILL,
    expand_competing_risk_intervals_native,
)
from research.governance.paths import verify_path_identity

DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = (
    FAMILY_DOCS / "placement_fill_request_state_race_v2_fit_spec_v2_20260728.json"
)
DEFAULT_REQUEST_STATE = (
    DATA_ROOT
    / "reports"
    / "placement_fill_request_state_race_v2_development_20260728_v3"
    / "request_state"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "placement_fill_request_state_race_v2_development_20260728_v3"
    / "oof"
)
SCHEMA_VERSION = "placement_fill_request_state_race_oof.v1"

IDENTITY_COLUMNS = (
    "cohort_id",
    "day",
    "side",
    "inventory_role",
    "action",
    "action_lifecycle_id",
)
PHASE_COLUMNS = (
    "activation_ts_ns",
    "cancel_request_ts_ns",
    "pre_request_first_fill",
    "pre_request_observed",
    "pre_request_right_censored_by_gap",
    "pre_request_exposure_ms",
    "request_model_risk_set",
    "pending_cancel_fill",
    "cancel_ack_observed",
    "pending_right_censored_by_gap",
    "pending_risk_duration_ms",
    "first_pending_cancel_fill_ts_ns",
    "actual_cancel_ack_ts_ns",
)


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
        verify_path_identity(path, str(expected))
    except (FileNotFoundError, RuntimeError) as exc:
        raise RuntimeError(f"{label} identity check failed: {exc}") from exc


def _load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != (
        "narrowgate_placement_fill_request_state_race_fit_spec.v1"
    ):
        raise RuntimeError("unsupported request-state race fit specification")
    mechanics = Path(str(spec["mechanics_spec"]["path"])).expanduser().resolve()
    _require_identity(
        mechanics,
        str(spec["mechanics_spec"]["sha256"]),
        "mechanics spec",
    )
    for key in ("request_state_manifest", "request_state_index"):
        identity = spec["source_identity"][key]
        _require_identity(
            Path(str(identity["path"])).expanduser().resolve(),
            str(identity["sha256"]),
            key,
        )
    evaluator = ROOT / str(spec["implementation"]["evaluator"])
    model = ROOT / str(spec["implementation"]["model"])
    _require_identity(
        evaluator,
        str(spec["implementation"]["evaluator_sha256"]),
        "evaluator",
    )
    _require_identity(
        model,
        str(spec["implementation"]["model_sha256"]),
        "request-state model",
    )
    return spec


def _required_columns() -> list[str]:
    return sorted(
        set(IDENTITY_COLUMNS)
        | set(PHASE_COLUMNS)
        | set(STATIC_MODEL_FEATURES)
        | set(REQUEST_SHARED_NUMERIC_FEATURES)
        | set(REQUEST_CATEGORICAL_FEATURES)
    )


def _load_days(index: pd.DataFrame, days: Sequence[str]) -> pd.DataFrame:
    selected = index.loc[index["day"].astype(str).isin(set(days))]
    if len(selected) != len(set(days)):
        missing = sorted(set(days) - set(selected["day"].astype(str)))
        raise RuntimeError(f"request-state index lacks days: {missing}")
    pieces: list[pd.DataFrame] = []
    columns = _required_columns()
    for row in selected.sort_values("day").itertuples(index=False):
        path = Path(str(row.payload_path)).expanduser().resolve()
        _require_identity(path, str(row.payload_sha256), f"request state {row.day}")
        pieces.append(pd.read_parquet(path, columns=columns))
    return pd.concat(pieces, ignore_index=True)


def _phase_frame(frame: pd.DataFrame, phase: str) -> pd.DataFrame:
    if phase == "pre_request":
        return frame.loc[
            frame["pre_request_observed"].to_numpy(np.int8) != 0
        ].copy()
    if phase == "pending":
        return frame.loc[
            frame["request_model_risk_set"].to_numpy(np.int8) != 0
        ].copy()
    raise ValueError(f"unknown phase: {phase}")


def _pre_scheduled_duration_ms(frame: pd.DataFrame) -> np.ndarray:
    activation = pd.to_numeric(
        frame["activation_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    request = pd.to_numeric(
        frame["cancel_request_ts_ns"], errors="coerce"
    ).fillna(0).to_numpy(np.int64)
    scheduled = np.maximum(0.0, (request - activation) / 1_000_000.0)
    fallback = pd.to_numeric(
        frame["pre_request_exposure_ms"], errors="coerce"
    ).fillna(0.0).to_numpy(float)
    return np.where(scheduled > 0.0, scheduled, fallback)


def _event_kind(frame: pd.DataFrame, phase: str) -> np.ndarray:
    if phase == "pre_request":
        return np.where(
            frame["pre_request_first_fill"].to_numpy(np.int8) != 0,
            EVENT_FILL,
            EVENT_CENSOR,
        ).astype(np.uint8)
    return pending_event_kind(frame)


def _observed_duration_ms(frame: pd.DataFrame, phase: str) -> np.ndarray:
    name = (
        "pre_request_exposure_ms"
        if phase == "pre_request"
        else "pending_risk_duration_ms"
    )
    return np.maximum(
        0.0,
        pd.to_numeric(frame[name], errors="coerce").fillna(0.0).to_numpy(float),
    )


def _prediction_duration_ms(
    frame: pd.DataFrame, phase: str, horizon_ms: int
) -> np.ndarray:
    horizon = float(horizon_ms)
    if phase == "pre_request":
        return np.minimum(horizon, _pre_scheduled_duration_ms(frame))
    return np.full(len(frame), horizon, dtype=float)


def _known_targets(
    frame: pd.DataFrame,
    phase: str,
    horizon_ms: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    duration = _observed_duration_ms(frame, phase)
    kind = _event_kind(frame, phase)
    horizon = float(horizon_ms)
    if phase == "pre_request":
        censored = (
            frame["pre_request_right_censored_by_gap"].to_numpy(np.int8) != 0
        )
        known = ~censored
    else:
        censored = (
            frame["pending_right_censored_by_gap"].to_numpy(np.int8) != 0
        )
        event_before = (kind != EVENT_CENSOR) & (duration <= horizon)
        known = event_before | (duration >= horizon)
        known &= ~(censored & (duration < horizon))
    fill = ((kind == EVENT_FILL) & (duration <= horizon)).astype(np.int8)
    ack = ((kind == EVENT_ACK) & (duration <= horizon)).astype(np.int8)
    return known, fill, ack


def _interval_rates(
    model: CauseSpecificRateModel,
    frame: pd.DataFrame,
    duration_ms: np.ndarray,
) -> dict[str, np.ndarray]:
    duration = np.clip(
        np.asarray(duration_ms, dtype=float),
        1e-6,
        float(model.bin_edges_ms[-1]),
    )
    expanded = expand_competing_risk_intervals_native(
        duration,
        np.zeros(len(frame), dtype=np.uint8),
        model.bin_edges_ms,
    )
    base_columns = tuple(
        name
        for name in model.encoded_columns
        if name not in {"risk_elapsed_log1p", "risk_interval_width_log1p"}
    )
    base, _ = _encoded_base(
        frame,
        numeric_features=model.numeric_features,
        categorical_features=model.categorical_features,
        encoded_columns=base_columns,
    )
    row_index = expanded["row_index"].to_numpy(np.int64)
    features = base.iloc[row_index].reset_index(drop=True)
    start = expanded["interval_start_ms"].to_numpy(float)
    end = expanded["interval_end_ms"].to_numpy(float)
    features["risk_elapsed_log1p"] = np.log1p(start)
    features["risk_interval_width_log1p"] = np.log1p(end - start)
    features = features.reindex(columns=model.encoded_columns, fill_value=0.0)
    fill_rate = np.maximum(0.0, model.fill_model.predict(features))
    ack_rate = (
        np.maximum(0.0, model.ack_model.predict(features))
        if model.ack_model is not None
        else np.zeros(len(features), dtype=float)
    )
    return {
        "row_index": row_index,
        "dt_seconds": (end - start) / 1_000.0,
        "fill_rate": fill_rate,
        "ack_rate": ack_rate,
    }


def _integrated_hazard(
    bundle: Mapping[str, np.ndarray], rows: int
) -> tuple[np.ndarray, np.ndarray]:
    row_index = bundle["row_index"]
    dt = bundle["dt_seconds"]
    fill = np.bincount(
        row_index,
        weights=bundle["fill_rate"] * dt,
        minlength=rows,
    )
    ack = np.bincount(
        row_index,
        weights=bundle["ack_rate"] * dt,
        minlength=rows,
    )
    return fill, ack


def _probabilities_from_rates(
    bundle: Mapping[str, np.ndarray],
    rows: int,
    fill_scale: np.ndarray,
    ack_scale: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    row_index = bundle["row_index"]
    fill_rate = bundle["fill_rate"] * fill_scale[row_index]
    ack_rate = bundle["ack_rate"] * ack_scale[row_index]
    total = fill_rate + ack_rate
    event = 1.0 - np.exp(-total * bundle["dt_seconds"])
    fill_hazard = np.divide(
        event * fill_rate,
        total,
        out=np.zeros_like(total),
        where=total > 0.0,
    )
    ack_hazard = np.divide(
        event * ack_rate,
        total,
        out=np.zeros_like(total),
        where=total > 0.0,
    )
    survival_factor = np.clip(
        1.0 - fill_hazard - ack_hazard, 1e-15, 1.0
    )
    starts = np.r_[0, np.flatnonzero(np.diff(row_index)) + 1]
    counts = np.diff(np.r_[starts, len(row_index)])
    cumulative_log = np.cumsum(np.log(survival_factor))
    prior_global = np.r_[0.0, cumulative_log[:-1]]
    group_base = np.where(starts > 0, cumulative_log[starts - 1], 0.0)
    survival_before = np.exp(prior_global - np.repeat(group_base, counts))
    fill = np.add.reduceat(survival_before * fill_hazard, starts)
    ack = np.add.reduceat(survival_before * ack_hazard, starts)
    survival = np.exp(np.add.reduceat(np.log(survival_factor), starts))
    if len(fill) != rows:
        raise RuntimeError("risk expansion omitted prediction rows")
    return fill, ack, survival


def _role_scales(
    frame: pd.DataFrame,
    phase: str,
    model: CauseSpecificRateModel,
    horizons_ms: Sequence[int],
    *,
    minimum_events: int,
    log_scale_bound: float,
) -> dict[str, dict[str, float]]:
    role_data: dict[str, dict[str, Any]] = {}
    for role in ("opener", "add", "reducing"):
        role_frame = frame.loc[
            frame["inventory_role"].astype(str).str.lower().eq(role)
        ].reset_index(drop=True)
        if role_frame.empty:
            raise RuntimeError(f"calibration lacks role={role}")
        hazards: list[tuple[np.ndarray, np.ndarray]] = []
        targets: list[tuple[np.ndarray, np.ndarray]] = []
        for horizon in horizons_ms:
            known, fill_target, ack_target = _known_targets(
                role_frame, phase, int(horizon)
            )
            duration = _prediction_duration_ms(role_frame, phase, int(horizon))
            bundle = _interval_rates(model, role_frame, duration)
            fill_h, ack_h = _integrated_hazard(bundle, len(role_frame))
            hazards.append((fill_h[known], ack_h[known]))
            targets.append((fill_target[known], ack_target[known]))
        fill_h = np.concatenate([value[0] for value in hazards])
        ack_h = np.concatenate([value[1] for value in hazards])
        fill_target = np.concatenate([value[0] for value in targets])
        ack_target = np.concatenate([value[1] for value in targets])
        unique_fill_events = int(
            (
                _event_kind(role_frame, phase) == EVENT_FILL
            ).sum()
        )
        unique_ack_events = int(
            (
                _event_kind(role_frame, phase) == EVENT_ACK
            ).sum()
        )
        role_data[role] = {
            "fill_h": fill_h,
            "ack_h": ack_h,
            "fill_target": fill_target,
            "ack_target": ack_target,
            "unique_fill_events": unique_fill_events,
            "unique_ack_events": unique_ack_events,
        }

    def event_log_loss(
        fill_scaled: np.ndarray,
        ack_scaled: np.ndarray,
        fill_target: np.ndarray,
        ack_target: np.ndarray,
    ) -> float:
        total = fill_scaled + ack_scaled
        any_probability = 1.0 - np.exp(-total)
        fill_probability = np.divide(
            any_probability * fill_scaled,
            total,
            out=np.zeros_like(total),
            where=total > 0.0,
        )
        ack_probability = np.divide(
            any_probability * ack_scaled,
            total,
            out=np.zeros_like(total),
            where=total > 0.0,
        )
        no_event = np.clip(
            1.0 - fill_probability - ack_probability, 1e-9, 1.0
        )
        probability = np.clip(
            np.where(
                fill_target == 1,
                fill_probability,
                np.where(ack_target == 1, ack_probability, no_event),
            ),
            1e-9,
            1.0,
        )
        return float(-np.log(probability).mean())

    output: dict[str, dict[str, float]] = {}
    if phase == "pending":
        pooled_fill_events = sum(
            int(value["unique_fill_events"]) for value in role_data.values()
        )
        if pooled_fill_events < int(minimum_events):
            raise RuntimeError("calibration lacks side-pooled pending fill events")
        for role, value in role_data.items():
            if int(value["unique_ack_events"]) < int(minimum_events):
                raise RuntimeError(f"calibration lacks {role} ACK events")

        def pending_objective(parameters: np.ndarray) -> float:
            losses: list[float] = []
            weights: list[int] = []
            fill_scale = float(np.exp(parameters[0]))
            for role_index, role in enumerate(("opener", "add", "reducing")):
                value = role_data[role]
                loss = event_log_loss(
                    fill_scale * value["fill_h"],
                    float(np.exp(parameters[role_index + 1])) * value["ack_h"],
                    value["fill_target"],
                    value["ack_target"],
                )
                losses.append(loss)
                weights.append(len(value["fill_target"]))
            return float(np.average(losses, weights=weights))

        result = minimize(
            pending_objective,
            np.zeros(4),
            method="L-BFGS-B",
            bounds=[(-log_scale_bound, log_scale_bound)] * 4,
        )
        if not bool(result.success):
            raise RuntimeError(f"pending partial-pooling calibrator failed: {result.message}")
        for role_index, role in enumerate(("opener", "add", "reducing")):
            value = role_data[role]
            output[role] = {
                "fill": float(np.exp(result.x[0])),
                "ack": float(np.exp(result.x[role_index + 1])),
                "rows": int(len(value["fill_target"])),
                "fill_events": int(value["unique_fill_events"]),
                "ack_events": int(value["unique_ack_events"]),
                "fill_calibration_pool": "side_phase",
                "ack_calibration_pool": "side_phase_role",
            }
        return output

    for role, value in role_data.items():
        if int(value["unique_fill_events"]) < int(minimum_events):
            raise RuntimeError(f"calibration lacks {role} pre-request fill events")

        def objective(
            parameters: np.ndarray,
            fill_h: np.ndarray = value["fill_h"],
            fill_target: np.ndarray = value["fill_target"],
        ) -> float:
            fill_scaled = np.exp(parameters[0]) * fill_h
            return event_log_loss(
                fill_scaled,
                np.zeros_like(fill_scaled),
                fill_target,
                np.zeros_like(fill_target),
            )

        result = minimize(
            objective,
            np.zeros(1),
            method="L-BFGS-B",
            bounds=[(-log_scale_bound, log_scale_bound)],
        )
        if not bool(result.success):
            raise RuntimeError(f"pre-request {role} calibrator failed: {result.message}")
        output[role] = {
            "fill": float(np.exp(result.x[0])),
            "ack": 1.0,
            "rows": int(len(value["fill_target"])),
            "fill_events": int(value["unique_fill_events"]),
            "ack_events": 0,
            "fill_calibration_pool": "side_phase_role",
            "ack_calibration_pool": "not_applicable",
        }
    return output


def _scale_array(
    frame: pd.DataFrame,
    calibrators: Mapping[str, Mapping[str, float]],
    cause: str,
) -> np.ndarray:
    role = frame["inventory_role"].astype(str).str.lower()
    return role.map(
        {name: float(value[cause]) for name, value in calibrators.items()}
    ).to_numpy(float)


def _constant_rates(
    frame: pd.DataFrame,
    phase: str,
) -> dict[tuple[str, str], tuple[float, float]]:
    duration = _observed_duration_ms(frame, phase) / 1_000.0
    kind = _event_kind(frame, phase)
    output: dict[tuple[str, str], tuple[float, float]] = {}
    group_frame = frame.assign(_duration=duration, _kind=kind)
    for (role, action), group in group_frame.groupby(
        ["inventory_role", "action"], observed=True
    ):
        exposure = max(float(group["_duration"].sum()), 1e-6)
        fill = (float((group["_kind"] == EVENT_FILL).sum()) + 0.5) / exposure
        ack = (
            (float((group["_kind"] == EVENT_ACK).sum()) + 0.5) / exposure
            if phase == "pending"
            else 0.0
        )
        output[(str(role).lower(), str(action))] = (fill, ack)
    return output


def _baseline_probabilities(
    frame: pd.DataFrame,
    duration_ms: np.ndarray,
    rates: Mapping[tuple[str, str], tuple[float, float]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    keys = list(
        zip(
            frame["inventory_role"].astype(str).str.lower(),
            frame["action"].astype(str),
            strict=True,
        )
    )
    fill_rate = np.asarray([rates[key][0] for key in keys], dtype=float)
    ack_rate = np.asarray([rates[key][1] for key in keys], dtype=float)
    total = fill_rate + ack_rate
    any_probability = 1.0 - np.exp(-total * duration_ms / 1_000.0)
    fill = np.divide(
        any_probability * fill_rate,
        total,
        out=np.zeros_like(total),
        where=total > 0.0,
    )
    ack = np.divide(
        any_probability * ack_rate,
        total,
        out=np.zeros_like(total),
        where=total > 0.0,
    )
    return fill, ack, np.clip(1.0 - fill - ack, 0.0, 1.0)


def _predict_horizon(
    frame: pd.DataFrame,
    phase: str,
    model: CauseSpecificRateModel,
    calibrators: Mapping[str, Mapping[str, float]],
    baseline_rates: Mapping[tuple[str, str], tuple[float, float]],
    horizon_ms: int,
    fold: int,
) -> pd.DataFrame:
    known, fill_target, ack_target = _known_targets(frame, phase, horizon_ms)
    selected = frame.loc[known].reset_index(drop=True)
    duration = np.minimum(
        _prediction_duration_ms(selected, phase, horizon_ms),
        float(model.bin_edges_ms[-1]),
    )
    bundle = _interval_rates(model, selected, duration)
    fill, ack, survival = _probabilities_from_rates(
        bundle,
        len(selected),
        _scale_array(selected, calibrators, "fill"),
        _scale_array(selected, calibrators, "ack"),
    )
    baseline_fill, baseline_ack, baseline_survival = _baseline_probabilities(
        selected, duration, baseline_rates
    )
    target_fill = fill_target[known]
    target_ack = ack_target[known]
    target_survival = 1 - target_fill - target_ack
    output = selected.loc[:, IDENTITY_COLUMNS].copy()
    output["fold"] = int(fold)
    output["phase"] = phase
    output["horizon_ms"] = int(horizon_ms)
    output["fill_target"] = target_fill
    output["ack_target"] = target_ack
    output["no_event_target"] = target_survival
    output["fill_probability"] = fill
    output["ack_probability"] = ack
    output["no_event_probability"] = survival
    output["baseline_fill_probability"] = baseline_fill
    output["baseline_ack_probability"] = baseline_ack
    output["baseline_no_event_probability"] = baseline_survival
    return output


def _bootstrap_mean(values: np.ndarray, *, samples: int, seed: int) -> dict[str, float]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return {"mean": math.nan, "lower": math.nan, "upper": math.nan}
    rng = np.random.default_rng(int(seed))
    draws = rng.choice(finite, size=(int(samples), finite.size), replace=True).mean(axis=1)
    return {
        "mean": float(finite.mean()),
        "lower": float(np.quantile(draws, 0.025)),
        "upper": float(np.quantile(draws, 0.975)),
    }


def _daily_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (day, side, role, phase, horizon), group in predictions.groupby(
        ["day", "side", "inventory_role", "phase", "horizon_ms"],
        observed=True,
    ):
        causes = ("fill", "ack") if phase == "pending" else ("fill",)
        for name in causes:
            target = group[f"{name}_target"].to_numpy(float)
            probability = group[f"{name}_probability"].to_numpy(float)
            baseline = group[f"baseline_{name}_probability"].to_numpy(float)
            rows.append(
                {
                    "day": str(day),
                    "side": str(side),
                    "inventory_role": str(role),
                    "phase": str(phase),
                    "cause": name,
                    "horizon_ms": int(horizon),
                    "rows": int(len(group)),
                    "events": int(target.sum()),
                    "brier_improvement": float(
                        np.mean((target - baseline) ** 2 - (target - probability) ** 2)
                    ),
                    "calibration_bias": float(np.mean(probability - target)),
                }
            )
        if phase == "pending":
            target = group[["fill_target", "ack_target", "no_event_target"]].to_numpy(float)
            probability = group[["fill_probability", "ack_probability", "no_event_probability"]].to_numpy(float)
            baseline = group[["baseline_fill_probability", "baseline_ack_probability", "baseline_no_event_probability"]].to_numpy(float)
            rows.append(
                {
                    "day": str(day),
                    "side": str(side),
                    "inventory_role": str(role),
                    "phase": str(phase),
                    "cause": "joint",
                    "horizon_ms": int(horizon),
                    "rows": int(len(group)),
                    "events": int((target[:, :2].sum(axis=1) > 0).sum()),
                    "brier_improvement": float(
                        np.mean(np.square(target - baseline).sum(axis=1) - np.square(target - probability).sum(axis=1))
                    ),
                    "calibration_bias": 0.0,
                }
            )
    return pd.DataFrame(rows)


def _curve_gate(
    daily: pd.DataFrame,
    *,
    spec: Mapping[str, Any],
) -> list[dict[str, Any]]:
    gate = spec["curve_gate"]
    integrated = (
        daily.groupby(
            ["day", "side", "inventory_role", "phase", "cause"],
            observed=True,
        )
        .agg(
            rows=("rows", "sum"),
            events=("events", "max"),
            brier_improvement=("brier_improvement", "mean"),
            calibration_bias=("calibration_bias", "mean"),
        )
        .reset_index()
    )
    output: list[dict[str, Any]] = []
    for keys, group in integrated.groupby(
        ["side", "inventory_role", "phase", "cause"], observed=True
    ):
        side, role, phase, cause = keys
        brier = _bootstrap_mean(
            group["brier_improvement"].to_numpy(),
            samples=int(gate["bootstrap_samples"]),
            seed=int(gate["bootstrap_seed"]) + len(output) * 10,
        )
        calibration = _bootstrap_mean(
            group["calibration_bias"].to_numpy(),
            samples=int(gate["bootstrap_samples"]),
            seed=int(gate["bootstrap_seed"]) + len(output) * 10 + 1,
        )
        minimum_events = int(
            gate["minimum_events"][
                "pending_fill" if phase == "pending" and cause == "fill" else cause
            ]
        )
        support = bool(
            group["day"].nunique() == int(gate["required_oof_days"])
            and int(group["events"].sum()) >= minimum_events
        )
        proper = bool(brier["lower"] > 0.0)
        calibrated = bool(
            cause == "joint"
            or calibration["lower"] <= 0.0 <= calibration["upper"]
        )
        output.append(
            {
                "side": str(side),
                "inventory_role": str(role),
                "phase": str(phase),
                "cause": str(cause),
                "days": int(group["day"].nunique()),
                "events": int(group["events"].sum()),
                "brier_improvement": brier,
                "calibration_bias": calibration,
                "support_pass": support,
                "proper_score_pass": proper,
                "calibration_pass": calibrated,
                "curve_pass": bool(support and proper and calibrated),
            }
        )
    return output


def _derive_horizons(frame: pd.DataFrame, quantiles: Sequence[float]) -> dict[str, list[int]]:
    output: dict[str, list[int]] = {}
    for phase in ("pre_request", "pending"):
        phase_frame = _phase_frame(frame, phase)
        duration = (
            _pre_scheduled_duration_ms(phase_frame)
            if phase == "pre_request"
            else _observed_duration_ms(phase_frame, phase)
        )
        finite = duration[np.isfinite(duration) & (duration > 0.0)]
        values = sorted({max(1, int(round(value))) for value in np.quantile(finite, quantiles)})
        output[phase] = values
    return output


def run(spec_path: Path, request_state_dir: Path, output_dir: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    request_state_dir = request_state_dir.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    source_manifest = request_state_dir / "manifest.json"
    source_index = request_state_dir / "development_index.csv"
    if source_manifest.resolve() != Path(
        spec["source_identity"]["request_state_manifest"]["path"]
    ).expanduser().resolve():
        raise RuntimeError("request-state directory differs from frozen fit source")
    index = pd.read_csv(source_index, dtype={"day": str})
    days = list(spec["panels"]["development_days"])
    folds = make_expanding_folds(
        days,
        min_train_days=int(spec["outer_folds"]["minimum_train_days"]),
        embargo_days=int(spec["outer_folds"]["embargo_days"]),
        test_days=int(spec["outer_folds"]["test_days"]),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = output_dir / "code_checkpoint"
    if not checkpoint.exists():
        write_code_checkpoint(
            checkpoint,
            repo_root=ROOT,
            code_identity=git_workspace_identity(ROOT),
        )
    horizons: dict[str, list[int]] | None = None
    fold_records: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    for fold in folds:
        outer_train = list(fold["train_days"])
        calibration_days = outer_train[-int(spec["inner_calibration"]["days"]):]
        inner_embargo = outer_train[
            -int(spec["inner_calibration"]["days"])
            - int(spec["inner_calibration"]["embargo_days"]):
            -int(spec["inner_calibration"]["days"])
        ]
        base_days = outer_train[: len(outer_train) - len(calibration_days) - len(inner_embargo)]
        if max(base_days) >= min(calibration_days):
            raise RuntimeError("inner calibration is not past-only")
        base = _load_days(index, base_days)
        calibration = _load_days(index, calibration_days)
        test = _load_days(index, fold["test_days"])
        if horizons is None:
            horizons = _derive_horizons(
                base,
                spec["reporting"]["duration_quantiles"],
            )
            _atomic_json(
                {
                    "source": "first_outer_base_train_only",
                    "base_days": base_days,
                    "quantiles": spec["reporting"]["duration_quantiles"],
                    "horizons_ms": horizons,
                },
                output_dir / "frozen_report_horizons.json",
            )
        models = fit_three_phase_by_side(
            base,
            maximum_bins=int(spec["model"]["maximum_bins"]),
            random_seed=int(spec["model"]["random_seed"]) + int(fold["fold"]),
        )
        fold_predictions: list[pd.DataFrame] = []
        calibrator_identity: dict[str, Any] = {}
        for side in ("BUY", "SELL"):
            base_side = base.loc[base["side"].astype(str).eq(side)]
            calibration_side = calibration.loc[calibration["side"].astype(str).eq(side)]
            test_side = test.loc[test["side"].astype(str).eq(side)]
            calibrator_identity[side] = {}
            for phase, model_key in (
                ("pre_request", "pre_request_fill"),
                ("pending", "pending_fill_ack"),
            ):
                base_phase = _phase_frame(base_side, phase)
                calibration_phase = _phase_frame(calibration_side, phase)
                test_phase = _phase_frame(test_side, phase)
                phase_model = models[side][model_key]
                calibrators = _role_scales(
                    calibration_phase,
                    phase,
                    phase_model,
                    horizons[phase],
                    minimum_events=int(spec["inner_calibration"]["minimum_events_per_role_cause"]),
                    log_scale_bound=float(spec["inner_calibration"]["absolute_log_scale_bound"]),
                )
                calibrator_identity[side][phase] = calibrators
                baseline_rates = _constant_rates(base_phase, phase)
                for horizon in horizons[phase]:
                    fold_predictions.append(
                        _predict_horizon(
                            test_phase,
                            phase,
                            phase_model,
                            calibrators,
                            baseline_rates,
                            int(horizon),
                            int(fold["fold"]),
                        )
                    )
        predictions = pd.concat(fold_predictions, ignore_index=True)
        simplex = predictions[["fill_probability", "ack_probability", "no_event_probability"]].sum(axis=1)
        if not bool(np.allclose(simplex, 1.0, atol=float(spec["curve_gate"]["simplex_tolerance"]))):
            raise RuntimeError("OOF probabilities violate the simplex")
        fold_path = output_dir / f"fold_{int(fold['fold']):02d}_oof.parquet"
        predictions.to_parquet(fold_path, index=False, compression="zstd")
        daily = _daily_metrics(predictions)
        daily_parts.append(daily)
        fold_records.append(
            {
                "fold": int(fold["fold"]),
                "base_days": base_days,
                "inner_embargo_days": inner_embargo,
                "calibration_days": calibration_days,
                "outer_embargo_days": list(fold["embargo_days"]),
                "test_days": list(fold["test_days"]),
                "rows": int(len(predictions)),
                "output": _identity(fold_path),
                "calibrators": calibrator_identity,
            }
        )
        del base, calibration, test, models, predictions, fold_predictions
        gc.collect()
    if horizons is None:
        raise RuntimeError("no chronological folds were evaluated")
    daily = pd.concat(daily_parts, ignore_index=True)
    daily_path = output_dir / "daily_curve_metrics.parquet"
    daily.to_parquet(daily_path, index=False, compression="zstd")
    curves = _curve_gate(daily, spec=spec)
    required = [row for row in curves if row["cause"] != "joint"]
    joint = [row for row in curves if row["cause"] == "joint"]
    side_pass = {
        side: bool(
            all(row["curve_pass"] for row in required if row["side"] == side)
            and all(row["curve_pass"] for row in joint if row["side"] == side)
        )
        for side in ("BUY", "SELL")
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "status": "development_complete",
        "spec": _identity(spec_path),
        "request_state_manifest": _identity(source_manifest),
        "request_state_index": _identity(source_index),
        "folds": fold_records,
        "report_horizons_ms": horizons,
        "daily_metrics": _identity(daily_path),
        "curves": curves,
        "prediction_gate_passed_sides": [side for side, passed in side_pass.items() if passed],
        "side_gate": side_pass,
        "validation_access_allowed": bool(any(side_pass.values())),
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "decision": (
            "freeze_passing_side_before_validation"
            if any(side_pass.values())
            else "close_prediction_family_on_development"
        ),
        "git": git_workspace_identity(ROOT),
    }
    report["report_identity_sha256"] = canonical_sha256(report)
    _atomic_json(report, output_dir / "report.json")
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--request-state-dir", type=Path, default=DEFAULT_REQUEST_STATE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = run(args.spec, args.request_state_dir, args.output_dir)
    print(
        json.dumps(
            {
                "decision": report["decision"],
                "prediction_gate_passed_sides": report["prediction_gate_passed_sides"],
                "validation_access_allowed": report["validation_access_allowed"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
