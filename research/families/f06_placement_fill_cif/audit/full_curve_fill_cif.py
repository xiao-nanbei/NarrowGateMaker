#!/usr/bin/env python3
"""Fit a full placement fill-before-cancel-ACK cumulative incidence curve.

The training row is an action lifecycle, not an action-by-fixed-horizon label.
An active order contributes sampled 100ms risk intervals until first fill,
cancel ACK, rejection, or administrative censoring.  Fixed horizons are report
cuts only.  KEEP, REPLACE, and campaign repair deliberately remain outside this
placement estimand.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.special import expit, logit
from sklearn.metrics import average_precision_score, roc_auc_score

from data_paths import data_root
from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.direct_fill_cif import make_expanding_folds
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import ACTION_ORDER
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)
from research.governance.historical_reproduction import (
    add_historical_reproduction_argument,
    require_historical_reproduction,
    stamp_historical_reproduction_output,
    verify_frozen_source_identity,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = FAMILY_DOCS / "placement_fill_full_curve_cif_v1_spec_20260727.json"
DEFAULT_OUTPUT = DATA_ROOT / "reports" / "placement_fill_full_curve_cif_v1_development_20260727"

SCHEMA_VERSION = "placement_fill_full_curve_cif.v1"
MODEL_KIND = "side_specific_sampled_discrete_time_hazard"
TICK_SIZE = 0.1

STATIC_SOURCE_COLUMNS = (
    "cohort_id",
    "day",
    "side",
    "inventory_role",
    "submit_ts_ns",
    "feature_ready_ts_ns",
    "observation_end_ts_ns",
    "best_bid",
    "best_ask",
    "inventory",
    "inventory_ratio",
    "campaign_active",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "depth_age_s",
    "sigma_sq_raw",
    "sigma_sq_blended",
    "kappa_used",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "final_pair_spread",
    "final_quote_skew",
    "allow_exposure_increase",
    "exposure_increasing",
    "side_adverse_pause",
    "defense_guard",
    "defense_pause",
    "local_extreme_pause",
    "monotonicity_violation_count",
)

ACTION_SOURCE_SUFFIXES = (
    "price_tick",
    "activation_ts_ns",
    "activation_status",
    "first_fill_ts_ns",
    "cancel_request_ts_ns",
    "cancel_ack_ts_ns",
    "terminal_ts_ns",
    "terminal_reason",
    "terminal_observed",
)

STATIC_MODEL_FEATURES = (
    "distance_ticks",
    "bbo_spread_ticks",
    "inventory_ratio",
    "campaign_active",
    "campaign_age_log1p",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "depth_age_log1p",
    "sigma_sq_raw_log1p",
    "sigma_sq_blended_log1p",
    "kappa_used",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total_log1p",
    "final_pair_spread",
    "final_quote_skew",
    "allow_exposure_increase",
    "exposure_increasing",
    "side_adverse_pause",
    "defense_guard",
    "defense_pause",
    "local_extreme_pause",
    "role_opener",
    "role_add",
    "role_reducing",
)

DYNAMIC_MODEL_FEATURES = (
    "elapsed_s",
    "log_elapsed_ms",
    "sqrt_elapsed_s",
    "distance_vol_units_elapsed",
)

MODEL_FEATURES = STATIC_MODEL_FEATURES + DYNAMIC_MODEL_FEATURES


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _numeric(frame: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce").fillna(default)


def lifecycle_input_columns() -> tuple[str, ...]:
    columns = list(STATIC_SOURCE_COLUMNS)
    for action in ACTION_ORDER:
        columns.extend(f"{action}__{suffix}" for suffix in ACTION_SOURCE_SUFFIXES)
    return tuple(columns)


def _positive_min(*arrays: np.ndarray) -> np.ndarray:
    stacked = np.column_stack(
        [np.where(np.asarray(value) > 0, value, np.iinfo(np.int64).max) for value in arrays]
    )
    result = stacked.min(axis=1)
    return np.where(result == np.iinfo(np.int64).max, 0, result)


def expand_action_lifecycles(wide: pd.DataFrame) -> pd.DataFrame:
    """Return one causal placement lifecycle per cohort and action."""

    required = set(lifecycle_input_columns())
    missing = sorted(required - set(wide.columns))
    if missing:
        raise ValueError(f"full-curve placement panel is missing columns: {missing}")
    if not bool(
        (_numeric(wide, "feature_ready_ts_ns") <= _numeric(wide, "submit_ts_ns")).all()
    ):
        raise ValueError("placement features are not ready by submit time")
    if int(_numeric(wide, "monotonicity_violation_count").sum()) != 0:
        raise ValueError("placement panel contains observed path monotonicity violations")

    parts: list[pd.DataFrame] = []
    identity = list(STATIC_SOURCE_COLUMNS)
    for action in ACTION_ORDER:
        prefix = f"{action}__"
        part = wide[identity].copy()
        for suffix in ACTION_SOURCE_SUFFIXES:
            part[suffix] = wide[f"{prefix}{suffix}"]
        part["action"] = action
        parts.append(part)
    actions = pd.concat(parts, ignore_index=True)
    actions["action_lifecycle_id"] = (
        actions["cohort_id"].astype(str) + ":" + actions["action"].astype(str)
    )

    submit = _numeric(actions, "submit_ts_ns").to_numpy(dtype=np.int64)
    activation = _numeric(actions, "activation_ts_ns").to_numpy(dtype=np.int64)
    first_fill = _numeric(actions, "first_fill_ts_ns").to_numpy(dtype=np.int64)
    cancel_ack = _numeric(actions, "cancel_ack_ts_ns").to_numpy(dtype=np.int64)
    terminal = _numeric(actions, "terminal_ts_ns").to_numpy(dtype=np.int64)
    observation_end = _numeric(actions, "observation_end_ts_ns").to_numpy(
        dtype=np.int64
    )
    active = actions["activation_status"].astype(str).eq("active").to_numpy()
    fill_before_ack = (cancel_ack <= 0) | (first_fill <= cancel_ack)
    event = active & (first_fill >= activation) & (first_fill > 0) & fill_before_ack

    nonfill_terminal = np.where(event, 0, terminal)
    risk_end = _positive_min(cancel_ack, nonfill_terminal, observation_end)
    risk_end = np.where(event, first_fill, risk_end)
    risk_valid = active & (risk_end > activation)
    event &= risk_valid

    actions["activation_observed"] = (
        activation > 0
    ).astype(np.int8)
    actions["activation_outcome"] = active.astype(np.int8)
    actions["activation_latency_ms"] = np.where(
        activation > submit, (activation - submit) / 1_000_000.0, np.nan
    ).astype(np.float32)
    actions["risk_valid"] = risk_valid.astype(np.int8)
    actions["event_observed"] = event.astype(np.int8)
    actions["event_time_ms"] = np.where(
        event, (first_fill - activation) / 1_000_000.0, np.nan
    ).astype(np.float32)
    actions["risk_end_ms"] = np.where(
        risk_valid, (risk_end - activation) / 1_000_000.0, np.nan
    ).astype(np.float32)
    actions["placement_event_time_ms"] = np.where(
        event, (first_fill - submit) / 1_000_000.0, np.nan
    ).astype(np.float32)
    actions["placement_observation_end_ms"] = np.maximum(
        0.0, (observation_end - submit) / 1_000_000.0
    ).astype(np.float32)
    terminal_observed = _numeric(actions, "terminal_observed").to_numpy(
        dtype=bool
    )
    actions["placement_nonfill_terminal_ms"] = np.where(
        (~event) & (terminal > submit) & terminal_observed,
        (terminal - submit) / 1_000_000.0,
        np.nan,
    ).astype(np.float32)
    actions["cancel_ack_active_ms"] = np.where(
        active & (cancel_ack > activation),
        (cancel_ack - activation) / 1_000_000.0,
        np.nan,
    ).astype(np.float32)
    cancel_event = (
        active
        & (~event)
        & (cancel_ack > activation)
        & (risk_end == cancel_ack)
    )
    actions["cancel_event_observed"] = cancel_event.astype(np.int8)
    actions["cancel_event_time_ms"] = np.where(
        cancel_event,
        (cancel_ack - activation) / 1_000_000.0,
        np.nan,
    ).astype(np.float32)

    side = actions["side"].astype(str).str.upper()
    price = _numeric(actions, "price_tick") * TICK_SIZE
    distance = np.where(
        side.eq("BUY"),
        (_numeric(actions, "best_bid") - price) / TICK_SIZE,
        (price - _numeric(actions, "best_ask")) / TICK_SIZE,
    )
    actions["distance_ticks"] = np.maximum(0.0, distance).astype(np.float32)
    actions["bbo_spread_ticks"] = np.maximum(
        1.0,
        (_numeric(actions, "best_ask") - _numeric(actions, "best_bid")) / TICK_SIZE,
    ).astype(np.float32)
    actions["campaign_age_log1p"] = np.log1p(
        np.maximum(0.0, _numeric(actions, "campaign_age_s"))
    ).astype(np.float32)
    actions["depth_age_log1p"] = np.log1p(
        np.maximum(0.0, _numeric(actions, "depth_age_s") * 1000.0)
    ).astype(np.float32)
    actions["sigma_sq_raw_log1p"] = np.log1p(
        np.maximum(0.0, _numeric(actions, "sigma_sq_raw"))
    ).astype(np.float32)
    actions["sigma_sq_blended_log1p"] = np.log1p(
        np.maximum(0.0, _numeric(actions, "sigma_sq_blended"))
    ).astype(np.float32)
    actions["l2_near_depth_total_log1p"] = np.log1p(
        np.maximum(0.0, _numeric(actions, "l2_near_depth_total"))
    ).astype(np.float32)
    role = actions["inventory_role"].astype(str).str.lower()
    for name in ("opener", "add", "reducing"):
        actions[f"role_{name}"] = role.eq(name).astype(np.float32)
    for column in STATIC_MODEL_FEATURES:
        actions[column] = _numeric(actions, column).astype(np.float32)
    return actions


def derive_duration_contract(
    lifecycles: pd.DataFrame,
    *,
    interval_ms: int,
    report_quantiles: Sequence[float],
    maximum_support_quantile: float,
) -> dict[str, Any]:
    """Derive report cuts and support from Development censor timing only."""

    cancel = _numeric(lifecycles, "cancel_ack_active_ms", math.nan)
    cancel = cancel[np.isfinite(cancel) & (cancel > 0.0)]
    if cancel.empty:
        raise ValueError("Development has no cancel-ACK exposure distribution")
    activation = _numeric(lifecycles, "activation_latency_ms", math.nan)
    activation = activation[np.isfinite(activation) & (activation >= 0.0)]
    quantile_values = {
        f"p{int(round(100.0 * float(q))):02d}": int(round(float(cancel.quantile(q))))
        for q in report_quantiles
    }
    raw_max = float(cancel.quantile(float(maximum_support_quantile)))
    max_support_ms = int(math.ceil(raw_max / int(interval_ms)) * int(interval_ms))
    return {
        "source": "Development active-order cancel-ACK exposure distribution",
        "rows": int(len(cancel)),
        "report_quantiles": quantile_values,
        "maximum_support_quantile": float(maximum_support_quantile),
        "maximum_support_ms": max_support_ms,
        "activation_latency_ms": {
            "p25": float(activation.quantile(0.25)),
            "p50": float(activation.quantile(0.50)),
            "p75": float(activation.quantile(0.75)),
            "p95": float(activation.quantile(0.95)),
        },
    }


def _dynamic_features(static: pd.DataFrame, elapsed_ms: np.ndarray) -> pd.DataFrame:
    elapsed_ms = np.maximum(np.asarray(elapsed_ms, dtype=np.float64), 1.0)
    elapsed_s = elapsed_ms / 1000.0
    out = static.loc[:, STATIC_MODEL_FEATURES].reset_index(drop=True).copy()
    out["elapsed_s"] = elapsed_s.astype(np.float32)
    out["log_elapsed_ms"] = np.log1p(elapsed_ms).astype(np.float32)
    out["sqrt_elapsed_s"] = np.sqrt(elapsed_s).astype(np.float32)
    variance = np.expm1(out["sigma_sq_blended_log1p"].to_numpy(dtype=float))
    expected_move = np.sqrt(np.maximum(0.0, variance) * elapsed_s)
    out["distance_vol_units_elapsed"] = (
        out["distance_ticks"].to_numpy(dtype=float)
        * TICK_SIZE
        / np.maximum(expected_move, TICK_SIZE)
    ).astype(np.float32)
    return out.loc[:, MODEL_FEATURES]


def build_sampled_risk_rows(
    lifecycles: pd.DataFrame,
    *,
    interval_ms: int,
    maximum_support_ms: int,
    maximum_negative_intervals_per_action: int,
    sampling_strategy: str = "hash_stratified_v1",
    hazard_causes: Sequence[str] = ("fill",),
) -> pd.DataFrame:
    """Build a weighted approximation to the complete person-period likelihood."""

    if interval_ms <= 0 or maximum_support_ms < interval_ms:
        raise ValueError("invalid discrete-time support")
    if maximum_negative_intervals_per_action <= 0:
        raise ValueError("maximum_negative_intervals_per_action must be positive")
    if sampling_strategy != "hash_stratified_v1":
        raise ValueError(f"unsupported risk sampling strategy={sampling_strategy!r}")
    causes = tuple(str(value) for value in hazard_causes)
    if causes not in {("fill",), ("fill", "cancel_ack")}:
        raise ValueError(f"unsupported hazard causes={causes!r}")
    risk = lifecycles.loc[_numeric(lifecycles, "risk_valid").astype(bool)].copy()
    if risk.empty:
        raise ValueError("no active placement risk rows")
    end_ms = np.minimum(
        _numeric(risk, "risk_end_ms").to_numpy(dtype=float),
        float(maximum_support_ms),
    )
    fill_time = _numeric(risk, "event_time_ms", math.nan).to_numpy(dtype=float)
    fill_event = (
        _numeric(risk, "event_observed").to_numpy(dtype=bool)
        & np.isfinite(fill_time)
        & (fill_time <= float(maximum_support_ms))
    )
    cancel_time = _numeric(
        risk, "cancel_event_time_ms", math.nan
    ).to_numpy(dtype=float)
    cancel_event = (
        ("cancel_ack" in causes)
        & _numeric(risk, "cancel_event_observed").to_numpy(dtype=bool)
        & np.isfinite(cancel_time)
        & (cancel_time <= float(maximum_support_ms))
    )
    event = fill_event | cancel_event
    event_time = np.where(fill_event, fill_time, cancel_time)
    end_bin = np.maximum(1, np.ceil(end_ms / float(interval_ms)).astype(np.int32))
    event_bin = np.zeros(len(risk), dtype=np.int32)
    event_bin[event] = np.maximum(
        1, np.ceil(event_time[event] / float(interval_ms)).astype(np.int32)
    )
    negative_count = np.where(event, event_bin - 1, end_bin).astype(np.int32)
    sample_count = np.minimum(
        negative_count, int(maximum_negative_intervals_per_action)
    ).astype(np.int32)
    hashed = pd.util.hash_pandas_object(
        risk["action_lifecycle_id"], index=False
    ).to_numpy(dtype=np.uint64, copy=True)
    hashed ^= hashed >> np.uint64(30)
    hashed *= np.uint64(0xBF58476D1CE4E5B9)
    hashed ^= hashed >> np.uint64(27)
    hashed *= np.uint64(0x94D049BB133111EB)
    hashed ^= hashed >> np.uint64(31)
    unit_offset = (hashed >> np.uint64(11)).astype(np.float64) / float(1 << 53)

    pieces: list[pd.DataFrame] = []
    for slot in range(int(maximum_negative_intervals_per_action)):
        mask = sample_count > slot
        if not bool(mask.any()):
            continue
        source = risk.loc[mask, STATIC_MODEL_FEATURES].reset_index(drop=True)
        n_negative = negative_count[mask].astype(np.float64)
        n_sample = sample_count[mask].astype(np.float64)
        interval_index = (
            np.floor(
                (float(slot) + unit_offset[mask]) * n_negative / n_sample
            ).astype(np.int32)
            + 1
        )
        part = _dynamic_features(source, interval_index * int(interval_ms))
        part["target"] = np.int8(0)
        part["sample_weight"] = (n_negative / n_sample).astype(np.float32)
        pieces.append(part)

    if bool(event.any()):
        source = risk.loc[event, STATIC_MODEL_FEATURES].reset_index(drop=True)
        part = _dynamic_features(source, event_bin[event] * int(interval_ms))
        part["target"] = np.where(fill_event[event], 1, 2).astype(np.int8)
        part["sample_weight"] = np.float32(1.0)
        pieces.append(part)
    sampled = pd.concat(pieces, ignore_index=True)
    sampled.attrs["full_negative_interval_weight"] = int(negative_count.sum())
    sampled.attrs["fill_event_intervals"] = int(fill_event.sum())
    sampled.attrs["cancel_event_intervals"] = int(cancel_event.sum())
    sampled.attrs["event_intervals"] = int(event.sum())
    return sampled


def _new_model(contract: Mapping[str, Any], *, monotone_distance: bool):
    from lightgbm import LGBMClassifier

    constraints = [
        (
            -1
            if monotone_distance
            and name in {"distance_ticks", "distance_vol_units_elapsed"}
            else 0
        )
        for name in MODEL_FEATURES
    ]
    return LGBMClassifier(
        objective="binary",
        learning_rate=float(contract["learning_rate"]),
        n_estimators=int(contract["n_estimators"]),
        num_leaves=int(contract["num_leaves"]),
        max_depth=int(contract["max_depth"]),
        min_child_samples=int(contract["min_child_samples"]),
        reg_lambda=float(contract["reg_lambda"]),
        max_bin=int(contract["max_bin"]),
        random_state=int(contract["random_state"]),
        n_jobs=int(contract.get("n_jobs", 4)),
        verbosity=-1,
        monotone_constraints=constraints,
    )


def fit_hazard_model(rows: pd.DataFrame, contract: Mapping[str, Any]):
    target = rows["target"].to_numpy(dtype=np.int8)
    causes = tuple(str(value) for value in contract.get("hazard_causes", ["fill"]))
    if not bool((target == 1).any()) or not bool((target == 0).any()):
        raise ValueError("hazard training requires fill and non-event intervals")

    def fit_binary(binary_target: np.ndarray, *, monotone_distance: bool):
        model = _new_model(contract, monotone_distance=monotone_distance)
        model.fit(
            rows.loc[:, MODEL_FEATURES],
            binary_target,
            sample_weight=rows["sample_weight"].to_numpy(dtype=float),
        )
        return model

    fill_model = fit_binary((target == 1).astype(np.int8), monotone_distance=True)
    if causes == ("fill",):
        return fill_model
    if causes != ("fill", "cancel_ack") or not bool((target == 2).any()):
        raise ValueError("fill/cancel hazard training lacks cancel-ACK events")
    cancel_model = fit_binary(
        (target == 2).astype(np.int8), monotone_distance=False
    )
    return {"fill": fill_model, "cancel_ack": cancel_model}


def _adjusted_binary_hazard(
    model: Any, rows: pd.DataFrame, offset: float
) -> np.ndarray:
    raw = np.clip(
        model.predict_proba(rows.loc[:, MODEL_FEATURES])[:, 1],
        1e-7,
        1.0 - 1e-7,
    )
    return np.clip(expit(logit(raw) + float(offset)), 1e-7, 1.0 - 1e-7)


def _hazard_probabilities(
    model: Any,
    rows: pd.DataFrame,
    offset: float | Mapping[str, float],
) -> tuple[np.ndarray, np.ndarray]:
    if isinstance(model, Mapping):
        if not isinstance(offset, Mapping):
            raise TypeError("competing-risk hazard requires cause-specific offsets")
        fill = _adjusted_binary_hazard(
            model["fill"], rows, float(offset["fill"])
        )
        cancel = _adjusted_binary_hazard(
            model["cancel_ack"], rows, float(offset["cancel_ack"])
        )
        total = fill + cancel
        scale = np.maximum(1.0, total / (1.0 - 1e-7))
        return fill / scale, cancel / scale
    if isinstance(offset, Mapping):
        raise TypeError("single-cause hazard received cause-specific offsets")
    fill = _adjusted_binary_hazard(model, rows, float(offset))
    return fill, np.zeros_like(fill)


def _fit_binary_hazard_offset(
    model: Any, calibration_rows: pd.DataFrame, target: np.ndarray
) -> float:
    raw = np.clip(
        model.predict_proba(calibration_rows.loc[:, MODEL_FEATURES])[:, 1],
        1e-7,
        1.0 - 1e-7,
    )
    target = np.asarray(target, dtype=float)
    weight = calibration_rows["sample_weight"].to_numpy(dtype=float)
    score = logit(raw)

    def residual(offset: float) -> float:
        return float(np.sum(weight * (expit(score + offset) - target)))

    return float(brentq(residual, -20.0, 20.0))


def fit_hazard_offset(
    model: Any, calibration_rows: pd.DataFrame
) -> float | dict[str, float]:
    target = calibration_rows["target"].to_numpy(dtype=np.int8)
    if isinstance(model, Mapping):
        return {
            "fill": _fit_binary_hazard_offset(
                model["fill"], calibration_rows, (target == 1).astype(float)
            ),
            "cancel_ack": _fit_binary_hazard_offset(
                model["cancel_ack"],
                calibration_rows,
                (target == 2).astype(float),
            ),
        }
    return _fit_binary_hazard_offset(
        model, calibration_rows, (target == 1).astype(float)
    )


def fit_activation_contract(lifecycles: pd.DataFrame) -> dict[str, Any]:
    observed = lifecycles.loc[_numeric(lifecycles, "activation_observed").astype(bool)]
    if observed.empty:
        raise ValueError("activation contract has no observed placements")
    side_rates = observed.groupby("side", observed=True)["activation_outcome"].mean()
    cells: dict[str, dict[str, float]] = {}
    for (side, role, action), group in observed.groupby(
        ["side", "inventory_role", "action"], observed=True
    ):
        side_rate = float(side_rates.loc[side])
        active = float(group["activation_outcome"].sum())
        probability = (active + 200.0 * side_rate) / (len(group) + 200.0)
        latency = _numeric(
            group.loc[group["activation_outcome"].astype(bool)],
            "activation_latency_ms",
            math.nan,
        )
        latency = latency[np.isfinite(latency) & (latency >= 0.0)]
        cells[f"{side}|{str(role).lower()}|{action}"] = {
            "probability": float(probability),
            "latency_p50_ms": float(latency.quantile(0.50)),
            "latency_p95_ms": float(latency.quantile(0.95)),
            "rows": int(len(group)),
            "active": int(active),
        }
    return {"smoothing_prior_rows": 200, "cells": cells}


def _activation_values(
    lifecycles: pd.DataFrame,
    contract: Mapping[str, Any],
    *,
    latency_key: str,
) -> tuple[np.ndarray, np.ndarray]:
    cells = contract["cells"]
    probabilities: list[float] = []
    latencies: list[float] = []
    for side, role, action in zip(
        lifecycles["side"],
        lifecycles["inventory_role"],
        lifecycles["action"],
        strict=False,
    ):
        cell = cells[f"{side}|{str(role).lower()}|{action}"]
        probabilities.append(float(cell["probability"]))
        latencies.append(float(cell[latency_key]))
    return np.asarray(probabilities), np.asarray(latencies)


def predict_cif_at_horizons(
    model: Any,
    lifecycles: pd.DataFrame,
    horizons_ms: Sequence[int],
    *,
    activation_contract: Mapping[str, Any],
    hazard_offset: float | Mapping[str, float],
    interval_ms: int,
    maximum_support_ms: int,
    chunk_size: int = 5000,
    activation_latency_key: str = "latency_p50_ms",
) -> pd.DataFrame:
    """Predict decision-time fill CIF at arbitrary report horizons."""

    horizons = sorted({int(value) for value in horizons_ms})
    if not horizons or horizons[0] <= 0:
        raise ValueError("CIF horizons must be positive")
    if horizons[-1] > maximum_support_ms:
        raise ValueError("requested CIF horizon exceeds frozen model support")
    outputs: list[pd.DataFrame] = []
    for start in range(0, len(lifecycles), int(chunk_size)):
        chunk = lifecycles.iloc[start : start + int(chunk_size)].reset_index(drop=True)
        activation_probability, activation_latency = _activation_values(
            chunk, activation_contract, latency_key=activation_latency_key
        )
        maximum_active_ms = np.maximum(0.0, float(horizons[-1]) - activation_latency)
        maximum_bins = int(math.ceil(float(maximum_active_ms.max()) / interval_ms))
        if maximum_bins <= 0:
            conditional = np.zeros((len(chunk), len(horizons)), dtype=float)
        else:
            repeated = chunk.loc[chunk.index.repeat(maximum_bins), STATIC_MODEL_FEATURES]
            elapsed = np.tile(
                np.arange(1, maximum_bins + 1, dtype=np.int32) * int(interval_ms),
                len(chunk),
            )
            dynamic = _dynamic_features(repeated.reset_index(drop=True), elapsed)
            fill_flat, cancel_flat = _hazard_probabilities(
                model, dynamic, hazard_offset
            )
            fill_hazard = fill_flat.reshape(len(chunk), maximum_bins)
            cancel_hazard = cancel_flat.reshape(len(chunk), maximum_bins)
            no_event = np.clip(
                1.0 - fill_hazard - cancel_hazard, 1e-7, 1.0
            )
            survival_after = np.cumprod(no_event, axis=1)
            survival_before = np.column_stack(
                [np.ones(len(chunk), dtype=float), survival_after[:, :-1]]
            )
            fill_cif_after = np.cumsum(
                survival_before * fill_hazard, axis=1
            )
            conditional_columns: list[np.ndarray] = []
            for horizon in horizons:
                active_ms = np.maximum(0.0, float(horizon) - activation_latency)
                full_bins = np.floor(active_ms / interval_ms).astype(np.int32)
                remainder = active_ms - full_bins * int(interval_ms)
                cif = np.zeros(len(chunk), dtype=float)
                survival = np.ones(len(chunk), dtype=float)
                full_mask = full_bins > 0
                if bool(full_mask.any()):
                    row_index = np.flatnonzero(full_mask)
                    cif[full_mask] = fill_cif_after[
                        np.flatnonzero(full_mask), full_bins[full_mask] - 1
                    ]
                    survival[full_mask] = survival_after[
                        row_index, full_bins[full_mask] - 1
                    ]
                partial_mask = (remainder > 0.0) & (full_bins < maximum_bins)
                if bool(partial_mask.any()):
                    row_index = np.flatnonzero(partial_mask)
                    column_index = full_bins[partial_mask]
                    next_fill = fill_hazard[row_index, column_index]
                    next_cancel = cancel_hazard[row_index, column_index]
                    total = next_fill + next_cancel
                    fraction = remainder[partial_mask] / float(interval_ms)
                    partial_event = 1.0 - np.power(
                        np.clip(1.0 - total, 1e-7, 1.0), fraction
                    )
                    fill_share = np.divide(
                        next_fill,
                        total,
                        out=np.ones_like(next_fill),
                        where=total > 0.0,
                    )
                    cif[partial_mask] += (
                        survival[partial_mask] * partial_event * fill_share
                    )
                conditional_columns.append(cif)
            conditional = np.column_stack(conditional_columns)
        probability = activation_probability[:, None] * conditional
        base = chunk[
            [
                "action_lifecycle_id",
                "cohort_id",
                "day",
                "side",
                "inventory_role",
                "action",
            ]
        ].copy()
        for index, horizon in enumerate(horizons):
            part = base.copy()
            part["horizon_ms"] = int(horizon)
            part["probability"] = probability[:, index].astype(np.float32)
            outputs.append(part)
    return pd.concat(outputs, ignore_index=True)


def lifecycle_labels_at_horizons(
    lifecycles: pd.DataFrame, horizons_ms: Sequence[int]
) -> pd.DataFrame:
    outputs: list[pd.DataFrame] = []
    event_time = _numeric(lifecycles, "placement_event_time_ms", math.nan).to_numpy(
        dtype=float
    )
    terminal_time = _numeric(
        lifecycles, "placement_nonfill_terminal_ms", math.nan
    ).to_numpy(dtype=float)
    observation_end = _numeric(
        lifecycles, "placement_observation_end_ms"
    ).to_numpy(dtype=float)
    base = lifecycles[
        [
            "action_lifecycle_id",
            "cohort_id",
            "day",
            "side",
            "inventory_role",
            "action",
        ]
    ].copy()
    for horizon in sorted({int(value) for value in horizons_ms}):
        event = np.isfinite(event_time) & (event_time <= float(horizon))
        terminal = np.isfinite(terminal_time) & (terminal_time <= float(horizon))
        observed = event | terminal | (observation_end >= float(horizon))
        part = base.loc[observed].copy()
        part["horizon_ms"] = int(horizon)
        part["target"] = event[observed].astype(np.int8)
        outputs.append(part)
    return pd.concat(outputs, ignore_index=True)


def _baseline_rates(
    train: pd.DataFrame, horizons_ms: Sequence[int]
) -> dict[str, float]:
    labels = lifecycle_labels_at_horizons(train, horizons_ms)
    rates: dict[str, float] = {}
    side_rate = labels.groupby(["side", "horizon_ms"], observed=True)["target"].mean()
    for (side, role, action, horizon), group in labels.groupby(
        ["side", "inventory_role", "action", "horizon_ms"], observed=True
    ):
        prior = float(side_rate.loc[(side, horizon)])
        rates[f"{side}|{str(role).lower()}|{action}|{int(horizon)}"] = float(
            (group["target"].sum() + 200.0 * prior) / (len(group) + 200.0)
        )
    return rates


def _apply_baseline(frame: pd.DataFrame, rates: Mapping[str, float]) -> np.ndarray:
    return np.asarray(
        [
            float(rates[f"{side}|{str(role).lower()}|{action}|{int(horizon)}"])
            for side, role, action, horizon in zip(
                frame["side"],
                frame["inventory_role"],
                frame["action"],
                frame["horizon_ms"],
                strict=False,
            )
        ],
        dtype=float,
    )


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    target = group["target"].to_numpy(dtype=float)
    probability = group["probability"].to_numpy(dtype=float)
    baseline = group["baseline_probability"].to_numpy(dtype=float)
    brier = float(np.mean((target - probability) ** 2))
    baseline_brier = float(np.mean((target - baseline) ** 2))
    result: dict[str, Any] = {
        "rows": int(len(group)),
        "days": int(group["day"].nunique()),
        "events": int(target.sum()),
        "observed_rate": float(target.mean()),
        "predicted_rate": float(probability.mean()),
        "brier": brier,
        "baseline_brier": baseline_brier,
        "brier_improvement": baseline_brier - brier,
        "average_precision": float(average_precision_score(target, probability)),
    }
    result["roc_auc"] = (
        float(roc_auc_score(target, probability))
        if np.unique(target).size == 2
        else math.nan
    )
    return result


def _load_spec(path: Path) -> dict[str, Any]:
    payload = load_placement_fill_spec(path)
    expected_status = {
        "narrowgate_placement_fill_full_curve_spec.v1": (
            "frozen_before_full_curve_development_fit"
        ),
        "narrowgate_placement_fill_full_curve_spec.v2": (
            "frozen_before_v2_full_curve_development_fit"
        ),
        "narrowgate_placement_fill_full_curve_spec.v3": (
            "frozen_before_v3_competing_risk_development_fit"
        ),
    }.get(str(payload.get("schema_version")))
    if expected_status is None:
        raise RuntimeError("unsupported full-curve placement spec")
    if payload.get("research_status") != expected_status:
        raise RuntimeError("full-curve placement spec is not frozen")
    verify_frozen_source_identity(
        str(payload["lineage"]["implementation"]),
        str(payload["lineage"]["implementation_sha256"]),
    )
    return payload


def _load_partitions(spec: Mapping[str, Any]) -> pd.DataFrame:
    days = [str(day) for day in spec["panels"]["development"]["days"]]
    roots = spec["source_identity"]["placement_panel_roots"]
    by_day: dict[str, Path] = {}
    for source in roots:
        root = Path(str(source["path"])).expanduser().resolve()
        manifest = Path(str(source["manifest"])).expanduser().resolve()
        if _sha256(manifest) != str(source["manifest_sha256"]):
            raise RuntimeError(f"placement panel manifest changed: {manifest}")
        for path in root.glob("partitions/day=*/placement.parquet"):
            day = path.parent.name.removeprefix("day=")
            if day in by_day:
                raise RuntimeError(f"duplicate placement partition for {day}")
            by_day[day] = path
    missing = sorted(set(days) - set(by_day))
    if missing:
        raise FileNotFoundError(f"missing Development placement partitions: {missing}")
    frames = [pd.read_parquet(by_day[day], columns=lifecycle_input_columns()) for day in days]
    return pd.concat(frames, ignore_index=True)


def _fit_side(
    train: pd.DataFrame,
    *,
    spec: Mapping[str, Any],
    maximum_support_ms: int,
) -> tuple[Any, float | dict[str, float], dict[str, Any]]:
    contract = spec["development_fit"]
    days = sorted(train["day"].astype(str).unique())
    calibration_days = int(contract["hazard_calibration_days"])
    if len(days) <= calibration_days:
        raise ValueError("hazard fit lacks pre-calibration days")
    fit_days = days[:-calibration_days]
    calibration = days[-calibration_days:]
    kwargs = {
        "interval_ms": int(contract["risk_interval_ms"]),
        "maximum_support_ms": int(maximum_support_ms),
        "maximum_negative_intervals_per_action": int(
            contract["maximum_negative_intervals_per_action"]
        ),
        "sampling_strategy": str(contract["risk_sampling"]),
        "hazard_causes": tuple(contract.get("hazard_causes", ["fill"])),
    }
    fit_rows = build_sampled_risk_rows(
        train.loc[train["day"].isin(fit_days)], **kwargs
    )
    model_contract = {
        **contract["model"],
        "hazard_causes": list(contract.get("hazard_causes", ["fill"])),
    }
    model = fit_hazard_model(fit_rows, model_contract)
    calibration_rows = build_sampled_risk_rows(
        train.loc[train["day"].isin(calibration)], **kwargs
    )
    offset = fit_hazard_offset(model, calibration_rows)
    identity = {
        "fit_days": fit_days,
        "calibration_days": calibration,
        "fit_sampled_rows": int(len(fit_rows)),
        "calibration_sampled_rows": int(len(calibration_rows)),
        "hazard_offset": offset,
    }
    del fit_rows, calibration_rows
    gc.collect()
    return model, offset, identity


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_historical_reproduction_argument(parser)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke-days", type=int, default=0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    args.spec = args.spec.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    reproduction_identity = require_historical_reproduction(
        runner_id="f06.full_curve_fill_cif",
        enabled=bool(args.historical_reproduction),
        spec_path=args.spec,
    )
    spec = _load_spec(args.spec)
    wide = _load_partitions(spec)
    if int(args.smoke_days) > 0:
        smoke_days = sorted(wide["day"].astype(str).unique())[: int(args.smoke_days)]
        wide = wide.loc[wide["day"].isin(smoke_days)].copy()
    lifecycles = expand_action_lifecycles(wide)
    contract = derive_duration_contract(
        lifecycles,
        interval_ms=int(spec["development_fit"]["risk_interval_ms"]),
        report_quantiles=spec["reporting"]["development_exposure_quantiles"],
        maximum_support_quantile=float(spec["reporting"]["maximum_support_quantile"]),
    )
    frozen_horizons = {
        str(key): int(value)
        for key, value in spec["reporting"]["frozen_empirical_horizons_ms"].items()
    }
    if not args.smoke_days and frozen_horizons != contract["report_quantiles"]:
        raise RuntimeError("Development exposure quantiles changed after spec freeze")
    frozen_maximum_support_ms = int(
        spec["reporting"]["frozen_maximum_support_ms"]
    )
    if not args.smoke_days and frozen_maximum_support_ms != int(
        contract["maximum_support_ms"]
    ):
        raise RuntimeError("Development maximum support changed after spec freeze")
    report_horizons = sorted(
        set(frozen_horizons.values())
        | {int(value) for value in spec["reporting"]["legacy_diagnostic_horizons_ms"]}
    )
    maximum_support_ms = int(contract["maximum_support_ms"])

    days = sorted(lifecycles["day"].astype(str).unique())
    fit_contract = spec["development_fit"]
    minimum_train_days = int(fit_contract["minimum_train_days"])
    if args.smoke_days:
        minimum_train_days = max(2, min(len(days) - 2, minimum_train_days))
    folds = make_expanding_folds(
        days,
        min_train_days=minimum_train_days,
        embargo_days=int(fit_contract["embargo_days"]),
        test_days=int(fit_contract["outer_test_days"]),
    )
    oof_parts: list[pd.DataFrame] = []
    fold_identity: list[dict[str, Any]] = []
    for fold in folds:
        for side in ("BUY", "SELL"):
            train = lifecycles.loc[
                lifecycles["day"].isin(fold["train_days"])
                & lifecycles["side"].eq(side)
            ]
            test = lifecycles.loc[
                lifecycles["day"].isin(fold["test_days"])
                & lifecycles["side"].eq(side)
            ].copy()
            if train.empty or test.empty:
                continue
            model, offset, fit_identity = _fit_side(
                train, spec=spec, maximum_support_ms=maximum_support_ms
            )
            activation = fit_activation_contract(train)
            prediction = predict_cif_at_horizons(
                model,
                test,
                report_horizons,
                activation_contract=activation,
                hazard_offset=offset,
                interval_ms=int(fit_contract["risk_interval_ms"]),
                maximum_support_ms=maximum_support_ms,
                chunk_size=int(fit_contract["prediction_chunk_size"]),
            )
            labels = lifecycle_labels_at_horizons(test, report_horizons)
            scored = prediction.merge(
                labels,
                on=[
                    "action_lifecycle_id",
                    "cohort_id",
                    "day",
                    "side",
                    "inventory_role",
                    "action",
                    "horizon_ms",
                ],
                how="inner",
                validate="one_to_one",
            )
            rates = _baseline_rates(train, report_horizons)
            scored["baseline_probability"] = _apply_baseline(scored, rates).astype(
                np.float32
            )
            scored["fold"] = int(fold["fold"])
            oof_parts.append(scored)
            fold_identity.append(
                {
                    "fold": int(fold["fold"]),
                    "side": side,
                    "train_days": list(fold["train_days"]),
                    "embargo_days": list(fold["embargo_days"]),
                    "test_days": list(fold["test_days"]),
                    **fit_identity,
                }
            )
            del train, test, model, prediction, labels, scored
            gc.collect()
    if not oof_parts:
        raise RuntimeError("full-curve chronological fit produced no OOF rows")
    oof = pd.concat(oof_parts, ignore_index=True)
    metric_rows: list[dict[str, Any]] = []
    for (side, role, horizon), group in oof.groupby(
        ["side", "inventory_role", "horizon_ms"], observed=True
    ):
        metric_rows.append(
            {
                "side": str(side),
                "inventory_role": str(role),
                "horizon_ms": int(horizon),
                "horizon_origin": (
                    "empirical_exposure_quantile"
                    if int(horizon) in set(frozen_horizons.values())
                    else "legacy_report_only"
                ),
                **_metrics(group),
            }
        )
    metrics = pd.DataFrame(metric_rows)

    final_models: dict[str, Any] = {}
    final_fit: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        side_rows = lifecycles.loc[lifecycles["side"].eq(side)]
        model, offset, identity = _fit_side(
            side_rows, spec=spec, maximum_support_ms=maximum_support_ms
        )
        final_models[side] = {"model": model, "hazard_offset": offset}
        final_fit[side] = identity
    activation_contract = fit_activation_contract(lifecycles)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    oof_path = args.output_dir / "oof_predictions.parquet"
    metrics_path = args.output_dir / "metrics.parquet"
    artifact_path = args.output_dir / "full_curve_fill_cif.joblib"
    oof.to_parquet(oof_path, index=False, compression="zstd")
    metrics.to_parquet(metrics_path, index=False, compression="zstd")
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "model_features": MODEL_FEATURES,
        "models": final_models,
        "activation_contract": activation_contract,
        "duration_contract": contract,
        "risk_interval_ms": int(fit_contract["risk_interval_ms"]),
        "maximum_support_ms": maximum_support_ms,
        "placement_estimand": "P(fill by t before cancel ACK | do(placement), x0)",
        "fixed_horizons_are_report_only": True,
        "active_order_keep_replace": "separate_not_built",
        "campaign_repair": "separate_not_built",
        "action_or_live_authorization": False,
    }
    joblib.dump(artifact, artifact_path)
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "development_days": days,
        "development_cohorts": int(lifecycles["cohort_id"].nunique()),
        "development_action_lifecycles": int(len(lifecycles)),
        "duration_contract": contract,
        "report_horizons_ms": report_horizons,
        "legacy_horizons_are_report_only": True,
        "horizon_cell_prediction_gate": False,
        "curve_level_status": "development_diagnostic_only",
        "validation_access_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "active_order_keep_replace": "separate_not_built",
        "campaign_repair": "separate_not_built",
        "folds": fold_identity,
        "final_fit": final_fit,
        "spec_sha256": _sha256(args.spec),
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "oof_predictions": {"path": str(oof_path), "sha256": _sha256(oof_path)},
            "metrics": {"path": str(metrics_path), "sha256": _sha256(metrics_path)},
            "artifact": {"path": str(artifact_path), "sha256": _sha256(artifact_path)},
        },
    }
    _atomic_json(report, args.output_dir / "report.json")
    stamp_historical_reproduction_output(args.output_dir, reproduction_identity)
    print(
        json.dumps(
            {
                "development_days": len(days),
                "oof_rows": len(oof),
                "maximum_support_ms": maximum_support_ms,
                "report_horizons_ms": report_horizons,
                "validation_read": False,
                "action_or_live_authorization": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
