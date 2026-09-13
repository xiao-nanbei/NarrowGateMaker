#!/usr/bin/env python3
"""Fit the Development-only direct new-placement fill CIF surface.

The target is P(fill before min(decision+h, cancel ACK) | do(placement), x0).
It includes action-specific activation and GTX rejection.  It does not model
KEEP or REPLACE, whose risk origin is an already-active order snapshot.
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
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score

from data_paths import data_root
from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.paired_order_lifecycle import (
    ACTION_ORDER,
    HORIZONS_MS,
)
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)
from research.governance.historical_reproduction import (
    add_historical_reproduction_argument,
    require_historical_reproduction,
    stamp_historical_reproduction_output,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = FAMILY_DOCS / "placement_fill_cif_v1_spec_20260726.json"
DEFAULT_PANEL = DATA_ROOT / "reports" / "placement_fill_cif_v1_development_20260726"
DEFAULT_OUTPUT = DATA_ROOT / "reports" / "direct_fill_cif_v1_development_20260726"
SCHEMA_VERSION = "direct_placement_fill_cif.v1"
MODEL_KIND = "side_specific_monotone_direct_cif"
TICK_SIZE = 0.1

BASE_FEATURES = (
    "distance_ticks",
    "log_horizon_ms",
    "distance_vol_units",
    "bbo_spread_ticks",
    "inventory",
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

# Only these decision-visible columns are needed to fit the direct placement
# surface.  The full lifecycle partition remains authoritative on disk, while
# training avoids materializing its path diagnostics nine times.
PLACEMENT_IDENTITY_COLUMNS = (
    "cohort_id",
    "day",
    "side",
    "inventory_role",
    "submit_ts_ns",
    "feature_ready_ts_ns",
    "best_bid",
    "best_ask",
    "mid",
    "sigma_sq_raw",
    "sigma_sq_blended",
    "quote_horizon_s",
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


def placement_input_columns() -> tuple[str, ...]:
    columns = list(PLACEMENT_IDENTITY_COLUMNS)
    for action in ACTION_ORDER:
        columns.append(f"{action}__price_tick")
        for horizon_ms in HORIZONS_MS:
            columns.extend(
                (
                    f"{action}__placement_observed_{horizon_ms}ms",
                    f"{action}__placement_filled_{horizon_ms}ms",
                )
            )
    return tuple(columns)

MODEL_CONTRACT = {
    "model": "HistGradientBoostingClassifier",
    "loss": "log_loss",
    "learning_rate": 0.05,
    "max_iter": 160,
    "max_depth": 2,
    "min_samples_leaf": 200,
    "l2_regularization": 10.0,
    "max_bins": 127,
    "random_state": 20260726,
    "monotonic": {
        "distance_ticks": -1,
        "log_horizon_ms": 1,
        "distance_vol_units": -1,
    },
    "calibration": "inner_expanding_oof_affine_logit",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def make_expanding_folds(
    days: Sequence[str],
    *,
    min_train_days: int,
    embargo_days: int,
    test_days: int,
) -> list[dict[str, Any]]:
    ordered = sorted({str(day) for day in days})
    first_test = int(min_train_days) + int(embargo_days)
    if len(ordered) < first_test + 1:
        raise ValueError("not enough days for chronological OOF")
    folds: list[dict[str, Any]] = []
    for test_start in range(first_test, len(ordered), int(test_days)):
        train_end = test_start - int(embargo_days)
        test = ordered[test_start : test_start + int(test_days)]
        if not test:
            continue
        folds.append(
            {
                "fold": len(folds),
                "train_days": ordered[:train_end],
                "embargo_days": ordered[train_end:test_start],
                "test_days": test,
            }
        )
    return folds


def _numeric(frame: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce").fillna(default)


def expand_placement_panel(wide: pd.DataFrame) -> pd.DataFrame:
    """Expand one cohort into action-by-horizon direct-CIF observations."""

    required = {
        "cohort_id",
        "day",
        "side",
        "inventory_role",
        "submit_ts_ns",
        "feature_ready_ts_ns",
        "best_bid",
        "best_ask",
        "mid",
        "sigma_sq_blended",
        "quote_horizon_s",
    }
    missing = sorted(required - set(wide.columns))
    if missing:
        raise ValueError(f"placement panel is missing columns: {missing}")
    if not bool(
        (_numeric(wide, "feature_ready_ts_ns") <= _numeric(wide, "submit_ts_ns")).all()
    ):
        raise ValueError("placement features are not causally ready")

    identity = [column for column in PLACEMENT_IDENTITY_COLUMNS if column in wide]
    action_parts: list[pd.DataFrame] = []
    for action in ACTION_ORDER:
        prefix = f"{action}__"
        action_columns = [
            f"{action}__price_tick",
            *(
                column
                for horizon_ms in HORIZONS_MS
                for column in (
                    f"{action}__placement_observed_{horizon_ms}ms",
                    f"{action}__placement_filled_{horizon_ms}ms",
                )
            ),
        ]
        part = wide[identity].copy()
        for column in action_columns:
            part[column[len(prefix) :]] = wide[column]
        part["action"] = action
        action_parts.append(part)
    actions = pd.concat(action_parts, ignore_index=True)
    if {"keep", "replace"} & set(actions["action"].astype(str).str.lower()):
        raise ValueError("active-order actions leaked into placement panel")

    side = actions["side"].astype(str).str.upper()
    price = _numeric(actions, "price_tick") * TICK_SIZE
    distance = np.where(
        side.eq("BUY"),
        (_numeric(actions, "best_bid") - price) / TICK_SIZE,
        (price - _numeric(actions, "best_ask")) / TICK_SIZE,
    )
    actions["distance_ticks"] = np.maximum(0.0, np.round(distance, 6)).astype(
        np.float32
    )
    actions["bbo_spread_ticks"] = np.maximum(
        1.0,
        (_numeric(actions, "best_ask") - _numeric(actions, "best_bid"))
        / TICK_SIZE,
    ).astype(np.float32)
    variance = np.maximum(0.0, _numeric(actions, "sigma_sq_blended"))
    horizon_s = np.maximum(1e-6, _numeric(actions, "quote_horizon_s", 1.0))
    expected_move = np.sqrt(variance * horizon_s)
    actions["distance_vol_units"] = (
        actions["distance_ticks"] * TICK_SIZE / np.maximum(expected_move, TICK_SIZE)
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
    actions["sigma_sq_blended_log1p"] = np.log1p(variance).astype(np.float32)
    actions["l2_near_depth_total_log1p"] = np.log1p(
        np.maximum(0.0, _numeric(actions, "l2_near_depth_total"))
    ).astype(np.float32)
    role = actions["inventory_role"].astype(str).str.lower()
    for name in ("opener", "add", "reducing"):
        actions[f"role_{name}"] = role.eq(name).astype(np.float32)

    horizon_parts: list[pd.DataFrame] = []
    for horizon_ms in HORIZONS_MS:
        observed = _numeric(actions, f"placement_observed_{horizon_ms}ms").astype(bool)
        part = actions.loc[observed].copy()
        part["horizon_ms"] = int(horizon_ms)
        part["log_horizon_ms"] = np.float32(math.log(float(horizon_ms)))
        part["target"] = _numeric(
            actions.loc[observed], f"placement_filled_{horizon_ms}ms"
        ).astype(np.int8)
        horizon_parts.append(part)
    long = pd.concat(horizon_parts, ignore_index=True)
    long["row_weight"] = np.float32(
        1.0 / (len(ACTION_ORDER) * len(HORIZONS_MS))
    )
    for column in ("day", "side", "inventory_role", "action"):
        long[column] = long[column].astype("category")
    return long


def _feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=frame.index)
    for name in BASE_FEATURES:
        out[name] = _numeric(frame, name).astype(np.float32)
    return out


def _new_model() -> HistGradientBoostingClassifier:
    constraints = [
        int(MODEL_CONTRACT["monotonic"].get(name, 0)) for name in BASE_FEATURES
    ]
    return HistGradientBoostingClassifier(
        loss="log_loss",
        learning_rate=float(MODEL_CONTRACT["learning_rate"]),
        max_iter=int(MODEL_CONTRACT["max_iter"]),
        max_depth=int(MODEL_CONTRACT["max_depth"]),
        min_samples_leaf=int(MODEL_CONTRACT["min_samples_leaf"]),
        l2_regularization=float(MODEL_CONTRACT["l2_regularization"]),
        max_bins=int(MODEL_CONTRACT["max_bins"]),
        random_state=int(MODEL_CONTRACT["random_state"]),
        monotonic_cst=constraints,
    )


def _fit_model(frame: pd.DataFrame) -> HistGradientBoostingClassifier:
    target = frame["target"].to_numpy(dtype=int)
    if np.unique(target).size < 2:
        raise ValueError("direct CIF fit requires fills and non-fills")
    model = _new_model()
    model.fit(
        _feature_frame(frame),
        target,
        sample_weight=frame["row_weight"].to_numpy(dtype=float),
    )
    return model


def _predict_raw(model: HistGradientBoostingClassifier, frame: pd.DataFrame) -> np.ndarray:
    return np.clip(
        model.predict_proba(_feature_frame(frame))[:, 1],
        1e-7,
        1.0 - 1e-7,
    )


def _fit_calibrator(raw: np.ndarray, target: np.ndarray) -> dict[str, float]:
    raw = np.clip(np.asarray(raw, dtype=float), 1e-7, 1.0 - 1e-7)
    target = np.asarray(target, dtype=int)
    if target.sum() < 20 or (target == 0).sum() < 100:
        raise ValueError("inner OOF calibration lacks event support")
    score = logit(raw)
    center = float(score.mean())
    scale = max(float(score.std()), 1e-6)
    model = LogisticRegression(C=100.0, solver="lbfgs", max_iter=1000)
    model.fit(((score - center) / scale).reshape(-1, 1), target)
    slope = float(model.coef_[0, 0]) / scale
    return {
        "intercept": float(model.intercept_[0]) - slope * center,
        "slope": slope,
    }


def _apply_calibrator(raw: np.ndarray, calibrator: Mapping[str, float]) -> np.ndarray:
    score = logit(np.clip(raw, 1e-7, 1.0 - 1e-7))
    return np.clip(
        expit(float(calibrator["intercept"]) + float(calibrator["slope"]) * score),
        1e-7,
        1.0 - 1e-7,
    )


def _inner_oof_predictions(
    train: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    days = sorted(train["day"].astype(str).unique())
    folds = make_expanding_folds(
        days, min_train_days=10, embargo_days=1, test_days=3
    )
    pieces: list[pd.DataFrame] = []
    identities: list[dict[str, Any]] = []
    for fold in folds:
        fit = train.loc[train["day"].isin(fold["train_days"])]
        test = train.loc[train["day"].isin(fold["test_days"])].copy()
        model = _fit_model(fit)
        test["raw"] = _predict_raw(model, test)
        pieces.append(
            test[
                [
                    "cohort_id",
                    "day",
                    "side",
                    "inventory_role",
                    "action",
                    "horizon_ms",
                    "target",
                    "raw",
                ]
            ]
        )
        identities.append(fold)
        del fit, test, model
        gc.collect()
    if not pieces:
        raise ValueError("inner chronological calibration produced no OOF rows")
    return pd.concat(pieces, ignore_index=True), identities


def _inner_calibrator(
    train: pd.DataFrame,
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    oof, identities = _inner_oof_predictions(train)
    return (
        _fit_calibrator(
            oof["raw"].to_numpy(dtype=float),
            oof["target"].to_numpy(dtype=int),
        ),
        identities,
    )


def _fit_intercept_offset(probability: np.ndarray, target: np.ndarray) -> float:
    """Fit an intercept-only logit offset with slope fixed at one."""

    probability = np.clip(
        np.asarray(probability, dtype=float), 1e-7, 1.0 - 1e-7
    )
    target = np.asarray(target, dtype=float)
    if target.size == 0:
        raise ValueError("cell calibration received no rows")
    target_mean = float(target.mean())
    if not 0.0 < target_mean < 1.0:
        raise ValueError("cell calibration requires events and non-events")
    score = logit(probability)

    def residual(offset: float) -> float:
        return float(expit(score + float(offset)).mean() - target_mean)

    return float(brentq(residual, -20.0, 20.0))


def _cell_key(role: object, horizon_ms: object) -> tuple[str, int]:
    return str(role).lower(), int(horizon_ms)


def _fit_cell_offsets(
    frame: pd.DataFrame,
    *,
    probability_column: str = "probability",
) -> dict[tuple[str, int], float]:
    """Fit side-local role x horizon intercepts, pooling placement actions."""

    offsets: dict[tuple[str, int], float] = {}
    for role in ("opener", "add", "reducing"):
        for horizon_ms in HORIZONS_MS:
            group = frame.loc[
                frame["inventory_role"].astype(str).str.lower().eq(role)
                & frame["horizon_ms"].eq(int(horizon_ms))
            ]
            if group.empty:
                raise ValueError(
                    f"cell calibration lacks {role}/{int(horizon_ms)} rows"
                )
            offsets[(role, int(horizon_ms))] = _fit_intercept_offset(
                group[probability_column].to_numpy(dtype=float),
                group["target"].to_numpy(dtype=int),
            )
    return offsets


def _apply_cell_offsets(
    frame: pd.DataFrame,
    probability: np.ndarray,
    offsets: Mapping[tuple[str, int], float],
) -> np.ndarray:
    probability = np.clip(
        np.asarray(probability, dtype=float), 1e-7, 1.0 - 1e-7
    )
    applied = np.asarray(
        [
            float(offsets[_cell_key(role, horizon_ms)])
            for role, horizon_ms in zip(  # noqa: B905 - local audit runtime is py3.9
                frame["inventory_role"], frame["horizon_ms"]
            )
        ],
        dtype=float,
    )
    return np.clip(expit(logit(probability) + applied), 1e-7, 1.0 - 1e-7)


def _cell_drift_envelopes(
    frame: pd.DataFrame,
    *,
    probability_column: str,
    quantile: float,
) -> dict[tuple[str, int], float]:
    """Estimate a past-only daily base-rate drift envelope per cell."""

    envelopes: dict[tuple[str, int], float] = {}
    for role in ("opener", "add", "reducing"):
        for horizon_ms in HORIZONS_MS:
            group = frame.loc[
                frame["inventory_role"].astype(str).str.lower().eq(role)
                & frame["horizon_ms"].eq(int(horizon_ms))
            ]
            daily = group.groupby("day", observed=True).agg(
                observed=("target", "mean"),
                predicted=(probability_column, "mean"),
            )
            if daily.empty:
                raise ValueError(
                    f"drift envelope lacks {role}/{int(horizon_ms)} days"
                )
            envelopes[(role, int(horizon_ms))] = float(
                np.quantile(
                    np.abs(daily["observed"] - daily["predicted"]),
                    float(quantile),
                )
            )
    return envelopes


def _apply_cell_values(
    frame: pd.DataFrame,
    values: Mapping[tuple[str, int], float],
) -> np.ndarray:
    return np.asarray(
        [
            float(values[_cell_key(role, horizon_ms)])
            for role, horizon_ms in zip(  # noqa: B905 - local audit runtime is py3.9
                frame["inventory_role"], frame["horizon_ms"]
            )
        ],
        dtype=float,
    )


def _baseline_rates(train: pd.DataFrame) -> dict[tuple[str, int], float]:
    rates: dict[tuple[str, int], float] = {}
    side_rate = (float(train["target"].sum()) + 2.0) / (len(train) + 40.0)
    for role in ("opener", "add", "reducing"):
        for horizon in HORIZONS_MS:
            group = train.loc[
                train["inventory_role"].astype(str).str.lower().eq(role)
                & train["horizon_ms"].eq(int(horizon))
            ]
            probability = (float(group["target"].sum()) + 40.0 * side_rate) / (
                len(group) + 40.0
            )
            rates[(role, int(horizon))] = float(probability)
    return rates


def _apply_baseline(frame: pd.DataFrame, rates: Mapping[tuple[str, int], float]) -> np.ndarray:
    return np.asarray(
        [
            rates[(str(role).lower(), int(horizon))]
            for role, horizon in zip(  # noqa: B905 - local audit runtime is py3.9
                frame["inventory_role"], frame["horizon_ms"]
            )
        ],
        dtype=float,
    )


def _calibration_line(target: np.ndarray, probability: np.ndarray) -> tuple[float, float]:
    target = np.asarray(target, dtype=int)
    probability = np.clip(np.asarray(probability, dtype=float), 1e-7, 1.0 - 1e-7)
    if np.unique(target).size < 2:
        return math.nan, math.nan
    score = logit(probability)
    center = float(score.mean())
    scale = max(float(score.std()), 1e-6)
    normalized = (score - center) / scale
    prevalence = np.clip(float(target.mean()), 1e-7, 1.0 - 1e-7)
    coefficient = np.asarray([float(logit(prevalence)), 1.0], dtype=float)
    for _ in range(50):
        fitted = expit(coefficient[0] + coefficient[1] * normalized)
        residual = fitted - target
        weight = np.maximum(fitted * (1.0 - fitted), 1e-10)
        gradient = np.asarray(
            [residual.mean(), np.mean(residual * normalized)], dtype=float
        )
        hessian = np.asarray(
            [
                [weight.mean(), np.mean(weight * normalized)],
                [
                    np.mean(weight * normalized),
                    np.mean(weight * normalized * normalized),
                ],
            ],
            dtype=float,
        )
        step = np.linalg.solve(hessian, gradient)
        coefficient -= step
        if float(np.max(np.abs(step))) < 1e-10:
            break
    slope = float(coefficient[1]) / scale
    return float(coefficient[0]) - slope * center, slope


def apply_past_only_rolling_calibration(
    frame: pd.DataFrame,
    *,
    window_days: int,
    minimum_history_days: int,
    source_action: str,
    minimum_history_events: int = 0,
) -> pd.DataFrame:
    """Apply a prequential cell intercept using only completed prior UTC days."""

    if int(window_days) <= 0 or int(minimum_history_days) <= 0:
        raise ValueError("rolling calibration windows must be positive")
    out_parts: list[pd.DataFrame] = []
    for _, cell in frame.groupby(
        ["side", "inventory_role", "horizon_ms"], observed=True, sort=True
    ):
        cell = cell.copy().reset_index(drop=True)
        day_key = cell["day"].astype(str).to_numpy()
        action_key = cell["action"].astype(str).to_numpy()
        days = sorted(set(day_key))
        day_indices = {
            day: np.flatnonzero(day_key == day) for day in days
        }
        source_indices = {
            day: np.flatnonzero(
                (day_key == day) & (action_key == str(source_action))
            )
            for day in days
        }
        calibrated = np.empty(len(cell), dtype=float)
        static_probability = np.clip(
            cell["probability"].to_numpy(dtype=float), 1e-7, 1.0 - 1e-7
        )
        target = cell["target"].to_numpy(dtype=int)
        offsets = np.zeros(len(cell), dtype=float)
        history_counts = np.zeros(len(cell), dtype=np.int16)
        eligible = np.zeros(len(cell), dtype=bool)
        for index, day in enumerate(days):
            history_days = days[max(0, index - int(window_days)) : index]
            history_index = np.concatenate(
                [source_indices[value] for value in history_days]
            ) if history_days else np.empty(0, dtype=np.int64)
            offset = 0.0
            if (
                len(history_days) >= int(minimum_history_days)
                and history_index.size > 0
                and max(1, int(minimum_history_events))
                <= int(target[history_index].sum())
                < history_index.size
            ):
                offset = _fit_intercept_offset(
                    static_probability[history_index],
                    target[history_index],
                )
            current_index = day_indices[day]
            calibrated[current_index] = np.clip(
                expit(logit(static_probability[current_index]) + float(offset)),
                1e-7,
                1.0 - 1e-7,
            )
            offsets[current_index] = float(offset)
            history_counts[current_index] = int(len(history_days))
            eligible[current_index] = bool(
                len(history_days) >= int(minimum_history_days)
            )
        cell["static_probability"] = static_probability
        cell["probability"] = calibrated
        cell["rolling_calibration_offset"] = offsets
        cell["rolling_calibration_history_days"] = history_counts
        cell["rolling_calibration_eligible"] = eligible
        out_parts.append(cell)
    return pd.concat(out_parts, ignore_index=True)


def _day_bootstrap(
    frame: pd.DataFrame, *, samples: int = 2000, seed: int = 20260726
) -> dict[str, float]:
    daily = []
    for day, group in frame.groupby("day", sort=True):
        y = group["target"].to_numpy(dtype=float)
        improvement = np.square(group["baseline_probability"] - y).sum() - np.square(
            group["probability"] - y
        ).sum()
        daily.append((str(day), float(improvement), int(len(group))))
    if not daily:
        return {"lower": math.nan, "upper": math.nan, "probability_positive": math.nan}
    rng = np.random.default_rng(int(seed))
    estimates = np.empty(int(samples), dtype=float)
    for index in range(int(samples)):
        picks = rng.integers(0, len(daily), size=len(daily))
        numerator = sum(daily[pick][1] for pick in picks)
        denominator = sum(daily[pick][2] for pick in picks)
        estimates[index] = numerator / max(1, denominator)
    return {
        "lower": float(np.quantile(estimates, 0.025)),
        "upper": float(np.quantile(estimates, 0.975)),
        "probability_positive": float(np.mean(estimates > 0.0)),
    }


def _day_bootstrap_calibration(
    frame: pd.DataFrame,
    *,
    samples: int = 2000,
    seed: int = 20260727,
) -> dict[str, float]:
    daily = (
        frame.groupby("day", observed=True)
        .agg(
            observed_events=("target", "sum"),
            predicted_events=("probability", "sum"),
            rows=("target", "size"),
        )
        .reset_index(drop=True)
    )
    if daily.empty:
        return {
            "difference_lower": math.nan,
            "difference_upper": math.nan,
            "oe_lower": math.nan,
            "oe_upper": math.nan,
        }
    rng = np.random.default_rng(int(seed))
    differences = np.empty(int(samples), dtype=float)
    ratios = np.empty(int(samples), dtype=float)
    for index in range(int(samples)):
        selected = daily.iloc[
            rng.integers(0, len(daily), size=len(daily))
        ]
        observed = float(selected["observed_events"].sum())
        predicted = float(selected["predicted_events"].sum())
        rows = max(1.0, float(selected["rows"].sum()))
        differences[index] = (observed - predicted) / rows
        ratios[index] = observed / max(predicted, 1e-12)
    return {
        "difference_lower": float(np.quantile(differences, 0.025)),
        "difference_upper": float(np.quantile(differences, 0.975)),
        "oe_lower": float(np.quantile(ratios, 0.025)),
        "oe_upper": float(np.quantile(ratios, 0.975)),
    }


def _daily_rank_fraction(frame: pd.DataFrame) -> float:
    signs = []
    for _, group in frame.groupby("day", sort=True):
        if group["target"].sum() < 2 or len(group) < 20:
            continue
        rank = group["probability"].rank(method="first", pct=True)
        high = group.loc[rank >= 0.8, "target"].mean()
        low = group.loc[rank <= 0.2, "target"].mean()
        signs.append(float(high > low))
    return float(np.mean(signs)) if signs else math.nan


def _metrics(
    frame: pd.DataFrame,
    *,
    bootstrap_seed: int,
    bootstrap_samples: int = 2000,
) -> dict[str, Any]:
    y = frame["target"].to_numpy(dtype=float)
    p = frame["probability"].to_numpy(dtype=float)
    b = frame["baseline_probability"].to_numpy(dtype=float)
    brier = float(np.mean(np.square(p - y)))
    baseline_brier = float(np.mean(np.square(b - y)))
    intercept, slope = _calibration_line(y.astype(int), p)
    prevalence = float(np.mean(y))
    predicted_prevalence = float(np.mean(p))
    ap = float(average_precision_score(y, p)) if 0 < y.sum() < len(y) else math.nan
    calibration_bootstrap = _day_bootstrap_calibration(
        frame,
        samples=int(bootstrap_samples),
        seed=int(bootstrap_seed) + 10_000,
    )
    return {
        "rows": int(len(frame)),
        "events": int(y.sum()),
        "days": int(frame["day"].nunique()),
        "prevalence": prevalence,
        "predicted_prevalence": predicted_prevalence,
        "calibration_in_the_large": prevalence - predicted_prevalence,
        "observed_expected_ratio": float(y.sum()) / max(float(p.sum()), 1e-12),
        "brier": brier,
        "baseline_brier": baseline_brier,
        "brier_improvement": baseline_brier - brier,
        "brier_skill": 1.0 - brier / max(baseline_brier, 1e-12),
        "average_precision": ap,
        "average_precision_lift": ap / max(prevalence, 1e-12) if math.isfinite(ap) else math.nan,
        "calibration_intercept": intercept,
        "calibration_slope": slope,
        "daily_rank_positive_fraction": _daily_rank_fraction(frame),
        "day_cluster_brier_improvement": _day_bootstrap(
            frame, samples=int(bootstrap_samples), seed=int(bootstrap_seed)
        ),
        "day_cluster_calibration": calibration_bootstrap,
        "empirical_abs_bias_tolerance": float(
            frame["empirical_abs_bias_tolerance"].mean()
        )
        if "empirical_abs_bias_tolerance" in frame
        else math.nan,
    }


def _pathwise_prediction_violations(oof: pd.DataFrame) -> int:
    pivot = oof.pivot_table(
        index=["day", "cohort_id", "horizon_ms"],
        columns="action",
        values="probability",
        aggfunc="first",
    ).dropna()
    return int(
        (
            (pivot["closer_1tick"] + 1e-12 < pivot["current"])
            | (pivot["current"] + 1e-12 < pivot["farther_1tick"])
        ).sum()
    )


def _load_partitions(panel_dir: Path, expected_days: Sequence[str]) -> pd.DataFrame:
    manifest = json.loads((panel_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "development_panel_complete":
        raise RuntimeError("placement Development panel is incomplete")
    if bool(manifest.get("validation_read")) or bool(manifest.get("sealed_holdout_read")):
        raise RuntimeError("placement panel manifest indicates sealed data access")
    if manifest.get("active_order_keep_replace") != "separate_not_built":
        raise RuntimeError("placement panel mixed the active-order estimand")
    frames = []
    for day in expected_days:
        directory = panel_dir / "partitions" / f"day={day}"
        part_manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        panel = directory / "placement.parquet"
        if part_manifest.get("panel_sha256") != _sha256(panel):
            raise RuntimeError(f"placement partition checksum mismatch: {day}")
        frame = pd.read_parquet(panel, columns=placement_input_columns())
        if set(frame["day"].astype(str)) != {str(day)}:
            raise RuntimeError(f"placement partition day mismatch: {day}")
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _apply_prediction_gates(
    metrics: pd.DataFrame,
    gates: Mapping[str, Any],
    *,
    empirical_calibration: bool,
) -> pd.DataFrame:
    metrics = metrics.copy()
    metrics["support_pass"] = (
        metrics["days"].ge(int(gates["minimum_oof_days_per_side_role"]))
        & metrics["events"].ge(int(gates["minimum_fill_events_per_side_role"]))
    )
    metrics["brier_pass"] = metrics["day_cluster_brier_improvement"].map(
        lambda value: float(value["lower"])
        > float(gates["day_clustered_brier_skill_lower_bound_vs_exposure_only"])
    )
    lo, hi = gates["calibration_slope_range"]
    metrics["calibration_slope_pass"] = metrics["calibration_slope"].between(
        float(lo), float(hi)
    )
    if empirical_calibration:
        metrics["calibration_difference_ci_pass"] = metrics[
            "day_cluster_calibration"
        ].map(
            lambda value: float(value["difference_lower"]) <= 0.0
            <= float(value["difference_upper"])
        )
        metrics["calibration_oe_ci_pass"] = metrics[
            "day_cluster_calibration"
        ].map(
            lambda value: float(value["oe_lower"]) <= 1.0
            <= float(value["oe_upper"])
        )
        metrics["calibration_drift_envelope_pass"] = metrics[
            "calibration_in_the_large"
        ].abs().le(metrics["empirical_abs_bias_tolerance"])
        calibration_contract = gates["calibration_level"]
        level_pass = pd.Series(True, index=metrics.index, dtype=bool)
        if bool(
            calibration_contract[
                "day_clustered_observed_minus_expected_ci_must_contain_zero"
            ]
        ):
            level_pass &= metrics["calibration_difference_ci_pass"]
        if bool(
            calibration_contract[
                "day_clustered_observed_expected_ratio_ci_must_contain_one"
            ]
        ):
            level_pass &= metrics["calibration_oe_ci_pass"]
        if bool(
            calibration_contract[
                "pooled_absolute_bias_must_not_exceed_inner_oof_drift_envelope"
            ]
        ):
            level_pass &= metrics["calibration_drift_envelope_pass"]
        metrics["calibration_level_pass"] = level_pass
    else:
        metrics["calibration_level_pass"] = metrics[
            "calibration_intercept"
        ].abs().le(float(gates["calibration_intercept_abs_max"]))
    metrics["calibration_pass"] = (
        metrics["calibration_slope_pass"]
        & metrics["calibration_level_pass"]
    )
    metrics["rank_pass"] = metrics["daily_rank_positive_fraction"].ge(
        float(gates["daily_rank_direction_required_fraction"])
    )
    metrics["prediction_gate_passed"] = (
        metrics["support_pass"]
        & metrics["brier_pass"]
        & metrics["calibration_pass"]
        & metrics["rank_pass"]
    )
    return metrics


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_historical_reproduction_argument(parser)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--panel-dir", type=Path, default=DEFAULT_PANEL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--bootstrap-samples", type=int, default=2000)
    parser.add_argument("--bootstrap-seed", type=int, default=20260726)
    return parser.parse_args(argv)


def _rolling_seed_from_oof(
    oof: pd.DataFrame,
    contract: Mapping[str, Any],
) -> dict[str, dict[tuple[str, int], dict[str, Any]]]:
    window_days = int(contract["window_utc_days"])
    source_action = str(contract["outcome_source_action"])
    seeds: dict[str, dict[tuple[str, int], dict[str, Any]]] = {}
    for side in ("BUY", "SELL"):
        side_seed: dict[tuple[str, int], dict[str, Any]] = {}
        for role in ("opener", "add", "reducing"):
            for horizon_ms in HORIZONS_MS:
                cell = oof.loc[
                    oof["side"].eq(side)
                    & oof["inventory_role"].astype(str).str.lower().eq(role)
                    & oof["horizon_ms"].eq(int(horizon_ms))
                    & oof["action"].astype(str).eq(source_action)
                ]
                days = sorted(cell["day"].astype(str).unique())[-window_days:]
                history = cell.loc[cell["day"].astype(str).isin(days)]
                side_seed[(role, int(horizon_ms))] = {
                    "days": days,
                    "offset": _fit_intercept_offset(
                        history["probability"].to_numpy(dtype=float),
                        history["target"].to_numpy(dtype=int),
                    ),
                    "rows": int(len(history)),
                    "events": int(history["target"].sum()),
                }
        seeds[side] = side_seed
    return seeds


def _run_rolling_revision(
    args: argparse.Namespace,
    spec: Mapping[str, Any],
) -> int:
    lineage = spec["lineage"]
    base_report_path = Path(str(lineage["v2_development_report"])).resolve()
    if _sha256(base_report_path) != str(
        lineage["v2_development_report_sha256"]
    ):
        raise RuntimeError("v2 Development report identity changed")
    base_report = json.loads(base_report_path.read_text(encoding="utf-8"))
    if bool(base_report.get("validation_read")) or bool(
        base_report.get("sealed_holdout_read")
    ):
        raise RuntimeError("rolling revision base accessed sealed evidence")
    oof_identity = base_report["outputs"]["oof_predictions"]
    artifact_identity = base_report["outputs"]["artifact"]
    base_oof_path = Path(str(oof_identity["path"])).resolve()
    base_artifact_path = Path(str(artifact_identity["path"])).resolve()
    if _sha256(base_oof_path) != str(oof_identity["sha256"]):
        raise RuntimeError("v2 OOF identity changed")
    if _sha256(base_artifact_path) != str(artifact_identity["sha256"]):
        raise RuntimeError("v2 model artifact identity changed")

    static_oof = pd.read_parquet(base_oof_path)
    contract = dict(spec["development_fit"]["rolling_calibration"])
    oof = apply_past_only_rolling_calibration(
        static_oof,
        window_days=int(contract["window_utc_days"]),
        minimum_history_days=int(
            contract["minimum_history_days_before_scoring"]
        ),
        source_action=str(contract["outcome_source_action"]),
        minimum_history_events=int(contract.get("minimum_history_events", 0)),
    )
    metric_oof = oof.loc[oof["rolling_calibration_eligible"]].copy()
    violations = _pathwise_prediction_violations(oof)
    metric_rows: list[dict[str, Any]] = []
    for side in ("BUY", "SELL"):
        for role in ("opener", "add", "reducing"):
            for horizon_ms in HORIZONS_MS:
                group = metric_oof.loc[
                    metric_oof["side"].eq(side)
                    & metric_oof["inventory_role"].eq(role)
                    & metric_oof["horizon_ms"].eq(int(horizon_ms))
                ]
                metric_rows.append(
                    {
                        "side": side,
                        "inventory_role": role,
                        "horizon_ms": int(horizon_ms),
                        **_metrics(
                            group,
                            bootstrap_seed=int(args.bootstrap_seed)
                            + len(metric_rows),
                            bootstrap_samples=int(args.bootstrap_samples),
                        ),
                    }
                )
    metrics = _apply_prediction_gates(
        pd.DataFrame(metric_rows),
        spec["prediction_gates"],
        empirical_calibration=True,
    )
    development_passed = bool(metrics["prediction_gate_passed"].all()) and (
        violations == 0
    )

    artifact = joblib.load(base_artifact_path)
    seeds = _rolling_seed_from_oof(static_oof, contract)
    artifact["schema_version"] = "direct_placement_fill_cif.v3"
    artifact["rolling_calibration_contract"] = contract
    artifact["development_prediction_gate_passed"] = development_passed
    artifact["action_or_live_authorization"] = False
    artifact["base_v2_artifact"] = {
        "path": str(base_artifact_path),
        "sha256": str(artifact_identity["sha256"]),
    }
    for side in ("BUY", "SELL"):
        artifact["models"][side]["rolling_calibration_seed"] = seeds[side]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    oof_path = args.output_dir / "oof_predictions.parquet"
    metrics_path = args.output_dir / "metrics.parquet"
    artifact_path = args.output_dir / "direct_fill_cif.joblib"
    oof.to_parquet(oof_path, index=False, compression="zstd")
    metrics.to_parquet(metrics_path, index=False, compression="zstd")
    joblib.dump(artifact, artifact_path)
    report = {
        "schema_version": "direct_placement_fill_cif.v3",
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "estimand": "new_placement_direct_fill_cif_before_cancel_ack",
        "development_days": list(base_report["development_days"]),
        "development_rows_wide": int(base_report["development_rows_wide"]),
        "development_rows_model": int(base_report["development_rows_model"]),
        "oof_rows": int(len(oof)),
        "metric_oof_rows": int(len(metric_oof)),
        "rolling_calibration_contract": contract,
        "base_v2_report": {
            "path": str(base_report_path),
            "sha256": str(lineage["v2_development_report_sha256"]),
        },
        "predicted_pathwise_monotonicity_violations": int(violations),
        "development_prediction_gate_passed": development_passed,
        "validation_access_allowed": development_passed,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "active_order_keep_replace": "separate_not_built",
        "spec_sha256": _sha256(args.spec),
        "panel_manifest_sha256": str(base_report["panel_manifest_sha256"]),
        "model_contract_sha256": str(base_report["model_contract_sha256"]),
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "oof_predictions": {"path": str(oof_path), "sha256": _sha256(oof_path)},
            "metrics": {"path": str(metrics_path), "sha256": _sha256(metrics_path)},
            "artifact": {"path": str(artifact_path), "sha256": _sha256(artifact_path)},
        },
    }
    _atomic_json(report, args.output_dir / "report.json")
    print(
        json.dumps(
            {
                "development_prediction_gate_passed": development_passed,
                "metric_oof_rows": int(len(metric_oof)),
                "prediction_cells_passed": int(
                    metrics["prediction_gate_passed"].sum()
                ),
                "validation_read": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _run_transfer_gate_revision(
    args: argparse.Namespace,
    spec: Mapping[str, Any],
) -> int:
    lineage = spec["lineage"]
    base_report_path = Path(str(lineage["v2_development_report"])).resolve()
    if _sha256(base_report_path) != str(
        lineage["v2_development_report_sha256"]
    ):
        raise RuntimeError("v2 Development report identity changed")
    base_report = json.loads(base_report_path.read_text(encoding="utf-8"))
    metrics_identity = base_report["outputs"]["metrics"]
    artifact_identity = base_report["outputs"]["artifact"]
    metrics_path = Path(str(metrics_identity["path"])).resolve()
    artifact_path = Path(str(artifact_identity["path"])).resolve()
    if _sha256(metrics_path) != str(metrics_identity["sha256"]):
        raise RuntimeError("v2 metrics identity changed")
    if _sha256(artifact_path) != str(artifact_identity["sha256"]):
        raise RuntimeError("v2 artifact identity changed")
    metrics = pd.read_parquet(metrics_path)
    gate_columns = [
        column
        for column in metrics.columns
        if column.endswith("_pass") or column == "prediction_gate_passed"
    ]
    metrics = metrics.drop(columns=gate_columns, errors="ignore")
    metrics = _apply_prediction_gates(
        metrics,
        spec["prediction_gates"],
        empirical_calibration=True,
    )
    development_passed = bool(metrics["prediction_gate_passed"].all()) and (
        int(base_report["predicted_pathwise_monotonicity_violations"]) == 0
    )
    artifact = joblib.load(artifact_path)
    artifact["schema_version"] = "direct_placement_fill_cif.v4"
    artifact["development_prediction_gate_passed"] = development_passed
    artifact["prediction_qualification"] = str(
        spec["prediction_gates"]["qualification_name"]
    )
    artifact["action_or_live_authorization"] = False
    artifact["base_v2_artifact"] = {
        "path": str(artifact_path),
        "sha256": str(artifact_identity["sha256"]),
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    revised_metrics_path = args.output_dir / "metrics.parquet"
    revised_artifact_path = args.output_dir / "direct_fill_cif.joblib"
    metrics.to_parquet(revised_metrics_path, index=False, compression="zstd")
    joblib.dump(artifact, revised_artifact_path)
    report = {
        "schema_version": "direct_placement_fill_cif.v4",
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "estimand": "new_placement_direct_fill_cif_before_cancel_ack",
        "prediction_qualification": str(
            spec["prediction_gates"]["qualification_name"]
        ),
        "development_days": list(base_report["development_days"]),
        "development_rows_wide": int(base_report["development_rows_wide"]),
        "oof_rows": int(base_report["oof_rows"]),
        "base_v2_report": {
            "path": str(base_report_path),
            "sha256": str(lineage["v2_development_report_sha256"]),
        },
        "predicted_pathwise_monotonicity_violations": int(
            base_report["predicted_pathwise_monotonicity_violations"]
        ),
        "development_prediction_gate_passed": development_passed,
        "prediction_cells_passed": int(metrics["prediction_gate_passed"].sum()),
        "validation_access_allowed": development_passed,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "absolute_probability_ev_authorized": False,
        "active_order_keep_replace": "separate_not_built",
        "spec_sha256": _sha256(args.spec),
        "panel_manifest_sha256": str(base_report["panel_manifest_sha256"]),
        "model_contract_sha256": str(base_report["model_contract_sha256"]),
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "base_oof_predictions": base_report["outputs"]["oof_predictions"],
            "metrics": {
                "path": str(revised_metrics_path),
                "sha256": _sha256(revised_metrics_path),
            },
            "artifact": {
                "path": str(revised_artifact_path),
                "sha256": _sha256(revised_artifact_path),
            },
        },
    }
    _atomic_json(report, args.output_dir / "report.json")
    print(
        json.dumps(
            {
                "development_prediction_gate_passed": development_passed,
                "prediction_cells_passed": int(
                    metrics["prediction_gate_passed"].sum()
                ),
                "qualification": report["prediction_qualification"],
                "validation_read": False,
            },
            sort_keys=True,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    args.spec = args.spec.expanduser().resolve()
    args.panel_dir = args.panel_dir.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    reproduction_identity = require_historical_reproduction(
        runner_id="f06.direct_fill_cif",
        enabled=bool(args.historical_reproduction),
        spec_path=args.spec,
    )
    spec = load_placement_fill_spec(args.spec)
    schema_version = str(spec.get("schema_version", ""))
    is_v4 = schema_version == "narrowgate_placement_fill_cif_spec.v4"
    is_v3 = schema_version == "narrowgate_placement_fill_cif_spec.v3"
    is_v2 = schema_version in {
        "narrowgate_placement_fill_cif_spec.v2",
        "narrowgate_placement_fill_cif_spec.v3",
        "narrowgate_placement_fill_cif_spec.v4",
    }
    expected_status = {
        "narrowgate_placement_fill_cif_spec.v1": "frozen_before_development_outcomes",
        "narrowgate_placement_fill_cif_spec.v2": "frozen_before_v2_development_outcomes",
        "narrowgate_placement_fill_cif_spec.v3": "frozen_before_v3_formal_development_run",
        "narrowgate_placement_fill_cif_spec.v4": "frozen_before_v4_validation_access",
    }.get(schema_version)
    if expected_status is None:
        raise RuntimeError(f"unsupported placement CIF spec: {schema_version}")
    if spec.get("research_status") != expected_status:
        raise RuntimeError("direct CIF spec is not frozen")
    if spec["active_order_estimand"]["status"] != "separate_and_not_built_by_this_pipeline":
        raise RuntimeError("KEEP/REPLACE estimand boundary is missing")
    if is_v4:
        status = _run_transfer_gate_revision(args, spec)
        stamp_historical_reproduction_output(args.output_dir, reproduction_identity)
        return status
    if is_v3:
        status = _run_rolling_revision(args, spec)
        stamp_historical_reproduction_output(args.output_dir, reproduction_identity)
        return status
    days = [str(day) for day in spec["panels"]["development"]["days"]]
    wide = _load_partitions(args.panel_dir, days)
    if int(wide["monotonicity_violation_count"].sum()) != 0:
        raise RuntimeError("observed placement paths are not monotone")
    panel = expand_placement_panel(wide)
    folds = make_expanding_folds(
        days,
        min_train_days=int(spec["development_fit"]["minimum_train_days"]),
        embargo_days=int(spec["development_fit"]["embargo_days"]),
        test_days=int(spec["development_fit"]["outer_test_days"]),
    )

    inner_cache: dict[str, tuple[pd.DataFrame, list[dict[str, Any]]]] = {}
    for side in ("BUY", "SELL"):
        side_panel = panel.loc[panel["side"].eq(side)]
        inner_cache[side] = _inner_oof_predictions(side_panel)

    oof_parts: list[pd.DataFrame] = []
    fold_identity: list[dict[str, Any]] = []
    for fold in folds:
        for side in ("BUY", "SELL"):
            train = panel.loc[
                panel["day"].isin(fold["train_days"])
                & panel["side"].eq(side)
            ]
            test = panel.loc[
                panel["day"].isin(fold["test_days"])
                & panel["side"].eq(side)
            ].copy()
            if test.empty:
                continue
            inner_oof, all_inner_folds = inner_cache[side]
            calibration_oof = inner_oof.loc[
                inner_oof["day"].isin(fold["train_days"])
            ].copy()
            calibrator = _fit_calibrator(
                calibration_oof["raw"].to_numpy(dtype=float),
                calibration_oof["target"].to_numpy(dtype=int),
            )
            cell_offsets: dict[tuple[str, int], float] = {}
            drift_envelopes: dict[tuple[str, int], float] = {}
            if is_v2:
                calibration_oof["probability"] = _apply_calibrator(
                    calibration_oof["raw"].to_numpy(dtype=float), calibrator
                )
                cell_offsets = _fit_cell_offsets(calibration_oof)
                calibration_oof["probability"] = _apply_cell_offsets(
                    calibration_oof,
                    calibration_oof["probability"].to_numpy(dtype=float),
                    cell_offsets,
                )
                drift_quantile = float(
                    spec["prediction_gates"]["calibration_level"][
                        "inner_oof_empirical_drift_quantile"
                    ]
                )
                drift_envelopes = _cell_drift_envelopes(
                    calibration_oof,
                    probability_column="probability",
                    quantile=drift_quantile,
                )
            inner_folds = [
                inner_fold
                for inner_fold in all_inner_folds
                if set(inner_fold["test_days"]) & set(fold["train_days"])
            ]
            model = _fit_model(train)
            raw = _predict_raw(model, test)
            test["raw_probability"] = raw
            test["probability"] = _apply_calibrator(raw, calibrator)
            if is_v2:
                test["probability"] = _apply_cell_offsets(
                    test,
                    test["probability"].to_numpy(dtype=float),
                    cell_offsets,
                )
                test["empirical_abs_bias_tolerance"] = _apply_cell_values(
                    test, drift_envelopes
                )
            test["baseline_probability"] = _apply_baseline(
                test, _baseline_rates(train)
            )
            test["outer_fold"] = int(fold["fold"])
            oof_parts.append(
                test[
                    [
                        "cohort_id",
                        "day",
                        "side",
                        "inventory_role",
                        "action",
                        "horizon_ms",
                        "distance_ticks",
                        "distance_vol_units",
                        "target",
                        "raw_probability",
                        "probability",
                        "baseline_probability",
                        *(
                            ["empirical_abs_bias_tolerance"]
                            if is_v2
                            else []
                        ),
                        "outer_fold",
                    ]
                ]
            )
            fold_identity.append(
                {
                    **fold,
                    "side": side,
                    "inner_folds": inner_folds,
                    "calibrator": calibrator,
                    "cell_offsets": {
                        f"{role}|{horizon}": value
                        for (role, horizon), value in cell_offsets.items()
                    },
                    "empirical_drift_envelopes": {
                        f"{role}|{horizon}": value
                        for (role, horizon), value in drift_envelopes.items()
                    },
                    "inner_calibration_rows": int(len(calibration_oof)),
                    "train_rows": int(len(train)),
                    "test_rows": int(len(test)),
                }
            )
            del train, test, model, calibration_oof
            gc.collect()
    if not oof_parts:
        raise RuntimeError("chronological direct CIF produced no OOF rows")
    oof = pd.concat(oof_parts, ignore_index=True)
    metric_oof = oof
    rolling_contract: dict[str, Any] | None = None
    if is_v3:
        rolling_contract = dict(spec["development_fit"]["rolling_calibration"])
        oof = apply_past_only_rolling_calibration(
            oof,
            window_days=int(rolling_contract["window_utc_days"]),
            minimum_history_days=int(
                rolling_contract["minimum_history_days_before_scoring"]
            ),
            source_action=str(rolling_contract["outcome_source_action"]),
            minimum_history_events=int(
                rolling_contract.get("minimum_history_events", 0)
            ),
        )
        metric_oof = oof.loc[oof["rolling_calibration_eligible"]].copy()
    violations = _pathwise_prediction_violations(oof)

    metric_rows = []
    for side in ("BUY", "SELL"):
        for role in ("opener", "add", "reducing"):
            for horizon in HORIZONS_MS:
                group = metric_oof.loc[
                    metric_oof["side"].eq(side)
                    & metric_oof["inventory_role"].eq(role)
                    & metric_oof["horizon_ms"].eq(int(horizon))
                ]
                if group.empty:
                    continue
                metric_rows.append(
                    {
                        "side": side,
                        "inventory_role": role,
                        "horizon_ms": int(horizon),
                        **_metrics(
                            group,
                            bootstrap_seed=int(args.bootstrap_seed) + len(metric_rows),
                            bootstrap_samples=int(args.bootstrap_samples),
                        ),
                    }
                )
    metrics = pd.DataFrame(metric_rows)
    gates = spec["prediction_gates"]
    metrics = _apply_prediction_gates(
        metrics,
        gates,
        empirical_calibration=is_v2,
    )
    development_passed = bool(metrics["prediction_gate_passed"].all()) and violations == 0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    oof_path = args.output_dir / "oof_predictions.parquet"
    metrics_path = args.output_dir / "metrics.parquet"
    oof.to_parquet(oof_path, index=False, compression="zstd")
    metrics.to_parquet(metrics_path, index=False, compression="zstd")

    final_models: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        side_panel = panel.loc[panel["side"].eq(side)]
        inner_oof, inner_folds = inner_cache[side]
        calibrator = _fit_calibrator(
            inner_oof["raw"].to_numpy(dtype=float),
            inner_oof["target"].to_numpy(dtype=int),
        )
        cell_offsets: dict[tuple[str, int], float] = {}
        drift_envelopes: dict[tuple[str, int], float] = {}
        if is_v2:
            inner_oof = inner_oof.copy()
            inner_oof["probability"] = _apply_calibrator(
                inner_oof["raw"].to_numpy(dtype=float), calibrator
            )
            cell_offsets = _fit_cell_offsets(inner_oof)
            inner_oof["probability"] = _apply_cell_offsets(
                inner_oof,
                inner_oof["probability"].to_numpy(dtype=float),
                cell_offsets,
            )
            drift_envelopes = _cell_drift_envelopes(
                inner_oof,
                probability_column="probability",
                quantile=float(
                    gates["calibration_level"][
                        "inner_oof_empirical_drift_quantile"
                    ]
                ),
            )
        rolling_seed: dict[tuple[str, int], dict[str, Any]] = {}
        if is_v3 and rolling_contract is not None:
            window_days = int(rolling_contract["window_utc_days"])
            source_action = str(rolling_contract["outcome_source_action"])
            calibrated_inner = inner_oof.copy()
            calibrated_inner["probability"] = _apply_calibrator(
                calibrated_inner["raw"].to_numpy(dtype=float), calibrator
            )
            calibrated_inner["probability"] = _apply_cell_offsets(
                calibrated_inner,
                calibrated_inner["probability"].to_numpy(dtype=float),
                cell_offsets,
            )
            for role in ("opener", "add", "reducing"):
                for horizon_ms in HORIZONS_MS:
                    cell = calibrated_inner.loc[
                        calibrated_inner["inventory_role"]
                        .astype(str)
                        .str.lower()
                        .eq(role)
                        & calibrated_inner["horizon_ms"].eq(int(horizon_ms))
                        & calibrated_inner["action"].astype(str).eq(source_action)
                    ]
                    seed_days = sorted(cell["day"].astype(str).unique())[
                        -window_days:
                    ]
                    history = cell.loc[cell["day"].astype(str).isin(seed_days)]
                    rolling_seed[(role, int(horizon_ms))] = {
                        "days": seed_days,
                        "offset": _fit_intercept_offset(
                            history["probability"].to_numpy(dtype=float),
                            history["target"].to_numpy(dtype=int),
                        ),
                        "rows": int(len(history)),
                        "events": int(history["target"].sum()),
                    }
        final_models[side] = {
            "model": _fit_model(side_panel),
            "calibrator": calibrator,
            "cell_offsets": cell_offsets,
            "empirical_drift_envelopes": drift_envelopes,
            "rolling_calibration_seed": rolling_seed,
            "inner_folds": inner_folds,
            "baseline_rates": _baseline_rates(side_panel),
        }
    artifact_path = args.output_dir / "direct_fill_cif.joblib"
    joblib.dump(
        {
            "schema_version": (
                "direct_placement_fill_cif.v3"
                if is_v3
                else "direct_placement_fill_cif.v2"
                if is_v2
                else SCHEMA_VERSION
            ),
            "model_kind": MODEL_KIND,
            "features": BASE_FEATURES,
            "model_contract": MODEL_CONTRACT,
            "models": final_models,
            "development_prediction_gate_passed": development_passed,
            "action_or_live_authorization": False,
            "active_order_keep_replace": "separate_not_built",
            "placement_actions_pooled_inside_calibration_cells": True,
            "rolling_calibration_contract": rolling_contract,
        },
        artifact_path,
    )
    report = {
        "schema_version": (
            "direct_placement_fill_cif.v3"
            if is_v3
            else "direct_placement_fill_cif.v2"
            if is_v2
            else SCHEMA_VERSION
        ),
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "estimand": "new_placement_direct_fill_cif_before_cancel_ack",
        "development_days": days,
        "development_rows_wide": int(len(wide)),
        "development_rows_model": int(len(panel)),
        "oof_rows": int(len(oof)),
        "metric_oof_rows": int(len(metric_oof)),
        "rolling_calibration_contract": rolling_contract,
        "folds": fold_identity,
        "observed_pathwise_monotonicity_violations": int(
            wide["monotonicity_violation_count"].sum()
        ),
        "predicted_pathwise_monotonicity_violations": int(violations),
        "development_prediction_gate_passed": development_passed,
        "validation_access_allowed": development_passed,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "active_order_keep_replace": "separate_not_built",
        "spec_sha256": _sha256(args.spec),
        "panel_manifest_sha256": _sha256(args.panel_dir / "manifest.json"),
        "model_contract_sha256": _canonical_hash(MODEL_CONTRACT),
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
                "development_prediction_gate_passed": development_passed,
                "oof_rows": int(len(oof)),
                "predicted_monotonicity_violations": int(violations),
                "validation_read": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
