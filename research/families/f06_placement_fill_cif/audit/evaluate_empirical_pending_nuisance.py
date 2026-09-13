#!/usr/bin/env python3
"""Evaluate action-specific placement curves with empirical pending-fill risk."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import beta as beta_distribution
from scipy.stats import t as student_t

from data_paths import data_root
from models.audit.content_addressed_cache import canonical_sha256, file_sha256
from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.evaluate_request_state_race import (
    _known_targets,
    _phase_frame,
)
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import ROOT
from research.governance.paths import verify_path_identity

DATA_ROOT = data_root(ROOT)
DEFAULT_SPEC = (
    FAMILY_DOCS / "placement_fill_empirical_pending_nuisance_v3_fit_spec_20260728.json"
)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports"
    / "placement_fill_empirical_pending_nuisance_v3_development_20260728"
)
SCHEMA_VERSION = "placement_fill_empirical_pending_nuisance_development.v1"

ACTIONS = ("closer_1tick", "current", "farther_1tick")
ROLES = ("opener", "add", "reducing")
SIDES = ("BUY", "SELL")
PENDING_FEATURES = (
    "request_active_order_count",
    "request_pending_cancel_before_count",
    "request_request_batch_size",
    "abs_request_market_return_bps_100ms",
    "abs_request_taker_imbalance_100ms",
    "request_book_age_ms",
)
RAW_COLUMNS = (
    "cohort_id",
    "day",
    "side",
    "inventory_role",
    "action",
    "action_lifecycle_id",
    "activation_status",
    "activation_ts_ns",
    "cancel_request_ts_ns",
    "cancel_request_reason",
    "request_model_risk_set",
    "request_remaining_qty",
    "request_mid",
    "pending_cancel_fill",
    "pending_cancel_fill_qty",
    "cancel_ack_observed",
    "pending_right_censored_by_gap",
    "pending_risk_duration_ms",
    "first_pending_cancel_fill_ts_ns",
    "actual_cancel_ack_ts_ns",
    "request_active_order_count",
    "request_pending_cancel_before_count",
    "request_request_batch_size",
    "request_market_return_bps_100ms",
    "request_taker_imbalance_100ms",
    "request_book_age_ms",
)


@dataclass(frozen=True)
class BetaCell:
    alpha: float
    beta: float
    events: int
    trials: int

    @property
    def mean(self) -> float:
        return float(self.alpha / (self.alpha + self.beta))

    def upper(self, probability: float) -> float:
        return float(beta_distribution.ppf(probability, self.alpha, self.beta))

    def as_dict(self, probability: float) -> dict[str, Any]:
        return {
            "alpha": float(self.alpha),
            "beta": float(self.beta),
            "events": int(self.events),
            "trials": int(self.trials),
            "posterior_mean": self.mean,
            "posterior_upper": self.upper(probability),
            "posterior_probability": float(probability),
        }


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
        "narrowgate_placement_fill_empirical_pending_fit_spec.v1"
    ):
        raise RuntimeError("unsupported empirical-pending fit specification")
    for label, identity in spec["source_identity"].items():
        _require_identity(
            Path(str(identity["path"])).expanduser().resolve(),
            str(identity["sha256"]),
            label,
        )
    evaluator = ROOT / str(spec["implementation"]["evaluator"])
    _require_identity(
        evaluator,
        str(spec["implementation"]["evaluator_sha256"]),
        "empirical-pending evaluator",
    )
    return spec


def _bootstrap_mean(
    values: np.ndarray,
    *,
    samples: int,
    seed: int,
) -> dict[str, float]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return {"mean": math.nan, "lower": math.nan, "upper": math.nan}
    rng = np.random.default_rng(int(seed))
    draws = rng.choice(
        finite,
        size=(int(samples), finite.size),
        replace=True,
    ).mean(axis=1)
    return {
        "mean": float(finite.mean()),
        "lower": float(np.quantile(draws, 0.025)),
        "upper": float(np.quantile(draws, 0.975)),
    }


def _load_index(spec: Mapping[str, Any]) -> pd.DataFrame:
    identity = spec["source_identity"]["request_state_index"]
    index = pd.read_csv(identity["path"], dtype={"day": str})
    expected = set(spec["panels"]["development_days"])
    actual = set(index["day"].astype(str))
    if expected != actual:
        raise RuntimeError(
            "request-state index differs from frozen Development days: "
            f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
        )
    return index


def _load_days(
    index: pd.DataFrame,
    days: Sequence[str],
    *,
    columns: Sequence[str] = RAW_COLUMNS,
) -> pd.DataFrame:
    day_set = set(days)
    selected = index.loc[index["day"].astype(str).isin(day_set)]
    if len(selected) != len(day_set):
        missing = sorted(day_set - set(selected["day"].astype(str)))
        raise RuntimeError(f"request-state index lacks days: {missing}")
    pieces: list[pd.DataFrame] = []
    for row in selected.sort_values("day").itertuples(index=False):
        path = Path(str(row.payload_path)).expanduser().resolve()
        _require_identity(path, str(row.payload_sha256), f"request state {row.day}")
        pieces.append(pd.read_parquet(path, columns=list(columns)))
    return pd.concat(pieces, ignore_index=True)


def _request_features(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    output["abs_request_market_return_bps_100ms"] = np.abs(
        pd.to_numeric(
            output["request_market_return_bps_100ms"], errors="coerce"
        ).fillna(0.0)
    )
    output["abs_request_taker_imbalance_100ms"] = np.abs(
        pd.to_numeric(
            output["request_taker_imbalance_100ms"], errors="coerce"
        ).fillna(0.0)
    )
    return output


def _full_race_targets(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    risk = frame["request_model_risk_set"].to_numpy(np.int8) != 0
    fill = frame["pending_cancel_fill"].to_numpy(np.int8) != 0
    ack = frame["cancel_ack_observed"].to_numpy(np.int8) != 0
    censored = frame["pending_right_censored_by_gap"].to_numpy(np.int8) != 0
    known = risk & (fill | ack) & ~(censored & ~fill & ~ack)
    return known, fill.astype(np.int8)


def _pending_targets(
    frame: pd.DataFrame,
    horizon_ms: int | None,
) -> tuple[pd.DataFrame, np.ndarray]:
    pending = _phase_frame(frame, "pending")
    if horizon_ms is None:
        known, target = _full_race_targets(pending)
    else:
        known, target, _ = _known_targets(pending, "pending", int(horizon_ms))
    return pending.loc[known].reset_index(drop=True), target[known]


def _fit_parent_cells(
    frame: pd.DataFrame,
    target: np.ndarray,
    *,
    equivalent_trials: float,
) -> dict[tuple[str, str], BetaCell]:
    work = frame.loc[:, ["side", "action"]].copy()
    work["target"] = np.asarray(target, dtype=np.int8)
    output: dict[tuple[str, str], BetaCell] = {}
    for side in SIDES:
        side_rows = work.loc[work["side"].astype(str).str.upper().eq(side)]
        side_events = int(side_rows["target"].sum())
        side_trials = int(len(side_rows))
        side_mean = (side_events + 0.5) / (side_trials + 1.0)
        for action in ACTIONS:
            cell = side_rows.loc[side_rows["action"].astype(str).eq(action)]
            events = int(cell["target"].sum())
            trials = int(len(cell))
            alpha = float(equivalent_trials) * side_mean + events
            beta = float(equivalent_trials) * (1.0 - side_mean) + trials - events
            output[(side, action)] = BetaCell(
                alpha=max(alpha, 1e-9),
                beta=max(beta, 1e-9),
                events=events,
                trials=trials,
            )
    return output


def _fit_child_cells(
    frame: pd.DataFrame,
    target: np.ndarray,
    parents: Mapping[tuple[str, str], BetaCell],
    *,
    child: str,
    equivalent_trials: float,
) -> dict[tuple[str, str, str], BetaCell]:
    work = frame.loc[:, ["side", "action", child]].copy()
    work[child] = work[child].fillna("missing").astype(str)
    work["target"] = np.asarray(target, dtype=np.int8)
    output: dict[tuple[str, str, str], BetaCell] = {}
    for (side, action, value), cell in work.groupby(
        ["side", "action", child], observed=True
    ):
        key = (str(side).upper(), str(action))
        parent = parents[key]
        events = int(cell["target"].sum())
        trials = int(len(cell))
        output[(key[0], key[1], str(value))] = BetaCell(
            alpha=float(equivalent_trials) * parent.mean + events,
            beta=float(equivalent_trials) * (1.0 - parent.mean) + trials - events,
            events=events,
            trials=trials,
        )
    return output


def _daily_action_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    keys = [
        "day",
        "side",
        "inventory_role",
        "action",
        "phase",
        "horizon_ms",
    ]
    for values, group in predictions.groupby(keys, observed=True):
        day, side, role, action, phase, horizon = values
        causes = ("fill", "ack") if phase == "pending" else ("fill",)
        for cause in causes:
            target = group[f"{cause}_target"].to_numpy(float)
            probability = group[f"{cause}_probability"].to_numpy(float)
            baseline = group[f"baseline_{cause}_probability"].to_numpy(float)
            rows.append(
                {
                    "day": str(day),
                    "side": str(side),
                    "inventory_role": str(role),
                    "action": str(action),
                    "phase": str(phase),
                    "cause": cause,
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
            target = group[
                ["fill_target", "ack_target", "no_event_target"]
            ].to_numpy(float)
            probability = group[
                ["fill_probability", "ack_probability", "no_event_probability"]
            ].to_numpy(float)
            baseline = group[
                [
                    "baseline_fill_probability",
                    "baseline_ack_probability",
                    "baseline_no_event_probability",
                ]
            ].to_numpy(float)
            rows.append(
                {
                    "day": str(day),
                    "side": str(side),
                    "inventory_role": str(role),
                    "action": str(action),
                    "phase": str(phase),
                    "cause": "joint",
                    "horizon_ms": int(horizon),
                    "rows": int(len(group)),
                    "events": int((target[:, :2].sum(axis=1) > 0).sum()),
                    "brier_improvement": float(
                        np.mean(
                            np.square(target - baseline).sum(axis=1)
                            - np.square(target - probability).sum(axis=1)
                        )
                    ),
                    "calibration_bias": 0.0,
                }
            )
    return pd.DataFrame(rows)


def _action_curve_gate(
    daily: pd.DataFrame,
    *,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    integrated = (
        daily.groupby(
            [
                "day",
                "side",
                "inventory_role",
                "action",
                "phase",
                "cause",
            ],
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
    rows: list[dict[str, Any]] = []
    groups = ["side", "inventory_role", "action", "phase", "cause"]
    for index, (values, group) in enumerate(
        integrated.groupby(groups, observed=True)
    ):
        side, role, action, phase, cause = values
        brier = _bootstrap_mean(
            group["brier_improvement"].to_numpy(),
            samples=int(config["bootstrap_samples"]),
            seed=int(config["bootstrap_seed"]) + 2 * index,
        )
        calibration = _bootstrap_mean(
            group["calibration_bias"].to_numpy(),
            samples=int(config["bootstrap_samples"]),
            seed=int(config["bootstrap_seed"]) + 2 * index + 1,
        )
        required = phase == "pre_request" and cause == "fill" or (
            phase == "pending" and cause == "ack"
        )
        minimum = (
            int(config["pre_request_minimum_events"])
            if phase == "pre_request"
            else int(config["ack_minimum_events"])
        )
        support = bool(
            group["day"].nunique() == int(config["required_oof_days"])
            and int(group["events"].sum()) >= minimum
        )
        proper = bool(brier["lower"] > 0.0)
        calibrated = bool(
            cause == "joint"
            or calibration["lower"] <= 0.0 <= calibration["upper"]
        )
        rows.append(
            {
                "side": str(side),
                "inventory_role": str(role),
                "action": str(action),
                "phase": str(phase),
                "cause": str(cause),
                "days": int(group["day"].nunique()),
                "events": int(group["events"].sum()),
                "brier_improvement": brier,
                "calibration_bias": calibration,
                "support_pass": support,
                "proper_score_pass": proper,
                "calibration_pass": calibrated,
                "required_component": required,
                "curve_pass": bool(support and proper and calibrated),
            }
        )
    return rows


def _fit_thresholds(
    train: pd.DataFrame,
    quantiles: Sequence[float],
) -> dict[str, dict[str, tuple[float, float]]]:
    prepared = _request_features(train)
    output: dict[str, dict[str, tuple[float, float]]] = {}
    for side in SIDES:
        side_rows = prepared.loc[
            prepared["side"].astype(str).str.upper().eq(side)
        ]
        output[side] = {}
        for feature in PENDING_FEATURES:
            values = pd.to_numeric(side_rows[feature], errors="coerce")
            finite = values[np.isfinite(values)]
            if finite.empty:
                output[side][feature] = (math.nan, math.nan)
            else:
                low, high = np.quantile(finite, quantiles)
                output[side][feature] = (float(low), float(high))
    return output


def _residual_rows(
    merged: pd.DataFrame,
    thresholds: Mapping[str, Mapping[str, tuple[float, float]]],
    *,
    fold: int,
) -> list[dict[str, Any]]:
    prepared = _request_features(merged)
    rows: list[dict[str, Any]] = []
    for side in SIDES:
        side_rows = prepared.loc[
            prepared["side"].astype(str).str.upper().eq(side)
        ]
        for feature in PENDING_FEATURES:
            low, high = thresholds[side][feature]
            if not np.isfinite(low) or not np.isfinite(high) or low >= high:
                continue
            values = pd.to_numeric(side_rows[feature], errors="coerce")
            for tail, mask in (
                ("low", values <= low),
                ("high", values >= high),
            ):
                tail_rows = side_rows.loc[mask]
                for (day, action), group in tail_rows.groupby(
                    ["day", "action"], observed=True
                ):
                    rows.append(
                        {
                            "fold": int(fold),
                            "day": str(day),
                            "side": side,
                            "action": str(action),
                            "feature": feature,
                            "tail": tail,
                            "rows": int(len(group)),
                            "ack_events": int(group["ack_target"].sum()),
                            "ack_bias": float(
                                np.mean(
                                    group["ack_probability"].to_numpy(float)
                                    - group["ack_target"].to_numpy(float)
                                )
                            ),
                            "pending_events": int(group["fill_target"].sum()),
                            "pending_predicted": float(
                                group["empirical_pending_probability"].sum()
                            ),
                            "posterior_alpha": float(
                                group["pending_alpha"].iloc[0]
                            ),
                            "posterior_beta": float(
                                group["pending_beta"].iloc[0]
                            ),
                        }
                    )
    return rows


def _bonferroni_interval(
    daily_values: np.ndarray,
    *,
    comparisons: int,
) -> dict[str, float]:
    values = np.asarray(daily_values, dtype=float)
    values = values[np.isfinite(values)]
    if values.size < 2:
        return {"mean": math.nan, "lower": math.nan, "upper": math.nan}
    mean = float(values.mean())
    standard_error = float(values.std(ddof=1) / math.sqrt(values.size))
    alpha = 0.05 / max(1, int(comparisons))
    critical = float(student_t.ppf(1.0 - alpha / 2.0, values.size - 1))
    return {
        "mean": mean,
        "lower": mean - critical * standard_error,
        "upper": mean + critical * standard_error,
    }


def _pending_predictive_interval(
    groups: pd.DataFrame,
    *,
    probability: float,
    samples: int,
    seed: int,
) -> tuple[float, float]:
    rng = np.random.default_rng(int(seed))
    draws = np.zeros(int(samples), dtype=np.int64)
    for row in groups.itertuples(index=False):
        probability_draw = rng.beta(
            float(row.posterior_alpha),
            float(row.posterior_beta),
            size=int(samples),
        )
        draws += rng.binomial(int(row.rows), probability_draw)
    tail = (1.0 - float(probability)) / 2.0
    return float(np.quantile(draws, tail)), float(np.quantile(draws, 1.0 - tail))


def _residual_audit(
    daily: pd.DataFrame,
    *,
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    keys = ["side", "action", "feature", "tail"]
    grouped = list(daily.groupby(keys, observed=True))
    supported = [
        (values, group)
        for values, group in grouped
        if group["day"].nunique() >= int(config["minimum_days"])
        and int(group["ack_events"].sum()) >= int(config["minimum_ack_events"])
    ]
    comparisons = len(supported)
    rows: list[dict[str, Any]] = []
    supported_keys = {tuple(values) for values, _ in supported}
    for index, (values, group) in enumerate(grouped):
        side, action, feature, tail = values
        is_supported = tuple(values) in supported_keys
        ack_interval = _bonferroni_interval(
            group["ack_bias"].to_numpy(),
            comparisons=max(1, comparisons),
        )
        posterior_groups = (
            group.groupby(
                ["fold", "posterior_alpha", "posterior_beta"], observed=True
            )
            .agg(rows=("rows", "sum"))
            .reset_index()
        )
        pending_lower, pending_upper = _pending_predictive_interval(
            posterior_groups,
            probability=float(config["pending_posterior_probability"]),
            samples=int(config["pending_predictive_samples"]),
            seed=int(config["seed"]) + index,
        )
        pending_events = int(group["pending_events"].sum())
        rows.append(
            {
                "side": str(side),
                "action": str(action),
                "feature": str(feature),
                "tail": str(tail),
                "days": int(group["day"].nunique()),
                "rows": int(group["rows"].sum()),
                "ack_events": int(group["ack_events"].sum()),
                "ack_bias_bonferroni_interval": ack_interval,
                "ack_supported": bool(is_supported),
                "ack_residual_pass": bool(
                    not is_supported
                    or ack_interval["lower"] <= 0.0 <= ack_interval["upper"]
                ),
                "pending_events": pending_events,
                "pending_predicted": float(group["pending_predicted"].sum()),
                "pending_predictive_lower": pending_lower,
                "pending_predictive_upper": pending_upper,
                "pending_predictive_pass": bool(
                    pending_lower <= pending_events <= pending_upper
                ),
                "latent_independence_identified": False,
            }
        )
    return rows


def _activation_metrics(raw: pd.DataFrame, panel: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (side, action), group in raw.groupby(["side", "action"], observed=True):
        status = group["activation_status"].fillna("missing").astype(str)
        counts = status.value_counts().to_dict()
        rows.append(
            {
                "panel": panel,
                "side": str(side),
                "action": str(action),
                "rows": int(len(group)),
                "active": int(counts.get("active", 0)),
                "activation_rate": float(status.eq("active").mean()),
                "gtx_reject": int(counts.get("gtx_reject", 0)),
                "gtx_reject_rate": float(status.eq("gtx_reject").mean()),
                "other_outcomes": {
                    key: int(value)
                    for key, value in sorted(counts.items())
                    if key not in {"active", "gtx_reject"}
                },
            }
        )
    return rows


def _common_support_actions(
    predictions: pd.DataFrame,
    raw: pd.DataFrame,
    *,
    phase: str,
    horizon_ms: int,
    support_column: str,
) -> pd.DataFrame:
    identity = raw.loc[
        :,
        [
            "action_lifecycle_id",
            "cohort_id",
            "side",
            "inventory_role",
            "action",
            "activation_status",
            "request_model_risk_set",
        ],
    ]
    selected = predictions.loc[
        (predictions["phase"].astype(str) == phase)
        & (predictions["horizon_ms"].astype(int) == int(horizon_ms))
    ].merge(identity, on="action_lifecycle_id", how="inner", validate="many_to_one")
    for name in ("cohort_id", "side", "inventory_role", "action"):
        source = f"{name}_x"
        if source in selected:
            selected[name] = selected[source]
    if support_column == "activation_status":
        selected["_supported"] = selected["activation_status"].astype(str).eq(
            "active"
        )
    else:
        selected["_supported"] = (
            pd.to_numeric(selected[support_column], errors="coerce").fillna(0)
            != 0
        )
    common = selected.groupby("cohort_id", observed=True)["_supported"].transform(
        lambda values: bool(len(values) == 3 and values.all())
    )
    return selected.loc[common].copy()


def _pre_monotonicity_metrics(
    predictions: pd.DataFrame,
    raw: pd.DataFrame,
    *,
    fold: int,
    horizons_ms: Sequence[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for horizon_ms in horizons_ms:
        selected = _common_support_actions(
            predictions,
            raw,
            phase="pre_request",
            horizon_ms=int(horizon_ms),
            support_column="activation_status",
        )
        for (side, role), group in selected.groupby(
            ["side", "inventory_role"], observed=True
        ):
            predicted = group.pivot(
                index="cohort_id",
                columns="action",
                values="fill_probability",
            ).dropna(subset=list(ACTIONS))
            observed = group.pivot(
                index="cohort_id",
                columns="action",
                values="fill_target",
            ).dropna(subset=list(ACTIONS))
            predicted_violation = (
                (predicted["closer_1tick"] + 1e-12 < predicted["current"])
                | (predicted["current"] + 1e-12 < predicted["farther_1tick"])
            )
            observed_violation = (
                (observed["closer_1tick"] < observed["current"])
                | (observed["current"] < observed["farther_1tick"])
            )
            rows.append(
                {
                    "fold": int(fold),
                    "phase": "pre_request",
                    "side": str(side),
                    "inventory_role": str(role),
                    "horizon_ms": int(horizon_ms),
                    "common_support_cohorts": int(len(predicted)),
                    "prediction_violations": int(predicted_violation.sum()),
                    "prediction_violation_rate": float(predicted_violation.mean())
                    if len(predicted_violation)
                    else math.nan,
                    "observed_path_violations": int(observed_violation.sum()),
                    "observed_path_violation_rate": float(observed_violation.mean())
                    if len(observed_violation)
                    else math.nan,
                    "action_probabilities": None,
                    "gtx_reject_in_denominator": False,
                }
            )
    return rows


def _pending_monotonicity_metrics(
    predictions: pd.DataFrame,
    raw: pd.DataFrame,
    cells_by_horizon: Mapping[int, Mapping[tuple[str, str], BetaCell]],
    *,
    fold: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for horizon_ms, cells in sorted(cells_by_horizon.items()):
        selected = _common_support_actions(
            predictions,
            raw,
            phase="pending",
            horizon_ms=int(horizon_ms),
            support_column="request_model_risk_set",
        )
        for (side, role), group in selected.groupby(
            ["side", "inventory_role"], observed=True
        ):
            observed = group.pivot(
                index="cohort_id",
                columns="action",
                values="fill_target",
            ).dropna(subset=list(ACTIONS))
            observed_violation = (
                (observed["closer_1tick"] < observed["current"])
                | (observed["current"] < observed["farther_1tick"])
            )
            probabilities = [cells[(str(side), action)].mean for action in ACTIONS]
            predicted_violation = bool(
                probabilities[0] + 1e-12 < probabilities[1]
                or probabilities[1] + 1e-12 < probabilities[2]
            )
            rows.append(
                {
                    "fold": int(fold),
                    "phase": "pending",
                    "side": str(side),
                    "inventory_role": str(role),
                    "horizon_ms": int(horizon_ms),
                    "common_support_cohorts": int(len(observed)),
                    "prediction_violations": int(predicted_violation),
                    "prediction_violation_rate": float(predicted_violation),
                    "observed_path_violations": int(observed_violation.sum()),
                    "observed_path_violation_rate": float(observed_violation.mean())
                    if len(observed_violation)
                    else math.nan,
                    "action_probabilities": {
                        action: float(value)
                        for action, value in zip(ACTIONS, probabilities, strict=True)
                    },
                    "gtx_reject_in_denominator": False,
                }
            )
    return rows


def _quantity_metrics(
    frame: pd.DataFrame,
    target: np.ndarray,
    cells: Mapping[tuple[str, str], BetaCell],
    *,
    posterior_probability: float,
    stress_bps: Sequence[float],
    negligible_usdc: float,
) -> list[dict[str, Any]]:
    work = frame.copy()
    work["target"] = np.asarray(target, dtype=np.int8)
    work["pending_qty"] = np.where(
        work["target"].to_numpy(np.int8) != 0,
        np.maximum(
            0.0,
            pd.to_numeric(
                work["pending_cancel_fill_qty"], errors="coerce"
            ).fillna(0.0),
        ),
        0.0,
    )
    remaining = np.maximum(
        0.0,
        pd.to_numeric(work["request_remaining_qty"], errors="coerce").fillna(0.0),
    )
    work["pending_fraction"] = np.divide(
        work["pending_qty"].to_numpy(float),
        remaining.to_numpy(float),
        out=np.zeros(len(work), dtype=float),
        where=remaining.to_numpy(float) > 0.0,
    )
    work["remaining_notional_usdc"] = (
        remaining.to_numpy(float)
        * np.maximum(
            0.0,
            pd.to_numeric(work["request_mid"], errors="coerce").fillna(0.0),
        ).to_numpy(float)
    )
    rows: list[dict[str, Any]] = []
    for (side, action), group in work.groupby(["side", "action"], observed=True):
        key = (str(side).upper(), str(action))
        cell = cells[key]
        positive = group.loc[group["target"].astype(bool)]
        probability_uncertainty = max(
            0.0,
            cell.upper(posterior_probability) - cell.mean,
        )
        ev_bounds = {
            str(int(value)): float(
                probability_uncertainty
                * group["remaining_notional_usdc"].mean()
                * float(value)
                / 10_000.0
            )
            for value in stress_bps
        }
        maximum_bound = max(ev_bounds.values()) if ev_bounds else math.nan
        rows.append(
            {
                "side": key[0],
                "action": key[1],
                "known_race_rows": int(len(group)),
                "pending_fill_events": int(group["target"].sum()),
                "expected_pending_fill_qty": float(group["pending_qty"].mean()),
                "pending_fill_probability": float(group["target"].mean()),
                "conditional_pending_fill_qty": float(positive["pending_qty"].mean())
                if len(positive)
                else 0.0,
                "conditional_pending_fill_fraction": float(
                    positive["pending_fraction"].mean()
                )
                if len(positive)
                else 0.0,
                "unconditional_pending_fill_fraction": float(
                    group["pending_fraction"].mean()
                ),
                "posterior": cell.as_dict(posterior_probability),
                "probability_uncertainty_upper_minus_mean": probability_uncertainty,
                "ev_uncertainty_usdc_by_stress_bps": ev_bounds,
                "maximum_ev_uncertainty_usdc": maximum_bound,
                "economically_negligible_at_maximum_stress": bool(
                    maximum_bound <= float(negligible_usdc)
                ),
            }
        )
    return rows


def _pending_artifact(
    frame: pd.DataFrame,
    *,
    hierarchy: Mapping[str, Any],
) -> dict[str, Any]:
    known, target = _pending_targets(frame, None)
    parents = _fit_parent_cells(
        known,
        target,
        equivalent_trials=float(hierarchy["side_action_parent_equivalent_trials"]),
    )
    reason = _fit_child_cells(
        known,
        target,
        parents,
        child="cancel_request_reason",
        equivalent_trials=float(hierarchy["child_equivalent_trials"]),
    )
    role = _fit_child_cells(
        known,
        target,
        parents,
        child="inventory_role",
        equivalent_trials=float(hierarchy["child_equivalent_trials"]),
    )
    probability = float(hierarchy["posterior_interval"])
    return {
        "schema_version": "pending_fill_hierarchical_beta_binomial.v1",
        "fit_panel": "all 50 Development days; artifact has no Validation/action/live authority",
        "primary_level": "side x action",
        "request_reason_and_role_not_fully_crossed": True,
        "posterior_probability": probability,
        "parents": {
            "|".join(key): value.as_dict(probability)
            for key, value in sorted(parents.items())
        },
        "request_reason_children": {
            "|".join(key): value.as_dict(probability)
            for key, value in sorted(reason.items())
        },
        "role_children": {
            "|".join(key): value.as_dict(probability)
            for key, value in sorted(role.items())
        },
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
    }


def run(spec_path: Path, output_dir: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    family_spec = json.loads(
        Path(spec["source_identity"]["family_spec"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    v2_report = json.loads(
        Path(spec["source_identity"]["v2_report"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    if v2_report.get("validation_read") or v2_report.get("sealed_holdout_read"):
        raise RuntimeError("frozen v2 report unexpectedly read Validation/holdout")
    index = _load_index(spec)
    output_dir.mkdir(parents=True, exist_ok=False)
    write_code_checkpoint(
        output_dir / "code_checkpoint",
        repo_root=ROOT,
        code_identity=git_workspace_identity(ROOT),
    )

    action_daily_parts: list[pd.DataFrame] = []
    pending_daily_rows: list[dict[str, Any]] = []
    residual_daily_rows: list[dict[str, Any]] = []
    monotonic_rows: list[dict[str, Any]] = []
    activation_oof_parts: list[pd.DataFrame] = []
    posterior_probability = float(
        family_spec["pending_hierarchy"]["posterior_interval"]
    )
    pending_horizons = list(family_spec["report_horizons_ms"]["pending"])
    pre_horizons = list(family_spec["report_horizons_ms"]["pre_request"])

    for fold_record in v2_report["folds"]:
        fold = int(fold_record["fold"])
        fold_path = Path(fold_record["output"]["path"])
        _require_identity(
            fold_path,
            str(fold_record["output"]["sha256"]),
            f"v2 OOF fold {fold}",
        )
        predictions = pd.read_parquet(fold_path)
        action_daily_parts.append(_daily_action_metrics(predictions))
        base = _load_days(index, fold_record["base_days"])
        test = _load_days(index, fold_record["test_days"])
        activation_oof_parts.append(test)
        thresholds = _fit_thresholds(
            _phase_frame(base, "pending"),
            family_spec["residual_sufficiency_audit"]["tail_thresholds"],
        )

        cells_by_horizon: dict[int, dict[tuple[str, str], BetaCell]] = {}
        for horizon in pending_horizons:
            base_known, base_target = _pending_targets(base, int(horizon))
            cells = _fit_parent_cells(
                base_known,
                base_target,
                equivalent_trials=float(
                    family_spec["pending_hierarchy"]
                    ["side_action_parent_equivalent_trials"]
                ),
            )
            cells_by_horizon[int(horizon)] = cells
            pending_predictions = predictions.loc[
                (predictions["phase"].astype(str) == "pending")
                & (predictions["horizon_ms"].astype(int) == int(horizon))
            ].copy()
            raw_columns = [
                name
                for name in RAW_COLUMNS
                if name not in set(pending_predictions.columns)
                and name != "action_lifecycle_id"
            ]
            merged = pending_predictions.merge(
                test.loc[:, ["action_lifecycle_id", *raw_columns]],
                on="action_lifecycle_id",
                how="inner",
                validate="many_to_one",
            )
            if len(merged) != len(pending_predictions):
                raise RuntimeError(f"fold {fold} pending OOF/raw join lost rows")
            keys = list(
                zip(
                    merged["side"].astype(str).str.upper(),
                    merged["action"].astype(str),
                    strict=True,
                )
            )
            merged["empirical_pending_probability"] = [
                cells[key].mean for key in keys
            ]
            merged["pending_alpha"] = [cells[key].alpha for key in keys]
            merged["pending_beta"] = [cells[key].beta for key in keys]
            for (day, side, role, action), group in merged.groupby(
                ["day", "side", "inventory_role", "action"], observed=True
            ):
                pending_daily_rows.append(
                    {
                        "fold": fold,
                        "day": str(day),
                        "side": str(side),
                        "inventory_role": str(role),
                        "action": str(action),
                        "horizon_ms": int(horizon),
                        "rows": int(len(group)),
                        "events": int(group["fill_target"].sum()),
                        "predicted": float(
                            group["empirical_pending_probability"].sum()
                        ),
                        "calibration_bias": float(
                            np.mean(
                                group["empirical_pending_probability"].to_numpy(float)
                                - group["fill_target"].to_numpy(float)
                            )
                        ),
                    }
                )
            if int(horizon) == max(pending_horizons):
                residual_daily_rows.extend(
                    _residual_rows(merged, thresholds, fold=fold)
                )

        monotonic_rows.extend(
            _pre_monotonicity_metrics(
                predictions,
                test,
                fold=fold,
                horizons_ms=pre_horizons,
            )
        )
        monotonic_rows.extend(
            _pending_monotonicity_metrics(
                predictions,
                test,
                cells_by_horizon,
                fold=fold,
            )
        )

    action_daily = pd.concat(action_daily_parts, ignore_index=True)
    action_daily_path = output_dir / "action_curve_daily_metrics.parquet"
    action_daily.to_parquet(action_daily_path, index=False, compression="zstd")
    action_curves = _action_curve_gate(
        action_daily,
        config=family_spec["action_specific_gate"],
    )

    pending_daily = pd.DataFrame(pending_daily_rows)
    pending_daily_path = output_dir / "pending_nuisance_daily_metrics.parquet"
    pending_daily.to_parquet(pending_daily_path, index=False, compression="zstd")

    residual_daily = pd.DataFrame(residual_daily_rows)
    residual_daily_path = output_dir / "residual_daily_metrics.parquet"
    residual_daily.to_parquet(residual_daily_path, index=False, compression="zstd")
    residual_config = dict(family_spec["residual_sufficiency_audit"])
    residual_config.update(
        {
            "pending_posterior_probability": posterior_probability,
            "pending_predictive_samples": 10000,
            "seed": 20260728,
        }
    )
    residual_audit = _residual_audit(
        residual_daily,
        config=residual_config,
    )

    development = _load_days(index, family_spec["panels"]["development_days"])
    artifact = _pending_artifact(
        development,
        hierarchy=family_spec["pending_hierarchy"],
    )
    artifact["family_spec"] = _identity(
        Path(spec["source_identity"]["family_spec"]["path"])
    )
    artifact_path = output_dir / "pending_nuisance_artifact.json"
    _atomic_json(artifact, artifact_path)

    full_known, full_target = _pending_targets(development, None)
    final_cells = _fit_parent_cells(
        full_known,
        full_target,
        equivalent_trials=float(
            family_spec["pending_hierarchy"]
            ["side_action_parent_equivalent_trials"]
        ),
    )
    quantity = _quantity_metrics(
        full_known,
        full_target,
        final_cells,
        posterior_probability=posterior_probability,
        stress_bps=family_spec["pending_ev_uncertainty"]["stress_fill_value_bps"],
        negligible_usdc=float(
            family_spec["pending_ev_uncertainty"]
            ["negligible_reference_usdc_per_decision"]
        ),
    )

    activation_full = _activation_metrics(development, "development_50")
    activation_oof = _activation_metrics(
        pd.concat(activation_oof_parts, ignore_index=True),
        "oof_29",
    )
    monotonic_limit = float(
        family_spec["common_support_monotonicity"]["maximum_violation_rate"]
    )
    monotonic_pass = bool(
        monotonic_rows
        and all(
            row["prediction_violation_rate"] <= monotonic_limit
            and row["observed_path_violations"] == 0
            for row in monotonic_rows
            if np.isfinite(row["prediction_violation_rate"])
        )
    )
    required_action_curves = [
        row for row in action_curves if row["required_component"]
    ]
    action_curve_pass = bool(
        required_action_curves
        and all(row["curve_pass"] for row in required_action_curves)
    )
    supported_residual = [row for row in residual_audit if row["ack_supported"]]
    residual_pass = bool(
        supported_residual
        and all(row["ack_residual_pass"] for row in supported_residual)
    )
    nuisance_predictive_pass = bool(
        residual_audit
        and all(row["pending_predictive_pass"] for row in residual_audit)
    )

    joint_attribution: list[dict[str, Any]] = []
    lookup = {
        (
            row["side"],
            row["inventory_role"],
            row["action"],
            row["phase"],
            row["cause"],
        ): row
        for row in action_curves
    }
    for side in SIDES:
        for role in ROLES:
            for action in ACTIONS:
                ack = lookup[(side, role, action, "pending", "ack")]
                fill = lookup[(side, role, action, "pending", "fill")]
                joint = lookup[(side, role, action, "pending", "joint")]
                terminal = int(ack["events"]) + int(fill["events"])
                ack_share = float(ack["events"] / terminal) if terminal else math.nan
                joint_attribution.append(
                    {
                        "side": side,
                        "inventory_role": role,
                        "action": action,
                        "ack_event_share": ack_share,
                        "ack_brier_improvement": ack["brier_improvement"]["mean"],
                        "pending_fill_brier_improvement": fill["brier_improvement"]["mean"],
                        "joint_brier_improvement": joint["brier_improvement"]["mean"],
                        "label": "ACK-dominated joint pass"
                        if ack_share >= 0.99
                        else "joint pass not attributed",
                        "pending_fill_evidence_from_joint": False,
                    }
                )

    prediction_surface_pass = bool(
        action_curve_pass
        and residual_pass
        and nuisance_predictive_pass
        and monotonic_pass
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(family_spec["family_id"]),
        "status": "development_complete",
        "fit_spec": _identity(spec_path),
        "family_spec": _identity(
            Path(spec["source_identity"]["family_spec"]["path"])
        ),
        "v2_report": _identity(
            Path(spec["source_identity"]["v2_report"]["path"])
        ),
        "action_curve_daily_metrics": _identity(action_daily_path),
        "pending_nuisance_daily_metrics": _identity(pending_daily_path),
        "residual_daily_metrics": _identity(residual_daily_path),
        "pending_nuisance_artifact": _identity(artifact_path),
        "action_curves": action_curves,
        "joint_attribution": joint_attribution,
        "residual_audit": residual_audit,
        "common_support_monotonicity": monotonic_rows,
        "activation_outcomes": activation_full + activation_oof,
        "pending_quantity_and_ev": quantity,
        "gates": {
            "action_specific_pre_and_ack_curves": action_curve_pass,
            "residual_sufficiency": residual_pass,
            "pending_posterior_predictive": nuisance_predictive_pass,
            "common_support_monotonicity": monotonic_pass,
            "prediction_surface_pass": prediction_surface_pass,
        },
        "residual_audit_identifies_latent_conditional_independence": False,
        "joint_brier_is_pending_fill_evidence": False,
        "validation_access_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "decision": (
            "freeze_new_identity_before_validation_request"
            if prediction_surface_pass
            else "close_empirical_pending_family_on_development"
        ),
        "git": git_workspace_identity(ROOT),
    }
    report["report_identity_sha256"] = canonical_sha256(report)
    _atomic_json(report, output_dir / "report.json")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    report = run(args.spec, args.output)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
