#!/usr/bin/env python3
"""Causal dynamic fill-hazard gate for active local maker orders.

This module deliberately stops at prediction and experiment registration.  It
does not authorize a keep/cancel action or a live policy.  Fill is modeled on
dynamic start-stop intervals, cancel request censors the order, native price
jumps update later states without absorbing the order, and campaign repair is
evaluated only after its delayed-entry risk condition becomes true.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import scipy
from scipy.optimize import minimize
from sklearn.metrics import average_precision_score, roc_auc_score

from models.audit.evidence_split import (
    load_evidence_panel,
    validate_evidence_split,
)
from models.audit.experiment_manifest import (
    build_manifest,
    git_workspace_identity,
    write_code_checkpoint,
    write_manifest,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "dynamic_fill_hazard_m0.v2"
BUNDLE_SCHEMA_VERSION = "dynamic_fill_hazard_bundle.v2"
FAMILY_ID = "dynamic_fill_hazard_m0_v2"
ACTION_EXPERIMENT_ID = "queue_value_keep_cancel_dynamic_fill_m0_v1"
CAUSES = ("favorable_fill", "adverse_fill")
SIDES = ("BUY", "SELL")

RAW_SNAPSHOT_COLUMNS = (
    "risk_snapshot_elapsed_ms",
    "visible_state_age_ms",
    "spread_ticks",
    "quote_distance_ticks",
    "top_bid_size",
    "top_ask_size",
    "book_imbalance",
    "side_microprice_adverse_ticks",
    "policy_queue_initial",
    "policy_queue_remaining",
    "policy_queue_fraction_left",
    "policy_queue_progress",
    "visible_cancel_events",
    "visible_cancel_size",
    "visible_refill_events",
    "visible_refill_size",
    "visible_refill_event_share",
    "price_adverse_ticks",
    "price_worst_adverse_ticks",
    "price_recovery_ratio",
    "microprice_adverse_ticks",
    "microprice_worst_adverse_ticks",
    "microprice_recovery_ratio",
    "visible_depth_recovery_ratio",
    "native_adverse_jump_seen",
    "time_since_native_adverse_jump_ms",
    "clock_hour_sin",
    "clock_hour_cos",
)

MODEL_FEATURES = (
    "elapsed_log1p",
    "visible_state_age_log1p",
    "spread_ticks",
    "quote_distance_ticks",
    "top_bid_size_log1p",
    "top_ask_size_log1p",
    "book_imbalance",
    "side_microprice_adverse_ticks",
    "queue_initial_log1p",
    "queue_remaining_log1p",
    "policy_queue_fraction_left",
    "policy_queue_progress",
    "visible_cancel_events_log1p",
    "visible_cancel_size_log1p",
    "visible_refill_events_log1p",
    "visible_refill_size_log1p",
    "visible_refill_event_share",
    "price_adverse_ticks",
    "price_worst_adverse_ticks",
    "price_recovery_ratio",
    "microprice_adverse_ticks",
    "microprice_worst_adverse_ticks",
    "microprice_recovery_ratio",
    "visible_depth_recovery_ratio",
    "native_adverse_jump_seen",
    "time_since_native_adverse_jump_log1p",
    "clock_hour_sin",
    "clock_hour_cos",
    "role_opener",
    "role_add",
    "role_reducing",
)

BANNED_POLICY_FEATURE_TOKENS = (
    "notional",
    "historical_quantity",
    "trade_quantity",
    "aggregate_quantity",
    "child_count",
    "first_trade_id",
    "last_trade_id",
    "f_count",
    "l_count",
)

DEFAULT_GATES: dict[str, Any] = {
    "minimum_oof_days": 4,
    "minimum_events_per_side_cause": 20,
    "minimum_average_precision_lift": 1.10,
    "minimum_within_day_top20_lift": 1.15,
    "minimum_daily_high_low_positive_rate": 0.55,
    "observed_to_expected_min": 0.70,
    "observed_to_expected_max": 1.30,
    "minimum_brier_skill": 0.0,
    "minimum_day_cluster_brier_improvement_lower": 0.0,
    "minimum_repair_events_per_campaign_side": 10,
}

OPTIMIZER_CONTRACT: dict[str, Any] = {
    "method": "L-BFGS-B",
    "max_iterations": 1_500,
    "ftol": 1e-8,
    "gtol": 1e-5,
    "standardized_feature_clip": 12.0,
    "eta_bounds": [-25.0, 20.0],
    "intercept_bounds": [-25.0, 20.0],
    "coefficient_bounds": [-8.0, 8.0],
    "clipped_eta_gradient": "zero_outside_bounds",
    "require_success": True,
}

CALIBRATOR_CONTRACT: dict[str, Any] = {
    "type": "affine_cloglog",
    "method": "L-BFGS-B",
    "max_iterations": 1_000,
    "ftol": 1e-10,
    "gtol": 1e-7,
    "probability_clip": [1e-9, 1.0 - 1e-9],
    "intercept_bounds": [-6.0, 6.0],
    "slope_bounds": [0.25, 4.0],
    "slope_l2": 0.01,
    "minimum_rows": 1_000,
    "minimum_events": 20,
    "minimum_nonevents": 100,
    "inner_min_train_days": 10,
    "inner_embargo_days": 1,
    "inner_test_days": 5,
    "minimum_inner_oof_days": 5,
    "day_cluster_bootstrap_samples": 5_000,
    "day_cluster_bootstrap_seed": 20_260_724,
}


@dataclass(frozen=True)
class ChronologicalFold:
    fold: int
    train_days: tuple[str, ...]
    embargo_days: tuple[str, ...]
    test_days: tuple[str, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _runtime_identity() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "numpy": str(np.__version__),
        "pandas": str(pd.__version__),
        "scipy": str(scipy.__version__),
    }


def _numeric(frame: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce").fillna(default)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    return value


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
    first_test = min_train_days + embargo_days
    if len(ordered) < first_test + test_days:
        raise ValueError("not enough days for chronological dynamic hazard OOF")
    folds: list[ChronologicalFold] = []
    for test_start in range(first_test, len(ordered), test_days):
        test = ordered[test_start : test_start + test_days]
        if not test:
            continue
        train_end = test_start - embargo_days
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


def _feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(set(RAW_SNAPSHOT_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"dynamic snapshot rows are missing features: {missing}")
    selected_names = (*RAW_SNAPSHOT_COLUMNS, *MODEL_FEATURES)
    bad = sorted(
        name
        for name in selected_names
        if any(token in name.lower() for token in BANNED_POLICY_FEATURE_TOKENS)
    )
    if bad:
        raise ValueError(f"banned historical aggregate feature selected: {bad}")
    output = pd.DataFrame(index=frame.index)
    output["elapsed_log1p"] = np.log1p(
        _numeric(frame, "risk_snapshot_elapsed_ms").clip(lower=0.0)
    )
    output["visible_state_age_log1p"] = np.log1p(
        _numeric(frame, "visible_state_age_ms").clip(lower=0.0)
    )
    for name in (
        "spread_ticks",
        "quote_distance_ticks",
        "book_imbalance",
        "side_microprice_adverse_ticks",
        "policy_queue_fraction_left",
        "policy_queue_progress",
        "visible_refill_event_share",
        "price_adverse_ticks",
        "price_worst_adverse_ticks",
        "price_recovery_ratio",
        "microprice_adverse_ticks",
        "microprice_worst_adverse_ticks",
        "microprice_recovery_ratio",
        "visible_depth_recovery_ratio",
        "native_adverse_jump_seen",
        "clock_hour_sin",
        "clock_hour_cos",
    ):
        output[name] = _numeric(frame, name)
    for source, target in (
        ("top_bid_size", "top_bid_size_log1p"),
        ("top_ask_size", "top_ask_size_log1p"),
        ("policy_queue_initial", "queue_initial_log1p"),
        ("policy_queue_remaining", "queue_remaining_log1p"),
        ("visible_cancel_events", "visible_cancel_events_log1p"),
        ("visible_cancel_size", "visible_cancel_size_log1p"),
        ("visible_refill_events", "visible_refill_events_log1p"),
        ("visible_refill_size", "visible_refill_size_log1p"),
    ):
        output[target] = np.log1p(_numeric(frame, source).clip(lower=0.0))
    jump_age = _numeric(frame, "time_since_native_adverse_jump_ms", -1.0)
    output["time_since_native_adverse_jump_log1p"] = np.where(
        jump_age >= 0.0,
        np.log1p(jump_age.clip(lower=0.0)),
        0.0,
    )
    role = frame["current_inventory_role"].astype(str).str.lower()
    output["role_opener"] = role.eq("opener").astype(float)
    output["role_add"] = role.eq("add").astype(float)
    output["role_reducing"] = role.eq("reducing").astype(float)
    output = output.loc[:, list(MODEL_FEATURES)].replace(
        [np.inf, -np.inf], np.nan
    )
    if output.isna().any().any():
        missing_counts = output.isna().sum()
        raise ValueError(
            "dynamic features contain non-finite values: "
            f"{missing_counts[missing_counts > 0].to_dict()}"
        )
    return output


def _ordered_events(events: pd.DataFrame) -> pd.DataFrame:
    required = {
        "day",
        "decision_id",
        "order_id",
        "campaign_id",
        "side",
        "event_type",
        "event_ts_ns",
        "event_seq",
        "state_after",
        "remaining_qty",
        "feature_source_ts_ns",
        "feature_ready_ts_ns",
        "same_ms_ordering_resolved",
        "exact_queue_path_valid",
        "queue_path_ambiguous",
        "current_inventory_role",
        *RAW_SNAPSHOT_COLUMNS,
    }
    missing = sorted(required - set(events.columns))
    if missing:
        raise ValueError(f"lifecycle v2 panel missing columns: {missing}")
    output = events.copy()
    output["day"] = output["day"].astype(str)
    output["side"] = output["side"].astype(str).str.upper()
    if set(output["side"]) - set(SIDES):
        raise ValueError("lifecycle side must be BUY or SELL")
    return output.sort_values(
        ["day", "order_id", "event_ts_ns", "event_seq"], kind="mergesort"
    ).reset_index(drop=True)


def build_dynamic_fill_risk_set(events: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build one dynamic start-stop row per causal risk snapshot."""

    ordered = _ordered_events(events)
    rows: list[dict[str, Any]] = []
    diagnostics = {
        "snapshot_rows": 0,
        "strict_snapshot_rows": 0,
        "future_feature_rows": 0,
        "queue_unsupported_rows": 0,
        "same_ms_ambiguous_rows": 0,
        "zero_duration_rows": 0,
        "fill_horizon_censored_rows": 0,
        "cancel_censor_rows": 0,
        "native_jump_transition_rows": 0,
    }
    stopping_types = {
        "risk_snapshot",
        "partial_fill",
        "full_fill",
        "cancel_request",
        "reject",
        "cancel_ack",
        "day_end_censor",
    }
    for _, group in ordered.groupby(["day", "order_id"], sort=False):
        records = group.to_dict("records")
        for index, current in enumerate(records):
            if str(current["event_type"]) != "risk_snapshot":
                continue
            diagnostics["snapshot_rows"] += 1
            event_ts_ns = int(current["event_ts_ns"])
            source_ts_ns = int(current.get("feature_source_ts_ns", 0) or 0)
            ready_ts_ns = int(current.get("feature_ready_ts_ns", 0) or 0)
            if source_ts_ns > ready_ts_ns or ready_ts_ns > event_ts_ns:
                diagnostics["future_feature_rows"] += 1
                continue
            if not bool(current.get("same_ms_ordering_resolved", 0)):
                diagnostics["same_ms_ambiguous_rows"] += 1
                continue
            if not bool(current.get("exact_queue_path_valid", 0)) or bool(
                current.get("queue_path_ambiguous", 0)
            ):
                diagnostics["queue_unsupported_rows"] += 1
                continue
            if str(current.get("state_after", "")) != "open" or float(
                current.get("remaining_qty", 0.0) or 0.0
            ) <= 0.0:
                continue
            diagnostics["strict_snapshot_rows"] += 1
            following = next(
                (
                    row
                    for row in records[index + 1 :]
                    if str(row["event_type"]) in stopping_types
                ),
                None,
            )
            if following is None:
                continue
            end_ts_ns = int(following["event_ts_ns"])
            interval_ms = (end_ts_ns - event_ts_ns) / 1_000_000.0
            if interval_ms <= 0.0:
                diagnostics["zero_duration_rows"] += 1
                continue
            next_type = str(following["event_type"])
            event = "censored"
            markout_bps = math.nan
            if next_type in {"partial_fill", "full_fill"}:
                horizon_censored = bool(
                    int(following.get("fill_value_horizon_censored", 1) or 0)
                )
                markout_bps = float(
                    following.get("fill_value_markout_bps", math.nan)
                )
                if horizon_censored or not math.isfinite(markout_bps):
                    diagnostics["fill_horizon_censored_rows"] += 1
                    continue
                event = (
                    "favorable_fill" if markout_bps >= 0.0 else "adverse_fill"
                )
            elif next_type == "cancel_request":
                diagnostics["cancel_censor_rows"] += 1
            jump_count = sum(
                str(row["event_type"]) == "native_price_jump"
                for row in records[index + 1 :]
                if int(row["event_ts_ns"]) <= end_ts_ns
            )
            diagnostics["native_jump_transition_rows"] += int(jump_count)
            row = dict(current)
            row.update(
                {
                    "risk_row_id": (
                        f"fill-risk:{current['day']}:{int(current['order_id'])}:"
                        f"{int(current['event_seq'])}"
                    ),
                    "risk_interval_start_ts_ns": event_ts_ns,
                    "risk_interval_end_ts_ns": end_ts_ns,
                    "risk_interval_ms": float(interval_ms),
                    "next_event_type": next_type,
                    "fill_event": event,
                    "favorable_fill": int(event == "favorable_fill"),
                    "adverse_fill": int(event == "adverse_fill"),
                    "cancel_action_censor": int(next_type == "cancel_request"),
                    "native_jump_transitions_in_interval": int(jump_count),
                    "fill_value_markout_bps": markout_bps,
                    "formal_dynamic_fill_eligible": 1,
                }
            )
            rows.append(row)
    risk = pd.DataFrame(rows)
    if risk.empty:
        raise ValueError("lifecycle panel produced no strict dynamic fill risk rows")
    features = _feature_frame(risk)
    for name in MODEL_FEATURES:
        risk[name] = features[name].to_numpy(dtype=float)
    diagnostics.update(
        {
            "formal_rows": int(len(risk)),
            "orders": int(
                risk[["day", "order_id"]].drop_duplicates().shape[0]
            ),
            "days": sorted(risk["day"].astype(str).unique()),
            "event_counts": {
                str(key): int(value)
                for key, value in risk["fill_event"].value_counts().items()
            },
            "role_counts": {
                str(key): int(value)
                for key, value in risk["current_inventory_role"]
                .astype(str)
                .value_counts()
                .items()
            },
            "cancel_is_action_or_censor": True,
            "jump_is_nonabsorbing_transition": True,
        }
    )
    return risk, diagnostics


def build_delayed_entry_repair_risk_set(
    events: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Construct campaign repair intervals only after repair eligibility begins."""

    ordered = _ordered_events(events)
    snapshots = ordered[
        ordered["event_type"].astype(str).eq("risk_snapshot")
        & _numeric(ordered, "repair_at_risk").eq(1.0)
        & _numeric(ordered, "campaign_active").eq(1.0)
        & _numeric(ordered, "reducing_quote_active").eq(1.0)
        & _numeric(ordered, "reducing_quote_eligible").eq(1.0)
        & _numeric(ordered, "inventory").abs().gt(1e-12)
        & ordered["current_inventory_role"].astype(str).str.lower().eq("reducing")
        & _numeric(ordered, "same_ms_ordering_resolved").eq(1.0)
        & _numeric(ordered, "exact_queue_path_valid").eq(1.0)
        & _numeric(ordered, "queue_path_ambiguous").eq(0.0)
    ].copy()
    snapshots = snapshots[_numeric(snapshots, "campaign_id").gt(0.0)].copy()
    if snapshots.empty:
        return pd.DataFrame(), {
            "formal_rows": 0,
            "campaigns": 0,
            "repair_events": 0,
            "delayed_entry_identity_passed": False,
        }
    snapshots["campaign_side"] = np.where(
        _numeric(snapshots, "inventory") > 0.0,
        "BUY",
        "SELL",
    )
    snapshots = snapshots.sort_values(
        ["day", "campaign_id", "event_ts_ns", "quote_distance_ticks", "order_id"],
        kind="mergesort",
    ).drop_duplicates(["day", "campaign_id", "event_ts_ns"], keep="first")
    repair_times = (
        ordered[ordered["event_type"].astype(str).eq("campaign_repair")]
        .groupby(["day", "campaign_id"], sort=False)["event_ts_ns"]
        .min()
        .to_dict()
    )
    exit_times = (
        ordered[
            ordered["event_type"].astype(str).isin(
                {"repair_risk_exit", "campaign_end_censor", "day_end_censor"}
            )
        ]
        .groupby(["day", "campaign_id"], sort=False)["event_ts_ns"]
        .apply(lambda values: sorted({int(value) for value in values}))
        .to_dict()
    )
    rows: list[dict[str, Any]] = []
    for (day, campaign_id), group in snapshots.groupby(
        ["day", "campaign_id"], sort=False
    ):
        records = group.sort_values("event_ts_ns", kind="mergesort").to_dict("records")
        campaign_key = (str(day), int(campaign_id))
        repair_ts = int(repair_times.get(campaign_key, 0) or 0)
        exits = exit_times.get(campaign_key, [])
        for index, current in enumerate(records):
            start_ns = int(current["event_ts_ns"])
            candidates: list[tuple[int, str]] = []
            if index + 1 < len(records):
                candidates.append((int(records[index + 1]["event_ts_ns"]), "snapshot"))
            if repair_ts > start_ns:
                candidates.append((repair_ts, "campaign_repair"))
            candidates.extend(
                (int(value), "repair_risk_exit")
                for value in exits
                if int(value) > start_ns
            )
            if not candidates:
                continue
            end_ns, event_type = min(candidates, key=lambda item: item[0])
            interval_ms = (end_ns - start_ns) / 1_000_000.0
            if interval_ms <= 0.0:
                continue
            row = dict(current)
            row.update(
                {
                    "repair_risk_row_id": (
                        f"repair-risk:{day}:{int(campaign_id)}:"
                        f"{int(current['event_ts_ns'])}"
                    ),
                    "campaign_side": str(current["campaign_side"]),
                    "risk_interval_start_ts_ns": start_ns,
                    "risk_interval_end_ts_ns": int(end_ns),
                    "risk_interval_ms": float(interval_ms),
                    "repair_event": int(event_type == "campaign_repair"),
                    "repair_next_event_type": event_type,
                    "formal_delayed_entry_repair_eligible": 1,
                }
            )
            rows.append(row)
    risk = pd.DataFrame(rows)
    if not risk.empty:
        features = _feature_frame(risk)
        for name in MODEL_FEATURES:
            risk[name] = features[name].to_numpy(dtype=float)
    summary = {
        "formal_rows": int(len(risk)),
        "campaigns": (
            int(risk[["day", "campaign_id"]].drop_duplicates().shape[0])
            if not risk.empty
            else 0
        ),
        "repair_events": int(_numeric(risk, "repair_event").sum()) if not risk.empty else 0,
        "pre_entry_rows": 0,
        "delayed_entry_identity_passed": bool(not risk.empty),
    }
    return risk, summary


def _fit_cloglog_hazard(
    frame: pd.DataFrame,
    *,
    target: str,
    l2_penalty: float,
    side: str,
    cause: str,
    train_days: Sequence[str],
) -> dict[str, Any]:
    x_raw = frame.loc[:, list(MODEL_FEATURES)].to_numpy(dtype=float)
    if not np.isfinite(x_raw).all():
        raise ValueError(f"{side}/{cause} has non-finite model features")
    mean = x_raw.mean(axis=0)
    scale = x_raw.std(axis=0)
    scale = np.where(scale > 1e-9, scale, 1.0)
    feature_clip = float(OPTIMIZER_CONTRACT["standardized_feature_clip"])
    x = np.clip((x_raw - mean) / scale, -feature_clip, feature_clip)
    y = _numeric(frame, target).to_numpy(dtype=float)
    exposure_s = np.maximum(
        _numeric(frame, "risk_interval_ms").to_numpy(dtype=float) / 1_000.0,
        0.001,
    )
    if not np.isfinite(y).all() or not np.isfinite(exposure_s).all():
        raise ValueError(f"{side}/{cause} has non-finite target or exposure")
    events = int(y.sum())
    if events <= 0 or events >= len(y):
        raise ValueError(f"{side}/{cause} lacks binary event support")
    base_rate = float(events / max(exposure_s.sum(), 1e-12))
    initial = np.zeros(x.shape[1] + 1, dtype=float)
    initial[0] = math.log(max(base_rate, 1e-9))
    eta_lower, eta_upper = (
        float(value) for value in OPTIMIZER_CONTRACT["eta_bounds"]
    )

    def objective(theta: np.ndarray) -> tuple[float, np.ndarray]:
        eta_raw = theta[0] + np.einsum(
            "ij,j->i",
            x,
            theta[1:],
            optimize=True,
        )
        eta = np.clip(eta_raw, eta_lower, eta_upper)
        z = np.exp(eta) * exposure_s
        log_event = np.log(-np.expm1(-np.clip(z, 1e-12, 50.0)))
        loss = float(-(y * log_event - (1.0 - y) * z).sum())
        loss += 0.5 * float(l2_penalty) * float(np.dot(theta[1:], theta[1:]))
        event_gradient = -z / np.expm1(np.clip(z, 1e-12, 50.0))
        d_eta = np.where(y > 0.0, event_gradient, z)
        d_eta *= (eta_raw > eta_lower) & (eta_raw < eta_upper)
        gradient = np.empty_like(theta)
        gradient[0] = float(d_eta.sum())
        gradient[1:] = np.einsum(
            "ij,i->j",
            x,
            d_eta,
            optimize=True,
        ) + float(l2_penalty) * theta[1:]
        return loss, gradient

    fit = minimize(
        objective,
        initial,
        method=str(OPTIMIZER_CONTRACT["method"]),
        jac=True,
        bounds=[
            tuple(
                float(value)
                for value in OPTIMIZER_CONTRACT["intercept_bounds"]
            ),
            *[
                tuple(
                    float(value)
                    for value in OPTIMIZER_CONTRACT["coefficient_bounds"]
                )
                for _ in range(x.shape[1])
            ],
        ],
        options={
            "maxiter": int(OPTIMIZER_CONTRACT["max_iterations"]),
            "ftol": float(OPTIMIZER_CONTRACT["ftol"]),
            "gtol": float(OPTIMIZER_CONTRACT["gtol"]),
        },
    )
    if not fit.success:
        raise RuntimeError(f"{side}/{cause} dynamic hazard fit failed: {fit.message}")
    if not np.isfinite(fit.fun) or not np.isfinite(fit.x).all():
        raise RuntimeError(f"{side}/{cause} dynamic hazard fit is non-finite")
    return {
        "schema_version": "cause_specific_discrete_cloglog.v1",
        "side": str(side),
        "cause": str(cause),
        "feature_names": list(MODEL_FEATURES),
        "feature_mean": mean.tolist(),
        "feature_scale": scale.tolist(),
        "intercept": float(fit.x[0]),
        "coefficients": fit.x[1:].tolist(),
        "l2_penalty": float(l2_penalty),
        "baseline_rate_per_second": base_rate,
        "train_days": list(train_days),
        "train_rows": int(len(frame)),
        "train_events": events,
        "optimizer_iterations": int(fit.nit),
        "optimizer_objective": float(fit.fun),
        "optimizer_gradient_inf_norm": float(
            np.max(np.abs(np.asarray(fit.jac, dtype=float)))
        ),
        "optimizer_contract": dict(OPTIMIZER_CONTRACT),
    }


def _predict_model(model: Mapping[str, Any], frame: pd.DataFrame) -> np.ndarray:
    feature_names = tuple(str(name) for name in model["feature_names"])
    if feature_names != MODEL_FEATURES:
        raise ValueError("dynamic hazard artifact feature identity changed")
    x_raw = frame.loc[:, list(feature_names)].to_numpy(dtype=float)
    mean = np.asarray(model["feature_mean"], dtype=float)
    scale = np.asarray(model["feature_scale"], dtype=float)
    coefficients = np.asarray(model["coefficients"], dtype=float)
    feature_clip = float(OPTIMIZER_CONTRACT["standardized_feature_clip"])
    x = np.clip((x_raw - mean) / scale, -feature_clip, feature_clip)
    eta_lower, eta_upper = (
        float(value) for value in OPTIMIZER_CONTRACT["eta_bounds"]
    )
    eta = np.clip(
        float(model["intercept"])
        + np.einsum("ij,j->i", x, coefficients, optimize=True),
        eta_lower,
        eta_upper,
    )
    exposure_s = np.maximum(
        _numeric(frame, "risk_interval_ms").to_numpy(dtype=float) / 1_000.0,
        0.001,
    )
    return -np.expm1(-np.clip(np.exp(eta) * exposure_s, 0.0, 50.0))


def _predict_baseline(model: Mapping[str, Any], frame: pd.DataFrame) -> np.ndarray:
    exposure_s = np.maximum(
        _numeric(frame, "risk_interval_ms").to_numpy(dtype=float) / 1_000.0,
        0.001,
    )
    return -np.expm1(
        -np.clip(
            float(model["baseline_rate_per_second"]) * exposure_s,
            0.0,
            50.0,
        )
    )


def _cloglog_score(probability: np.ndarray, contract: Mapping[str, Any]) -> np.ndarray:
    lower, upper = (float(value) for value in contract["probability_clip"])
    clipped = np.clip(np.asarray(probability, dtype=float), lower, upper)
    return np.log(-np.log1p(-clipped))


def _fit_probability_calibrator(
    probability: np.ndarray,
    target: np.ndarray,
    days: Sequence[str],
    *,
    contract: Mapping[str, Any],
    inner_folds: Sequence[ChronologicalFold],
) -> dict[str, Any]:
    raw_probability = np.asarray(probability, dtype=float)
    y = np.asarray(target, dtype=float)
    day_values = np.asarray([str(day) for day in days], dtype=object)
    if (
        raw_probability.ndim != 1
        or y.shape != raw_probability.shape
        or day_values.shape != raw_probability.shape
    ):
        raise ValueError("calibrator inputs must be aligned one-dimensional arrays")
    if not np.isfinite(raw_probability).all() or not np.isfinite(y).all():
        raise ValueError("calibrator inputs contain non-finite values")
    rows = int(len(y))
    events = int(y.sum())
    nonevents = rows - events
    unique_days = sorted(set(day_values.tolist()))
    if rows < int(contract["minimum_rows"]):
        raise ValueError("calibrator lacks row support")
    if events < int(contract["minimum_events"]):
        raise ValueError("calibrator lacks event support")
    if nonevents < int(contract["minimum_nonevents"]):
        raise ValueError("calibrator lacks non-event support")
    if len(unique_days) < int(contract["minimum_inner_oof_days"]):
        raise ValueError("calibrator lacks inner-OOF day support")

    score = _cloglog_score(raw_probability, contract)
    slope_l2 = float(contract["slope_l2"])

    def objective(theta: np.ndarray) -> tuple[float, np.ndarray]:
        eta = theta[0] + theta[1] * score
        cumulative_hazard = np.exp(np.clip(eta, -25.0, 20.0))
        log_event = np.log(
            -np.expm1(-np.clip(cumulative_hazard, 1e-12, 50.0))
        )
        loss = float(
            -np.mean(
                y * log_event - (1.0 - y) * cumulative_hazard
            )
        )
        loss += 0.5 * slope_l2 * float((theta[1] - 1.0) ** 2)
        event_gradient = -cumulative_hazard / np.expm1(
            np.clip(cumulative_hazard, 1e-12, 50.0)
        )
        d_eta = np.where(y > 0.0, event_gradient, cumulative_hazard)
        gradient = np.asarray(
            [
                float(d_eta.mean()),
                float(np.mean(d_eta * score))
                + slope_l2 * float(theta[1] - 1.0),
            ],
            dtype=float,
        )
        return loss, gradient

    fit = minimize(
        objective,
        np.asarray([0.0, 1.0], dtype=float),
        method=str(contract["method"]),
        jac=True,
        bounds=[
            tuple(float(value) for value in contract["intercept_bounds"]),
            tuple(float(value) for value in contract["slope_bounds"]),
        ],
        options={
            "maxiter": int(contract["max_iterations"]),
            "ftol": float(contract["ftol"]),
            "gtol": float(contract["gtol"]),
        },
    )
    if not fit.success:
        raise RuntimeError(f"nested probability calibration failed: {fit.message}")
    calibrated = _apply_probability_calibrator(
        {
            "contract": dict(contract),
            "intercept": float(fit.x[0]),
            "slope": float(fit.x[1]),
        },
        raw_probability,
    )
    return {
        "schema_version": "nested_affine_cloglog_calibrator.v1",
        "contract": dict(contract),
        "intercept": float(fit.x[0]),
        "slope": float(fit.x[1]),
        "train_rows": rows,
        "train_events": events,
        "train_nonevents": nonevents,
        "train_days": unique_days,
        "train_day_count": len(unique_days),
        "inner_folds": [asdict(fold) for fold in inner_folds],
        "raw_brier": float(np.mean((raw_probability - y) ** 2)),
        "calibrated_brier": float(np.mean((calibrated - y) ** 2)),
        "optimizer_iterations": int(fit.nit),
        "optimizer_objective": float(fit.fun),
    }


def _apply_probability_calibrator(
    calibrator: Mapping[str, Any],
    probability: np.ndarray,
) -> np.ndarray:
    contract = calibrator["contract"]
    score = _cloglog_score(np.asarray(probability, dtype=float), contract)
    eta = float(calibrator["intercept"]) + float(calibrator["slope"]) * score
    cumulative_hazard = np.exp(np.clip(eta, -25.0, 20.0))
    return -np.expm1(-np.clip(cumulative_hazard, 0.0, 50.0))


def _fit_nested_calibrated_hazard(
    frame: pd.DataFrame,
    *,
    target: str,
    l2_penalty: float,
    side: str,
    cause: str,
    train_days: Sequence[str],
    calibration_contract: Mapping[str, Any],
) -> dict[str, Any]:
    ordered_days = tuple(sorted({str(day) for day in train_days}))
    inner_folds = make_chronological_folds(
        ordered_days,
        min_train_days=int(calibration_contract["inner_min_train_days"]),
        embargo_days=int(calibration_contract["inner_embargo_days"]),
        test_days=int(calibration_contract["inner_test_days"]),
    )
    prediction_parts: list[pd.DataFrame] = []
    for fold in inner_folds:
        inner_train = frame[frame["day"].isin(fold.train_days)]
        inner_test = frame[frame["day"].isin(fold.test_days)]
        if inner_train.empty or inner_test.empty:
            raise ValueError(
                f"{side}/{cause} inner fold {fold.fold} lacks support"
            )
        model = _fit_cloglog_hazard(
            inner_train,
            target=target,
            l2_penalty=l2_penalty,
            side=side,
            cause=cause,
            train_days=fold.train_days,
        )
        prediction_parts.append(
            pd.DataFrame(
                {
                    "day": inner_test["day"].astype(str).to_numpy(),
                    "target": _numeric(inner_test, target).to_numpy(dtype=float),
                    "raw_probability": _predict_model(model, inner_test),
                }
            )
        )
    inner_oof = pd.concat(prediction_parts, ignore_index=True)
    calibrator = _fit_probability_calibrator(
        inner_oof["raw_probability"].to_numpy(dtype=float),
        inner_oof["target"].to_numpy(dtype=float),
        inner_oof["day"].astype(str).to_numpy(),
        contract=calibration_contract,
        inner_folds=inner_folds,
    )
    model = _fit_cloglog_hazard(
        frame,
        target=target,
        l2_penalty=l2_penalty,
        side=side,
        cause=cause,
        train_days=ordered_days,
    )
    model["nested_calibrator"] = calibrator
    return model


def _predict_nested_model(
    model: Mapping[str, Any],
    frame: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    raw = _predict_model(model, frame)
    calibrator = model.get("nested_calibrator")
    if not isinstance(calibrator, Mapping):
        return raw, raw
    return raw, _apply_probability_calibrator(calibrator, raw)


def _within_day_ranking(frame: pd.DataFrame, target: str, prediction: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for day, daily in frame.groupby("day", sort=True):
        y = _numeric(daily, target).to_numpy(dtype=float)
        if y.sum() <= 0.0 or len(y) < 10:
            continue
        order = np.argsort(-_numeric(daily, prediction).to_numpy(dtype=float))
        count = max(1, int(math.ceil(0.20 * len(order))))
        top = y[order[:count]]
        rest = y[order[count:]]
        all_rate = float(y.mean())
        top_rate = float(top.mean())
        rest_rate = float(rest.mean()) if rest.size else 0.0
        rows.append(
            {
                "day": str(day),
                "top20_lift": top_rate / max(all_rate, 1e-12),
                "high_minus_low": top_rate - rest_rate,
            }
        )
    if not rows:
        return {
            "days": 0,
            "mean_top20_lift": math.nan,
            "daily_high_low_positive_rate": math.nan,
        }
    daily = pd.DataFrame(rows)
    return {
        "days": int(len(daily)),
        "mean_top20_lift": float(daily["top20_lift"].mean()),
        "daily_high_low_positive_rate": float(
            daily["high_minus_low"].gt(0.0).mean()
        ),
    }


def _day_cluster_brier_improvement(
    frame: pd.DataFrame,
    *,
    cause: str,
    bootstrap_samples: int,
    bootstrap_seed: int,
) -> dict[str, float]:
    daily_rows: list[dict[str, float]] = []
    for _day, daily in frame.groupby("day", sort=True):
        y = _numeric(daily, cause).to_numpy(dtype=float)
        probability = np.clip(
            _numeric(daily, f"probability_{cause}").to_numpy(dtype=float),
            1e-9,
            1.0 - 1e-9,
        )
        baseline = np.clip(
            _numeric(
                daily, f"baseline_probability_{cause}"
            ).to_numpy(dtype=float),
            1e-9,
            1.0 - 1e-9,
        )
        daily_rows.append(
            {
                "rows": float(len(daily)),
                "improvement": float(
                    np.mean((baseline - y) ** 2 - (probability - y) ** 2)
                ),
            }
        )
    if not daily_rows:
        return {
            "brier_improvement": math.nan,
            "brier_improvement_ci_lower": math.nan,
            "brier_improvement_ci_upper": math.nan,
            "brier_improvement_bootstrap_probability_positive": math.nan,
            "daily_brier_improvement_positive_rate": math.nan,
        }
    daily = pd.DataFrame(daily_rows)
    values = daily["improvement"].to_numpy(dtype=float)
    weights = daily["rows"].to_numpy(dtype=float)
    point = float(np.average(values, weights=weights))
    rng = np.random.default_rng(int(bootstrap_seed))
    draws = rng.integers(
        0,
        len(values),
        size=(int(bootstrap_samples), len(values)),
    )
    sampled_values = values[draws]
    sampled_weights = weights[draws]
    estimates = np.sum(
        sampled_values * sampled_weights,
        axis=1,
    ) / np.maximum(np.sum(sampled_weights, axis=1), 1.0)
    return {
        "brier_improvement": point,
        "brier_improvement_ci_lower": float(np.quantile(estimates, 0.025)),
        "brier_improvement_ci_upper": float(np.quantile(estimates, 0.975)),
        "brier_improvement_bootstrap_probability_positive": float(
            np.mean(estimates > 0.0)
        ),
        "daily_brier_improvement_positive_rate": float(
            np.mean(values > 0.0)
        ),
    }


def _prediction_metrics(
    frame: pd.DataFrame,
    cause: str,
    *,
    calibration_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    y = _numeric(frame, cause).to_numpy(dtype=float)
    probability = np.clip(
        _numeric(frame, f"probability_{cause}").to_numpy(dtype=float),
        1e-9,
        1.0 - 1e-9,
    )
    baseline = np.clip(
        _numeric(frame, f"baseline_probability_{cause}").to_numpy(dtype=float),
        1e-9,
        1.0 - 1e-9,
    )
    event_rate = float(y.mean())
    ap = float(average_precision_score(y, probability)) if y.sum() > 0 else math.nan
    auc = (
        float(roc_auc_score(y, probability))
        if np.unique(y).size == 2
        else math.nan
    )
    brier = float(np.mean((probability - y) ** 2))
    baseline_brier = float(np.mean((baseline - y) ** 2))
    ranking = _within_day_ranking(frame, cause, f"probability_{cause}")
    cluster_contract = calibration_contract or CALIBRATOR_CONTRACT
    brier_cluster = _day_cluster_brier_improvement(
        frame,
        cause=cause,
        bootstrap_samples=int(
            cluster_contract["day_cluster_bootstrap_samples"]
        ),
        bootstrap_seed=int(
            cluster_contract["day_cluster_bootstrap_seed"]
        ),
    )
    return {
        "rows": int(len(frame)),
        "events": int(y.sum()),
        "days": int(frame["day"].astype(str).nunique()),
        "event_rate": event_rate,
        "average_precision": ap,
        "average_precision_lift": ap / max(event_rate, 1e-12),
        "roc_auc": auc,
        "predicted_events": float(probability.sum()),
        "observed_to_expected": float(y.sum() / max(probability.sum(), 1e-12)),
        "brier": brier,
        "baseline_brier": baseline_brier,
        "brier_skill": 1.0 - brier / max(baseline_brier, 1e-12),
        **ranking,
        **brier_cluster,
    }


def _cause_gate(metrics: Mapping[str, Any], gates: Mapping[str, Any]) -> tuple[bool, list[str]]:
    failures: list[str] = []
    checks = {
        "event_support": int(metrics["events"])
        >= int(gates["minimum_events_per_side_cause"]),
        "oof_day_support": int(metrics["days"]) >= int(gates["minimum_oof_days"]),
        "average_precision_lift": float(metrics["average_precision_lift"])
        >= float(gates["minimum_average_precision_lift"]),
        "within_day_top20_lift": float(metrics["mean_top20_lift"])
        >= float(gates["minimum_within_day_top20_lift"]),
        "daily_high_low_sign": float(metrics["daily_high_low_positive_rate"])
        >= float(gates["minimum_daily_high_low_positive_rate"]),
        "observed_to_expected": float(gates["observed_to_expected_min"])
        <= float(metrics["observed_to_expected"])
        <= float(gates["observed_to_expected_max"]),
        "brier_skill": float(metrics["brier_skill"])
        > float(gates["minimum_brier_skill"]),
    }
    if "minimum_day_cluster_brier_improvement_lower" in gates:
        checks["day_cluster_brier_improvement_lower"] = float(
            metrics["brier_improvement_ci_lower"]
        ) > float(gates["minimum_day_cluster_brier_improvement_lower"])
    if (
        "minimum_day_cluster_brier_improvement_positive_probability"
        in gates
    ):
        checks["day_cluster_brier_improvement_positive_probability"] = float(
            metrics["brier_improvement_bootstrap_probability_positive"]
        ) >= float(
            gates[
                "minimum_day_cluster_brier_improvement_positive_probability"
            ]
        )
    failures.extend(name for name, passed in checks.items() if not passed)
    return not failures, failures


def fit_development(
    fill_risk: pd.DataFrame,
    repair_risk: pd.DataFrame,
    *,
    gates: Mapping[str, Any] = DEFAULT_GATES,
    min_train_days: int = 8,
    embargo_days: int = 1,
    test_days: int = 2,
    l2_penalty: float = 1.0,
    family_id: str = FAMILY_ID,
    action_experiment_id: str = ACTION_EXPERIMENT_ID,
    outer_folds: Sequence[ChronologicalFold] | None = None,
    calibration_contract: Mapping[str, Any] | None = None,
    require_repair_prediction_gate: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any]]:
    days = sorted(fill_risk["day"].astype(str).unique())
    folds = tuple(outer_folds or ())
    if not folds:
        folds = make_chronological_folds(
            days,
            min_train_days=min_train_days,
            embargo_days=embargo_days,
            test_days=test_days,
        )
    calibration = (
        dict(calibration_contract)
        if calibration_contract is not None
        else None
    )

    def fit_hazard(
        frame: pd.DataFrame,
        *,
        target: str,
        side: str,
        cause: str,
        train_days: Sequence[str],
    ) -> dict[str, Any]:
        if calibration is None:
            return _fit_cloglog_hazard(
                frame,
                target=target,
                l2_penalty=l2_penalty,
                side=side,
                cause=cause,
                train_days=train_days,
            )
        return _fit_nested_calibrated_hazard(
            frame,
            target=target,
            l2_penalty=l2_penalty,
            side=side,
            cause=cause,
            train_days=train_days,
            calibration_contract=calibration,
        )

    def outer_model_audit(
        model: Mapping[str, Any],
        *,
        fold: ChronologicalFold,
        side: str,
        cause: str,
    ) -> dict[str, Any]:
        model_train_days = tuple(str(day) for day in model["train_days"])
        if model_train_days != tuple(fold.train_days):
            raise ValueError(
                f"{side}/{cause} outer model train-day identity changed"
            )
        calibrator = model.get("nested_calibrator")
        if calibration is not None and not isinstance(calibrator, Mapping):
            raise ValueError(f"{side}/{cause} nested calibrator is missing")
        calibration_days = (
            tuple(str(day) for day in calibrator["train_days"])
            if isinstance(calibrator, Mapping)
            else ()
        )
        if set(calibration_days) & set(fold.test_days):
            raise ValueError(
                f"{side}/{cause} calibrator leaked outer-test days"
            )
        return {
            "fold": int(fold.fold),
            "side": str(side),
            "cause": str(cause),
            "outer_train_days": list(model_train_days),
            "outer_embargo_days": list(fold.embargo_days),
            "outer_test_days": list(fold.test_days),
            "calibration_train_days": list(calibration_days),
            "calibration_inner_folds": (
                list(calibrator["inner_folds"])
                if isinstance(calibrator, Mapping)
                else []
            ),
            "outer_test_used_for_model": False,
            "outer_test_used_for_calibration": False,
        }

    fill_outer_model_audits: list[dict[str, Any]] = []
    prediction_parts: list[pd.DataFrame] = []
    for fold in folds:
        train = fill_risk[fill_risk["day"].isin(fold.train_days)].copy()
        test = fill_risk[fill_risk["day"].isin(fold.test_days)].copy()
        fold_output = test[
            [
                "risk_row_id",
                "day",
                "side",
                "order_id",
                "campaign_id",
                "current_inventory_role",
                "risk_interval_ms",
                "fill_event",
                "fill_value_markout_bps",
                *CAUSES,
            ]
        ].copy()
        fold_output["fold"] = int(fold.fold)
        for side in SIDES:
            train_side = train[train["side"].eq(side)].copy()
            test_side = test[test["side"].eq(side)].copy()
            if train_side.empty or test_side.empty:
                raise ValueError(f"fold {fold.fold} lacks {side} support")
            mask = fold_output["side"].eq(side)
            for cause in CAUSES:
                model = fit_hazard(
                    train_side,
                    target=cause,
                    side=side,
                    cause=cause,
                    train_days=fold.train_days,
                )
                fill_outer_model_audits.append(
                    outer_model_audit(
                        model,
                        fold=fold,
                        side=side,
                        cause=cause,
                    )
                )
                raw_probability, probability = _predict_nested_model(
                    model,
                    test_side,
                )
                fold_output.loc[
                    mask, f"raw_probability_{cause}"
                ] = raw_probability
                fold_output.loc[mask, f"probability_{cause}"] = probability
                fold_output.loc[
                    mask, f"baseline_probability_{cause}"
                ] = _predict_baseline(
                    model,
                    test_side,
                )
        prediction_parts.append(fold_output)
    predictions = pd.concat(prediction_parts, ignore_index=True)
    models: dict[str, dict[str, Any]] = {}
    side_reports: dict[str, Any] = {}
    for side in SIDES:
        side_frame = predictions[predictions["side"].eq(side)].copy()
        side_reports[side] = {"causes": {}}
        fill_side_passed = True
        for cause in CAUSES:
            metrics = _prediction_metrics(
                side_frame,
                cause,
                calibration_contract=calibration,
            )
            passed, failures = _cause_gate(metrics, gates)
            side_reports[side]["causes"][cause] = {
                **metrics,
                "gate_passed": bool(passed),
                "failed_checks": failures,
            }
            fill_side_passed = fill_side_passed and passed
        side_reports[side]["fill_prediction_gate_passed"] = bool(
            fill_side_passed
        )
        train_side = fill_risk[fill_risk["side"].eq(side)].copy()
        models[side] = {
            cause: fit_hazard(
                train_side,
                target=cause,
                side=side,
                cause=cause,
                train_days=days,
            )
            for cause in CAUSES
        }
        fill_rows = train_side[train_side["fill_event"].isin(CAUSES)]
        models[side]["fill_value_means_bps"] = {
            cause: float(
                _numeric(
                    fill_rows[fill_rows["fill_event"].eq(cause)],
                    "fill_value_markout_bps",
                ).mean()
            )
            for cause in CAUSES
        }

    repair_report: dict[str, Any] = {
        "delayed_entry_identity_passed": not repair_risk.empty,
        "campaign_sides": {},
        "outer_oof_enabled": True,
        "required_for_side_gate": bool(require_repair_prediction_gate),
    }
    repair_models: dict[str, Any] = {}
    repair_outer_model_audits: list[dict[str, Any]] = []
    repair_prediction_parts: list[pd.DataFrame] = []
    if not repair_risk.empty:
        for fold in folds:
            train = repair_risk[
                repair_risk["day"].isin(fold.train_days)
            ].copy()
            test = repair_risk[
                repair_risk["day"].isin(fold.test_days)
            ].copy()
            fold_output = test[
                [
                    "repair_risk_row_id",
                    "day",
                    "campaign_id",
                    "campaign_side",
                    "risk_interval_ms",
                    "repair_event",
                    "repair_next_event_type",
                ]
            ].copy()
            fold_output["fold"] = int(fold.fold)
            for side in SIDES:
                train_side = train[train["campaign_side"].eq(side)].copy()
                test_side = test[test["campaign_side"].eq(side)].copy()
                if train_side.empty or test_side.empty:
                    raise ValueError(
                        f"repair fold {fold.fold} lacks {side} support"
                    )
                model = fit_hazard(
                    train_side,
                    target="repair_event",
                    side=side,
                    cause="campaign_repair",
                    train_days=fold.train_days,
                )
                repair_outer_model_audits.append(
                    outer_model_audit(
                        model,
                        fold=fold,
                        side=side,
                        cause="campaign_repair",
                    )
                )
                raw_probability, probability = _predict_nested_model(
                    model,
                    test_side,
                )
                mask = fold_output["campaign_side"].eq(side)
                fold_output.loc[
                    mask, "raw_probability_repair_event"
                ] = raw_probability
                fold_output.loc[
                    mask, "probability_repair_event"
                ] = probability
                fold_output.loc[
                    mask, "baseline_probability_repair_event"
                ] = _predict_baseline(model, test_side)
            repair_prediction_parts.append(fold_output)
        repair_predictions = pd.concat(
            repair_prediction_parts,
            ignore_index=True,
        )
        for side in SIDES:
            side_repair = repair_risk[repair_risk["campaign_side"].eq(side)].copy()
            side_predictions = repair_predictions[
                repair_predictions["campaign_side"].eq(side)
            ].copy()
            events = int(_numeric(side_repair, "repair_event").sum())
            metrics = _prediction_metrics(
                side_predictions,
                "repair_event",
                calibration_contract=calibration,
            )
            repair_gates = dict(gates)
            repair_gates["minimum_events_per_side_cause"] = int(
                gates["minimum_repair_events_per_campaign_side"]
            )
            passed, failures = _cause_gate(metrics, repair_gates)
            repair_report["campaign_sides"][side] = {
                "rows": int(len(side_repair)),
                "campaigns": int(
                    side_repair[["day", "campaign_id"]]
                    .drop_duplicates()
                    .shape[0]
                ),
                "repair_events": events,
                "support_passed": events >= int(
                    gates["minimum_repair_events_per_campaign_side"]
                ),
                "metrics": metrics,
                "prediction_gate_passed": bool(passed),
                "failed_checks": failures,
            }
            if events > 0 and events < len(side_repair):
                repair_models[side] = fit_hazard(
                    side_repair,
                    target="repair_event",
                    side=side,
                    cause="campaign_repair",
                    train_days=days,
                )
    else:
        repair_predictions = pd.DataFrame()

    passed_sides: list[str] = []
    for side in SIDES:
        repair_passed = bool(
            repair_report["campaign_sides"]
            .get(side, {})
            .get("prediction_gate_passed", False)
        )
        side_passed = bool(side_reports[side]["fill_prediction_gate_passed"])
        if require_repair_prediction_gate:
            side_passed = side_passed and repair_passed
        side_reports[side]["repair_prediction_gate_passed"] = repair_passed
        side_reports[side]["prediction_gate_passed"] = bool(side_passed)
        side_reports[side]["action_experiment_registration_eligible"] = bool(
            side_passed
        )
        side_reports[side]["action_family_allowed"] = False
        if side_passed:
            passed_sides.append(side)

    summary = {
        "schema_version": SCHEMA_VERSION,
        "family_id": family_id,
        "stage": "development_chronological_oof",
        "estimand": {
            "fill": "dynamic discrete-time cause-specific hazard",
            "cancel_request": "action_or_censor",
            "cancel_ack": "post-request lifecycle outcome, not a natural cause",
            "native_price_jump": "nonabsorbing state transition",
            "campaign_repair": "delayed-entry campaign transition",
        },
        "feature_contract": {
            "input_scope": "local_m0_exact_safe",
            "feature_names": list(MODEL_FEATURES),
            "banned_feature_tokens": list(BANNED_POLICY_FEATURE_TOKENS),
            "historical_trade_quantity_used": False,
            "historical_trade_notional_used": False,
            "historical_child_count_used": False,
        },
        "folds": [asdict(fold) for fold in folds],
        "nested_calibration": {
            "enabled": calibration is not None,
            "contract": calibration,
            "outer_test_used_for_calibration": False,
            "validation_used_for_calibration": False,
            "fill_outer_model_audits": fill_outer_model_audits,
            "repair_outer_model_audits": repair_outer_model_audits,
        },
        "gates": dict(gates),
        "sides": side_reports,
        "repair": repair_report,
        "prediction_gate_passed_sides": passed_sides,
        "prediction_gate_any_side": bool(passed_sides),
        "followup_randomized_action_experiment_id": action_experiment_id,
        "followup_randomized_experiment_registration_eligible": bool(
            passed_sides
        ),
        "action_family_allowed": False,
        "live_change_allowed": False,
        "validation_access_allowed": bool(passed_sides),
        "sealed_holdout_access_allowed": False,
        "decision": (
            "register_randomized_keep_cancel_experiment"
            if passed_sides
            else "close_prediction_family_on_development"
        ),
    }
    bundle = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "family_id": family_id,
        "feature_names": list(MODEL_FEATURES),
        "gates": dict(gates),
        "development_days": days,
        "models": models,
        "repair_models": repair_models,
        "nested_calibration": {
            "enabled": calibration is not None,
            "contract": calibration,
        },
        "prediction_gate_passed_sides": passed_sides,
        "action_experiment_id": action_experiment_id,
        "action_family_allowed": False,
    }
    bundle["bundle_sha256"] = _canonical_sha256(bundle)
    return predictions, repair_predictions, summary, bundle


def evaluate_validation(
    fill_risk: pd.DataFrame,
    repair_risk: pd.DataFrame,
    *,
    bundle: Mapping[str, Any],
    strict_gates: Mapping[str, Any],
    admitted_sides: Sequence[str],
    minimum_favorable_probability_positive: float,
    calibration_contract: Mapping[str, Any] = CALIBRATOR_CONTRACT,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Evaluate frozen Development models once on Validation.

    Validation is prediction-only. It cannot refit a model, alter an action,
    or open sealed holdout.
    """

    sides = tuple(str(side) for side in admitted_sides)
    if not sides or any(side not in SIDES for side in sides):
        raise ValueError("Validation admission has invalid sides")
    development_days = [str(day) for day in bundle["development_days"]]
    validation_days = sorted(fill_risk["day"].astype(str).unique())
    if not validation_days:
        raise ValueError("Validation fill risk set is empty")
    if set(development_days) & set(validation_days):
        raise ValueError("Development and Validation days overlap")
    if validation_days[0] <= development_days[-1]:
        raise ValueError("Validation is not after Development")

    fill_parts: list[pd.DataFrame] = []
    side_reports: dict[str, Any] = {}
    for side in sides:
        side_frame = fill_risk[fill_risk["side"].eq(side)].copy()
        if side_frame.empty:
            raise ValueError(f"Validation lacks {side} fill support")
        output = side_frame[
            [
                "risk_row_id",
                "day",
                "side",
                "order_id",
                "campaign_id",
                "current_inventory_role",
                "risk_interval_ms",
                "fill_event",
                "fill_value_markout_bps",
                *CAUSES,
            ]
        ].copy()
        report = {"causes": {}}
        for cause in CAUSES:
            model = bundle["models"][side][cause]
            if [str(day) for day in model["train_days"]] != development_days:
                raise ValueError(
                    f"{side}/{cause} model is not the frozen full-Development fit"
                )
            raw, probability = _predict_nested_model(model, side_frame)
            output[f"raw_probability_{cause}"] = raw
            output[f"probability_{cause}"] = probability
            output[f"baseline_probability_{cause}"] = _predict_baseline(
                model,
                side_frame,
            )
        fill_parts.append(output)

        for cause in CAUSES:
            metrics = _prediction_metrics(
                output,
                cause,
                calibration_contract=calibration_contract,
            )
            gates = dict(strict_gates)
            gate_type = "original_strict_lower_bound"
            if cause == "favorable_fill":
                gates.pop(
                    "minimum_day_cluster_brier_improvement_lower",
                    None,
                )
                gates[
                    "minimum_day_cluster_brier_improvement_positive_probability"
                ] = float(minimum_favorable_probability_positive)
                gate_type = "frozen_probability_screen"
            passed, failures = _cause_gate(metrics, gates)
            report["causes"][cause] = {
                **metrics,
                "gate_type": gate_type,
                "gate_passed": bool(passed),
                "failed_checks": failures,
            }
        report["fill_prediction_gate_passed"] = all(
            bool(item["gate_passed"])
            for item in report["causes"].values()
        )
        side_reports[side] = report

    fill_predictions = pd.concat(fill_parts, ignore_index=True)
    repair_parts: list[pd.DataFrame] = []
    repair_reports: dict[str, Any] = {}
    for side in sides:
        side_frame = repair_risk[
            repair_risk["campaign_side"].eq(side)
        ].copy()
        if side_frame.empty:
            raise ValueError(f"Validation lacks {side} repair support")
        model = bundle["repair_models"][side]
        if [str(day) for day in model["train_days"]] != development_days:
            raise ValueError(
                f"{side} repair model is not the frozen full-Development fit"
            )
        output = side_frame[
            [
                "repair_risk_row_id",
                "day",
                "campaign_id",
                "campaign_side",
                "risk_interval_ms",
                "repair_event",
                "repair_next_event_type",
            ]
        ].copy()
        raw, probability = _predict_nested_model(model, side_frame)
        output["raw_probability_repair_event"] = raw
        output["probability_repair_event"] = probability
        output["baseline_probability_repair_event"] = _predict_baseline(
            model,
            side_frame,
        )
        repair_parts.append(output)
        metrics = _prediction_metrics(
            output,
            "repair_event",
            calibration_contract=calibration_contract,
        )
        gates = dict(strict_gates)
        gates["minimum_events_per_side_cause"] = int(
            strict_gates["minimum_repair_events_per_campaign_side"]
        )
        passed, failures = _cause_gate(metrics, gates)
        repair_reports[side] = {
            **metrics,
            "gate_type": "original_strict_lower_bound",
            "gate_passed": bool(passed),
            "failed_checks": failures,
        }
        side_reports[side]["repair_prediction_gate_passed"] = bool(passed)
        side_reports[side]["prediction_validation_passed"] = bool(
            side_reports[side]["fill_prediction_gate_passed"] and passed
        )

    repair_predictions = pd.concat(repair_parts, ignore_index=True)
    passed_sides = [
        side
        for side in sides
        if side_reports[side]["prediction_validation_passed"]
    ]
    summary = {
        "schema_version": "dynamic_fill_hazard_validation.v1",
        "family_id": str(bundle["family_id"]),
        "stage": "validation_one_shot",
        "admitted_sides": list(sides),
        "development_days": development_days,
        "validation_days": validation_days,
        "validation_day_count": len(validation_days),
        "model_refit_on_validation": False,
        "calibrator_refit_on_validation": False,
        "favorable_fill_gate": {
            "type": "day_cluster_probability_positive",
            "minimum": float(minimum_favorable_probability_positive),
        },
        "sides": side_reports,
        "repair": repair_reports,
        "prediction_validation_passed_sides": passed_sides,
        "prediction_validation_passed": bool(passed_sides),
        "randomized_action_experiment_registration_eligible": bool(
            passed_sides
        ),
        "action_family_allowed": False,
        "live_change_allowed": False,
        "sealed_holdout_access_allowed": False,
        "decision": (
            "register_separate_randomized_keep_cancel_experiment"
            if passed_sides
            else "close_prediction_family_on_validation"
        ),
    }
    return fill_predictions, repair_predictions, summary


def _read_split_days(split_path: Path, panel: str) -> list[str]:
    split = json.loads(split_path.read_text(encoding="utf-8"))
    try:
        days = [str(day) for day in split["panels"][panel]["days"]]
    except KeyError as exc:
        raise ValueError(f"split has no {panel} panel") from exc
    if days != sorted(set(days)):
        raise ValueError(f"{panel} days must be sorted and unique")
    return days


def _read_split_development_folds(
    split_path: Path,
    development_days: Sequence[str],
) -> tuple[ChronologicalFold, ...]:
    split = json.loads(split_path.read_text(encoding="utf-8"))
    payloads = split.get("development_folds", [])
    if not payloads:
        contract = split.get("fold_contract", {})
        return make_chronological_folds(
            development_days,
            min_train_days=int(contract.get("min_train_days", 8)),
            embargo_days=int(contract.get("embargo_days", 1)),
            test_days=int(contract.get("test_days", 2)),
        )
    allowed = set(str(day) for day in development_days)
    folds: list[ChronologicalFold] = []
    previous_test_end = ""
    for index, payload in enumerate(payloads):
        train = tuple(str(day) for day in payload["train_days"])
        embargo = tuple(str(day) for day in payload["embargo_days"])
        test = tuple(str(day) for day in payload["test_days"])
        if not train or not embargo or not test:
            raise ValueError("frozen Development fold is empty")
        if (
            set((*train, *embargo, *test)) - allowed
            or tuple(sorted(set(train))) != train
            or tuple(sorted(set(embargo))) != embargo
            or tuple(sorted(set(test))) != test
        ):
            raise ValueError("frozen Development fold days are invalid")
        if not max(train) < min(embargo) < min(test):
            raise ValueError("frozen Development fold is not chronological")
        if previous_test_end and min(test) <= previous_test_end:
            raise ValueError("frozen Development test folds overlap")
        previous_test_end = max(test)
        folds.append(
            ChronologicalFold(
                fold=int(payload.get("fold", index)),
                train_days=train,
                embargo_days=embargo,
                test_days=test,
            )
        )
    frozen_oof = sorted(
        str(day) for day in split.get("development_oof_days", [])
    )
    actual_oof = sorted(day for fold in folds for day in fold.test_days)
    if frozen_oof and frozen_oof != actual_oof:
        raise ValueError("frozen Development OOF day identity changed")
    return tuple(folds)


def _lifecycle_partition_path(source: Path, day: str) -> Path:
    if source.is_dir():
        path = source / f"{day}.lifecycle.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"missing lifecycle partition: {path}")
        return path
    if not source.is_file():
        raise FileNotFoundError(f"lifecycle source does not exist: {source}")
    return source


def _read_lifecycle_partition(source: Path, day: str) -> tuple[pd.DataFrame, Path]:
    path = _lifecycle_partition_path(source, day)
    if source.is_dir():
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_parquet(path, filters=[("day", "=", str(day))])
    actual_days = sorted(frame["day"].astype(str).unique()) if not frame.empty else []
    if actual_days != [str(day)]:
        raise ValueError(
            f"lifecycle partition {path} has days {actual_days}, expected {day}"
        )
    return frame, path


def _compact_fill_risk(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "risk_row_id",
        "day",
        "side",
        "order_id",
        "campaign_id",
        "current_inventory_role",
        "risk_interval_start_ts_ns",
        "risk_interval_end_ts_ns",
        "risk_interval_ms",
        "next_event_type",
        "fill_event",
        "favorable_fill",
        "adverse_fill",
        "cancel_action_censor",
        "native_jump_transitions_in_interval",
        "fill_value_markout_bps",
        "formal_dynamic_fill_eligible",
        *MODEL_FEATURES,
    ]
    return frame.loc[:, columns].copy()


def _compact_repair_risk(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    columns = [
        "repair_risk_row_id",
        "day",
        "campaign_id",
        "campaign_side",
        "order_id",
        "current_inventory_role",
        "risk_interval_start_ts_ns",
        "risk_interval_end_ts_ns",
        "risk_interval_ms",
        "repair_event",
        "repair_next_event_type",
        "formal_delayed_entry_repair_eligible",
        *MODEL_FEATURES,
    ]
    return frame.loc[:, columns].copy()


def _sum_count_maps(
    identities: Sequence[Mapping[str, Any]], name: str
) -> dict[str, int]:
    output: dict[str, int] = {}
    for identity in identities:
        for key, value in dict(identity.get(name) or {}).items():
            output[str(key)] = output.get(str(key), 0) + int(value)
    return output


def _aggregate_fill_identities(
    identities: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    numeric = (
        "snapshot_rows",
        "strict_snapshot_rows",
        "future_feature_rows",
        "queue_unsupported_rows",
        "same_ms_ambiguous_rows",
        "zero_duration_rows",
        "fill_horizon_censored_rows",
        "cancel_censor_rows",
        "native_jump_transition_rows",
        "formal_rows",
        "orders",
    )
    return {
        **{
            name: int(sum(int(item.get(name, 0) or 0) for item in identities))
            for name in numeric
        },
        "days": sorted(
            {
                str(day)
                for item in identities
                for day in item.get("days", ())
            }
        ),
        "event_counts": _sum_count_maps(identities, "event_counts"),
        "role_counts": _sum_count_maps(identities, "role_counts"),
        "cancel_is_action_or_censor": all(
            bool(item.get("cancel_is_action_or_censor", False))
            for item in identities
        ),
        "jump_is_nonabsorbing_transition": all(
            bool(item.get("jump_is_nonabsorbing_transition", False))
            for item in identities
        ),
        "partition_count": len(identities),
    }


def _aggregate_repair_identities(
    identities: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "formal_rows": int(
            sum(int(item.get("formal_rows", 0) or 0) for item in identities)
        ),
        "campaigns": int(
            sum(int(item.get("campaigns", 0) or 0) for item in identities)
        ),
        "repair_events": int(
            sum(int(item.get("repair_events", 0) or 0) for item in identities)
        ),
        "pre_entry_rows": int(
            sum(int(item.get("pre_entry_rows", 0) or 0) for item in identities)
        ),
        "delayed_entry_identity_passed": all(
            bool(item.get("delayed_entry_identity_passed", False))
            for item in identities
        ),
        "partition_count": len(identities),
    }


def _build_partitioned_risk_sets(
    lifecycle_source: Path,
    expected_days: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    fill_parts: list[pd.DataFrame] = []
    repair_parts: list[pd.DataFrame] = []
    fill_identities: list[dict[str, Any]] = []
    repair_identities: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    for day in expected_days:
        lifecycle, path = _read_lifecycle_partition(lifecycle_source, str(day))
        fill_risk, fill_identity = build_dynamic_fill_risk_set(lifecycle)
        repair_risk, repair_identity = build_delayed_entry_repair_risk_set(
            lifecycle
        )
        fill_parts.append(_compact_fill_risk(fill_risk))
        if not repair_risk.empty:
            repair_parts.append(_compact_repair_risk(repair_risk))
        fill_identities.append(fill_identity)
        repair_identities.append(repair_identity)
        files.append(
            {
                "day": str(day),
                "path": str(path),
                "sha256": _sha256(path),
                "lifecycle_rows": int(len(lifecycle)),
                "fill_risk_rows": int(len(fill_risk)),
                "repair_risk_rows": int(len(repair_risk)),
            }
        )
        del lifecycle, fill_risk, repair_risk
    fill = pd.concat(fill_parts, ignore_index=True)
    repair = (
        pd.concat(repair_parts, ignore_index=True)
        if repair_parts
        else pd.DataFrame()
    )
    source_identity = {
        "source": str(lifecycle_source),
        "partitions": files,
        "partition_count": len(files),
    }
    source_identity["sha256"] = _canonical_sha256(source_identity)
    return (
        fill,
        repair,
        _aggregate_fill_identities(fill_identities),
        _aggregate_repair_identities(repair_identities),
        source_identity,
    )


def _source_split_identity(
    split: Mapping[str, Any],
    source_split: Path,
) -> dict[str, str]:
    schema = str(split.get("schema_version", ""))
    if schema == "narrowgate_evidence_split.v1":
        validate_evidence_split(split)
        return {
            "manifest_path": str(split["source_manifest_path"]),
            "manifest_sha256": str(split["source_manifest_sha256"]),
            "daily_manifest_sha256": str(
                split.get("source_daily_manifest_sha256", "")
            ),
        }
    if schema != "strict_native_evidence_split.v1":
        raise ValueError(f"unsupported source split schema: {schema}")

    panel_order = (
        "development",
        "embargo_1",
        "validation",
        "embargo_2",
        "sealed_holdout",
    )
    panels = split.get("panels", {})
    ordered_days: list[str] = []
    for name in panel_order:
        panel = panels.get(name)
        if not isinstance(panel, Mapping):
            raise ValueError(f"strict source split is missing panel: {name}")
        days = [str(day) for day in panel.get("days", [])]
        if days != sorted(set(days)):
            raise ValueError(f"strict source panel {name} is not chronological")
        if int(panel.get("day_count", len(days))) != len(days):
            raise ValueError(f"strict source panel {name} day count changed")
        ordered_days.extend(days)
    if ordered_days != sorted(set(ordered_days)):
        raise ValueError("strict source panels overlap or are not chronological")
    if int(split.get("strict_days_count", -1)) != len(ordered_days):
        raise ValueError("strict source day count does not match panels")

    manifest = Path(str(split["strict_days_path"])).expanduser().resolve()
    if not manifest.is_file():
        raise FileNotFoundError(f"strict source manifest does not exist: {manifest}")
    manifest_sha256 = _sha256(manifest)
    if manifest_sha256 != str(split["strict_days_sha256"]):
        raise ValueError("strict source manifest identity changed")
    manifest_days = sorted(
        set(
            pd.to_datetime(
                pd.read_csv(manifest)["day"],
                utc=True,
                errors="raise",
            ).dt.strftime("%Y-%m-%d")
        )
    )
    if manifest_days != ordered_days:
        raise ValueError("strict source manifest days do not match frozen panels")
    return {
        "manifest_path": str(manifest),
        "manifest_sha256": manifest_sha256,
        "daily_manifest_sha256": manifest_sha256,
    }


def freeze_spec(args: argparse.Namespace) -> int:
    source_split = args.source_split.expanduser().resolve()
    split = json.loads(source_split.read_text(encoding="utf-8"))
    source_identity = _source_split_identity(split, source_split)
    family_id = str(getattr(args, "family_id", "") or FAMILY_ID)
    action_experiment_id = str(
        getattr(args, "action_experiment_id", "") or ACTION_EXPERIMENT_ID
    )
    panels = split["panels"]
    action_contract = {
        "family_id": action_experiment_id,
        "surface": "first eligible active exposure-increasing order per campaign",
        "sides": ["BUY", "SELL"],
        "inventory_role": "add",
        "actions": ["keep", "cancel_then_reenter_on_recovery"],
        "behavior_probabilities": {
            "keep": 0.5,
            "cancel_then_reenter_on_recovery": 0.5,
        },
        "one_intervention_per_campaign": True,
        "size_modified": False,
        "reducing_side_modified": False,
        "inventory_limit_modified": False,
        "model_selection_role": (
            "eligibility/support only; randomized replay identifies action value"
        ),
        "cancel_ack_modeled": True,
        "fill_before_cancel_ack_modeled": True,
        "queue_reset_on_reentry": True,
        "latency_replayed": True,
        "candidate_rate_budget": {"minimum": 0.05, "maximum": 0.30},
        "score_profile_contract": {
            "schema_version": "narrowgate_score_profile.v1",
            "profile_id": "action_execution_selective_v2",
            "profile_sha256": (
                "74f6d38c20b4279e6f7d8da069a9fae7467e5d851ffdcdc54a63b75207a97d14"
            ),
        },
        "primary_metrics": [
            "clipped_dr_action_uplift",
            "campaign_terminal_mtm",
            "campaign_tail",
            "effective_sample_size",
            "toxic_reduction_surplus",
            "toxic_selectivity_log_ratio",
        ],
        "nonlinear_diagnostic": (
            "toxic_fill_reduction / total_fill_reduction; proportional "
            "participation shutdown must score zero surplus"
        ),
        "live_change_allowed": False,
    }
    new_split = {
        "schema_version": "narrowgate_evidence_split.v1",
        "family_id": family_id,
        "split_mode": "explicit_chronological_existing_good_days",
        "evidence_scope": (
            "family-specific; dates may have been inspected by unrelated hypotheses"
        ),
        "panels": panels,
        "source_manifest_path": source_identity["manifest_path"],
        "source_manifest_sha256": source_identity["manifest_sha256"],
        "source_daily_manifest_sha256": source_identity[
            "daily_manifest_sha256"
        ],
        "source_split_path": str(source_split),
        "source_split_sha256": _sha256(source_split),
        "holdout_rule": (
            "Development prediction gate precedes Validation. Validation must "
            "pass unchanged before sealed_holdout. Prediction only registers "
            "a randomized action experiment and never authorizes live policy."
        ),
        "action_family": action_contract,
    }
    for name in (
        "development_folds",
        "development_oof_days",
        "development_oof_day_count",
        "fold_contract",
    ):
        if name in split:
            new_split[name] = split[name]
    new_split["action_family_sha256"] = _canonical_sha256(action_contract)
    validate_evidence_split(new_split)
    output_split = args.output_split.expanduser().resolve()
    output_split.parent.mkdir(parents=True, exist_ok=True)
    output_split.write_text(
        json.dumps(new_split, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_paths = [
        args.config,
        args.p3_artifact,
        args.queue_artifact,
        args.latency_artifact,
        args.visibility_artifact,
    ]
    artifact_names = ("config", "p3", "queue", "latency", "visibility")
    artifacts = {
        name: {
            "path": str(Path(artifact_paths[index]).expanduser().resolve()),
            "sha256": _sha256(
                Path(artifact_paths[index]).expanduser().resolve()
            ),
        }
        for index, name in enumerate(artifact_names)
    }
    code_identity = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_spec.expanduser().resolve().parent
        / f"{args.output_spec.stem}.code_checkpoint",
        repo_root=ROOT,
        code_identity=code_identity,
    )
    spec = {
        "schema_version": "dynamic_fill_hazard_family_spec.v2",
        "family_id": family_id,
        "prediction_stage": family_id,
        "action_stage": action_contract,
        "split_path": str(output_split),
        "split_sha256": _sha256(output_split),
        "snapshot_edges_ms": [
            0,
            100,
            200,
            300,
            500,
            750,
            1_000,
            1_500,
            2_500,
            4_000,
            6_000,
            10_000,
            20_000,
            40_000,
            85_000,
        ],
        "model_features": list(MODEL_FEATURES),
        "banned_policy_feature_tokens": list(BANNED_POLICY_FEATURE_TOKENS),
        "prediction_gates": dict(DEFAULT_GATES),
        "model": {
            "type": "side_specific_cause_specific_discrete_cloglog",
            "minimum_train_days": int(
                split.get("fold_contract", {}).get("min_train_days", 8)
            ),
            "embargo_days": int(
                split.get("fold_contract", {}).get("embargo_days", 1)
            ),
            "test_days": int(
                split.get("fold_contract", {}).get("test_days", 2)
            ),
            "l2_penalty": 1.0,
            "optimizer": dict(OPTIMIZER_CONTRACT),
            "nested_calibration": {
                "enabled": True,
                "contract": dict(CALIBRATOR_CONTRACT),
                "fit_source": "inner_expanding_oof_within_each_outer_train",
                "outer_test_used": False,
                "validation_used": False,
            },
            "repair_outer_oof": True,
            "require_repair_prediction_gate": True,
        },
        "runtime_identity": _runtime_identity(),
        "artifacts": artifacts,
        "code_identity": code_identity,
        "code_checkpoint": checkpoint,
        "outcome_access": {
            "development": "open",
            "validation": "locked_until_development_passes",
            "sealed_holdout": "locked_until_validation_passes",
        },
        "action_family_allowed": False,
        "live_change_allowed": False,
    }
    spec["spec_sha256"] = _canonical_sha256(spec)
    output_spec = args.output_spec.expanduser().resolve()
    output_spec.write_text(
        json.dumps(spec, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"split": str(output_split), "spec": str(output_spec)}, indent=2))
    return 0


def fit_development_command(args: argparse.Namespace) -> int:
    lifecycle_path = args.lifecycle.expanduser().resolve()
    split_path = args.split.expanduser().resolve()
    spec_path = args.spec.expanduser().resolve()
    expected_days = _read_split_days(split_path, "development")
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    family_id = str(spec.get("family_id", "")).strip()
    if not family_id:
        raise SystemExit("family spec identity is missing")
    split = json.loads(split_path.read_text(encoding="utf-8"))
    if str(split.get("family_id", "")).strip() != family_id:
        raise SystemExit("family spec/split identity mismatch")
    action_experiment_id = str(
        spec.get("action_stage", {}).get("family_id", "")
    ).strip()
    if not action_experiment_id:
        raise SystemExit("family spec action experiment identity is missing")
    if spec.get("model", {}).get("optimizer") != OPTIMIZER_CONTRACT:
        raise SystemExit("family spec optimizer contract mismatch")
    nested_calibration = spec.get("model", {}).get(
        "nested_calibration", {}
    )
    if not bool(nested_calibration.get("enabled", False)):
        raise SystemExit("nested chronological calibration must be enabled")
    if nested_calibration.get("contract") != CALIBRATOR_CONTRACT:
        raise SystemExit("family spec calibrator contract mismatch")
    if not bool(spec.get("model", {}).get("repair_outer_oof", False)):
        raise SystemExit("repair outer-OOF fitting must be enabled")
    if spec.get("runtime_identity") != _runtime_identity():
        raise SystemExit("family spec runtime identity mismatch")
    output = args.output_prefix.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    risk_prefix_arg = getattr(args, "risk_prefix", None)
    risk_prefix = (
        risk_prefix_arg.expanduser().resolve()
        if risk_prefix_arg is not None
        else output
    )
    paths = {
        "fill_risk": risk_prefix.with_suffix(".fill_risk.parquet"),
        "repair_risk": risk_prefix.with_suffix(".repair_risk.parquet"),
        "risk_build": output.with_suffix(".risk_build.json"),
        "predictions": output.with_suffix(".oof_predictions.parquet"),
        "repair_predictions": output.with_suffix(
            ".repair_oof_predictions.parquet"
        ),
        "summary": output.with_suffix(".summary.json"),
        "bundle": output.with_suffix(".bundle.json"),
        "dataset": output.with_suffix(".dataset.csv"),
    }
    reuse_risk_sets = bool(getattr(args, "reuse_risk_sets", False))
    if reuse_risk_sets:
        prior_risk_build_path = risk_prefix.with_suffix(".risk_build.json")
        reuse_paths = {
            "fill_risk": paths["fill_risk"],
            "repair_risk": paths["repair_risk"],
            "risk_build": prior_risk_build_path,
        }
        for name, path in reuse_paths.items():
            if not path.is_file():
                raise FileNotFoundError(
                    f"cannot reuse missing {name} artifact: {path}"
                )
        prior_risk_build = json.loads(
            prior_risk_build_path.read_text(encoding="utf-8")
        )
        if (
            str(prior_risk_build.get("schema_version", ""))
            != "dynamic_fill_hazard_risk_build.v1"
        ):
            raise ValueError("reused risk-set schema identity changed")
        fill_identity = dict(prior_risk_build["fill_risk_identity"])
        repair_identity = dict(prior_risk_build["repair_risk_identity"])
        lifecycle_identity = dict(prior_risk_build["lifecycle_partitions"])
        if [str(day) for day in fill_identity.get("days", [])] != expected_days:
            raise ValueError("reused risk-set Development days changed")
        if Path(str(lifecycle_identity["source"])).resolve() != lifecycle_path:
            raise ValueError("reused risk-set lifecycle source changed")
        for name in ("fill", "repair"):
            artifact = prior_risk_build["risk_artifacts"][name]
            artifact_path = reuse_paths[f"{name}_risk"]
            if Path(str(artifact["path"])).resolve() != artifact_path:
                raise ValueError(f"reused {name} risk path changed")
            if _sha256(artifact_path) != str(artifact["sha256"]):
                raise ValueError(f"reused {name} risk hash changed")
        fill_risk = pd.read_parquet(paths["fill_risk"])
        repair_risk = pd.read_parquet(paths["repair_risk"])
        if int(len(fill_risk)) != int(
            prior_risk_build["risk_artifacts"]["fill"]["rows"]
        ):
            raise ValueError("reused fill risk row count changed")
        if int(len(repair_risk)) != int(
            prior_risk_build["risk_artifacts"]["repair"]["rows"]
        ):
            raise ValueError("reused repair risk row count changed")
    else:
        (
            fill_risk,
            repair_risk,
            fill_identity,
            repair_identity,
            lifecycle_identity,
        ) = _build_partitioned_risk_sets(lifecycle_path, expected_days)
        fill_risk.to_parquet(paths["fill_risk"], index=False)
        repair_risk.to_parquet(paths["repair_risk"], index=False)
    fill_days = sorted(fill_risk["day"].astype(str).unique())
    if fill_days != expected_days:
        raise ValueError("Development fill risk-set days changed")
    fill_risk.groupby("day", sort=True).agg(
        rows=("risk_row_id", "size"),
        orders=("order_id", "nunique"),
        favorable_fills=("favorable_fill", "sum"),
        adverse_fills=("adverse_fill", "sum"),
    ).reset_index().to_csv(paths["dataset"], index=False)
    paths["risk_build"].write_text(
        json.dumps(
            _json_safe(
                {
                    "schema_version": "dynamic_fill_hazard_risk_build.v1",
                    "family_id": family_id,
                    "family_spec_path": str(spec_path),
                    "family_spec_sha256": _sha256(spec_path),
                    "evidence_split_path": str(split_path),
                    "evidence_split_sha256": _sha256(split_path),
                    "fill_risk_identity": fill_identity,
                    "repair_risk_identity": repair_identity,
                    "lifecycle_partitions": lifecycle_identity,
                    "risk_artifacts": {
                        "fill": {
                            "path": str(paths["fill_risk"]),
                            "sha256": _sha256(paths["fill_risk"]),
                            "rows": int(len(fill_risk)),
                        },
                        "repair": {
                            "path": str(paths["repair_risk"]),
                            "sha256": _sha256(paths["repair_risk"]),
                            "rows": int(len(repair_risk)),
                        },
                    },
                    "reused_prebuilt_risk_sets": reuse_risk_sets,
                    "source_risk_build_path": (
                        str(risk_prefix.with_suffix(".risk_build.json"))
                        if reuse_risk_sets
                        else ""
                    ),
                    "source_risk_build_family_id": (
                        str(prior_risk_build.get("family_id", ""))
                        if reuse_risk_sets
                        else family_id
                    ),
                    "source_risk_build_evidence_split_sha256": (
                        str(
                            prior_risk_build.get(
                                "evidence_split_sha256",
                                "",
                            )
                        )
                        if reuse_risk_sets
                        else _sha256(split_path)
                    ),
                    "cross_family_reuse_validated_by": (
                        "risk_schema+development_days+lifecycle_source+artifact_hash"
                        if reuse_risk_sets
                        else "not_applicable"
                    ),
                    "optimizer_contract": dict(OPTIMIZER_CONTRACT),
                    "runtime_identity": _runtime_identity(),
                }
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    outer_folds = _read_split_development_folds(
        split_path,
        expected_days,
    )
    predictions, repair_predictions, summary, bundle = fit_development(
        fill_risk,
        repair_risk,
        gates=spec["prediction_gates"],
        min_train_days=int(spec["model"]["minimum_train_days"]),
        embargo_days=int(spec["model"]["embargo_days"]),
        test_days=int(spec["model"]["test_days"]),
        l2_penalty=float(spec["model"]["l2_penalty"]),
        family_id=family_id,
        action_experiment_id=action_experiment_id,
        outer_folds=outer_folds,
        calibration_contract=CALIBRATOR_CONTRACT,
        require_repair_prediction_gate=bool(
            spec["model"]["require_repair_prediction_gate"]
        ),
    )
    summary["fill_risk_identity"] = fill_identity
    summary["repair_risk_identity"] = repair_identity
    summary["family_spec_path"] = str(spec_path)
    summary["family_spec_sha256"] = _sha256(spec_path)
    summary["evidence_split_path"] = str(split_path)
    summary["evidence_split_sha256"] = _sha256(split_path)
    summary["lifecycle_path"] = str(lifecycle_path)
    summary["lifecycle_sha256"] = str(lifecycle_identity["sha256"])
    summary["lifecycle_partitions"] = lifecycle_identity
    predictions.to_parquet(paths["predictions"], index=False)
    repair_predictions.to_parquet(paths["repair_predictions"], index=False)
    paths["summary"].write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    paths["bundle"].write_text(
        json.dumps(_json_safe(bundle), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    code_identity = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        output.parent / f"{output.name}.code_checkpoint",
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest = build_manifest(
        {
            "experiment_id": family_id,
            "config_path": spec["artifacts"]["config"]["path"],
            "dataset_manifest_path": str(paths["dataset"]),
            "feature_schema_version": "local_order_lifecycle.v2.dynamic_risk_snapshot",
            "model_versions": {
                "fill_hazard": (
                    "side_specific_cause_specific_discrete_cloglog."
                    "nested_affine_cloglog.v2"
                ),
                "repair": (
                    "delayed_entry_campaign_transition."
                    "nested_affine_cloglog.v2"
                ),
            },
            "label_versions": {
                "fill": "maker_signed_markout_at_frozen_horizon.v1",
                "cancel": "action_or_censor.v1",
                "jump": "nonabsorbing_native_transition.v1",
                "repair": "delayed_entry_campaign_transition.v1",
            },
            "splits": json.loads(split_path.read_text(encoding="utf-8"))["panels"],
            "baseline_definition": "current corrected causal replay; fresh start",
            "action_definition": "none; prediction gate only",
            "input_paths": [
                *[
                    str(item["path"])
                    for item in lifecycle_identity["partitions"]
                ],
                str(split_path),
                str(spec_path),
            ],
            "artifact_paths": [str(path) for path in paths.values()],
            "engine": "python_authoritative_tick_replay",
            "promotion_status": "diagnostic_only",
            "notes": (
                "Prediction may register a separate 50/50 randomized action "
                "experiment. It never authorizes an action or live policy."
            ),
        },
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest["code_checkpoint"] = checkpoint
    manifest_path = output.with_suffix(".experiment_manifest.json")
    write_manifest(manifest_path, manifest)
    print(
        json.dumps(
            {
                "summary": str(paths["summary"]),
                "bundle": str(paths["bundle"]),
                "prediction_gate_passed_sides": summary[
                    "prediction_gate_passed_sides"
                ],
                "validation_access_allowed": summary[
                    "validation_access_allowed"
                ],
                "manifest": str(manifest_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if summary["prediction_gate_any_side"] else 2


def evaluate_validation_command(args: argparse.Namespace) -> int:
    lifecycle_path = args.lifecycle.expanduser().resolve()
    split_path = args.split.expanduser().resolve()
    spec_path = args.spec.expanduser().resolve()
    bundle_path = args.development_bundle.expanduser().resolve()
    admission_path = args.admission_decision.expanduser().resolve()
    output = args.output_prefix.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    expected_days, access_identity = load_evidence_panel(
        split_path,
        "validation",
        access_decision_path=admission_path,
    )
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    admission = json.loads(admission_path.read_text(encoding="utf-8"))
    family_id = str(spec["family_id"])
    if str(bundle.get("family_id", "")) != family_id:
        raise ValueError("Development bundle family changed")
    if str(admission.get("family_id", "")) != family_id:
        raise ValueError("Validation admission family changed")

    (
        fill_risk,
        repair_risk,
        fill_identity,
        repair_identity,
        lifecycle_identity,
    ) = _build_partitioned_risk_sets(lifecycle_path, expected_days)
    minimum_probability = float(
        admission["admission_rule"][
            "minimum_day_cluster_probability_improvement_positive"
        ]
    )
    fill_predictions, repair_predictions, summary = evaluate_validation(
        fill_risk,
        repair_risk,
        bundle=bundle,
        strict_gates=spec["prediction_gates"],
        admitted_sides=admission["admitted_sides"],
        minimum_favorable_probability_positive=minimum_probability,
        calibration_contract=CALIBRATOR_CONTRACT,
    )
    summary.update(
        {
            "access_identity": access_identity,
            "admission_decision_path": str(admission_path),
            "admission_decision_sha256": _sha256(admission_path),
            "development_bundle_path": str(bundle_path),
            "development_bundle_sha256": _sha256(bundle_path),
            "family_spec_path": str(spec_path),
            "family_spec_sha256": _sha256(spec_path),
            "evidence_split_path": str(split_path),
            "evidence_split_sha256": _sha256(split_path),
            "lifecycle_partitions": lifecycle_identity,
            "fill_risk_identity": fill_identity,
            "repair_risk_identity": repair_identity,
        }
    )
    fill_path = output.with_suffix(".fill_predictions.parquet")
    repair_path = output.with_suffix(".repair_predictions.parquet")
    summary_path = output.with_suffix(".summary.json")
    fill_predictions.to_parquet(fill_path, index=False)
    repair_predictions.to_parquet(repair_path, index=False)
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    code_identity = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        output.parent / f"{output.name}.code_checkpoint",
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest = build_manifest(
        {
            "experiment_id": f"{family_id}.validation",
            "config_path": spec["artifacts"]["config"]["path"],
            "dataset_manifest_path": str(summary_path),
            "feature_schema_version": (
                "local_order_lifecycle.v2.dynamic_risk_snapshot"
            ),
            "model_versions": {
                "fill_hazard": (
                    "frozen_development_side_specific_cause_specific."
                    "nested_affine_cloglog.v2"
                ),
                "repair": (
                    "frozen_development_delayed_entry_campaign_transition."
                    "nested_affine_cloglog.v2"
                ),
            },
            "label_versions": {
                "fill": "maker_signed_markout_at_frozen_horizon.v1",
                "repair": "delayed_entry_campaign_transition.v1",
            },
            "splits": json.loads(split_path.read_text(encoding="utf-8"))[
                "panels"
            ],
            "baseline_definition": "exposure-only frozen Development hazard",
            "action_definition": "none; one-shot prediction Validation",
            "input_paths": [
                str(split_path),
                str(spec_path),
                str(bundle_path),
                str(admission_path),
                *[
                    str(item["path"])
                    for item in lifecycle_identity["partitions"]
                ],
            ],
            "artifact_paths": [
                str(fill_path),
                str(repair_path),
                str(summary_path),
            ],
            "engine": "python_prediction_validation",
            "promotion_status": (
                "randomized_action_experiment_registration_eligible"
                if summary["prediction_validation_passed"]
                else "closed_on_validation"
            ),
            "notes": (
                "Validation never refits the model and cannot authorize an "
                "action, live policy, or sealed-holdout access."
            ),
        },
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest["code_checkpoint"] = checkpoint
    manifest_path = output.with_suffix(".experiment_manifest.json")
    write_manifest(manifest_path, manifest)
    print(
        json.dumps(
            {
                "summary": str(summary_path),
                "prediction_validation_passed_sides": summary[
                    "prediction_validation_passed_sides"
                ],
                "decision": summary["decision"],
                "manifest": str(manifest_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if summary["prediction_validation_passed"] else 2


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-spec")
    freeze.add_argument("--source-split", type=Path, required=True)
    freeze.add_argument("--output-split", type=Path, required=True)
    freeze.add_argument("--output-spec", type=Path, required=True)
    freeze.add_argument("--config", type=Path, required=True)
    freeze.add_argument("--p3-artifact", type=Path, required=True)
    freeze.add_argument("--queue-artifact", type=Path, required=True)
    freeze.add_argument("--latency-artifact", type=Path, required=True)
    freeze.add_argument("--visibility-artifact", type=Path, required=True)
    freeze.add_argument("--family-id", default=FAMILY_ID)
    freeze.add_argument("--action-experiment-id", default=ACTION_EXPERIMENT_ID)
    freeze.set_defaults(func=freeze_spec)

    development = subparsers.add_parser("fit-development")
    development.add_argument("--lifecycle", type=Path, required=True)
    development.add_argument("--split", type=Path, required=True)
    development.add_argument("--spec", type=Path, required=True)
    development.add_argument("--output-prefix", type=Path, required=True)
    development.add_argument("--reuse-risk-sets", action="store_true")
    development.add_argument("--risk-prefix", type=Path)
    development.set_defaults(func=fit_development_command)

    validation = subparsers.add_parser("evaluate-validation")
    validation.add_argument("--lifecycle", type=Path, required=True)
    validation.add_argument("--split", type=Path, required=True)
    validation.add_argument("--spec", type=Path, required=True)
    validation.add_argument(
        "--development-bundle",
        type=Path,
        required=True,
    )
    validation.add_argument(
        "--admission-decision",
        type=Path,
        required=True,
    )
    validation.add_argument("--output-prefix", type=Path, required=True)
    validation.set_defaults(func=evaluate_validation_command)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
