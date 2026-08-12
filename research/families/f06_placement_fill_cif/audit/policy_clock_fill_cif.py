#!/usr/bin/env python3
"""Fit placement fill CIF with a replayed cancel clock and ACK-latency race.

Cancel request is supplied by the frozen baseline replay, not learned as a
stationary hazard.  A past-only empirical request-to-ACK contract supplies the
second stopping component.  The order remains fillable until ACK, so the fill
and ACK probabilities share one survival process.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from models.audit.experiment_manifest import (
    git_workspace_identity,
    write_code_checkpoint,
)
from research.families.f06_placement_fill_cif import FAMILY_DOCS
from research.families.f06_placement_fill_cif.audit.competing_curve_fill_cif import (
    IDENTITY_COLUMNS,
    competing_labels_at_horizons,
)
from research.families.f06_placement_fill_cif.audit.direct_fill_cif import make_expanding_folds
from research.families.f06_placement_fill_cif.audit.full_curve_fill_cif import (
    DATA_ROOT,
    ROOT,
    STATIC_MODEL_FEATURES,
    _activation_values,
    _dynamic_features,
    _fit_side,
    _numeric,
    _sha256,
    derive_duration_contract,
    expand_action_lifecycles,
    fit_activation_contract,
    lifecycle_input_columns,
)
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import load_placement_fill_spec

DEFAULT_SPEC = FAMILY_DOCS / "placement_fill_policy_clock_race_v1_fit_spec_20260728.json"
DEFAULT_OUTPUT = (
    DATA_ROOT / "reports" / "placement_fill_policy_clock_race_v1_development_20260728"
)

SCHEMA_VERSION = "placement_fill_policy_clock_race.v1"
MODEL_KIND = "fill_hazard_plus_deterministic_request_and_empirical_ack_latency"
POLICY_EXTRA_COLUMNS = (
    "cancel_request_reason",
    "fill_while_cancel_pending_qty",
    "fill_qty",
    "partial_fill_count",
    "full_fill_ts_ns",
)


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def policy_lifecycle_input_columns() -> tuple[str, ...]:
    columns = list(lifecycle_input_columns())
    columns.extend(
        ("cancel_request_ts_ns", "cancel_request_reason", "cancel_ack_ts_ns")
    )
    for action in ("closer_1tick", "current", "farther_1tick"):
        columns.extend(f"{action}__{name}" for name in POLICY_EXTRA_COLUMNS[1:])
    return tuple(dict.fromkeys(columns))


def _load_policy_partitions(spec: Mapping[str, Any]) -> pd.DataFrame:
    days = [str(day) for day in spec["panels"]["development"]["days"]]
    by_day: dict[str, Path] = {}
    for source in spec["source_identity"]["placement_panel_roots"]:
        root = Path(str(source["path"])).expanduser().resolve()
        manifest = Path(str(source["manifest"])).expanduser().resolve()
        if _sha256(manifest) != str(source["manifest_sha256"]):
            raise RuntimeError(f"policy-clock panel manifest changed: {manifest}")
        for path in root.glob("partitions/day=*/placement.parquet"):
            day = path.parent.name.removeprefix("day=")
            if day in by_day:
                raise RuntimeError(f"duplicate policy-clock partition for {day}")
            by_day[day] = path
    missing = sorted(set(days) - set(by_day))
    if missing:
        raise FileNotFoundError(f"missing policy-clock Development partitions: {missing}")
    return pd.concat(
        [
            pd.read_parquet(by_day[day], columns=policy_lifecycle_input_columns())
            for day in days
        ],
        ignore_index=True,
    )


def expand_policy_clock_lifecycles(
    wide: pd.DataFrame, *, actions: Sequence[str] = ("current",)
) -> pd.DataFrame:
    """Expand selected placements and retain the replayed policy-clock fields."""

    selected = tuple(dict.fromkeys(str(action) for action in actions))
    allowed = {"closer_1tick", "current", "farther_1tick"}
    if not selected or not set(selected).issubset(allowed):
        raise ValueError(f"unsupported policy-clock placement actions={selected!r}")
    required = set(policy_lifecycle_input_columns())
    missing = sorted(required - set(wide.columns))
    if missing:
        raise ValueError(f"policy-clock panel is missing columns: {missing}")

    lifecycles = expand_action_lifecycles(wide)
    lifecycles = lifecycles.loc[lifecycles["action"].isin(selected)].copy()
    baseline = wide.set_index("cohort_id")
    reasons = baseline["cancel_request_reason"].astype(str)
    lifecycles["cancel_request_reason"] = lifecycles["cohort_id"].map(reasons)
    lifecycles["baseline_cancel_request_ts_ns"] = lifecycles["cohort_id"].map(
        baseline["cancel_request_ts_ns"]
    )
    lifecycles["baseline_cancel_ack_ts_ns"] = lifecycles["cohort_id"].map(
        baseline["cancel_ack_ts_ns"]
    )
    extras: list[pd.DataFrame] = []
    for action in selected:
        part = pd.DataFrame({"cohort_id": wide["cohort_id"], "action": action})
        for name in POLICY_EXTRA_COLUMNS[1:]:
            part[name] = wide[f"{action}__{name}"]
        extras.append(part)
    extra = pd.concat(extras, ignore_index=True)
    lifecycles = lifecycles.merge(
        extra,
        on=["cohort_id", "action"],
        how="left",
        validate="one_to_one",
    )

    activation = _numeric(lifecycles, "activation_ts_ns").to_numpy(dtype=np.int64)
    request = _numeric(lifecycles, "cancel_request_ts_ns").to_numpy(dtype=np.int64)
    ack = _numeric(lifecycles, "cancel_ack_ts_ns").to_numpy(dtype=np.int64)
    has_request = request > 0
    has_latency = has_request & (ack >= request)
    lifecycles["cancel_request_active_ms"] = np.where(
        has_request, (request - activation) / 1_000_000.0, np.nan
    ).astype(np.float32)
    lifecycles["cancel_ack_latency_ms"] = np.where(
        has_latency, (ack - request) / 1_000_000.0, np.nan
    ).astype(np.float32)
    lifecycles["fill_while_cancel_pending"] = (
        _numeric(lifecycles, "fill_while_cancel_pending_qty") > 0.0
    ).astype(np.int8)
    return lifecycles


def audit_policy_request_parity(lifecycles: pd.DataFrame) -> dict[str, Any]:
    request = _numeric(lifecycles, "cancel_request_ts_ns").to_numpy(dtype=np.int64)
    ack = _numeric(lifecycles, "cancel_ack_ts_ns").to_numpy(dtype=np.int64)
    submit = _numeric(lifecycles, "submit_ts_ns").to_numpy(dtype=np.int64)
    activation = _numeric(lifecycles, "activation_ts_ns").to_numpy(dtype=np.int64)
    baseline_request = _numeric(
        lifecycles, "baseline_cancel_request_ts_ns"
    ).to_numpy(dtype=np.int64)
    baseline_ack = _numeric(
        lifecycles, "baseline_cancel_ack_ts_ns"
    ).to_numpy(dtype=np.int64)
    reason = lifecycles["cancel_request_reason"].fillna("").astype(str).str.strip()
    requested = request > 0
    reason_counts = (
        reason.loc[requested].value_counts(dropna=False).sort_index().to_dict()
    )
    result = {
        "rows": int(len(lifecycles)),
        "request_rows": int(requested.sum()),
        "ack_rows": int((ack > 0).sum()),
        "request_without_ack": int((requested & (ack <= 0)).sum()),
        "request_timestamp_mismatch_vs_baseline": int(
            (request != baseline_request).sum()
        ),
        "ack_timestamp_mismatch_vs_baseline": int((ack != baseline_ack).sum()),
        "ack_without_request": int(((ack > 0) & (~requested)).sum()),
        "ack_before_request": int(((ack > 0) & requested & (ack < request)).sum()),
        "request_before_submit": int((requested & (request < submit)).sum()),
        "request_before_activation": int(
            (requested & (activation > 0) & (request < activation)).sum()
        ),
        "missing_request_reason": int((requested & reason.eq("").to_numpy()).sum()),
        "pending_cancel_fill_rows": int(
            _numeric(lifecycles, "fill_while_cancel_pending").astype(bool).sum()
        ),
        "pending_cancel_fill_qty": float(
            _numeric(lifecycles, "fill_while_cancel_pending_qty").sum()
        ),
        "reason_counts": {str(key): int(value) for key, value in reason_counts.items()},
        "request_before_activation_is_inflight_order_diagnostic": True,
    }
    result["passed"] = not any(
        int(result[name])
        for name in (
            "ack_without_request",
            "ack_before_request",
            "request_before_submit",
            "missing_request_reason",
            "request_timestamp_mismatch_vs_baseline",
            "ack_timestamp_mismatch_vs_baseline",
        )
    )
    return result


def fit_exposure_only_fill_hazard_contract(
    lifecycles: pd.DataFrame,
    *,
    interval_ms: int,
    maximum_support_ms: int,
    prior_intervals: float,
) -> dict[str, Any]:
    """Fit a side/role constant fill hazard under the replayed risk window."""

    if interval_ms <= 0 or maximum_support_ms < interval_ms:
        raise ValueError("invalid exposure-only fill support")
    if prior_intervals < 0.0:
        raise ValueError("exposure-only fill prior must be non-negative")
    frame = lifecycles.loc[
        lifecycles["action"].eq("current")
        & _numeric(lifecycles, "risk_valid").astype(bool)
    ].copy()
    if frame.empty:
        raise ValueError("exposure-only fill baseline has no active risk rows")
    risk_end = np.minimum(
        _numeric(frame, "risk_end_ms").to_numpy(dtype=float),
        float(maximum_support_ms),
    )
    event_time = _numeric(frame, "event_time_ms", math.nan).to_numpy(dtype=float)
    event = (
        _numeric(frame, "event_observed").to_numpy(dtype=bool)
        & np.isfinite(event_time)
        & (event_time <= float(maximum_support_ms))
    )
    exposure = np.maximum(
        1,
        np.ceil(risk_end / float(interval_ms)).astype(np.int64),
    )
    frame["_event"] = event.astype(np.int8)
    frame["_exposure_intervals"] = exposure

    pooled_by_side: dict[str, dict[str, float | int]] = {}
    cells: dict[str, dict[str, float | int]] = {}
    for side, side_rows in frame.groupby("side", observed=True):
        side_events = int(side_rows["_event"].sum())
        side_exposure = int(side_rows["_exposure_intervals"].sum())
        side_hazard = float((side_events + 0.5) / (side_exposure + 1.0))
        pooled_by_side[str(side)] = {
            "hazard_per_interval": side_hazard,
            "events": side_events,
            "exposure_intervals": side_exposure,
        }
        for role, group in side_rows.groupby("inventory_role", observed=True):
            events = int(group["_event"].sum())
            intervals = int(group["_exposure_intervals"].sum())
            hazard = (
                events + float(prior_intervals) * side_hazard
            ) / max(1.0, intervals + float(prior_intervals))
            cells[f"{side}|{str(role).lower()}"] = {
                "hazard_per_interval": float(hazard),
                "events": events,
                "exposure_intervals": intervals,
            }
    return {
        "kind": "side_role_exposure_only_fill_hazard_with_policy_clock_ack_race",
        "interval_ms": int(interval_ms),
        "maximum_support_ms": int(maximum_support_ms),
        "prior_intervals": float(prior_intervals),
        "pooled_by_side": pooled_by_side,
        "cells": cells,
    }


def exposure_only_fill_hazard_matrix(
    contract: Mapping[str, Any],
    lifecycles: pd.DataFrame,
    *,
    bins: int,
) -> np.ndarray:
    """Return a constant fill-hazard path using only side and order role."""

    if bins <= 0:
        raise ValueError("exposure-only fill baseline requires positive bins")
    output = np.empty((len(lifecycles), int(bins)), dtype=float)
    keys = (
        lifecycles["side"].astype(str)
        + "|"
        + lifecycles["inventory_role"].astype(str).str.lower()
    )
    for key, indices in keys.groupby(keys, sort=False).groups.items():
        rows = np.asarray(list(indices), dtype=int)
        cell = contract["cells"].get(str(key))
        if cell is None:
            side = str(key).split("|", 1)[0]
            cell = contract["pooled_by_side"].get(side)
        if cell is None:
            raise KeyError(f"missing exposure-only fill baseline cell={key!r}")
        output[rows, :] = float(cell["hazard_per_interval"])
    return np.clip(output, 0.0, 1.0 - 1e-7)


def fit_ack_latency_contract(
    lifecycles: pd.DataFrame,
    *,
    bin_ms: int,
    maximum_latency_ms: int,
    prior_rows: float,
) -> dict[str, Any]:
    """Fit a side/role empirical request-to-ACK distribution."""

    if bin_ms <= 0 or maximum_latency_ms < bin_ms or prior_rows < 0.0:
        raise ValueError("invalid ACK-latency contract")
    frame = lifecycles.loc[lifecycles["action"].eq("current")].copy()
    latency = _numeric(frame, "cancel_ack_latency_ms", math.nan)
    valid = np.isfinite(latency) & latency.ge(0.0)
    frame = frame.loc[valid].copy()
    frame["latency_ms"] = latency.loc[valid]
    if frame.empty:
        raise ValueError("ACK-latency fit has no valid request/ACK pairs")
    maximum_bin = int(math.ceil(maximum_latency_ms / bin_ms))

    def counts(values: pd.Series) -> np.ndarray:
        index = np.ceil(values.to_numpy(dtype=float) / float(bin_ms)).astype(int)
        index = np.clip(index, 0, maximum_bin)
        return np.bincount(index, minlength=maximum_bin + 1).astype(float)

    pooled_counts = counts(frame["latency_ms"])
    pooled_pmf = pooled_counts / pooled_counts.sum()
    cells: dict[str, Any] = {}
    for (side, role), group in frame.groupby(
        ["side", "inventory_role"], observed=True
    ):
        observed = counts(group["latency_ms"])
        smoothed = observed + float(prior_rows) * pooled_pmf
        pmf = smoothed / smoothed.sum()
        cells[f"{side}|{str(role).lower()}"] = {
            "rows": int(len(group)),
            "pmf": pmf.tolist(),
        }
    quantiles = frame["latency_ms"].quantile([0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
    return {
        "kind": "past_only_empirical_request_to_ack_latency",
        "bin_ms": int(bin_ms),
        "maximum_latency_ms": int(maximum_latency_ms),
        "tail_is_capped_in_last_bin": True,
        "prior_rows": float(prior_rows),
        "rows": int(len(frame)),
        "pooled_pmf": pooled_pmf.tolist(),
        "cells": cells,
        "quantiles_ms": {str(key): float(value) for key, value in quantiles.items()},
    }


def predict_ack_latency_cdf(
    contract: Mapping[str, Any],
    lifecycles: pd.DataFrame,
    thresholds_ms: Sequence[int],
) -> pd.DataFrame:
    """Score request-to-ACK latency CDFs using a past-only contract."""

    thresholds = sorted({int(value) for value in thresholds_ms})
    if not thresholds or thresholds[0] < 0:
        raise ValueError("ACK-latency thresholds must be non-negative")
    latency = _numeric(lifecycles, "cancel_ack_latency_ms", math.nan).to_numpy(
        dtype=float
    )
    valid = np.isfinite(latency) & (latency >= 0.0)
    frame = lifecycles.loc[valid].reset_index(drop=True)
    latency = latency[valid]
    if frame.empty:
        return pd.DataFrame()

    keys = (
        frame["side"].astype(str)
        + "|"
        + frame["inventory_role"].astype(str).str.lower()
    )
    probability = np.zeros((len(frame), len(thresholds)), dtype=float)
    pooled = np.asarray(contract["pooled_pmf"], dtype=float)
    baseline = np.column_stack(
        [
            _cdf_at(
                pooled,
                np.full(len(frame), float(threshold)),
                int(contract["bin_ms"]),
            )
            for threshold in thresholds
        ]
    )
    for key, indices in keys.groupby(keys, sort=False).groups.items():
        rows = np.asarray(list(indices), dtype=int)
        cell = contract["cells"].get(str(key))
        pmf = np.asarray(
            cell["pmf"] if cell is not None else contract["pooled_pmf"],
            dtype=float,
        )
        probability[rows] = np.column_stack(
            [
                _cdf_at(
                    pmf,
                    np.full(len(rows), float(threshold)),
                    int(contract["bin_ms"]),
                )
                for threshold in thresholds
            ]
        )

    identity = frame.loc[
        :,
        [
            "action_lifecycle_id",
            "cohort_id",
            "day",
            "side",
            "inventory_role",
            "action",
            "cancel_request_reason",
        ],
    ].copy()
    outputs: list[pd.DataFrame] = []
    for index, threshold in enumerate(thresholds):
        part = identity.copy()
        part["latency_threshold_ms"] = int(threshold)
        part["observed_ack_latency_ms"] = latency.astype(np.float32)
        part["ack_latency_target"] = (latency <= float(threshold)).astype(np.int8)
        part["ack_latency_probability"] = probability[:, index].astype(np.float32)
        part["baseline_ack_latency_probability"] = baseline[:, index].astype(
            np.float32
        )
        outputs.append(part)
    return pd.concat(outputs, ignore_index=True)


def _cdf_at(pmf: np.ndarray, delay_ms: np.ndarray, bin_ms: int) -> np.ndarray:
    cdf = np.cumsum(np.asarray(pmf, dtype=float))
    index = np.floor(np.maximum(delay_ms, -1.0) / float(bin_ms)).astype(int)
    output = np.zeros_like(delay_ms, dtype=float)
    valid = index >= 0
    output[valid] = cdf[np.minimum(index[valid], len(cdf) - 1)]
    return output


def ack_conditional_hazard(
    contract: Mapping[str, Any],
    lifecycles: pd.DataFrame,
    interval_start_ms: np.ndarray,
    interval_end_ms: np.ndarray,
) -> np.ndarray:
    """Return ACK probability within arbitrary active-time intervals.

    The probability is conditional on ACK not having occurred at the interval
    start.  Request time is supplied by the replayed policy path, so intervals
    ending before that request have exactly zero ACK risk.
    """

    starts, ends = np.broadcast_arrays(
        np.asarray(interval_start_ms, dtype=float),
        np.asarray(interval_end_ms, dtype=float),
    )
    if starts.ndim == 0 or starts.shape[0] != len(lifecycles):
        raise ValueError("ACK intervals must have one leading row per lifecycle")
    if np.any(ends < starts):
        raise ValueError("ACK interval end precedes its start")

    request_ms = _numeric(
        lifecycles, "cancel_request_active_ms", math.nan
    ).to_numpy(dtype=float)
    output = np.zeros(starts.shape, dtype=float)
    keys = (
        lifecycles["side"].astype(str)
        + "|"
        + lifecycles["inventory_role"].astype(str).str.lower()
    )
    for key, indices in keys.groupby(keys, sort=False).groups.items():
        rows = np.asarray(list(indices), dtype=int)
        cell = contract["cells"].get(str(key))
        pmf = np.asarray(
            cell["pmf"] if cell is not None else contract["pooled_pmf"],
            dtype=float,
        )
        request_shape = (len(rows),) + (1,) * (starts.ndim - 1)
        row_request = request_ms[rows].reshape(request_shape)
        delay_start = starts[rows] - row_request
        delay_end = ends[rows] - row_request
        finite = np.isfinite(row_request)
        before = _cdf_at(pmf, delay_start, int(contract["bin_ms"]))
        after = _cdf_at(pmf, delay_end, int(contract["bin_ms"]))
        hazard = np.divide(
            np.maximum(0.0, after - before),
            np.maximum(1e-9, 1.0 - before),
        )
        hazard[(delay_end <= 0.0) | (~finite)] = 0.0
        output[rows] = np.clip(hazard, 0.0, 1.0 - 1e-7)
    return output


def ack_interval_hazard_matrix(
    contract: Mapping[str, Any],
    lifecycles: pd.DataFrame,
    *,
    interval_ms: int,
    bins: int,
) -> np.ndarray:
    """Return conditional ACK probabilities, exactly zero before request."""

    starts = np.arange(bins, dtype=float) * float(interval_ms)
    ends = starts + float(interval_ms)
    return ack_conditional_hazard(
        contract,
        lifecycles,
        np.broadcast_to(starts, (len(lifecycles), bins)),
        np.broadcast_to(ends, (len(lifecycles), bins)),
    )


def combine_fill_ack_hazards(
    fill_hazard: np.ndarray, ack_hazard: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """Combine interval event probabilities through cause-specific rates."""

    fill = np.clip(np.asarray(fill_hazard, dtype=float), 0.0, 1.0 - 1e-7)
    ack = np.clip(np.asarray(ack_hazard, dtype=float), 0.0, 1.0 - 1e-7)
    fill_rate = -np.log1p(-fill)
    ack_rate = -np.log1p(-ack)
    total_rate = fill_rate + ack_rate
    event = -np.expm1(-total_rate)
    fill_share = np.divide(
        fill_rate, total_rate, out=np.zeros_like(fill_rate), where=total_rate > 0.0
    )
    return event * fill_share, event * (1.0 - fill_share)


def fill_survival_at_times(
    fill_hazard: np.ndarray,
    times_ms: np.ndarray,
    *,
    interval_ms: int,
) -> np.ndarray:
    """Evaluate a piecewise-constant fill survival curve at arbitrary times."""

    hazard = np.clip(np.asarray(fill_hazard, dtype=float), 0.0, 1.0 - 1e-7)
    times = np.asarray(times_ms, dtype=float)
    if hazard.ndim != 2 or times.ndim == 0 or times.shape[0] != hazard.shape[0]:
        raise ValueError("fill survival inputs do not share a lifecycle dimension")
    if interval_ms <= 0:
        raise ValueError("fill survival interval must be positive")
    trailing_shape = times.shape[1:]
    flat_times = np.maximum(0.0, times.reshape(hazard.shape[0], -1))
    bins = hazard.shape[1]
    full_bins = np.minimum(
        bins,
        np.floor(flat_times / float(interval_ms)).astype(np.int32),
    )
    remainder = flat_times - full_bins * int(interval_ms)
    survival_after = np.cumprod(1.0 - hazard, axis=1)
    output = np.ones_like(flat_times, dtype=float)
    has_full = full_bins > 0
    rows, columns = np.nonzero(has_full)
    output[rows, columns] = survival_after[
        rows, full_bins[rows, columns] - 1
    ]
    partial = (remainder > 1e-9) & (full_bins < bins)
    rows, columns = np.nonzero(partial)
    if rows.size:
        output[rows, columns] *= np.power(
            1.0 - hazard[rows, full_bins[rows, columns]],
            remainder[rows, columns] / float(interval_ms),
        )
    return output.reshape((hazard.shape[0],) + trailing_shape)


def derive_policy_clock_cif(
    fill_hazard: np.ndarray,
    lifecycles: pd.DataFrame,
    active_horizons_ms: np.ndarray,
    *,
    ack_latency_contract: Mapping[str, Any],
    interval_ms: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Derive fill and ACK CIFs from latent fill and policy-ACK times.

    Fill follows the learned 100ms survival curve.  Cancel request time comes
    from causal baseline replay, and ACK time is request time plus the
    side/role empirical latency distribution.  This keeps the millisecond ACK
    race explicit instead of smearing it over a 100ms market-state interval.
    """

    horizons = np.maximum(0.0, np.asarray(active_horizons_ms, dtype=float))
    if horizons.ndim != 2 or horizons.shape[0] != len(lifecycles):
        raise ValueError("active horizons must be lifecycle-by-horizon")
    survival_at_horizon = fill_survival_at_times(
        fill_hazard,
        horizons,
        interval_ms=interval_ms,
    )
    fill = 1.0 - survival_at_horizon
    ack = np.zeros_like(fill)
    no_event = survival_at_horizon.copy()
    request_ms = _numeric(
        lifecycles, "cancel_request_active_ms", math.nan
    ).to_numpy(dtype=float)
    keys = (
        lifecycles["side"].astype(str)
        + "|"
        + lifecycles["inventory_role"].astype(str).str.lower()
    )
    for key, indices in keys.groupby(keys, sort=False).groups.items():
        rows = np.asarray(list(indices), dtype=int)
        requested = np.isfinite(request_ms[rows])
        rows = rows[requested]
        if not rows.size:
            continue
        cell = ack_latency_contract["cells"].get(str(key))
        pmf = np.asarray(
            cell["pmf"] if cell is not None else ack_latency_contract["pooled_pmf"],
            dtype=float,
        )
        support = np.flatnonzero(pmf > 0.0)
        pmf = pmf[support]
        delays = support.astype(float) * int(
            ack_latency_contract["bin_ms"]
        )
        ack_times = np.maximum(0.0, request_ms[rows, None] + delays[None, :])
        for horizon_index in range(horizons.shape[1]):
            horizon = horizons[rows, horizon_index]
            ack_before_horizon = ack_times <= horizon[:, None]
            survival_at_ack = fill_survival_at_times(
                fill_hazard[rows],
                np.minimum(ack_times, horizon[:, None]),
                interval_ms=interval_ms,
            )
            ack_probability = np.sum(
                pmf[None, :] * survival_at_ack * ack_before_horizon,
                axis=1,
            )
            ack_tail = np.sum(
                pmf[None, :] * (~ack_before_horizon),
                axis=1,
            )
            remaining = ack_tail * survival_at_horizon[rows, horizon_index]
            ack[rows, horizon_index] = ack_probability
            no_event[rows, horizon_index] = remaining
            fill[rows, horizon_index] = 1.0 - ack_probability - remaining
    fill = np.clip(fill, 0.0, 1.0)
    ack = np.clip(ack, 0.0, 1.0)
    return fill, ack


def predict_policy_clock_exposure_baseline_at_horizons(
    lifecycles: pd.DataFrame,
    horizons_ms: Sequence[int],
    *,
    activation_contract: Mapping[str, Any],
    fill_hazard_contract: Mapping[str, Any],
    ack_latency_contract: Mapping[str, Any],
    interval_ms: int,
    maximum_support_ms: int,
    chunk_size: int,
) -> pd.DataFrame:
    """Predict a mechanism-matched baseline with no market-state features."""

    horizons = sorted({int(value) for value in horizons_ms})
    if not horizons or horizons[0] <= 0 or horizons[-1] > maximum_support_ms:
        raise ValueError("invalid policy-clock baseline horizons")
    outputs: list[pd.DataFrame] = []
    for start in range(0, len(lifecycles), int(chunk_size)):
        chunk = lifecycles.iloc[start : start + int(chunk_size)].reset_index(drop=True)
        activation_probability, activation_latency = _activation_values(
            chunk, activation_contract, latency_key="latency_p50_ms"
        )
        active_horizons = np.column_stack(
            [
                np.maximum(0.0, float(horizon) - activation_latency)
                for horizon in horizons
            ]
        )
        bins = int(
            math.ceil(float(active_horizons.max(initial=0.0)) / int(interval_ms))
        )
        if bins <= 0:
            fill_conditional = np.zeros((len(chunk), len(horizons)), dtype=float)
            ack_conditional = np.zeros_like(fill_conditional)
        else:
            fill_hazard = exposure_only_fill_hazard_matrix(
                fill_hazard_contract,
                chunk,
                bins=bins,
            )
            fill_conditional, ack_conditional = derive_policy_clock_cif(
                fill_hazard,
                chunk,
                active_horizons,
                ack_latency_contract=ack_latency_contract,
                interval_ms=int(interval_ms),
            )
        fill_probability = activation_probability[:, None] * fill_conditional
        ack_probability = activation_probability[:, None] * ack_conditional
        no_event_probability = np.clip(
            1.0 - fill_probability - ack_probability, 0.0, 1.0
        )
        base = chunk.loc[:, IDENTITY_COLUMNS].copy()
        for index, horizon in enumerate(horizons):
            part = base.copy()
            part["horizon_ms"] = int(horizon)
            part["fill_probability"] = fill_probability[:, index].astype(np.float32)
            part["cancel_ack_probability"] = ack_probability[:, index].astype(
                np.float32
            )
            part["no_event_probability"] = no_event_probability[:, index].astype(
                np.float32
            )
            outputs.append(part)
    return pd.concat(outputs, ignore_index=True)


def predict_policy_clock_cif_at_horizons(
    model: Any,
    lifecycles: pd.DataFrame,
    horizons_ms: Sequence[int],
    *,
    activation_contract: Mapping[str, Any],
    fill_hazard_offset: float,
    ack_latency_contract: Mapping[str, Any],
    interval_ms: int,
    maximum_support_ms: int,
    chunk_size: int,
) -> pd.DataFrame:
    horizons = sorted({int(value) for value in horizons_ms})
    if not horizons or horizons[0] <= 0 or horizons[-1] > maximum_support_ms:
        raise ValueError("invalid policy-clock CIF horizons")
    outputs: list[pd.DataFrame] = []
    for start in range(0, len(lifecycles), int(chunk_size)):
        chunk = lifecycles.iloc[start : start + int(chunk_size)].reset_index(drop=True)
        activation_probability, activation_latency = _activation_values(
            chunk, activation_contract, latency_key="latency_p50_ms"
        )
        maximum_active_ms = np.maximum(0.0, float(horizons[-1]) - activation_latency)
        bins = int(math.ceil(float(maximum_active_ms.max()) / int(interval_ms)))
        if bins <= 0:
            fill_conditional = np.zeros((len(chunk), len(horizons)), dtype=float)
            ack_conditional = np.zeros_like(fill_conditional)
        else:
            repeated = chunk.loc[chunk.index.repeat(bins), STATIC_MODEL_FEATURES]
            elapsed = np.tile(
                np.arange(1, bins + 1, dtype=np.int32) * int(interval_ms),
                len(chunk),
            )
            dynamic = _dynamic_features(repeated.reset_index(drop=True), elapsed)
            raw_fill = np.clip(
                model.predict_proba(dynamic)[:, 1], 1e-7, 1.0 - 1e-7
            )
            score = np.log(raw_fill / (1.0 - raw_fill)) + float(fill_hazard_offset)
            fill_hazard = (1.0 / (1.0 + np.exp(-score))).reshape(len(chunk), bins)
            active_horizons = np.column_stack(
                [
                    np.maximum(0.0, float(horizon) - activation_latency)
                    for horizon in horizons
                ]
            )
            fill_conditional, ack_conditional = derive_policy_clock_cif(
                fill_hazard,
                chunk,
                active_horizons,
                ack_latency_contract=ack_latency_contract,
                interval_ms=int(interval_ms),
            )
        fill_probability = activation_probability[:, None] * fill_conditional
        ack_probability = activation_probability[:, None] * ack_conditional
        no_event_probability = np.clip(
            1.0 - fill_probability - ack_probability, 0.0, 1.0
        )
        base = chunk.loc[:, IDENTITY_COLUMNS].copy()
        for index, horizon in enumerate(horizons):
            part = base.copy()
            part["horizon_ms"] = int(horizon)
            part["activation_probability"] = activation_probability.astype(np.float32)
            part["fill_probability"] = fill_probability[:, index].astype(np.float32)
            part["cancel_ack_probability"] = ack_probability[:, index].astype(np.float32)
            part["no_event_probability"] = no_event_probability[:, index].astype(
                np.float32
            )
            outputs.append(part)
    return pd.concat(outputs, ignore_index=True)


def _load_spec(path: Path) -> dict[str, Any]:
    spec = load_placement_fill_spec(path)
    if spec.get("schema_version") != "narrowgate_placement_fill_policy_clock_race_fit_spec.v1":
        raise RuntimeError("unsupported policy-clock race fit spec")
    if spec.get("research_status") != "frozen_before_policy_clock_development_fit":
        raise RuntimeError("policy-clock race fit spec is not frozen")
    for name in ("implementation", "evaluator"):
        source = ROOT / str(spec["lineage"][name])
        if _sha256(source) != str(spec["lineage"][f"{name}_sha256"]):
            raise RuntimeError(f"policy-clock race {name} identity changed")
    return spec


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke-days", type=int, default=0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    args.spec = args.spec.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    spec = _load_spec(args.spec)
    wide = _load_policy_partitions(spec)
    if args.smoke_days:
        selected = sorted(wide["day"].astype(str).unique())[: int(args.smoke_days)]
        wide = wide.loc[wide["day"].isin(selected)].copy()
    actions = tuple(spec["placement_estimand"]["model_actions"])
    lifecycles = expand_policy_clock_lifecycles(wide, actions=actions)
    parity = audit_policy_request_parity(lifecycles)
    scheduled_reasons = set(
        str(value)
        for value in spec["policy_request_reason_contract"]["scheduled_classes"]
    )
    scheduled_rows = sum(
        int(count)
        for reason, count in parity["reason_counts"].items()
        if reason in scheduled_reasons
    )
    parity["scheduled_request_rows"] = int(scheduled_rows)
    parity["path_trigger_request_rows"] = int(
        parity["request_rows"] - scheduled_rows
    )
    parity["scheduled_request_fraction"] = float(
        scheduled_rows / max(1, int(parity["request_rows"]))
    )
    if not parity["passed"]:
        raise RuntimeError(f"policy request parity failed: {parity}")

    fit_contract = spec["development_fit"]
    duration = derive_duration_contract(
        lifecycles,
        interval_ms=int(fit_contract["risk_interval_ms"]),
        report_quantiles=spec["reporting"]["development_exposure_quantiles"],
        maximum_support_quantile=float(spec["reporting"]["maximum_support_quantile"]),
    )
    frozen_horizons = {
        str(key): int(value)
        for key, value in spec["reporting"]["frozen_empirical_horizons_ms"].items()
    }
    if not args.smoke_days and frozen_horizons != duration["report_quantiles"]:
        raise RuntimeError("Development exposure quantiles changed after family freeze")
    if not args.smoke_days and int(duration["maximum_support_ms"]) != int(
        spec["reporting"]["frozen_maximum_support_ms"]
    ):
        raise RuntimeError("Development maximum support changed after family freeze")
    horizons = sorted(
        set(frozen_horizons.values())
        | set(spec["reporting"]["legacy_diagnostic_horizons_ms"])
    )
    maximum_support_ms = int(duration["maximum_support_ms"])
    days = sorted(lifecycles["day"].astype(str).unique())
    minimum_train_days = int(fit_contract["minimum_train_days"])
    if args.smoke_days:
        minimum_train_days = max(2, min(len(days) - 2, minimum_train_days))
    folds = make_expanding_folds(
        days,
        min_train_days=minimum_train_days,
        embargo_days=int(fit_contract["embargo_days"]),
        test_days=int(fit_contract["outer_test_days"]),
    )
    if not args.smoke_days:
        scheduled_oof_days = sorted(
            {day for fold in folds for day in fold["test_days"]}
        )
        required_oof_days = int(
            spec["reporting"]["curve_level_gate"]["required_oof_days"]
        )
        if len(scheduled_oof_days) != required_oof_days:
            raise RuntimeError(
                "frozen OOF-day requirement does not match chronological split"
            )

    oof_parts: list[pd.DataFrame] = []
    latency_oof_parts: list[pd.DataFrame] = []
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
            if isinstance(offset, Mapping):
                raise TypeError("policy-clock fill fit must not contain a cancel head")
            activation = fit_activation_contract(train)
            latency = fit_ack_latency_contract(
                train,
                bin_ms=int(fit_contract["ack_latency_bin_ms"]),
                maximum_latency_ms=int(fit_contract["ack_latency_maximum_ms"]),
                prior_rows=float(fit_contract["ack_latency_prior_rows"]),
            )
            baseline_fill = fit_exposure_only_fill_hazard_contract(
                train,
                interval_ms=int(fit_contract["risk_interval_ms"]),
                maximum_support_ms=maximum_support_ms,
                prior_intervals=float(
                    fit_contract["exposure_baseline_prior_intervals"]
                ),
            )
            latency_oof = predict_ack_latency_cdf(
                latency,
                test,
                spec["reporting"]["ack_latency_evaluation_ms"],
            )
            if not latency_oof.empty:
                latency_oof["fold"] = int(fold["fold"])
                latency_oof_parts.append(latency_oof)
            prediction = predict_policy_clock_cif_at_horizons(
                model,
                test,
                horizons,
                activation_contract=activation,
                fill_hazard_offset=float(offset),
                ack_latency_contract=latency,
                interval_ms=int(fit_contract["risk_interval_ms"]),
                maximum_support_ms=maximum_support_ms,
                chunk_size=int(fit_contract["prediction_chunk_size"]),
            )
            labels = competing_labels_at_horizons(test, horizons)
            scored = prediction.merge(
                labels,
                on=list(IDENTITY_COLUMNS) + ["horizon_ms"],
                how="inner",
                validate="one_to_one",
            )
            baseline = predict_policy_clock_exposure_baseline_at_horizons(
                test,
                horizons,
                activation_contract=activation,
                fill_hazard_contract=baseline_fill,
                ack_latency_contract=latency,
                interval_ms=int(fit_contract["risk_interval_ms"]),
                maximum_support_ms=maximum_support_ms,
                chunk_size=int(fit_contract["prediction_chunk_size"]),
            ).rename(
                columns={
                    "fill_probability": "baseline_fill_probability",
                    "cancel_ack_probability": "baseline_cancel_ack_probability",
                    "no_event_probability": "baseline_no_event_probability",
                }
            )
            scored = scored.merge(
                baseline,
                on=list(IDENTITY_COLUMNS) + ["horizon_ms"],
                how="inner",
                validate="one_to_one",
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
                    "ack_latency_rows": int(latency["rows"]),
                    "ack_latency_quantiles_ms": latency["quantiles_ms"],
                    "exposure_only_fill_baseline": baseline_fill,
                    **fit_identity,
                }
            )
            del train, test, model, prediction, labels, baseline, scored
            gc.collect()
    if not oof_parts:
        raise RuntimeError("policy-clock race produced no OOF rows")
    oof = pd.concat(oof_parts, ignore_index=True)
    if not latency_oof_parts:
        raise RuntimeError("policy-clock race produced no OOF ACK-latency rows")
    latency_oof = pd.concat(latency_oof_parts, ignore_index=True)

    final_models: dict[str, Any] = {}
    final_fit: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        side_rows = lifecycles.loc[lifecycles["side"].eq(side)]
        model, offset, identity = _fit_side(
            side_rows, spec=spec, maximum_support_ms=maximum_support_ms
        )
        final_models[side] = {"model": model, "fill_hazard_offset": float(offset)}
        final_fit[side] = identity
    activation = fit_activation_contract(lifecycles)
    ack_latency = fit_ack_latency_contract(
        lifecycles,
        bin_ms=int(fit_contract["ack_latency_bin_ms"]),
        maximum_latency_ms=int(fit_contract["ack_latency_maximum_ms"]),
        prior_rows=float(fit_contract["ack_latency_prior_rows"]),
    )
    exposure_baseline = fit_exposure_only_fill_hazard_contract(
        lifecycles,
        interval_ms=int(fit_contract["risk_interval_ms"]),
        maximum_support_ms=maximum_support_ms,
        prior_intervals=float(fit_contract["exposure_baseline_prior_intervals"]),
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    code = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        args.output_dir / "code_checkpoint", repo_root=ROOT, code_identity=code
    )
    oof_path = args.output_dir / "oof_policy_clock_predictions.parquet"
    latency_oof_path = args.output_dir / "oof_ack_latency_predictions.parquet"
    artifact_path = args.output_dir / "policy_clock_fill_cif.joblib"
    oof.to_parquet(oof_path, index=False, compression="zstd")
    latency_oof.to_parquet(latency_oof_path, index=False, compression="zstd")
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["family_id"]),
        "model_kind": MODEL_KIND,
        "models": final_models,
        "activation_contract": activation,
        "ack_latency_contract": ack_latency,
        "exposure_only_fill_baseline": exposure_baseline,
        "duration_contract": duration,
        "risk_interval_ms": int(fit_contract["risk_interval_ms"]),
        "model_actions": list(actions),
        "stationary_cancel_hazard": False,
        "pending_cancel_fill_enabled": True,
        "requires_replayed_policy_request_path": True,
        "online_cancel_request_forecast": False,
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
        "policy_request_parity": parity,
        "ack_latency_contract": ack_latency,
        "exposure_only_fill_baseline": exposure_baseline,
        "duration_contract": duration,
        "report_horizons_ms": horizons,
        "ack_latency_evaluation_ms": [
            int(value) for value in spec["reporting"]["ack_latency_evaluation_ms"]
        ],
        "folds": fold_identity,
        "final_fit": final_fit,
        "stationary_cancel_hazard": False,
        "pending_cancel_fill_enabled": True,
        "requires_replayed_policy_request_path": True,
        "online_cancel_request_forecast": False,
        "curve_level_status": "not_evaluated",
        "validation_access_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authorization": False,
        "spec": {"path": str(args.spec), "sha256": _sha256(args.spec)},
        "spec_sha256": _sha256(args.spec),
        "git": code,
        "checkpoint": checkpoint,
        "outputs": {
            "oof_predictions": {"path": str(oof_path), "sha256": _sha256(oof_path)},
            "oof_ack_latency_predictions": {
                "path": str(latency_oof_path),
                "sha256": _sha256(latency_oof_path),
            },
            "artifact": {"path": str(artifact_path), "sha256": _sha256(artifact_path)},
        },
    }
    report_path = args.output_dir / "report.json"
    _atomic_json(report, report_path)
    print(
        json.dumps(
            {
                "development_days": len(days),
                "oof_rows": len(oof),
                "policy_request_parity": parity["passed"],
                "validation_read": False,
                "action_or_live_authorization": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
