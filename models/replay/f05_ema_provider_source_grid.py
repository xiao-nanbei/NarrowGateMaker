"""Provider-normalized EMA states on the source's native 100ms data grid.

The 100ms interval is the admitted provider artifact's data resolution.  This
module does not introduce an economic horizon or action cadence, and it does
not subsample the source onto the causal-v12 10-second materialization grid.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
from scipy.signal import lfilter

from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    EMA_HALF_LIVES_S,
    model_feature_names,
)

SOURCE_RESOLUTION_MS = 100


class F05ProviderSourceGridError(RuntimeError):
    """Raised when provider BBO cannot support source-grid EMA pretraining."""


def provider_encoder_feature_names() -> tuple[str, ...]:
    """Return EMA fields whose meaning does not require a separate time basis."""

    return tuple(
        name
        for name in model_feature_names()
        if not name.endswith("_volatility_normalized")
    )


def _day_start_ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1_000)


def _half_life_label(value: float) -> str:
    return f"h{float(value):g}s".replace(".", "p")


def _effective_sign_and_cross(sign: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    raw = np.asarray(sign, dtype=np.int8)
    indexes = np.arange(len(raw), dtype=np.int64)
    prior_nonzero = np.maximum.accumulate(np.where(raw != 0, indexes, -1))
    effective = np.zeros_like(raw)
    valid = prior_nonzero >= 0
    effective[valid] = raw[prior_nonzero[valid]]
    changed = valid & np.r_[True, effective[1:] != effective[:-1]]
    last_cross = np.maximum.accumulate(np.where(changed, indexes, -1))
    return effective, last_cross


def _irregular_ema(
    timestamps_ms: np.ndarray,
    values: np.ndarray,
    *,
    half_life_s: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Apply the live recurrence exactly on the admitted source clock."""

    ts = np.asarray(timestamps_ms, dtype=np.int64)
    x = np.asarray(values, dtype=np.float64)
    if len(ts) != len(x) or len(ts) == 0 or np.any(ts[1:] <= ts[:-1]):
        raise F05ProviderSourceGridError("irregular EMA input clock is invalid")
    ema = np.empty_like(x)
    velocity = np.zeros_like(x)
    ema[0] = x[0]
    boundaries = np.flatnonzero(np.diff(ts) != SOURCE_RESOLUTION_MS) + 1
    starts = np.r_[0, boundaries]
    ends = np.r_[boundaries, len(ts)]
    regular_decay = math.exp(
        -math.log(2.0)
        * (SOURCE_RESOLUTION_MS / 1_000.0)
        / float(half_life_s)
    )
    regular_alpha = 1.0 - regular_decay
    for start, end in zip(starts, ends, strict=True):
        cursor = int(start)
        if cursor > 0:
            delta_s = float(ts[cursor] - ts[cursor - 1]) / 1_000.0
            decay = math.exp(-math.log(2.0) * delta_s / float(half_life_s))
            ema[cursor] = decay * ema[cursor - 1] + (1.0 - decay) * x[cursor]
            velocity[cursor] = (ema[cursor] - ema[cursor - 1]) / delta_s
            cursor += 1
        if cursor >= end:
            continue
        initial = ema[cursor - 1] if cursor > 0 else x[0]
        segment, _ = lfilter(
            np.array([regular_alpha]),
            np.array([1.0, -regular_decay]),
            x[cursor:end],
            zi=np.array([regular_decay * initial]),
        )
        ema[cursor:end] = segment
        prior = np.r_[initial, segment[:-1]]
        velocity[cursor:end] = (
            segment - prior
        ) / (SOURCE_RESOLUTION_MS / 1_000.0)
    return ema, velocity


def _normalized_source(
    prior_bbo: pd.DataFrame,
    target_bbo: pd.DataFrame,
    *,
    day: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, Any]]:
    required = {"timestamp", "best_bid", "best_ask"}
    for role, frame in (("prior", prior_bbo), ("target", target_bbo)):
        if not required.issubset(frame) or frame.empty:
            raise F05ProviderSourceGridError(f"provider {role} BBO is incomplete")
    target_ts = target_bbo["timestamp"].to_numpy(dtype=np.int64, copy=False)
    if np.any(target_ts[1:] <= target_ts[:-1]):
        raise F05ProviderSourceGridError("provider target BBO clock is not increasing")
    day_start = _day_start_ms(day)
    day_end = day_start + 86_400_000
    if target_ts[0] < day_start or target_ts[-1] >= day_end:
        raise F05ProviderSourceGridError("provider target BBO escaped its UTC day")
    if np.any(target_ts % SOURCE_RESOLUTION_MS != 0) or np.any(
        np.diff(target_ts) % SOURCE_RESOLUTION_MS != 0
    ):
        raise F05ProviderSourceGridError("provider BBO left its admitted 100ms grid")

    source = pd.concat(
        (
            prior_bbo[["timestamp", "best_bid", "best_ask"]],
            target_bbo[["timestamp", "best_bid", "best_ask"]],
        ),
        ignore_index=True,
    ).sort_values("timestamp", kind="stable")
    duplicate = source["timestamp"].duplicated(keep=False)
    if duplicate.any():
        grouped = source.loc[duplicate].groupby("timestamp", sort=False)
        if any(
            len(group[["best_bid", "best_ask"]].drop_duplicates()) != 1
            for _, group in grouped
        ):
            raise F05ProviderSourceGridError("provider duplicate changed BBO content")
        source = source.drop_duplicates("timestamp", keep="last")
    source_ts = source["timestamp"].to_numpy(dtype=np.int64, copy=False)
    if np.any(source_ts[1:] <= source_ts[:-1]):
        raise F05ProviderSourceGridError("provider combined BBO clock is invalid")
    bid = source["best_bid"].to_numpy(dtype=np.float64, copy=False)
    ask = source["best_ask"].to_numpy(dtype=np.float64, copy=False)
    if np.any(bid <= 0.0) or np.any(ask <= bid):
        raise F05ProviderSourceGridError("provider BBO contains an invalid market")
    sample_index = np.searchsorted(source_ts, target_ts)
    if not np.array_equal(source_ts[sample_index], target_ts):
        raise F05ProviderSourceGridError("provider target rows lost source identity")
    delta = np.diff(target_ts)
    audit = {
        "day": day,
        "source_resolution_ms": SOURCE_RESOLUTION_MS,
        "sampling_stride": "none_all_admitted_source_rows",
        "target_rows": int(len(target_ts)),
        "target_first_ts_ms": int(target_ts[0]),
        "target_last_ts_ms": int(target_ts[-1]),
        "target_delta_min_ms": int(delta.min()) if len(delta) else None,
        "target_delta_median_ms": float(np.median(delta)) if len(delta) else None,
        "target_delta_max_ms": int(delta.max()) if len(delta) else None,
    }
    return source_ts, 0.5 * (bid + ask), sample_index, audit


def provider_ema_source_grid_batches(
    prior_bbo: pd.DataFrame,
    target_bbo: pd.DataFrame,
    *,
    day: str,
) -> Iterator[tuple[str, np.ndarray, dict[str, Any]]]:
    """Yield BUY then SELL EMA matrices without an invented sample clock."""

    source_ts, mid, sample_index, audit = _normalized_source(
        prior_bbo, target_bbo, day=day
    )
    sample_mid = mid[sample_index]
    sampled_ema: list[np.ndarray] = []
    sampled_velocity: list[np.ndarray] = []
    adjacent_z: list[np.ndarray] = []
    adjacent_cross_age: list[np.ndarray] = []
    adjacent_cross_missing: list[np.ndarray] = []
    adjacent_signs: list[np.ndarray] = []
    previous_full: np.ndarray | None = None
    for half_life_s in EMA_HALF_LIVES_S:
        full, full_velocity = _irregular_ema(
            source_ts, mid, half_life_s=half_life_s
        )
        sampled_ema.append(full[sample_index])
        sampled_velocity.append(full_velocity[sample_index])
        if previous_full is not None:
            z = 10_000.0 * (previous_full - full) / mid
            effective_sign, last_cross = _effective_sign_and_cross(np.sign(z))
            sampled_cross = last_cross[sample_index]
            missing = sampled_cross < 0
            age_s = np.where(
                missing,
                0.0,
                (
                    source_ts[sample_index]
                    - source_ts[np.maximum(sampled_cross, 0)]
                )
                / 1_000.0,
            )
            adjacent_z.append(z[sample_index])
            adjacent_cross_age.append(np.log1p(age_s))
            adjacent_cross_missing.append(missing.astype(np.float64))
            adjacent_signs.append(effective_sign)
        previous_full = full

    sign_matrix = np.column_stack(adjacent_signs)
    all_nonzero = np.all(sign_matrix != 0, axis=1)
    signature_changed = all_nonzero & np.r_[
        True, np.any(sign_matrix[1:] != sign_matrix[:-1], axis=1)
    ]
    indexes = np.arange(len(mid), dtype=np.int64)
    ordering_change = np.maximum.accumulate(
        np.where(signature_changed, indexes, -1)
    )[sample_index]
    ordering_missing = ordering_change < 0
    ordering_age = np.where(
        ordering_missing,
        0.0,
        (
            source_ts[sample_index]
            - source_ts[np.maximum(ordering_change, 0)]
        )
        / 1_000.0,
    )
    names = provider_encoder_feature_names()
    for side, side_sign in (("BUY", 1.0), ("SELL", -1.0)):
        values: dict[str, np.ndarray] = {}
        for half_life_s, ema, velocity in zip(
            EMA_HALF_LIVES_S, sampled_ema, sampled_velocity, strict=True
        ):
            label = _half_life_label(half_life_s)
            values[f"ema_rel_mid_bps_{label}"] = (
                side_sign * 10_000.0 * (ema - sample_mid) / sample_mid
            )
            values[f"ema_velocity_bps_per_s_{label}"] = (
                side_sign * 10_000.0 * velocity / sample_mid
            )
        signed_adjacent = [side_sign * value for value in adjacent_z]
        for index, (fast, slow) in enumerate(
            zip(EMA_HALF_LIVES_S[:-1], EMA_HALF_LIVES_S[1:], strict=True)
        ):
            prefix = f"ema_adjacent_{_half_life_label(fast)}_{_half_life_label(slow)}"
            values[f"{prefix}_favorable_bps"] = signed_adjacent[index]
            values[f"{prefix}_cross_age_log1p_s"] = adjacent_cross_age[index]
            values[f"{prefix}_cross_age_missing"] = adjacent_cross_missing[index]
        for index in range(len(signed_adjacent) - 1):
            left = _half_life_label(EMA_HALF_LIVES_S[index])
            center = _half_life_label(EMA_HALF_LIVES_S[index + 1])
            right = _half_life_label(EMA_HALF_LIVES_S[index + 2])
            values[f"ema_curvature_{left}_{center}_{right}_favorable_bps"] = (
                signed_adjacent[index] - signed_adjacent[index + 1]
            )
        adjacent_matrix = np.column_stack(signed_adjacent)
        values["ema_adjacent_favorable_positive_fraction"] = np.mean(
            adjacent_matrix > 0.0, axis=1
        )
        values["ema_adjacent_favorable_mean_bps"] = np.mean(
            adjacent_matrix, axis=1
        )
        values["ema_ordering_persistence_log1p_s"] = np.log1p(ordering_age)
        values["ema_ordering_persistence_missing"] = ordering_missing.astype(
            np.float64
        )
        matrix = np.column_stack([values[name] for name in names])
        if not np.isfinite(matrix).all():
            raise F05ProviderSourceGridError("provider EMA matrix is nonfinite")
        yield side, matrix, dict(audit)


__all__ = [
    "F05ProviderSourceGridError",
    "SOURCE_RESOLUTION_MS",
    "provider_ema_source_grid_batches",
    "provider_encoder_feature_names",
]
