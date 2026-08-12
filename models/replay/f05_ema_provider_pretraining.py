"""Vectorized 2025 provider EMA-state extraction for F05 pretraining."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from scipy.signal import lfilter

from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    EMA_HALF_LIVES_S,
    model_feature_names,
)


class F05ProviderPretrainingError(RuntimeError):
    """Raised when provider BBO cannot support the frozen EMA encoder."""


GRID_MS = 100
SAMPLE_MS = 10_000


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


def _canonical_mid_grid(
    prior_bbo: pd.DataFrame,
    target_bbo: pd.DataFrame,
    *,
    day: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    required = {"timestamp", "best_bid", "best_ask"}
    if not required.issubset(prior_bbo) or not required.issubset(target_bbo):
        raise F05ProviderPretrainingError("provider BBO schema is incomplete")
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
            raise F05ProviderPretrainingError("provider BBO duplicate changed content")
        source = source.drop_duplicates("timestamp", keep="last")
    source_ts = source["timestamp"].to_numpy(dtype=np.int64, copy=False)
    if len(source_ts) == 0 or np.any(source_ts[1:] <= source_ts[:-1]):
        raise F05ProviderPretrainingError("provider BBO clock is invalid")
    bid = source["best_bid"].to_numpy(dtype=np.float64, copy=False)
    ask = source["best_ask"].to_numpy(dtype=np.float64, copy=False)
    if np.any(bid <= 0.0) or np.any(ask <= bid):
        raise F05ProviderPretrainingError("provider BBO contains an invalid market")
    mid = 0.5 * (bid + ask)
    day_start = _day_start_ms(day)
    day_end = day_start + 86_400_000
    sample_ts = np.arange(day_start, day_end, SAMPLE_MS, dtype=np.int64)
    visible = np.searchsorted(source_ts, sample_ts, side="right") - 1
    if np.any(visible < 0) or np.any(visible >= len(source_ts)):
        raise F05ProviderPretrainingError("provider BBO misses canonical sample support")
    return source_ts, mid, sample_ts, visible.astype(np.int64, copy=False)


def _irregular_ema(
    timestamps_ms: np.ndarray,
    values: np.ndarray,
    *,
    half_life_s: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Apply the frozen recursive EMA exactly, vectorizing regular segments."""

    ts = np.asarray(timestamps_ms, dtype=np.int64)
    x = np.asarray(values, dtype=np.float64)
    if len(ts) != len(x) or len(ts) == 0 or np.any(ts[1:] <= ts[:-1]):
        raise F05ProviderPretrainingError("irregular EMA input clock is invalid")
    ema = np.empty_like(x)
    velocity = np.zeros_like(x)
    ema[0] = x[0]
    boundaries = np.flatnonzero(np.diff(ts) != GRID_MS) + 1
    starts = np.r_[0, boundaries]
    ends = np.r_[boundaries, len(ts)]
    regular_decay = math.exp(
        -math.log(2.0) * (GRID_MS / 1_000.0) / float(half_life_s)
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
        velocity[cursor:end] = (segment - prior) / (GRID_MS / 1_000.0)
    return ema, velocity


def provider_ema_feature_batches(
    prior_bbo: pd.DataFrame,
    target_bbo: pd.DataFrame,
    target_features: pd.DataFrame,
    *,
    day: str,
    tick_size: float = 0.1,
) -> Mapping[str, np.ndarray]:
    """Return BUY/SELL full EMA matrices on the frozen 10-second train grid."""

    bbo_ts, mid, sample_ts, sample_index = _canonical_mid_grid(
        prior_bbo, target_bbo, day=day
    )
    feature_clock = np.asarray(
        pd.to_datetime(target_features.index, utc=True).as_unit("ms").asi8,
        dtype=np.int64,
    )
    if not np.array_equal(feature_clock, sample_ts):
        raise F05ProviderPretrainingError("provider feature/BBO train grids differ")
    if "volatility_5s" not in target_features:
        raise F05ProviderPretrainingError("provider features lack volatility_5s")
    volatility_bps = (
        pd.to_numeric(target_features["volatility_5s"], errors="coerce")
        .abs()
        .to_numpy(dtype=np.float64)
        * 10_000.0
    )
    if not np.isfinite(volatility_bps).all():
        raise F05ProviderPretrainingError("provider volatility is nonfinite")
    sample_mid = mid[sample_index]
    tick_bps = float(tick_size) / sample_mid * 10_000.0
    volatility_scale = np.maximum.reduce(
        (volatility_bps, tick_bps, np.full_like(tick_bps, 1e-12))
    )
    sampled_ema: list[np.ndarray] = []
    sampled_velocity: list[np.ndarray] = []
    adjacent_z: list[np.ndarray] = []
    adjacent_cross_age: list[np.ndarray] = []
    adjacent_cross_missing: list[np.ndarray] = []
    adjacent_signs: list[np.ndarray] = []
    previous_full: np.ndarray | None = None
    for half_life_s in EMA_HALF_LIVES_S:
        full, full_velocity = _irregular_ema(
            bbo_ts, mid, half_life_s=half_life_s
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
                (bbo_ts[sample_index] - bbo_ts[np.maximum(sampled_cross, 0)])
                / 1_000.0,
            )
            adjacent_z.append(z[sample_index])
            adjacent_cross_age.append(np.log1p(age_s))
            adjacent_cross_missing.append(missing.astype(np.float64))
            adjacent_signs.append(effective_sign)
        previous_full = full
    assert len(adjacent_z) == len(EMA_HALF_LIVES_S) - 1
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
        (bbo_ts[sample_index] - bbo_ts[np.maximum(ordering_change, 0)])
        / 1_000.0,
    )
    names = model_feature_names()
    output: dict[str, np.ndarray] = {}
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
            values[f"{prefix}_volatility_normalized"] = (
                signed_adjacent[index] / volatility_scale
            )
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
            raise F05ProviderPretrainingError("provider EMA matrix is nonfinite")
        output[side] = matrix
    return output


__all__ = [
    "F05ProviderPretrainingError",
    "GRID_MS",
    "SAMPLE_MS",
    "provider_ema_feature_batches",
]
