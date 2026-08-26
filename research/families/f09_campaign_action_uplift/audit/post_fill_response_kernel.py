"""Fit post-fill flow persistence and adverse-price response causally.

This audit keeps two quantities separate:

1. an exponential Hawkes kernel for exposure-increasing fill arrivals;
2. an adverse-price response model expressed in execution ticks and as a
   fraction of the baseline add-side quote distance.

Only the chronological training split is used for fitting. Validation and late
splits are evaluation-only. The resulting JSON is a research bundle, not an
automatic live-promotion artifact.
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import lsq_linear, minimize
from scipy.stats import spearmanr

from models import backtest_tick as bt

HORIZONS_S = (1.0, 5.0, 20.0, 30.0)
FLOW_WINDOWS_S = ((0.0, 1.0), (1.0, 5.0), (5.0, 20.0), (20.0, 30.0))
FLOW_WINDOW_CENTERS_S = np.array([0.5, 3.0, 12.5, 25.0], dtype=float)


def _truthy(values: pd.Series) -> pd.Series:
    return values.astype(str).str.strip().str.lower().isin({"1", "true", "yes"})


def _timestamp_seconds(values: pd.Series) -> pd.Series:
    out = pd.to_numeric(values, errors="coerce").astype(float)
    finite = out[np.isfinite(out)]
    if finite.empty:
        return out
    scale_probe = float(finite.median())
    if scale_probe > 1e17:
        return out / 1e9
    if scale_probe > 1e14:
        return out / 1e6
    if scale_probe > 1e11:
        return out / 1e3
    return out


def _paths_from_filelist(path: Path) -> list[Path]:
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
        column = "order_level_csv" if "order_level_csv" in frame else frame.columns[-1]
        return [Path(value).expanduser() for value in frame[column].dropna().astype(str)]
    return [
        Path(line.strip()).expanduser()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def load_exposure_increasing_fills(paths: Iterable[Path]) -> tuple[pd.DataFrame, list[str]]:
    wanted = {
        "day",
        "side",
        "filled",
        "fill_ts",
        "avg_fill_price",
        "price",
        "mid",
        "quote_distance",
        "order_exposure_increasing",
        "campaign_id",
        "rv_10s_bps",
        "range_10s_bps",
        "l2_book_refresh_ratio",
        "l2_book_cancel_ratio",
        "markout_1s_bps",
        "markout_5s_bps",
        "markout_20s_bps",
        "markout_30s_bps",
    }
    frames: list[pd.DataFrame] = []
    all_days: set[str] = set()
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(path)
        frame = pd.read_csv(path, usecols=lambda name: name in wanted)
        if "day" not in frame:
            raise ValueError(f"{path}: missing day")
        all_days.update(frame["day"].dropna().astype(str).str[:10].unique())
        required = {"side", "filled", "fill_ts", "order_exposure_increasing"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{path}: missing required columns {missing}")
        keep = _truthy(frame["filled"]) & (
            pd.to_numeric(frame["order_exposure_increasing"], errors="coerce").fillna(0.0)
            > 0.0
        )
        if keep.any():
            frames.append(frame.loc[keep].copy())
    if not frames:
        raise ValueError("no exposure-increasing filled orders")
    fills = pd.concat(frames, ignore_index=True)
    fills["day"] = fills["day"].astype(str).str[:10]
    fills["side"] = fills["side"].astype(str).str.upper()
    fills = fills[fills["side"].isin({"BUY", "SELL"})].copy()
    fills["fill_ts_s"] = _timestamp_seconds(fills["fill_ts"])
    fills = fills[np.isfinite(fills["fill_ts_s"])].copy()
    fills["campaign_id"] = pd.to_numeric(
        fills.get("campaign_id", 0), errors="coerce"
    ).fillna(0).astype(int)
    for name in wanted:
        if name not in fills:
            fills[name] = np.nan
    fills["fill_price"] = pd.to_numeric(fills["avg_fill_price"], errors="coerce")
    fills["fill_price"] = fills["fill_price"].fillna(
        pd.to_numeric(fills["price"], errors="coerce")
    )
    fills["fill_price"] = fills["fill_price"].fillna(
        pd.to_numeric(fills["mid"], errors="coerce")
    )
    fills["volatility_bps"] = pd.to_numeric(
        fills["rv_10s_bps"], errors="coerce"
    ).fillna(pd.to_numeric(fills["range_10s_bps"], errors="coerce"))
    fills["refill_edge"] = (
        pd.to_numeric(fills["l2_book_refresh_ratio"], errors="coerce").fillna(0.0)
        - pd.to_numeric(fills["l2_book_cancel_ratio"], errors="coerce").fillna(0.0)
    )
    fills.sort_values(["day", "side", "fill_ts_s"], inplace=True)
    fills.reset_index(drop=True, inplace=True)
    return fills, sorted(all_days)


def attach_causal_repair_probability(
    fills: pd.DataFrame,
    sequence_path: Path | None,
    *,
    panel: str = "chronological",
    max_age_s: float = 120.0,
) -> pd.DataFrame:
    out = fills.copy()
    out["repair_probability"] = np.nan
    out["repair_probability_age_s"] = np.nan
    if sequence_path is None:
        return out
    sequence = pd.read_csv(sequence_path)
    if "panel" in sequence:
        sequence = sequence[sequence["panel"].astype(str) == panel].copy()
    if sequence.empty:
        return out
    sequence["day"] = sequence["day"].astype(str).str[:10]
    sequence["campaign_id"] = pd.to_numeric(
        sequence["campaign_id"], errors="coerce"
    ).fillna(0).astype(int)
    sequence["campaign_side"] = sequence["campaign_side"].astype(str).str.upper()
    if "timestamp" in sequence:
        sequence["score_ts_s"] = _timestamp_seconds(sequence["timestamp"])
    else:
        sequence["score_ts_s"] = _timestamp_seconds(sequence["ts_ns"])
    groups: dict[tuple[str, int, str], tuple[np.ndarray, np.ndarray]] = {}
    for key, group in sequence.groupby(["day", "campaign_id", "campaign_side"]):
        ordered = group.sort_values("score_ts_s")
        groups[(str(key[0]), int(key[1]), str(key[2]))] = (
            ordered["score_ts_s"].to_numpy(dtype=float),
            pd.to_numeric(ordered["repair_probability"], errors="coerce").to_numpy(
                dtype=float
            ),
        )
    probabilities = np.full(len(out), np.nan, dtype=float)
    ages = np.full(len(out), np.nan, dtype=float)
    for pos, row in enumerate(out.itertuples(index=False)):
        campaign_side = "LONG" if row.side == "BUY" else "SHORT"
        payload = groups.get((str(row.day), int(row.campaign_id), campaign_side))
        if payload is None:
            continue
        timestamps, values = payload
        idx = int(np.searchsorted(timestamps, float(row.fill_ts_s), side="right") - 1)
        if idx < 0:
            continue
        age = float(row.fill_ts_s) - float(timestamps[idx])
        if age < -1e-9 or age > max_age_s or not math.isfinite(values[idx]):
            continue
        probabilities[pos] = float(values[idx])
        ages[pos] = age
    out["repair_probability"] = probabilities
    out["repair_probability_age_s"] = ages
    return out


@dataclass(frozen=True)
class HawkesFit:
    side: str
    mu_per_s: float
    branching_ratio: float
    beta_per_s: float
    half_life_s: float
    negative_log_likelihood: float
    events: int
    days: int
    success: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "side": self.side,
            "mu_per_s": self.mu_per_s,
            "branching_ratio": self.branching_ratio,
            "beta_per_s": self.beta_per_s,
            "half_life_s": self.half_life_s,
            "negative_log_likelihood": self.negative_log_likelihood,
            "events": self.events,
            "days": self.days,
            "success": self.success,
        }


def _hawkes_nll(
    params: np.ndarray,
    events_by_day: list[np.ndarray],
    day_length_s: float,
) -> float:
    mu, eta, half_life = (float(value) for value in params)
    if mu <= 0.0 or not 0.0 <= eta < 1.0 or half_life <= 0.0:
        return 1e100
    beta = math.log(2.0) / half_life
    total = 0.0
    for raw_times in events_by_day:
        times = np.asarray(raw_times, dtype=float)
        total += mu * day_length_s
        if times.size == 0:
            continue
        total += eta * float(np.sum(1.0 - np.exp(-beta * (day_length_s - times))))
        excitation_post = 0.0
        previous = 0.0
        for event_time in times:
            excitation_pre = excitation_post * math.exp(
                -beta * max(0.0, float(event_time) - previous)
            )
            intensity = mu + eta * beta * excitation_pre
            if intensity <= 0.0 or not math.isfinite(intensity):
                return 1e100
            total -= math.log(intensity)
            excitation_post = excitation_pre + 1.0
            previous = float(event_time)
    return float(total)


def fit_exponential_hawkes(
    fills: pd.DataFrame,
    days: list[str],
    side: str,
    *,
    day_length_s: float = 86_400.0,
) -> HawkesFit:
    selected = fills[fills["side"] == side].copy()
    midnight = pd.to_datetime(selected["day"], utc=True).astype("int64") / 1e9
    selected["seconds_in_day"] = selected["fill_ts_s"].to_numpy(dtype=float) - midnight
    selected["seconds_in_day"] = selected["seconds_in_day"].clip(0.0, day_length_s)
    grouped = {
        day: np.sort(group["seconds_in_day"].to_numpy(dtype=float))
        for day, group in selected.groupby("day")
    }
    events_by_day = [grouped.get(day, np.empty(0, dtype=float)) for day in days]
    event_count = sum(len(values) for values in events_by_day)
    if event_count < 2:
        raise ValueError(f"not enough {side} events for Hawkes fit")
    unconditional = event_count / max(len(days) * day_length_s, 1.0)
    best = None
    bounds = [(1e-10, 0.5), (1e-6, 0.95), (0.25, 300.0)]
    for initial_half_life in (1.0, 5.0, 20.0, 60.0):
        initial_eta = 0.30
        initial_mu = max(1e-8, unconditional * (1.0 - initial_eta))
        result = minimize(
            _hawkes_nll,
            x0=np.array([initial_mu, initial_eta, initial_half_life], dtype=float),
            args=(events_by_day, day_length_s),
            method="L-BFGS-B",
            bounds=bounds,
        )
        if best is None or float(result.fun) < float(best.fun):
            best = result
    assert best is not None
    mu, eta, half_life = (float(value) for value in best.x)
    return HawkesFit(
        side=side,
        mu_per_s=mu,
        branching_ratio=eta,
        beta_per_s=math.log(2.0) / half_life,
        half_life_s=half_life,
        negative_log_likelihood=float(best.fun),
        events=event_count,
        days=len(days),
        success=bool(best.success),
    )


def add_hawkes_excitation(fills: pd.DataFrame, fits: dict[str, HawkesFit]) -> pd.DataFrame:
    out = fills.copy()
    excitation = np.ones(len(out), dtype=float)
    for (_, side), positions in out.groupby(["day", "side"], sort=False).groups.items():
        fit = fits[str(side)]
        ordered_positions = sorted(positions, key=lambda pos: float(out.at[pos, "fill_ts_s"]))
        state_post = 0.0
        previous = 0.0
        for pos in ordered_positions:
            current = float(out.at[pos, "fill_ts_s"])
            if previous <= 0.0:
                state_pre = 0.0
            else:
                state_pre = state_post * math.exp(
                    -fit.beta_per_s * max(0.0, current - previous)
                )
            state_post = state_pre + 1.0
            excitation[pos] = state_post
            previous = current
    out["hawkes_excitation_post_fill"] = excitation
    return out


def _window_quantity(
    timestamps_s: np.ndarray,
    cumulative_quantity: np.ndarray,
    starts_s: np.ndarray,
    ends_s: np.ndarray,
    *,
    start_side: str = "left",
    end_side: str = "right",
) -> np.ndarray:
    left = np.searchsorted(timestamps_s, starts_s, side=start_side)
    right = np.searchsorted(timestamps_s, ends_s, side=end_side)
    return cumulative_quantity[right] - cumulative_quantity[left]


def attach_local_aggressive_flow(
    fills: pd.DataFrame,
    *,
    aggtrade_root: Path,
    symbol: str,
) -> pd.DataFrame:
    """Attach causal local-flow state and post-fill flow response windows."""

    out = fills.copy()
    out["local_flow_excitation_at_fill"] = np.nan
    for index, _window in enumerate(FLOW_WINDOWS_S):
        out[f"future_flow_ratio_w{index}"] = np.nan
    for day, day_rows in out.groupby("day", sort=True):
        path = aggtrade_root / f"{symbol.upper()}-aggTrades-{day}.csv"
        if not path.exists():
            raise FileNotFoundError(path)
        trades = bt._read_aggtrade_csv(path)
        trade_ts_s = trades["transact_time"].to_numpy(dtype=float) / 1000.0
        quantity = trades["quantity"].to_numpy(dtype=float)
        seller_aggressive = trades["is_buyer_maker"].to_numpy(dtype=bool)
        for side, adverse_mask in (("BUY", seller_aggressive), ("SELL", ~seller_aggressive)):
            positions = day_rows.index[day_rows["side"] == side].to_numpy(dtype=int)
            if positions.size == 0:
                continue
            side_ts = trade_ts_s[adverse_mask]
            side_qty = quantity[adverse_mask]
            cumulative = np.concatenate([[0.0], np.cumsum(side_qty, dtype=float)])
            fill_ts = out.loc[positions, "fill_ts_s"].to_numpy(dtype=float)
            pre_1s = _window_quantity(
                side_ts, cumulative, fill_ts - 1.0, fill_ts
            )
            pre_30s = _window_quantity(
                side_ts, cumulative, fill_ts - 30.0, fill_ts
            )
            pre_60s = _window_quantity(
                side_ts, cumulative, fill_ts - 60.0, fill_ts
            )
            unconditional_rate = max(float(side_qty.sum()) / 86_400.0, 1e-9)
            baseline_rate = np.maximum(pre_60s / 60.0, unconditional_rate * 0.10)
            recent_rate = np.maximum(pre_1s, pre_30s / 30.0)
            out.loc[positions, "local_flow_excitation_at_fill"] = (
                recent_rate / baseline_rate
            )
            for window_index, (start_s, end_s) in enumerate(FLOW_WINDOWS_S):
                volume = _window_quantity(
                    side_ts,
                    cumulative,
                    fill_ts + start_s,
                    fill_ts + end_s,
                    start_side="right",
                    end_side="right",
                )
                rate = volume / max(end_s - start_s, 1e-9)
                out.loc[positions, f"future_flow_ratio_w{window_index}"] = (
                    rate / baseline_rate
                )
    return out


def fit_local_flow_response(
    fills: pd.DataFrame,
    train_days: list[str],
) -> dict[str, dict[str, Any]]:
    train = fills[fills["day"].isin(train_days)].copy()
    output: dict[str, dict[str, Any]] = {}
    for side in ("BUY", "SELL", "BOTH"):
        selected = train if side == "BOTH" else train[train["side"] == side]
        daily_profiles: list[list[float]] = []
        for _, day_rows in selected.groupby("day"):
            values = [
                float(
                    pd.to_numeric(
                        day_rows[f"future_flow_ratio_w{window_index}"],
                        errors="coerce",
                    ).median(skipna=True)
                )
                for window_index in range(len(FLOW_WINDOWS_S))
            ]
            if all(math.isfinite(value) for value in values):
                daily_profiles.append(values)
        if not daily_profiles:
            continue
        values = np.median(np.asarray(daily_profiles, dtype=float), axis=0)
        fit = _nonnegative_decay_fit(FLOW_WINDOW_CENTERS_S, values)
        output[side] = {
            "daily_profiles": len(daily_profiles),
            "observed_future_flow_ratio": {
                f"{start:g}_{end:g}": float(value)
                for (start, end), value in zip(FLOW_WINDOWS_S, values, strict=True)
            },
            "floor_ratio": float(fit["floor_ticks"]),
            "amplitude_ratio": float(fit["amplitude_ticks"]),
            "half_life_s": float(fit["half_life_s"]),
            "sse": float(fit["sse"]),
            "decay_supported": bool(fit["decay_supported"]),
        }
    return output


def _nonnegative_decay_fit(horizons: np.ndarray, values: np.ndarray) -> dict[str, float | bool]:
    best: dict[str, float | bool] | None = None
    for half_life in np.geomspace(0.25, 300.0, 400):
        kernel = np.exp(-math.log(2.0) * horizons / half_life)
        design = np.column_stack([np.ones_like(kernel), kernel])
        fit = lsq_linear(design, values, bounds=(0.0, np.inf))
        error = float(np.square(design @ fit.x - values).sum())
        candidate = {
            "floor_ticks": float(fit.x[0]),
            "amplitude_ticks": float(fit.x[1]),
            "half_life_s": float(half_life),
            "sse": error,
            "decay_supported": bool(fit.x[1] > 0.05 and values[0] > values[-1]),
        }
        if best is None or error < float(best["sse"]):
            best = candidate
    assert best is not None
    return best


def fit_price_response_profiles(
    fills: pd.DataFrame,
    train_days: list[str],
    *,
    tick_size: float,
) -> dict[str, dict[str, Any]]:
    train = fills[fills["day"].isin(train_days)].copy()
    output: dict[str, dict[str, Any]] = {}
    for side in ("BUY", "SELL", "BOTH"):
        selected = train if side == "BOTH" else train[train["side"] == side]
        daily_profiles: list[list[float]] = []
        for _, day_rows in selected.groupby("day"):
            profile: list[float] = []
            for horizon in HORIZONS_S:
                name = f"markout_{int(horizon)}s_bps"
                bps = pd.to_numeric(day_rows[name], errors="coerce")
                price = pd.to_numeric(day_rows["fill_price"], errors="coerce")
                adverse_ticks = (-bps * price / 10_000.0 / tick_size).clip(lower=0.0)
                profile.append(float(adverse_ticks.median(skipna=True)))
            if all(math.isfinite(value) for value in profile):
                daily_profiles.append(profile)
        if not daily_profiles:
            continue
        values = np.median(np.asarray(daily_profiles, dtype=float), axis=0)
        fit = _nonnegative_decay_fit(np.asarray(HORIZONS_S), values)
        output[side] = {
            "daily_profiles": len(daily_profiles),
            "observed_adverse_ticks": {
                str(int(horizon)): float(value)
                for horizon, value in zip(HORIZONS_S, values, strict=True)
            },
            **fit,
        }
    return output


def _state_matrix(
    frame: pd.DataFrame,
    *,
    volatility_ref: float,
    refill_ref: float,
    repair_anchor: float,
) -> tuple[np.ndarray, list[str]]:
    excitation_column = (
        "local_flow_excitation_at_fill"
        if "local_flow_excitation_at_fill" in frame
        else "hawkes_excitation_post_fill"
    )
    excitation = pd.to_numeric(frame[excitation_column], errors="coerce").fillna(
        pd.to_numeric(frame["hawkes_excitation_post_fill"], errors="coerce")
    ).fillna(1.0).clip(lower=0.0, upper=20.0)
    volatility = pd.to_numeric(frame["volatility_bps"], errors="coerce").fillna(
        volatility_ref
    )
    vol_excess = (volatility / max(volatility_ref, 1e-9) - 1.0).clip(lower=0.0, upper=4.0)
    refill = pd.to_numeric(frame["refill_edge"], errors="coerce").fillna(0.0)
    weak_refill = (-refill / max(refill_ref, 1e-9)).clip(lower=0.0, upper=4.0)
    repair = pd.to_numeric(frame["repair_probability"], errors="coerce").fillna(
        repair_anchor
    )
    low_repair = (repair_anchor - repair).clip(lower=0.0, upper=1.0)
    matrix = np.column_stack(
        [
            np.ones(len(frame), dtype=float),
            excitation.to_numpy(dtype=float),
            vol_excess.to_numpy(dtype=float),
            weak_refill.to_numpy(dtype=float),
            low_repair.to_numpy(dtype=float),
        ]
    )
    return matrix, ["intercept", "order_flow_excitation", "vol_excess", "weak_refill", "low_repair"]


def fit_adverse_amplitude_model(
    fills: pd.DataFrame,
    train_days: list[str],
    *,
    tick_size: float,
    policy_horizon_s: int,
    repair_anchor: float = 0.60,
    side: str = "BOTH",
) -> dict[str, Any]:
    train = fills[fills["day"].isin(train_days)].copy()
    side = str(side).upper()
    if side in {"BUY", "SELL"}:
        train = train[train["side"] == side].copy()
    if train.empty:
        raise ValueError("empty chronological training split")
    vol_values = pd.to_numeric(train["volatility_bps"], errors="coerce")
    volatility_ref = float(vol_values[vol_values > 0.0].median())
    if not math.isfinite(volatility_ref) or volatility_ref <= 0.0:
        volatility_ref = 1.0
    refill_values = pd.to_numeric(train["refill_edge"], errors="coerce").abs()
    refill_ref = float(refill_values[refill_values > 0.0].median())
    if not math.isfinite(refill_ref) or refill_ref <= 0.0:
        refill_ref = 0.10
    target_name = f"markout_{int(policy_horizon_s)}s_bps"
    target_bps = pd.to_numeric(train[target_name], errors="coerce")
    floor_bps = pd.to_numeric(train["markout_30s_bps"], errors="coerce")
    price = pd.to_numeric(train["fill_price"], errors="coerce")
    early_adverse = (-target_bps * price / 10_000.0 / tick_size).clip(lower=0.0)
    long_run_adverse = (-floor_bps * price / 10_000.0 / tick_size).clip(lower=0.0)
    target = (early_adverse - long_run_adverse).clip(lower=0.0)
    valid = np.isfinite(target.to_numpy(dtype=float))
    train = train.loc[valid].copy()
    target = target.loc[valid].to_numpy(dtype=float)
    target_cap = float(np.quantile(target, 0.99)) if len(target) else 0.0
    target = np.clip(target, 0.0, target_cap)
    matrix, feature_names = _state_matrix(
        train,
        volatility_ref=volatility_ref,
        refill_ref=refill_ref,
        repair_anchor=repair_anchor,
    )
    day_counts = train.groupby("day")["day"].transform("count").to_numpy(dtype=float)
    weights = np.sqrt(1.0 / np.maximum(day_counts, 1.0))
    fit = lsq_linear(matrix * weights[:, None], target * weights, bounds=(0.0, np.inf))
    coefficients = np.asarray(fit.x, dtype=float)
    baseline_distance_ticks = (
        pd.to_numeric(train["quote_distance"], errors="coerce").abs() / tick_size
    )
    baseline_distance_ticks = baseline_distance_ticks[
        np.isfinite(baseline_distance_ticks) & (baseline_distance_ticks > 0.0)
    ]
    distance_median = (
        float(baseline_distance_ticks.median()) if len(baseline_distance_ticks) else 1.0
    )
    reference_state = np.array([1.0, 1.0, 0.0, 0.0, 0.0], dtype=float)
    reference_adverse_ticks = float(reference_state @ coefficients)
    if len(feature_names) != len(coefficients):
        raise RuntimeError("adverse amplitude feature/coefficient length mismatch")
    return {
        "side": side,
        "policy_horizon_s": int(policy_horizon_s),
        "target_definition": (
            f"max(0, adverse_ticks_{int(policy_horizon_s)}s - adverse_ticks_30s)"
        ),
        "feature_names": feature_names,
        "coefficients_ticks": {
            name: float(value)
            for name, value in zip(feature_names, coefficients, strict=True)
        },
        "volatility_ref_bps": volatility_ref,
        "refill_edge_ref": refill_ref,
        "repair_probability_anchor": repair_anchor,
        "target_cap_ticks_p99": target_cap,
        "training_rows": int(len(train)),
        "baseline_add_distance_ticks_median": distance_median,
        "reference_expected_adverse_ticks": reference_adverse_ticks,
        "reference_adverse_to_add_distance_ratio": (
            reference_adverse_ticks / max(distance_median, 1e-9)
        ),
        "optimizer_cost": float(fit.cost),
        "optimizer_success": bool(fit.success),
    }


def evaluate_amplitude_model(
    fills: pd.DataFrame,
    days: list[str],
    model: dict[str, Any],
    *,
    tick_size: float,
) -> dict[str, Any]:
    frame = fills[fills["day"].isin(days)].copy()
    side = str(model.get("side", "BOTH") or "BOTH").upper()
    if side in {"BUY", "SELL"}:
        frame = frame[frame["side"] == side].copy()
    if frame.empty:
        return {"days": 0, "rows": 0}
    matrix, names = _state_matrix(
        frame,
        volatility_ref=float(model["volatility_ref_bps"]),
        refill_ref=float(model["refill_edge_ref"]),
        repair_anchor=float(model["repair_probability_anchor"]),
    )
    coefficients = np.array(
        [float(model["coefficients_ticks"][name]) for name in names], dtype=float
    )
    prediction = matrix @ coefficients
    horizon = int(model["policy_horizon_s"])
    bps = pd.to_numeric(frame[f"markout_{horizon}s_bps"], errors="coerce")
    floor_bps = pd.to_numeric(frame["markout_30s_bps"], errors="coerce")
    price = pd.to_numeric(frame["fill_price"], errors="coerce")
    early_adverse = (-bps * price / 10_000.0 / tick_size).clip(lower=0.0)
    long_run_adverse = (-floor_bps * price / 10_000.0 / tick_size).clip(lower=0.0)
    target = (early_adverse - long_run_adverse).clip(lower=0.0).to_numpy(dtype=float)
    valid = np.isfinite(target) & np.isfinite(prediction)
    target = target[valid]
    prediction = prediction[valid]
    if len(target) == 0:
        return {"days": len(days), "rows": 0}
    rho = spearmanr(prediction, target).statistic if len(target) > 2 else math.nan
    threshold = float(np.median(prediction))
    low = target[prediction <= threshold]
    high = target[prediction > threshold]
    return {
        "days": int(frame["day"].nunique()),
        "rows": int(len(target)),
        "mae_ticks": float(np.mean(np.abs(prediction - target))),
        "mean_predicted_ticks": float(np.mean(prediction)),
        "mean_observed_ticks": float(np.mean(target)),
        "spearman": float(rho) if math.isfinite(float(rho)) else math.nan,
        "high_minus_low_observed_ticks": (
            float(np.mean(high) - np.mean(low)) if len(high) and len(low) else math.nan
        ),
    }


def chronological_split(
    days: list[str],
    *,
    train_days: int,
    validation_days: int,
    embargo_days: int,
) -> dict[str, list[str]]:
    ordered = sorted(dict.fromkeys(days))
    train_end = min(len(ordered), max(1, int(train_days)))
    validation_start = min(len(ordered), train_end + max(0, int(embargo_days)))
    validation_end = min(len(ordered), validation_start + max(1, int(validation_days)))
    late_start = min(len(ordered), validation_end + max(0, int(embargo_days)))
    return {
        "train": ordered[:train_end],
        "embargo_after_train": ordered[train_end:validation_start],
        "validation": ordered[validation_start:validation_end],
        "embargo_after_validation": ordered[validation_end:late_start],
        "late": ordered[late_start:],
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    paths = _paths_from_filelist(args.order_level_filelist)
    fills, all_days = load_exposure_increasing_fills(paths)
    fills = attach_causal_repair_probability(
        fills,
        args.repair_sequence,
        panel=args.repair_panel,
        max_age_s=args.repair_max_age_s,
    )
    split = chronological_split(
        all_days,
        train_days=args.train_days,
        validation_days=args.validation_days,
        embargo_days=args.embargo_days,
    )
    train = fills[fills["day"].isin(split["train"])].copy()
    hawkes_fits = {
        side: fit_exponential_hawkes(train, split["train"], side)
        for side in ("BUY", "SELL")
    }
    fills = add_hawkes_excitation(fills, hawkes_fits)
    if args.aggtrade_root is not None:
        fills = attach_local_aggressive_flow(
            fills,
            aggtrade_root=args.aggtrade_root.expanduser(),
            symbol=args.symbol,
        )
    price_profiles = fit_price_response_profiles(
        fills, split["train"], tick_size=args.tick_size
    )
    flow_profiles = (
        fit_local_flow_response(fills, split["train"])
        if args.aggtrade_root is not None
        else {}
    )
    amplitudes = {
        side: fit_adverse_amplitude_model(
            fills,
            split["train"],
            tick_size=args.tick_size,
            policy_horizon_s=args.policy_horizon_s,
            repair_anchor=args.repair_anchor,
            side=side,
        )
        for side in ("BUY", "SELL", "BOTH")
    }
    evaluations = {
        side: {
            name: evaluate_amplitude_model(
                fills, days, amplitudes[side], tick_size=args.tick_size
            )
            for name, days in split.items()
            if name in {"train", "validation", "late"}
        }
        for side in amplitudes
    }
    supported_half_lives = [
        float(profile["half_life_s"])
        for profile in (
            flow_profiles.get("BOTH", {}),
            price_profiles.get("BOTH", {}),
        )
        if profile and bool(profile.get("decay_supported", False))
    ]
    recommended_half_life = (
        max(supported_half_lives)
        if supported_half_lives
        else max(fit.half_life_s for fit in hawkes_fits.values())
    )
    result = {
        "schema_version": "post_fill_response_kernel.v1",
        "source_filelist": str(args.order_level_filelist),
        "repair_sequence": str(args.repair_sequence or ""),
        "repair_panel": args.repair_panel,
        "aggtrade_root": str(args.aggtrade_root or ""),
        "tick_size": args.tick_size,
        "all_days": all_days,
        "split": split,
        "fills": int(len(fills)),
        "fills_with_causal_repair_probability": int(
            pd.to_numeric(fills["repair_probability"], errors="coerce").notna().sum()
        ),
        "hawkes": {side: fit.to_dict() for side, fit in hawkes_fits.items()},
        "hawkes_interpretation": (
            "diagnostic own-fill arrival process; policy cooldown and queue selection censor it"
        ),
        "local_flow_response": flow_profiles,
        "price_response": price_profiles,
        "adverse_amplitude": amplitudes,
        "evaluation": evaluations,
        "recommended_base_half_life_s": float(recommended_half_life),
        "promotion_status": "research_only",
    }
    prefix = args.out_prefix.expanduser()
    prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = prefix.with_suffix(".model.json")
    csv_path = prefix.with_suffix(".evaluation.csv")
    md_path = prefix.with_suffix(".md")
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    pd.DataFrame(
        [
            {"side": side, "panel": panel, **metrics}
            for side, side_evaluations in evaluations.items()
            for panel, metrics in side_evaluations.items()
        ]
    ).to_csv(csv_path, index=False)
    lines = [
        "# Post-Fill Response Kernel Fit",
        "",
        f"- Exposure-increasing fills: `{len(fills):,}`",
        f"- Chronological training days: `{len(split['train'])}`",
        f"- Validation days: `{len(split['validation'])}`",
        f"- Late days: `{len(split['late'])}`",
        f"- Causal repair coverage: `{result['fills_with_causal_repair_probability']:,}`",
        "",
        "## Hawkes Arrival Persistence",
        "",
        "| Side | Events | Branching ratio | Half-life (s) | Success |",
        "|---|---:|---:|---:|---:|",
    ]
    for side, fit in hawkes_fits.items():
        lines.append(
            f"| {side} | {fit.events:,} | {fit.branching_ratio:.4f} | "
            f"{fit.half_life_s:.4f} | {int(fit.success)} |"
        )
    lines.extend(
        [
            "",
            "## Adverse-Price Amplitude",
            "",
            f"- Policy horizon: `{amplitudes['BOTH']['policy_horizon_s']}s`",
            "",
            "| Side | Expected transient adverse ticks | Baseline add ticks | Ratio |",
            "|---|---:|---:|---:|",
        ]
    )
    for side, amplitude in amplitudes.items():
        lines.append(
            f"| {side} | {amplitude['reference_expected_adverse_ticks']:.4f} | "
            f"{amplitude['baseline_add_distance_ticks_median']:.4f} | "
            f"{amplitude['reference_adverse_to_add_distance_ratio']:.6f} |"
        )
    lines.extend(
        [
            f"- Recommended base half-life: `{recommended_half_life:.4f}s`",
            "",
            "## Evaluation",
            "",
            "| Panel | Rows | MAE ticks | Spearman | High-low observed ticks |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for side, side_evaluations in evaluations.items():
        for panel, metrics in side_evaluations.items():
            lines.append(
                f"| {side}/{panel} | {int(metrics.get('rows', 0)):,} | "
                f"{float(metrics.get('mae_ticks', math.nan)):.4f} | "
                f"{float(metrics.get('spearman', math.nan)):.4f} | "
                f"{float(metrics.get('high_minus_low_observed_ticks', math.nan)):.4f} |"
            )
    lines.extend(
        [
            "",
            "This fit is observational and chronological. It does not promote a live policy;",
            "the frozen kernel must still pass paired replay with queue/lifecycle costs.",
            "",
        ]
    )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved {json_path}")
    print(f"Saved {csv_path}")
    print(f"Saved {md_path}")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-level-filelist", type=Path, required=True)
    parser.add_argument("--repair-sequence", type=Path, default=None)
    parser.add_argument("--repair-panel", default="chronological")
    parser.add_argument("--repair-max-age-s", type=float, default=120.0)
    parser.add_argument("--aggtrade-root", type=Path, default=None)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--policy-horizon-s", type=int, choices=(1, 5, 20, 30), default=5)
    parser.add_argument("--repair-anchor", type=float, default=0.60)
    parser.add_argument("--train-days", type=int, default=60)
    parser.add_argument("--validation-days", type=int, default=25)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--out-prefix", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    run(build_parser().parse_args(argv))


if __name__ == "__main__":
    main()
