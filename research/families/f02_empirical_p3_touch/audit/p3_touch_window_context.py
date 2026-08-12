#!/usr/bin/env python3
"""Causal 10-second P3 window context and reusable cache primitives."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f02_empirical_p3_touch.audit.p3_touch_calibration import (
    _buyer_maker,
    _day_start_ms,
    _timestamp_ms,
)


CACHE_SCHEMA_VERSION = "narrowgate_p3_touch_window_context.v1"
CONTEXT_FIELDS = (
    "start_ts_ms",
    "feature_ready_ts_ms",
    "best_bid",
    "best_ask",
    "mid",
    "spread",
    "fast_variance",
    "slow_variance",
    "fast_sigma",
    "slow_sigma",
    "volatility_ratio",
    "book_age_ms",
    "BUY",
    "SELL",
)


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def window_context_cache_key(
    *,
    day: str,
    bbo_sha256: str,
    trade_sha256: str,
    extractor_sha256: str,
    horizon_s: float,
    max_bbo_age_ms: int,
    fast_window_s: int,
    slow_window_s: int,
    variance_floor: float,
) -> str:
    return canonical_sha256(
        {
            "schema_version": CACHE_SCHEMA_VERSION,
            "day": str(day),
            "bbo_sha256": str(bbo_sha256),
            "trade_sha256": str(trade_sha256),
            "extractor_sha256": str(extractor_sha256),
            "horizon_s": float(horizon_s),
            "max_bbo_age_ms": int(max_bbo_age_ms),
            "fast_window_s": int(fast_window_s),
            "slow_window_s": int(slow_window_s),
            "variance_floor": float(variance_floor),
        }
    )


def _atomic_npz(path: Path, payload: Mapping[str, np.ndarray]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        suffix=".npz",
        dir=path.parent,
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        np.savez_compressed(handle, **payload)
    os.replace(temporary, path)


def load_window_context_cache(
    path: Path,
    *,
    expected_key: str,
) -> dict[str, np.ndarray] | None:
    if not path.is_file():
        return None
    with np.load(path, allow_pickle=False) as cached:
        if str(cached["cache_key"].item()) != str(expected_key):
            return None
        missing = sorted(set(CONTEXT_FIELDS).difference(cached.files))
        if missing:
            raise ValueError(f"P3 context cache lacks fields: {missing}")
        return {
            field: cached[field].copy()
            for field in CONTEXT_FIELDS
        }


def write_window_context_cache(
    path: Path,
    *,
    cache_key: str,
    context: Mapping[str, np.ndarray],
) -> None:
    missing = sorted(set(CONTEXT_FIELDS).difference(context))
    if missing:
        raise ValueError(f"P3 context lacks fields: {missing}")
    payload = {
        "cache_key": np.asarray(str(cache_key)),
        **{field: np.asarray(context[field]) for field in CONTEXT_FIELDS},
    }
    _atomic_npz(path, payload)


def _last_known_indices(
    source_ts_ms: np.ndarray,
    query_ts_ms: np.ndarray,
) -> np.ndarray:
    """Return the latest source row available at each inclusive query edge."""

    return np.searchsorted(source_ts_ms, query_ts_ms, side="right") - 1


def _book_validity(
    *,
    source_ts_ms: np.ndarray,
    bids: np.ndarray,
    asks: np.ndarray,
    query_ts_ms: np.ndarray,
    indices: np.ndarray,
    max_bbo_age_ms: int,
) -> tuple[np.ndarray, np.ndarray]:
    safe = np.clip(indices, 0, max(len(source_ts_ms) - 1, 0))
    age = query_ts_ms - source_ts_ms[safe]
    valid = (
        (indices >= 0)
        & np.isfinite(bids[safe])
        & np.isfinite(asks[safe])
        & (bids[safe] > 0.0)
        & (asks[safe] > bids[safe])
        & (age >= 0)
        & (age <= int(max_bbo_age_ms))
    )
    return valid, safe


def extract_window_context(
    *,
    day: str,
    bbo_path: Path,
    trade_path: Path,
    horizon_s: float = 10.0,
    max_bbo_age_ms: int = 5_000,
    fast_window_s: int = 10,
    slow_window_s: int = 60,
    variance_floor: float = 1e-6,
) -> dict[str, np.ndarray]:
    """Build causal state and side-specific maximum reach for each 10s window.

    Volatility uses one-second last-known BBO mids ending at the window start.
    A window is retained only when every required historical BBO sample is
    causal, finite, non-crossed, and no older than ``max_bbo_age_ms``.
    """

    horizon_ms = int(round(float(horizon_s) * 1_000.0))
    if horizon_ms <= 0 or 86_400_000 % horizon_ms:
        raise ValueError("horizon must be positive and divide one UTC day")
    if int(fast_window_s) < 2:
        raise ValueError("fast_window_s must be at least two seconds")
    if int(slow_window_s) < int(fast_window_s):
        raise ValueError("slow_window_s must be at least fast_window_s")
    if float(variance_floor) <= 0.0:
        raise ValueError("variance_floor must be positive")

    bbo = pd.read_parquet(
        bbo_path,
        columns=["timestamp", "best_bid", "best_ask"],
    ).dropna()
    if bbo.empty:
        raise ValueError(f"empty P3 BBO input: {bbo_path}")
    bbo_ts = _timestamp_ms(bbo["timestamp"])
    order = np.argsort(bbo_ts, kind="stable")
    bbo_ts = bbo_ts[order]
    bids = pd.to_numeric(bbo["best_bid"], errors="coerce").to_numpy(
        dtype=np.float64
    )[order]
    asks = pd.to_numeric(bbo["best_ask"], errors="coerce").to_numpy(
        dtype=np.float64
    )[order]

    day_start = _day_start_ms(day)
    n_windows = 86_400_000 // horizon_ms
    starts = day_start + np.arange(n_windows, dtype=np.int64) * horizon_ms
    current_idx = _last_known_indices(bbo_ts, starts)
    valid_current, current_safe = _book_validity(
        source_ts_ms=bbo_ts,
        bids=bids,
        asks=asks,
        query_ts_ms=starts,
        indices=current_idx,
        max_bbo_age_ms=max_bbo_age_ms,
    )

    offsets = np.arange(
        int(slow_window_s),
        -1,
        -1,
        dtype=np.int64,
    )
    history_queries = starts[:, None] - offsets[None, :] * 1_000
    history_idx = _last_known_indices(bbo_ts, history_queries.ravel()).reshape(
        history_queries.shape
    )
    history_safe = np.clip(history_idx, 0, len(bbo_ts) - 1)
    history_age = history_queries - bbo_ts[history_safe]
    history_bids = bids[history_safe]
    history_asks = asks[history_safe]
    valid_history = np.all(
        (history_idx >= 0)
        & np.isfinite(history_bids)
        & np.isfinite(history_asks)
        & (history_bids > 0.0)
        & (history_asks > history_bids)
        & (history_age >= 0)
        & (history_age <= int(max_bbo_age_ms)),
        axis=1,
    )
    history_mid = 0.5 * (history_bids + history_asks)
    differences = np.diff(history_mid, axis=1)
    fast_differences = differences[:, -int(fast_window_s) :]
    with np.errstate(invalid="ignore"):
        fast_variance = np.var(fast_differences, axis=1, ddof=1)
        slow_variance = np.var(differences, axis=1, ddof=1)
    fast_variance = np.maximum(fast_variance, float(variance_floor))
    slow_variance = np.maximum(slow_variance, float(variance_floor))
    valid_variance = (
        np.isfinite(fast_variance)
        & np.isfinite(slow_variance)
        & (fast_variance > 0.0)
        & (slow_variance > 0.0)
    )

    trades = pd.read_csv(
        trade_path,
        usecols=["price", "transact_time", "is_buyer_maker"],
    ).dropna(subset=["price", "transact_time"])
    trade_ts = _timestamp_ms(trades["transact_time"])
    prices = pd.to_numeric(trades["price"], errors="coerce").to_numpy(
        dtype=np.float64
    )
    maker = _buyer_maker(trades["is_buyer_maker"])
    trade_bins = (trade_ts - day_start) // horizon_ms
    in_day = (
        (trade_bins >= 0)
        & (trade_bins < n_windows)
        & np.isfinite(prices)
    )
    min_sell = np.full(n_windows, np.inf, dtype=np.float64)
    max_buy = np.full(n_windows, -np.inf, dtype=np.float64)
    sell_rows = in_day & maker
    buy_rows = in_day & ~maker
    np.minimum.at(
        min_sell,
        trade_bins[sell_rows].astype(np.int64),
        prices[sell_rows],
    )
    np.maximum.at(
        max_buy,
        trade_bins[buy_rows].astype(np.int64),
        prices[buy_rows],
    )

    buy_reach = np.full(n_windows, -np.inf, dtype=np.float64)
    sell_reach = np.full(n_windows, -np.inf, dtype=np.float64)
    buy_touch = valid_current & np.isfinite(min_sell)
    sell_touch = valid_current & np.isfinite(max_buy)
    buy_reach[buy_touch] = bids[current_safe[buy_touch]] - min_sell[buy_touch]
    sell_reach[sell_touch] = max_buy[sell_touch] - asks[current_safe[sell_touch]]

    retained = valid_current & valid_history & valid_variance
    best_bid = bids[current_safe]
    best_ask = asks[current_safe]
    fast_sigma = np.sqrt(fast_variance)
    slow_sigma = np.sqrt(slow_variance)
    return {
        "start_ts_ms": starts[retained].astype(np.int64),
        "feature_ready_ts_ms": bbo_ts[current_safe[retained]].astype(np.int64),
        "best_bid": best_bid[retained].astype(np.float64),
        "best_ask": best_ask[retained].astype(np.float64),
        "mid": (0.5 * (best_bid[retained] + best_ask[retained])).astype(
            np.float64
        ),
        "spread": (best_ask[retained] - best_bid[retained]).astype(np.float64),
        "fast_variance": fast_variance[retained].astype(np.float64),
        "slow_variance": slow_variance[retained].astype(np.float64),
        "fast_sigma": fast_sigma[retained].astype(np.float64),
        "slow_sigma": slow_sigma[retained].astype(np.float64),
        "volatility_ratio": (
            fast_sigma[retained] / slow_sigma[retained]
        ).astype(np.float64),
        "book_age_ms": (starts[retained] - bbo_ts[current_safe[retained]]).astype(
            np.float64
        ),
        "BUY": buy_reach[retained].astype(np.float64),
        "SELL": sell_reach[retained].astype(np.float64),
    }


def align_contexts(
    left: Mapping[str, np.ndarray],
    right: Mapping[str, np.ndarray],
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Align two source contexts on common 10-second start timestamps."""

    left_ts = np.asarray(left["start_ts_ms"], dtype=np.int64)
    right_ts = np.asarray(right["start_ts_ms"], dtype=np.int64)
    common, left_idx, right_idx = np.intersect1d(
        left_ts,
        right_ts,
        assume_unique=True,
        return_indices=True,
    )
    if common.size == 0:
        raise ValueError("P3 source contexts have no common timestamps")
    return (
        {field: np.asarray(left[field])[left_idx] for field in CONTEXT_FIELDS},
        {field: np.asarray(right[field])[right_idx] for field in CONTEXT_FIELDS},
    )


def fit_source_translation(
    paired_by_day: Mapping[
        str,
        tuple[Mapping[str, np.ndarray], Mapping[str, np.ndarray]],
    ],
) -> dict[str, Any]:
    """Fit an outcome-blind provider-to-native BBO translation.

    Each day contributes one robust median before medians are combined across
    days, so a high-message day cannot dominate the source correction.
    """

    if not paired_by_day:
        raise ValueError("source translation requires at least one overlap day")
    rows: list[dict[str, float | int | str]] = []
    for day in sorted(paired_by_day):
        native, provider = paired_by_day[day]
        native_aligned, provider_aligned = align_contexts(native, provider)
        fast_ratio = np.asarray(native_aligned["fast_sigma"]) / np.asarray(
            provider_aligned["fast_sigma"]
        )
        slow_ratio = np.asarray(native_aligned["slow_sigma"]) / np.asarray(
            provider_aligned["slow_sigma"]
        )
        row = {
            "day": day,
            "common_windows": int(len(native_aligned["start_ts_ms"])),
            "bid_shift_native_minus_provider": float(
                np.median(
                    np.asarray(native_aligned["best_bid"])
                    - np.asarray(provider_aligned["best_bid"])
                )
            ),
            "ask_shift_native_minus_provider": float(
                np.median(
                    np.asarray(native_aligned["best_ask"])
                    - np.asarray(provider_aligned["best_ask"])
                )
            ),
            "log_fast_sigma_native_over_provider": float(
                np.median(np.log(np.maximum(fast_ratio, 1e-12)))
            ),
            "log_slow_sigma_native_over_provider": float(
                np.median(np.log(np.maximum(slow_ratio, 1e-12)))
            ),
        }
        rows.append(row)
    numeric = (
        "bid_shift_native_minus_provider",
        "ask_shift_native_minus_provider",
        "log_fast_sigma_native_over_provider",
        "log_slow_sigma_native_over_provider",
    )
    correction = {
        field: float(np.median([float(row[field]) for row in rows]))
        for field in numeric
    }
    return {
        "method": "median_of_daily_paired_window_medians",
        "days": sorted(paired_by_day),
        "daily": rows,
        "correction": correction,
        "future_touch_outcome_used": False,
    }


def apply_source_translation(
    context: Mapping[str, np.ndarray],
    translation: Mapping[str, Any],
) -> dict[str, np.ndarray]:
    """Project provider-local BBO state and reach onto the native scale."""

    correction = translation["correction"]
    bid_shift = float(correction["bid_shift_native_minus_provider"])
    ask_shift = float(correction["ask_shift_native_minus_provider"])
    fast_scale = float(
        np.exp(float(correction["log_fast_sigma_native_over_provider"]))
    )
    slow_scale = float(
        np.exp(float(correction["log_slow_sigma_native_over_provider"]))
    )
    out = {field: np.asarray(context[field]).copy() for field in CONTEXT_FIELDS}
    out["best_bid"] = out["best_bid"].astype(np.float64) + bid_shift
    out["best_ask"] = out["best_ask"].astype(np.float64) + ask_shift
    if np.any(out["best_ask"] <= out["best_bid"]):
        raise ValueError("source translation produced a crossed BBO")
    out["mid"] = 0.5 * (out["best_bid"] + out["best_ask"])
    out["spread"] = out["best_ask"] - out["best_bid"]
    out["fast_sigma"] = out["fast_sigma"].astype(np.float64) * fast_scale
    out["slow_sigma"] = out["slow_sigma"].astype(np.float64) * slow_scale
    out["fast_variance"] = np.square(out["fast_sigma"])
    out["slow_variance"] = np.square(out["slow_sigma"])
    out["volatility_ratio"] = out["fast_sigma"] / out["slow_sigma"]
    out["BUY"] = out["BUY"].astype(np.float64) + bid_shift
    out["SELL"] = out["SELL"].astype(np.float64) - ask_shift
    return out
