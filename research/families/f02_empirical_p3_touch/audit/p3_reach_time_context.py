#!/usr/bin/env python3
"""Causal context node for the F02 aggressive-reach time surface."""

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

SCHEMA_VERSION = "narrowgate_p3_reach_time_context_day.v1"
CONTEXT_COLUMNS = (
    "day",
    "source_profile",
    "origin_ts_ms",
    "feature_ready_ts_ms",
    "best_bid_ticks",
    "best_ask_ticks",
    "mid_usdc_per_btc",
    "spread_ticks",
    "spread_bps",
    "fast_variance_usdc2_per_s",
    "slow_variance_usdc2_per_s",
    "fast_sigma_usdc_per_sqrt_s",
    "slow_sigma_usdc_per_sqrt_s",
    "volatility_ratio",
    "book_age_ms",
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def context_cache_key(
    *,
    day: str,
    source_profile: str,
    bbo_sha256: str,
    extractor_sha256: str,
    tick_size: float,
    cadence_ms: int,
    administrative_censor_ms: int,
    max_bbo_age_ms: int,
    fast_window_s: int,
    slow_window_s: int,
    variance_floor: float,
) -> str:
    return canonical_sha256(
        {
            "schema_version": SCHEMA_VERSION,
            "day": str(day),
            "source_profile": str(source_profile),
            "bbo_sha256": str(bbo_sha256),
            "extractor_sha256": str(extractor_sha256),
            "tick_size": float(tick_size),
            "cadence_ms": int(cadence_ms),
            "administrative_censor_ms": int(administrative_censor_ms),
            "max_bbo_age_ms": int(max_bbo_age_ms),
            "fast_window_s": int(fast_window_s),
            "slow_window_s": int(slow_window_s),
            "variance_floor": float(variance_floor),
        }
    )


def _timestamp_ms(values: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=np.float64)
    finite = numeric[np.isfinite(numeric)]
    if finite.size and float(np.median(finite)) < 1e11:
        numeric *= 1000.0
    return numeric.astype(np.int64)


def _day_start_ms(day: str) -> int:
    return int(pd.Timestamp(str(day), tz="UTC").timestamp() * 1000)


def canonical_origins_ms(
    day: str,
    *,
    cadence_ms: int = 10_000,
    past_warmup_s: int = 60,
    administrative_censor_ms: int = 30_000,
) -> np.ndarray:
    """Return origins with complete past warmup and same-day future support."""

    cadence = int(cadence_ms)
    warmup_ms = int(past_warmup_s) * 1_000
    censor = int(administrative_censor_ms)
    if cadence <= 0 or 86_400_000 % cadence:
        raise ValueError("cadence_ms must be positive and divide one UTC day")
    if warmup_ms < 0 or warmup_ms % cadence:
        raise ValueError("past warmup must align to the canonical cadence")
    if censor <= 0 or censor % cadence:
        raise ValueError("administrative censor must align to the canonical cadence")
    start = _day_start_ms(day)
    offsets = np.arange(warmup_ms, 86_400_000 - censor, cadence, dtype=np.int64)
    return start + offsets


def _last_known_indices(source_ts_ms: np.ndarray, query_ts_ms: np.ndarray) -> np.ndarray:
    return np.searchsorted(source_ts_ms, query_ts_ms, side="right") - 1


def _price_ticks(prices: np.ndarray, *, tick_size: float, label: str) -> np.ndarray:
    values = np.asarray(prices, dtype=np.float64)
    if float(tick_size) <= 0.0:
        raise ValueError("tick_size must be positive")
    ticks = np.rint(values / float(tick_size)).astype(np.int64)
    reconstructed = ticks.astype(np.float64) * float(tick_size)
    tolerance = max(1e-9, float(tick_size) * 1e-7)
    if np.any(~np.isfinite(values)) or np.any(np.abs(values - reconstructed) > tolerance):
        raise ValueError(f"{label} contains prices outside the executable tick grid")
    return ticks


def validate_context_frame(frame: pd.DataFrame) -> None:
    if tuple(frame.columns) != CONTEXT_COLUMNS:
        raise ValueError(
            "P3 reach context schema mismatch: "
            f"observed={list(frame.columns)} expected={list(CONTEXT_COLUMNS)}"
        )
    if frame.empty:
        raise ValueError("P3 reach context must not be empty")
    if frame["origin_ts_ms"].duplicated().any():
        raise ValueError("P3 reach context origins must be unique")
    origins = frame["origin_ts_ms"].to_numpy(dtype=np.int64)
    if len(origins) > 1 and np.any(np.diff(origins) <= 0):
        raise ValueError("P3 reach context origins must be strictly increasing")
    ready = frame["feature_ready_ts_ms"].to_numpy(dtype=np.int64)
    if np.any(ready > origins):
        raise ValueError("P3 reach context contains future-visible features")
    bids = frame["best_bid_ticks"].to_numpy(dtype=np.int64)
    asks = frame["best_ask_ticks"].to_numpy(dtype=np.int64)
    if np.any(bids <= 0) or np.any(asks <= bids):
        raise ValueError("P3 reach context contains invalid BBO ticks")
    numeric = frame.drop(columns=["day", "source_profile"]).to_numpy(dtype=np.float64)
    if not np.all(np.isfinite(numeric)):
        raise ValueError("P3 reach context contains non-finite values")


def extract_reach_time_context(
    *,
    day: str,
    source_profile: str,
    bbo_path: Path,
    tick_size: float = 0.1,
    cadence_ms: int = 10_000,
    past_warmup_s: int = 60,
    administrative_censor_ms: int = 30_000,
    max_bbo_age_ms: int = 5_000,
    fast_window_s: int = 10,
    slow_window_s: int = 60,
    variance_floor: float = 1e-6,
) -> pd.DataFrame:
    """Extract a label-free, canonical-origin causal state table."""

    if int(fast_window_s) < 2:
        raise ValueError("fast_window_s must be at least two")
    if int(slow_window_s) < int(fast_window_s):
        raise ValueError("slow_window_s must be at least fast_window_s")
    if int(past_warmup_s) < int(slow_window_s):
        raise ValueError("past_warmup_s must cover the slow window")
    if int(max_bbo_age_ms) < 0:
        raise ValueError("max_bbo_age_ms must be non-negative")
    if float(variance_floor) <= 0.0:
        raise ValueError("variance_floor must be positive")
    if not str(source_profile).strip():
        raise ValueError("source_profile must be non-empty")

    bbo = pd.read_parquet(
        Path(bbo_path).expanduser().resolve(),
        columns=["timestamp", "best_bid", "best_ask"],
    ).dropna()
    if bbo.empty:
        raise ValueError(f"empty P3 BBO input: {bbo_path}")
    timestamps = _timestamp_ms(bbo["timestamp"])
    order = np.argsort(timestamps, kind="stable")
    timestamps = timestamps[order]
    bids = pd.to_numeric(bbo["best_bid"], errors="coerce").to_numpy(
        dtype=np.float64
    )[order]
    asks = pd.to_numeric(bbo["best_ask"], errors="coerce").to_numpy(
        dtype=np.float64
    )[order]
    if len(timestamps) > 1 and np.any(np.diff(timestamps) < 0):
        raise ValueError("P3 BBO timestamps must be non-decreasing")

    origins = canonical_origins_ms(
        day,
        cadence_ms=cadence_ms,
        past_warmup_s=past_warmup_s,
        administrative_censor_ms=administrative_censor_ms,
    )
    offsets_s = np.arange(int(slow_window_s), -1, -1, dtype=np.int64)
    queries = origins[:, None] - offsets_s[None, :] * 1_000
    indices = _last_known_indices(timestamps, queries.ravel()).reshape(queries.shape)
    safe = np.clip(indices, 0, len(timestamps) - 1)
    ages = queries - timestamps[safe]
    history_bids = bids[safe]
    history_asks = asks[safe]
    valid = np.all(
        (indices >= 0)
        & np.isfinite(history_bids)
        & np.isfinite(history_asks)
        & (history_bids > 0.0)
        & (history_asks > history_bids)
        & (ages >= 0)
        & (ages <= int(max_bbo_age_ms)),
        axis=1,
    )

    mids = 0.5 * (history_bids + history_asks)
    differences = np.diff(mids, axis=1)
    fast = differences[:, -int(fast_window_s) :]
    with np.errstate(invalid="ignore"):
        fast_variance = np.var(fast, axis=1, ddof=1)
        slow_variance = np.var(differences, axis=1, ddof=1)
    fast_variance = np.maximum(fast_variance, float(variance_floor))
    slow_variance = np.maximum(slow_variance, float(variance_floor))
    valid &= (
        np.isfinite(fast_variance)
        & np.isfinite(slow_variance)
        & (fast_variance > 0.0)
        & (slow_variance > 0.0)
    )

    current = safe[:, -1]
    current_bid = bids[current]
    current_ask = asks[current]
    bid_ticks = _price_ticks(current_bid, tick_size=tick_size, label="best_bid")
    ask_ticks = _price_ticks(current_ask, tick_size=tick_size, label="best_ask")
    valid &= (bid_ticks > 0) & (ask_ticks > bid_ticks)
    mid = 0.5 * (current_bid + current_ask)
    spread_ticks = ask_ticks - bid_ticks
    spread_bps = 10_000.0 * (current_ask - current_bid) / mid
    fast_sigma = np.sqrt(fast_variance)
    slow_sigma = np.sqrt(slow_variance)

    frame = pd.DataFrame(
        {
            "day": np.full(np.count_nonzero(valid), str(day), dtype=object),
            "source_profile": np.full(
                np.count_nonzero(valid), str(source_profile), dtype=object
            ),
            "origin_ts_ms": origins[valid],
            "feature_ready_ts_ms": timestamps[current[valid]],
            "best_bid_ticks": bid_ticks[valid],
            "best_ask_ticks": ask_ticks[valid],
            "mid_usdc_per_btc": mid[valid],
            "spread_ticks": spread_ticks[valid],
            "spread_bps": spread_bps[valid],
            "fast_variance_usdc2_per_s": fast_variance[valid],
            "slow_variance_usdc2_per_s": slow_variance[valid],
            "fast_sigma_usdc_per_sqrt_s": fast_sigma[valid],
            "slow_sigma_usdc_per_sqrt_s": slow_sigma[valid],
            "volatility_ratio": fast_sigma[valid] / slow_sigma[valid],
            "book_age_ms": origins[valid] - timestamps[current[valid]],
        },
        columns=CONTEXT_COLUMNS,
    )
    validate_context_frame(frame)
    return frame


def write_context_cache(
    path: Path,
    *,
    frame: pd.DataFrame,
    cache_key: str,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    validate_context_frame(frame)
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    with tempfile.NamedTemporaryFile(
        suffix=".parquet", dir=target.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "cache_key": str(cache_key),
        "rows": int(len(frame)),
        "columns": list(CONTEXT_COLUMNS),
        "data_path": str(target),
        "data_sha256": sha256_file(target),
        "identity": dict(identity),
        "economic_outcomes_read": False,
        "label_columns_present": False,
    }
    payload["canonical_manifest_sha256"] = canonical_sha256(payload)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        manifest_temporary = Path(handle.name)
    os.replace(manifest_temporary, manifest_path)
    return payload


def load_context_cache(
    path: Path,
    *,
    expected_cache_key: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    target = Path(path).expanduser().resolve()
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    observed_identity = dict(manifest)
    canonical = observed_identity.pop("canonical_manifest_sha256", None)
    if canonical_sha256(observed_identity) != canonical:
        raise ValueError("P3 reach context manifest canonical hash mismatch")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported P3 reach context cache schema")
    if manifest.get("cache_key") != str(expected_cache_key):
        raise ValueError("P3 reach context cache key mismatch")
    if sha256_file(target) != manifest.get("data_sha256"):
        raise ValueError("P3 reach context cache data hash mismatch")
    frame = pd.read_parquet(target)
    validate_context_frame(frame)
    if len(frame) != int(manifest["rows"]):
        raise ValueError("P3 reach context cache row-count mismatch")
    return frame, manifest
