#!/usr/bin/env python3
"""Map a causal conditional P3 curve to the existing quote-core P3 ABI."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import lightgbm as lgb
import numpy as np

from research.families.f02_empirical_p3_touch.audit.p3_touch_volatility_conditioned import (
    ConditionalTouchModel,
)


OVERLAY_SCHEMA_VERSION = "narrowgate_conditional_p3_quote_overlay.v1"
SIDES = ("BUY", "SELL")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def load_conditional_model(
    *,
    model_path: Path,
    calibration_path: Path,
    feature_contract: Mapping[str, Any],
) -> ConditionalTouchModel:
    calibration = json.loads(calibration_path.read_text(encoding="utf-8"))
    return ConditionalTouchModel(
        lgb.Booster(model_file=str(model_path)),
        calibration,
        feature_contract,
    )


def load_context(path: Path) -> dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as cached:
        context = {
            name: np.asarray(cached[name])
            for name in cached.files
            if name != "cache_key"
        }
    required = {
        "start_ts_ms",
        "feature_ready_ts_ms",
        "mid",
        "spread",
        "fast_sigma",
        "slow_sigma",
    }
    missing = sorted(required.difference(context))
    if missing:
        raise ValueError(f"conditional P3 context is missing fields: {missing}")
    starts = np.asarray(context["start_ts_ms"], dtype=np.int64)
    ready = np.asarray(context["feature_ready_ts_ms"], dtype=np.int64)
    if starts.size == 0 or starts.size != np.unique(starts).size:
        raise ValueError("conditional P3 context starts must be non-empty and unique")
    if np.any(np.diff(starts) <= 0):
        raise ValueError("conditional P3 context starts must be strictly increasing")
    if np.any(ready > starts):
        raise ValueError("conditional P3 context is not causally ready")
    return context


def map_context_curves(
    *,
    model: ConditionalTouchModel,
    context: Mapping[str, np.ndarray],
    distance_grid: np.ndarray,
    chunk_windows: int = 512,
) -> dict[str, np.ndarray]:
    """Return the canonical pair-curve mapping for each valid context row.

    The pair curve is the equal-opportunity arithmetic mean of BUY and SELL
    touch probabilities.  Ties in ``argmax d * P_pair`` resolve to the smallest
    distance through NumPy's first-index ``argmax``.  The local log slope uses
    the adjacent frozen grid points, matching the existing P3 finite-difference
    semantics without fitting another parameter.
    """

    grid = np.asarray(distance_grid, dtype=np.float64).reshape(-1)
    if grid.size < 3 or np.any(grid <= 0.0) or np.any(np.diff(grid) <= 0.0):
        raise ValueError("conditional P3 distance grid must be positive and increasing")
    if not np.allclose(np.diff(grid), np.diff(grid)[0], rtol=0.0, atol=1e-12):
        raise ValueError("conditional P3 quote mapping requires an equal-width grid")
    n = len(np.asarray(context["start_ts_ms"]))
    delta = np.full(n, np.nan, dtype=np.float64)
    kappa = np.full(n, np.nan, dtype=np.float64)
    p_buy_at_delta = np.full(n, np.nan, dtype=np.float64)
    p_sell_at_delta = np.full(n, np.nan, dtype=np.float64)
    mapping_valid = np.zeros(n, dtype=np.uint8)

    for begin in range(0, n, int(chunk_windows)):
        end = min(n, begin + int(chunk_windows))
        rows = np.arange(begin, end, dtype=np.int64)
        row_indices = np.repeat(rows, grid.size)
        distances = np.tile(grid, rows.size)
        curves = []
        for side in SIDES:
            curves.append(
                model.predict(
                    context,
                    side=side,
                    distances=distances,
                    row_indices=row_indices,
                ).reshape(rows.size, grid.size)
            )
        buy, sell = curves
        pair = 0.5 * (buy + sell)
        argmax = np.argmax(pair * grid[None, :], axis=1)
        local_rows = np.arange(rows.size, dtype=np.int64)
        lo = np.maximum(0, argmax - 1)
        hi = np.minimum(grid.size - 1, argmax + 1)
        p_lo = pair[local_rows, lo]
        p_hi = pair[local_rows, hi]
        local_kappa = np.log(p_lo / p_hi) / (grid[hi] - grid[lo])
        local_delta = grid[argmax]
        valid = (
            np.isfinite(local_delta)
            & (local_delta > 0.0)
            & (argmax > 0)
            & (argmax < grid.size - 1)
            & np.isfinite(local_kappa)
            & (local_kappa > 0.0)
            & np.isfinite(p_lo)
            & np.isfinite(p_hi)
            & (p_lo > p_hi)
            & (p_hi > 0.0)
        )
        target = rows
        delta[target] = local_delta
        kappa[target] = local_kappa
        p_buy_at_delta[target] = buy[local_rows, argmax]
        p_sell_at_delta[target] = sell[local_rows, argmax]
        mapping_valid[target] = valid.astype(np.uint8)

    return {
        "delta_star": delta,
        "kappa_eff": kappa,
        "p_buy_at_delta_star": p_buy_at_delta,
        "p_sell_at_delta_star": p_sell_at_delta,
        "mapping_valid": mapping_valid,
    }


def _day_start_ms(day: str) -> int:
    parsed = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def materialize_day_overlay(
    *,
    day: str,
    mapped_context: Mapping[str, np.ndarray],
    context: Mapping[str, np.ndarray],
    fallback_delta_star: float,
    fallback_kappa_eff: float,
) -> dict[str, np.ndarray]:
    start_ms = _day_start_ms(day)
    timeline = start_ms + np.arange(8640, dtype=np.int64) * 10_000
    delta = np.full(timeline.size, float(fallback_delta_star), dtype=np.float64)
    kappa = np.full(timeline.size, float(fallback_kappa_eff), dtype=np.float64)
    context_valid = np.zeros(timeline.size, dtype=np.uint8)
    mapping_valid = np.zeros(timeline.size, dtype=np.uint8)
    p_buy = np.full(timeline.size, np.nan, dtype=np.float64)
    p_sell = np.full(timeline.size, np.nan, dtype=np.float64)

    starts = np.asarray(context["start_ts_ms"], dtype=np.int64)
    offsets = starts - start_ms
    if np.any(offsets < 0) or np.any(offsets >= 86_400_000):
        raise ValueError(f"{day} conditional P3 context escapes the UTC day")
    if np.any(offsets % 10_000 != 0):
        raise ValueError(f"{day} conditional P3 context is not on the 10-second grid")
    positions = offsets // 10_000
    if positions.size != np.unique(positions).size:
        raise ValueError(f"{day} conditional P3 context maps duplicate buckets")
    context_valid[positions] = 1
    mapped_valid = np.asarray(mapped_context["mapping_valid"], dtype=np.uint8) == 1
    valid_positions = positions[mapped_valid]
    delta[valid_positions] = np.asarray(mapped_context["delta_star"])[mapped_valid]
    kappa[valid_positions] = np.asarray(mapped_context["kappa_eff"])[mapped_valid]
    mapping_valid[valid_positions] = 1
    p_buy[positions] = np.asarray(mapped_context["p_buy_at_delta_star"])
    p_sell[positions] = np.asarray(mapped_context["p_sell_at_delta_star"])
    if np.any(~np.isfinite(delta)) or np.any(delta <= 0.0):
        raise ValueError(f"{day} conditional P3 delta overlay is invalid")
    if np.any(~np.isfinite(kappa)) or np.any(kappa <= 0.0):
        raise ValueError(f"{day} conditional P3 kappa overlay is invalid")
    return {
        "ts_ms": timeline,
        "delta_star": delta,
        "kappa_eff": kappa,
        "context_valid": context_valid,
        "mapping_valid": mapping_valid,
        "p_buy_at_delta_star": p_buy,
        "p_sell_at_delta_star": p_sell,
    }


def overlay_summary(overlay: Mapping[str, np.ndarray]) -> dict[str, Any]:
    context_valid = np.asarray(overlay["context_valid"], dtype=bool)
    mapping_valid = np.asarray(overlay["mapping_valid"], dtype=bool)
    delta = np.asarray(overlay["delta_star"], dtype=np.float64)[mapping_valid]
    kappa = np.asarray(overlay["kappa_eff"], dtype=np.float64)[mapping_valid]
    side_gap = np.abs(
        np.asarray(overlay["p_buy_at_delta_star"], dtype=np.float64)[mapping_valid]
        - np.asarray(overlay["p_sell_at_delta_star"], dtype=np.float64)[mapping_valid]
    )

    def quantiles(values: np.ndarray) -> dict[str, float]:
        points = (0.0, 0.1, 0.5, 0.9, 1.0)
        labels = ("min", "p10", "p50", "p90", "max")
        return {
            label: float(value)
            for label, value in zip(labels, np.quantile(values, points), strict=True)
        }

    return {
        "total_10s_buckets": int(len(context_valid)),
        "context_valid_buckets": int(context_valid.sum()),
        "context_coverage": float(context_valid.mean()),
        "mapping_valid_buckets": int(mapping_valid.sum()),
        "mapping_valid_fraction": float(mapping_valid.mean()),
        "fallback_buckets": int((~mapping_valid).sum()),
        "delta_star_usdc_per_btc": quantiles(delta),
        "kappa_eff_inverse_usdc_per_btc": quantiles(kappa),
        "mean_buy_sell_probability_gap_at_delta_star": float(np.mean(side_gap)),
    }


def build_or_load_overlay(
    *,
    day: str,
    context_path: Path,
    model_path: Path,
    calibration_path: Path,
    feature_contract: Mapping[str, Any],
    distance_grid: np.ndarray,
    fallback_delta_star: float,
    fallback_kappa_eff: float,
    mapping_contract: Mapping[str, Any],
    cache_dir: Path,
) -> tuple[dict[str, np.ndarray], dict[str, Any], Path, bool]:
    identity = {
        "schema_version": OVERLAY_SCHEMA_VERSION,
        "day": day,
        "context_sha256": sha256_file(context_path),
        "model_sha256": sha256_file(model_path),
        "calibration_sha256": sha256_file(calibration_path),
        "feature_contract": dict(feature_contract),
        "distance_grid": np.asarray(distance_grid, dtype=np.float64).tolist(),
        "fallback_delta_star": float(fallback_delta_star),
        "fallback_kappa_eff": float(fallback_kappa_eff),
        "mapping_contract": dict(mapping_contract),
    }
    key = canonical_sha256(identity)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"BTCUSDC-{day}-{key}.npz"
    if cache_path.is_file():
        with np.load(cache_path, allow_pickle=False) as cached:
            observed_key = str(np.asarray(cached["cache_key"]).item())
            if observed_key != key:
                raise ValueError(f"conditional P3 overlay cache key mismatch: {cache_path}")
            overlay = {
                name: np.asarray(cached[name])
                for name in cached.files
                if name != "cache_key"
            }
        return overlay, overlay_summary(overlay), cache_path, True

    context = load_context(context_path)
    model = load_conditional_model(
        model_path=model_path,
        calibration_path=calibration_path,
        feature_contract=feature_contract,
    )
    mapped = map_context_curves(
        model=model,
        context=context,
        distance_grid=distance_grid,
    )
    overlay = materialize_day_overlay(
        day=day,
        mapped_context=mapped,
        context=context,
        fallback_delta_star=fallback_delta_star,
        fallback_kappa_eff=fallback_kappa_eff,
    )
    with tempfile.NamedTemporaryFile(
        suffix=".npz",
        dir=cache_dir,
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        np.savez_compressed(temporary, cache_key=np.asarray(key), **overlay)
        os.replace(temporary, cache_path)
    finally:
        temporary.unlink(missing_ok=True)
    return overlay, overlay_summary(overlay), cache_path, False
