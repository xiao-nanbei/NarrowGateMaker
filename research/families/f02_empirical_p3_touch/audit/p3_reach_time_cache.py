#!/usr/bin/env python3
"""Reusable label-cache node for F02 aggressive-reach first passage."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.cache_tier_lru import record_cache_access, register_cache_write
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_context import (
    canonical_sha256,
    validate_context_frame,
)
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_surface import (
    DEFAULT_GRID_SPEC,
    ReachTimeGridSpec,
    ReachTimeLabelSurface,
    build_cumulative_reach_ticks,
)

SCHEMA_VERSION = "narrowgate_p3_reach_label_day.v1"


def _record_label_cache_hit(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        record_cache_access(path, identity_sha256=identity_sha256)


def _register_label_cache_write(path: Path, *, identity_sha256: str) -> None:
    with suppress(Exception):
        register_cache_write(path, identity_sha256=identity_sha256)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def label_cache_key(
    *,
    day: str,
    context_cache_key: str,
    trade_sha256: str,
    label_kernel_sha256: str,
    tick_size: float,
    spec: ReachTimeGridSpec = DEFAULT_GRID_SPEC,
) -> str:
    return canonical_sha256(
        {
            "schema_version": SCHEMA_VERSION,
            "day": str(day),
            "context_cache_key": str(context_cache_key),
            "trade_sha256": str(trade_sha256),
            "label_kernel_sha256": str(label_kernel_sha256),
            "tick_size": float(tick_size),
            "time_step_ms": int(spec.time_step_ms),
            "administrative_censor_ms": int(spec.max_horizon_ms),
            "max_distance_ticks": int(spec.max_distance_ticks),
        }
    )


def _timestamp_ms(values: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=np.float64)
    finite = numeric[np.isfinite(numeric)]
    if finite.size and float(np.median(finite)) < 1e11:
        numeric *= 1000.0
    return numeric.astype(np.int64)


def _buyer_maker(values: pd.Series) -> np.ndarray:
    if pd.api.types.is_bool_dtype(values):
        return values.to_numpy(dtype=bool)
    return values.astype(str).str.lower().isin({"true", "1", "t", "yes"}).to_numpy()


def _price_ticks(prices: np.ndarray, *, tick_size: float) -> np.ndarray:
    values = np.asarray(prices, dtype=np.float64)
    ticks = np.rint(values / float(tick_size)).astype(np.int64)
    reconstructed = ticks.astype(np.float64) * float(tick_size)
    tolerance = max(1e-9, float(tick_size) * 1e-7)
    if np.any(~np.isfinite(values)) or np.any(np.abs(values - reconstructed) > tolerance):
        raise ValueError("aggressive trades contain prices outside the tick grid")
    return ticks


def build_reach_label_surface(
    *,
    context: pd.DataFrame,
    trade_path: Path,
    tick_size: float = 0.1,
    spec: ReachTimeGridSpec = DEFAULT_GRID_SPEC,
) -> ReachTimeLabelSurface:
    validate_context_frame(context)
    trades = pd.read_csv(
        Path(trade_path).expanduser().resolve(),
        usecols=["price", "transact_time", "is_buyer_maker"],
    ).dropna(subset=["price", "transact_time", "is_buyer_maker"])
    if trades.empty:
        raise ValueError(f"empty P3 aggressive-trade input: {trade_path}")
    trade_ts = _timestamp_ms(trades["transact_time"])
    prices = pd.to_numeric(trades["price"], errors="coerce").to_numpy(dtype=np.float64)
    maker = _buyer_maker(trades["is_buyer_maker"])
    order = np.argsort(trade_ts, kind="stable")
    trade_ts = trade_ts[order]
    prices = prices[order]
    maker = maker[order]
    return build_cumulative_reach_ticks(
        decision_ts_ms=context["origin_ts_ms"].to_numpy(dtype=np.int64),
        best_bid_ticks=context["best_bid_ticks"].to_numpy(dtype=np.int64),
        best_ask_ticks=context["best_ask_ticks"].to_numpy(dtype=np.int64),
        trade_ts_ms=trade_ts,
        trade_price_ticks=_price_ticks(prices, tick_size=tick_size),
        is_buyer_maker=maker,
        spec=spec,
    )


def _validate_surface(surface: ReachTimeLabelSurface, origins: np.ndarray) -> None:
    if surface.buy_cumulative_reach_ticks.shape != surface.sell_cumulative_reach_ticks.shape:
        raise ValueError("P3 reach label side matrices have different shapes")
    if surface.buy_cumulative_reach_ticks.shape[0] != len(origins):
        raise ValueError("P3 reach label row count differs from context")
    for side, matrix in (
        ("BUY", surface.buy_cumulative_reach_ticks),
        ("SELL", surface.sell_cumulative_reach_ticks),
    ):
        if np.any(np.diff(matrix.astype(np.int32), axis=1) < 0):
            raise ValueError(f"{side} cumulative reach is not monotone in time")


def write_label_cache(
    path: Path,
    *,
    origins_ms: np.ndarray,
    surface: ReachTimeLabelSurface,
    cache_key: str,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    origins = np.asarray(origins_ms, dtype=np.int64)
    _validate_surface(surface, origins)
    logical_target = Path(path).expanduser().absolute()
    target = logical_target.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".npz", dir=target.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        np.savez_compressed(
            handle,
            schema_version=np.asarray(SCHEMA_VERSION),
            cache_key=np.asarray(str(cache_key)),
            origin_ts_ms=origins,
            time_upper_ms=np.asarray(surface.time_upper_ms, dtype=np.int32),
            buy_cumulative_reach_ticks=np.asarray(
                surface.buy_cumulative_reach_ticks, dtype=np.int16
            ),
            sell_cumulative_reach_ticks=np.asarray(
                surface.sell_cumulative_reach_ticks, dtype=np.int16
            ),
        )
    os.replace(temporary, target)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "cache_key": str(cache_key),
        "rows": int(len(origins)),
        "time_bins": int(len(surface.time_upper_ms)),
        "data_path": str(target),
        "data_sha256": sha256_file(target),
        "identity": dict(identity),
        "economic_outcomes_read": False,
        "queue_fill_lifecycle_inputs_read": False,
    }
    manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, delete=False
    ) as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary_manifest = Path(handle.name)
    os.replace(temporary_manifest, manifest_path)
    _register_label_cache_write(logical_target, identity_sha256=str(cache_key))
    return manifest


def load_label_cache(
    path: Path,
    *,
    expected_cache_key: str,
) -> tuple[np.ndarray, ReachTimeLabelSurface, dict[str, Any]]:
    logical_target = Path(path).expanduser().absolute()
    target = logical_target.resolve()
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    canonical_payload = dict(manifest)
    observed_canonical = canonical_payload.pop("canonical_manifest_sha256", None)
    if canonical_sha256(canonical_payload) != observed_canonical:
        raise ValueError("P3 reach label manifest canonical hash mismatch")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported P3 reach label cache schema")
    if manifest.get("cache_key") != str(expected_cache_key):
        raise ValueError("P3 reach label cache key mismatch")
    if sha256_file(target) != manifest.get("data_sha256"):
        raise ValueError("P3 reach label cache data hash mismatch")
    with np.load(target, allow_pickle=False) as payload:
        if str(payload["schema_version"].item()) != SCHEMA_VERSION:
            raise ValueError("P3 reach label payload schema mismatch")
        if str(payload["cache_key"].item()) != str(expected_cache_key):
            raise ValueError("P3 reach label payload cache key mismatch")
        origins = payload["origin_ts_ms"].astype(np.int64, copy=True)
        surface = ReachTimeLabelSurface(
            time_upper_ms=payload["time_upper_ms"].astype(np.int32, copy=True),
            buy_cumulative_reach_ticks=payload["buy_cumulative_reach_ticks"].astype(
                np.int16, copy=True
            ),
            sell_cumulative_reach_ticks=payload["sell_cumulative_reach_ticks"].astype(
                np.int16, copy=True
            ),
        )
    _validate_surface(surface, origins)
    if len(origins) != int(manifest["rows"]):
        raise ValueError("P3 reach label cache row-count mismatch")
    _record_label_cache_hit(logical_target, identity_sha256=str(expected_cache_key))
    return origins, surface, manifest
