#!/usr/bin/env python3
"""Offline external-venue consensus/divergence feature builder.

Input files are daily causal 1s feature streams selected by a retained-day
manifest. This module deliberately does not touch live trading; it produces
research artifacts for validating whether external venues identify adverse
fills earlier than Binance-only features.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

BRIDGE_BAR_SCHEMA_VERSION = "binance_individual_trade_bar_1s.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_causal_consensus_1s(
    venue_frames: dict[str, pd.DataFrame],
    *,
    min_venues: int = 2,
    max_source_age_s: float = 2.0,
    dispersion_scale_bps: float = 1.0,
) -> pd.DataFrame:
    """Build a right-edge-visible 1s consensus from normalized venue trades.

    Each input row describes trades in ``[t, t+1s)`` and becomes visible at
    its ``timestamp`` (the right edge).  A venue price may be carried forward
    only while its last underlying trade remains inside ``max_source_age_s``.
    The consensus therefore never backfills a quiet venue from the future.
    """

    if not venue_frames:
        raise ValueError("no venue frames supplied")
    required = max(1, int(min_venues))
    if required > len(venue_frames):
        raise ValueError("min_venues exceeds supplied venue count")
    max_age_ms = max(1.0, float(max_source_age_s) * 1000.0)

    normalized: dict[str, pd.DataFrame] = {}
    starts: list[int] = []
    ends: list[int] = []
    for name, raw in sorted(venue_frames.items()):
        frame = raw.copy()
        if "timestamp" not in frame:
            raise ValueError(f"{name}: missing causal timestamp")
        if "close" not in frame:
            raise ValueError(f"{name}: missing close")
        ts = pd.to_numeric(frame["timestamp"], errors="coerce")
        close = pd.to_numeric(frame["close"], errors="coerce")
        if close.isna().all():
            raise ValueError(f"{name}: missing close")
        if "last_event_ts_ms" in frame:
            event_ts = pd.to_numeric(frame["last_event_ts_ms"], errors="coerce")
        elif "source_age_ms" in frame:
            event_ts = ts - pd.to_numeric(frame["source_age_ms"], errors="coerce")
        else:
            event_ts = ts
        flow = pd.to_numeric(frame.get("flow_imbalance", 0.0), errors="coerce")
        work = pd.DataFrame(
            {"timestamp": ts, "close": close, "event_ts_ms": event_ts, "flow": flow}
        ).dropna(subset=["timestamp", "close", "event_ts_ms"])
        work = work[(work["close"] > 0.0) & (work["event_ts_ms"] <= work["timestamp"])]
        if work.empty:
            raise ValueError(f"{name}: no valid causal rows")
        work["timestamp"] = work["timestamp"].round().astype("int64")
        work = work.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
        work = work.set_index("timestamp")
        normalized[name] = work
        starts.append(int(work.index.min()))
        ends.append(int(work.index.max()))

    start_ms = int(math.ceil(min(starts) / 1000.0) * 1000)
    end_ms = int(math.floor(max(ends) / 1000.0) * 1000)
    grid = pd.Index(np.arange(start_ms, end_ms + 1, 1000, dtype="int64"), name="timestamp")
    prices: dict[str, pd.Series] = {}
    flows: dict[str, pd.Series] = {}
    ages: dict[str, pd.Series] = {}
    exact: dict[str, pd.Series] = {}

    for name, frame in normalized.items():
        carried = frame[["close", "event_ts_ms"]].reindex(grid, method="ffill")
        age_ms = pd.Series(grid.to_numpy() - carried["event_ts_ms"].to_numpy(), index=grid)
        available = age_ms.ge(0.0) & age_ms.le(max_age_ms)
        prices[name] = carried["close"].where(available)
        ages[name] = age_ms.where(available)
        # Flow is a per-second event statistic.  Do not repeat it across quiet
        # seconds merely because the last price is still fresh.
        flows[name] = frame["flow"].reindex(grid)
        exact[name] = available.astype("int8")

    price_frame = pd.DataFrame(prices, index=grid)
    age_frame = pd.DataFrame(ages, index=grid)
    flow_frame = pd.DataFrame(flows, index=grid)
    available_frame = pd.DataFrame(exact, index=grid)
    available_count = price_frame.notna().sum(axis=1)
    valid = available_count.ge(required)

    log_prices = np.log(price_frame)
    consensus_close = np.exp(log_prices.median(axis=1, skipna=True)).where(valid)
    venue_returns = log_prices.diff()
    # Common innovation is the robust cross-section of venue returns.  Taking
    # a return *after* combining price levels creates false moves whenever the
    # fresh venue set changes and the venues have persistent basis offsets.
    return_count = venue_returns.notna().sum(axis=1)
    return_valid = return_count.ge(required)
    consensus_return = venue_returns.median(axis=1, skipna=True).where(return_valid)
    return_sign = np.sign(venue_returns)
    consensus_sign = np.sign(consensus_return)
    agreement = return_sign.eq(consensus_sign, axis=0).where(venue_returns.notna()).sum(axis=1)
    agreement = (agreement / venue_returns.notna().sum(axis=1).replace(0, np.nan)).where(valid)

    out = pd.DataFrame(index=grid)
    out["event_second_ts_ms"] = grid.to_numpy() - 1000
    out["timestamp"] = grid.to_numpy()
    out["level_median_close"] = consensus_close
    base_candidates = consensus_close.dropna()
    if base_candidates.empty:
        common_factor_close = pd.Series(np.nan, index=grid)
    else:
        common_factor_close = (
            float(base_candidates.iloc[0])
            * np.exp(consensus_return.fillna(0.0).cumsum())
        )
    out["common_factor_close"] = common_factor_close.where(valid)
    # Downstream pending/sorting runners consume ``close`` as a price-like
    # index.  It must encode common innovation, not changing venue-level basis.
    out["close"] = out["common_factor_close"]
    out["flow_imbalance"] = flow_frame.median(axis=1, skipna=True).where(valid)
    out["consensus_ret_1s"] = consensus_return
    out["agreement_score"] = agreement
    level_dispersion = price_frame.std(axis=1, ddof=0) / price_frame.mean(axis=1) * 10_000.0
    return_dispersion = venue_returns.std(axis=1, ddof=0) * 10_000.0
    out["level_dispersion_bps"] = level_dispersion.where(valid)
    out["return_dispersion_bps"] = return_dispersion.where(return_valid)
    out["dispersion_bps"] = out["return_dispersion_bps"]
    out["available_venues"] = available_count
    out["available_return_venues"] = return_count
    out["max_source_age_ms"] = age_frame.max(axis=1, skipna=True).where(valid)
    out["source_age_ms"] = out["max_source_age_ms"]
    positive = venue_returns.gt(0.0).sum(axis=1)
    negative = venue_returns.lt(0.0).sum(axis=1)
    out["majority_direction"] = np.select(
        [positive.ge(required), negative.ge(required)], [1, -1], default=0
    ).astype("int8")
    count_confidence = (return_count / max(3, len(normalized))).clip(upper=1.0)
    freshness_confidence = np.exp(
        -out["max_source_age_ms"].fillna(max_age_ms * 2.0) / max_age_ms
    )
    dispersion_confidence = np.exp(
        -out["return_dispersion_bps"].fillna(float("inf"))
        / max(1e-9, float(dispersion_scale_bps))
    )
    out["consensus_confidence"] = (
        count_confidence * agreement.fillna(0.0) * freshness_confidence * dispersion_confidence
    ).where(return_valid, 0.0).clip(0.0, 1.0)
    for name in sorted(normalized):
        out[f"{name}_close"] = price_frame[name]
        out[f"{name}_source_age_ms"] = age_frame[name]
        out[f"{name}_available"] = available_frame[name]
        out[f"{name}_flow_imbalance"] = flow_frame[name]
        other_returns = venue_returns.drop(columns=[name])
        other_count = other_returns.notna().sum(axis=1)
        out[f"leave_{name}_out_ret_1s"] = other_returns.median(
            axis=1, skipna=True
        ).where(other_count.ge(min(required, max(1, len(normalized) - 1))))
        out[f"{name}_return_deviation_bps"] = (
            venue_returns[name] - consensus_return
        ).abs() * 10_000.0
    if len(normalized) >= 3:
        deviation_cols = [f"{name}_return_deviation_bps" for name in sorted(normalized)]
        deviations = out[deviation_cols]
        out["outlier_venue"] = pd.Series(None, index=out.index, dtype="object")
        valid_deviations = deviations.loc[return_valid]
        out.loc[return_valid, "outlier_venue"] = valid_deviations.idxmax(axis=1).str.replace(
            "_return_deviation_bps", "", regex=False
        )
        out["outlier_distance_bps"] = deviations.max(axis=1).where(return_valid)
    else:
        out["outlier_venue"] = None
        out["outlier_distance_bps"] = np.nan
    return out.loc[valid].reset_index(drop=True).replace([np.inf, -np.inf], np.nan)


def build_spot_perp_state_1s(
    perp: pd.DataFrame,
    spot: pd.DataFrame,
    *,
    max_source_age_s: float = 2.0,
    shock_threshold_bps: float = 1.0,
    quiet_threshold_bps: float = 0.35,
) -> pd.DataFrame:
    """Build causal cross-instrument state from already-causal consensuses.

    The basis anchor is shifted by one row.  State labels therefore use only
    observations available at the current right edge and never future repair.
    """
    required = {"timestamp", "close"}
    if not required.issubset(perp) or not required.issubset(spot):
        raise ValueError("spot/perp consensus frames require timestamp and close")

    def prepare(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
        work = frame.copy()
        work["timestamp"] = pd.to_numeric(work["timestamp"], errors="coerce")
        work[f"{prefix}_close"] = pd.to_numeric(work["close"], errors="coerce")
        age_source = work.get("max_source_age_ms", work.get("source_age_ms", 0.0))
        work[f"{prefix}_source_age_ms"] = pd.to_numeric(age_source, errors="coerce")
        for source, target in (
            ("consensus_ret_1s", f"{prefix}_ret_1s"),
            ("agreement_score", f"{prefix}_venue_agreement"),
            ("dispersion_bps", f"{prefix}_venue_dispersion_bps"),
            ("flow_imbalance", f"{prefix}_flow_imbalance"),
            ("available_venues", f"{prefix}_available_venues"),
            ("available_return_venues", f"{prefix}_available_return_venues"),
            ("consensus_confidence", f"{prefix}_confidence"),
            ("majority_direction", f"{prefix}_majority_direction"),
        ):
            work[target] = pd.to_numeric(work.get(source, np.nan), errors="coerce")
        columns = ["timestamp"] + [col for col in work if col.startswith(f"{prefix}_")]
        return work[columns].dropna(subset=["timestamp", f"{prefix}_close"])

    left = prepare(perp, "perp")
    right = prepare(spot, "spot")
    joined = left.merge(right, on="timestamp", how="inner", validate="one_to_one")
    max_age_ms = max(1.0, float(max_source_age_s) * 1000.0)
    fresh = (
        joined["perp_source_age_ms"].between(0.0, max_age_ms)
        & joined["spot_source_age_ms"].between(0.0, max_age_ms)
    )
    joined = joined.loc[fresh].sort_values("timestamp").reset_index(drop=True)
    if joined.empty:
        return joined

    joined["perp_ret_1s_bps"] = joined["perp_ret_1s"] * 10_000.0
    joined["spot_ret_1s_bps"] = joined["spot_ret_1s"] * 10_000.0
    joined["global_perp_move_bps"] = joined["perp_ret_1s_bps"]
    joined["global_spot_move_bps"] = joined["spot_ret_1s_bps"]
    joined["spot_perp_divergence_bps"] = (
        joined["global_perp_move_bps"] - joined["global_spot_move_bps"]
    )
    joined["fresh_perp_venues"] = joined["perp_available_venues"]
    joined["fresh_spot_venues"] = joined["spot_available_venues"]
    joined["consensus_confidence"] = pd.concat(
        [joined["perp_confidence"], joined["spot_confidence"]], axis=1
    ).min(axis=1)
    basis = np.log(joined["perp_close"] / joined["spot_close"]) * 10_000.0
    basis_anchor = basis.rolling(360, min_periods=30).median().shift(1)
    joined["perp_spot_basis_bps"] = basis
    joined["perp_minus_spot_bps"] = basis - basis_anchor
    perp_sign = np.sign(joined["perp_ret_1s_bps"])
    spot_sign = np.sign(joined["spot_ret_1s_bps"])
    joined["spot_perp_agreement"] = np.where(
        (perp_sign != 0.0) & (spot_sign != 0.0), (perp_sign == spot_sign).astype(float), np.nan
    )
    joined["venue_divergence_bps"] = pd.concat(
        [
            joined["perp_venue_dispersion_bps"].abs(),
            joined["spot_venue_dispersion_bps"].abs(),
            joined["perp_minus_spot_bps"].abs(),
        ],
        axis=1,
    ).max(axis=1, skipna=True)

    perp_abs = joined["perp_ret_1s_bps"].abs()
    spot_abs = joined["spot_ret_1s_bps"].abs()
    same_sign = perp_sign.eq(spot_sign) & perp_sign.ne(0.0)
    state = np.full(len(joined), "neutral", dtype=object)
    direction = np.where(spot_sign != 0.0, spot_sign, perp_sign)
    confirmed = same_sign & perp_abs.ge(quiet_threshold_bps) & spot_abs.ge(quiet_threshold_bps)
    spot_leading = spot_abs.ge(shock_threshold_bps) & perp_abs.le(quiet_threshold_bps)
    perp_only = perp_abs.ge(shock_threshold_bps) & spot_abs.le(quiet_threshold_bps)
    divergent = (
        perp_abs.ge(quiet_threshold_bps)
        & spot_abs.ge(quiet_threshold_bps)
        & ~same_sign
    )
    for mask, label, sign_source in (
        (confirmed, "confirmed", direction),
        (spot_leading, "spot_leading", spot_sign),
        (perp_only, "perp_only", perp_sign),
    ):
        sign_values = np.asarray(sign_source)
        state[np.asarray(mask) & (sign_values > 0.0)] = f"{label}_up"
        state[np.asarray(mask) & (sign_values < 0.0)] = f"{label}_down"
    state[divergent] = "divergent"
    joined["cross_instrument_state"] = state
    joined["consensus_direction"] = np.where(
        confirmed, np.sign(joined["global_spot_move_bps"]), 0
    ).astype("int8")
    joined["cross_instrument_available"] = 1
    return joined.replace([np.inf, -np.inf], np.nan)


def _manifest_days(path: Path) -> list[str]:
    with path.expanduser().open(newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))
    days = sorted({str(row.get("day") or row.get("date") or "")[:10] for row in rows})
    return [day for day in days if len(day) == 10]


def _find_daily_feature(directory: Path, symbol: str, day: str) -> Path:
    matches = sorted(directory.expanduser().glob(f"{symbol}*{day}*.parquet"))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"{directory}: expected one {symbol} feature for {day}, got {len(matches)}"
        )
    path = matches[0]
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    if not meta_path.exists():
        raise FileNotFoundError(f"{path}: missing completeness metadata")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if not metadata.get("complete") or str(metadata.get("utc_day", "")) != day:
        raise ValueError(f"{path}: incomplete or wrong-day metadata")
    return path


def _build_daily_cross_instrument(
    day: str,
    perp_dir: Path,
    spot_dir: Path,
    out_dir: Path,
    symbol: str,
    max_source_age_s: float,
    shock_threshold_bps: float,
    quiet_threshold_bps: float,
) -> dict[str, object]:
    perp_path = _find_daily_feature(perp_dir, symbol, day)
    spot_path = _find_daily_feature(spot_dir, symbol, day)
    frame = build_spot_perp_state_1s(
        pd.read_parquet(perp_path),
        pd.read_parquet(spot_path),
        max_source_age_s=max_source_age_s,
        shock_threshold_bps=shock_threshold_bps,
        quiet_threshold_bps=quiet_threshold_bps,
    )
    if frame.empty:
        raise ValueError(f"{day}: empty spot/perp state")
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"{symbol}-spot-perp-state-1s-{day}.parquet"
    temp = target.with_suffix(target.suffix + ".tmp")
    frame.to_parquet(temp, index=False)
    os.replace(temp, target)
    metadata = {
        "complete": True,
        "utc_day": day,
        "symbol": symbol,
        "market_id": f"consensus:spot-perp:{symbol}",
        "perp_source": str(perp_path),
        "spot_source": str(spot_path),
        "rows": len(frame),
        "max_source_age_s": max_source_age_s,
        "shock_threshold_bps": shock_threshold_bps,
        "quiet_threshold_bps": quiet_threshold_bps,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "causal": True,
    }
    target.with_suffix(target.suffix + ".meta.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    return {"day": day, "status": "built", "rows": len(frame), "path": str(target)}


def _build_daily_consensus(
    day: str,
    venue_dirs: dict[str, Path],
    out_dir: Path,
    symbol: str,
    min_venues: int,
    max_source_age_s: float,
    instrument_type: str,
    dispersion_scale_bps: float,
) -> dict[str, object]:
    sources = {
        name: _find_daily_feature(path, symbol, day)
        for name, path in venue_dirs.items()
    }
    frames = {name: pd.read_parquet(path) for name, path in sources.items()}
    consensus = build_causal_consensus_1s(
        frames,
        min_venues=min_venues,
        max_source_age_s=max_source_age_s,
        dispersion_scale_bps=dispersion_scale_bps,
    )
    if consensus.empty:
        raise ValueError(f"{day}: empty consensus")
    day_start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    day_end_ms = day_start_ms + 86_400_000
    if (
        int(consensus["timestamp"].min()) <= day_start_ms
        or int(consensus["timestamp"].max()) > day_end_ms
    ):
        raise ValueError(f"{day}: consensus crosses UTC daily boundary")
    out_dir.mkdir(parents=True, exist_ok=True)
    venue_tag = "-".join(sorted(venue_dirs))
    target = out_dir / f"{symbol}-{venue_tag}-consensus-1s-{day}.parquet"
    temp = target.with_suffix(target.suffix + ".tmp")
    consensus.to_parquet(temp, index=False)
    os.replace(temp, target)
    meta = {
        "complete": True,
        "utc_day": day,
        "symbol": symbol,
        "market_id": f"consensus:{instrument_type}:{symbol}",
        "instrument_type": instrument_type,
        "venues": sorted(venue_dirs),
        "sources": {name: str(path) for name, path in sources.items()},
        "rows": len(consensus),
        "min_venues": min_venues,
        "max_source_age_s": max_source_age_s,
        "dispersion_scale_bps": dispersion_scale_bps,
        "min_ts_ms": int(consensus["timestamp"].min()),
        "max_ts_ms": int(consensus["timestamp"].max()),
        "built_at": datetime.now(timezone.utc).isoformat(),
        "causal_timestamp_rule": "venue trades [t,t+1s) visible at t+1s; no future backfill",
    }
    target.with_suffix(target.suffix + ".meta.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8"
    )
    return {"day": day, "status": "built", "rows": len(consensus), "path": str(target)}


def _numeric(frame: pd.DataFrame, column: str, default: float = math.nan) -> pd.Series:
    value = frame[column] if column in frame else default
    return pd.to_numeric(value, errors="coerce")


def build_hierarchical_reference_1s(
    perp_consensus: pd.DataFrame,
    spot_consensus: pd.DataFrame,
    binance_btcusdt_perp: pd.DataFrame,
    execution_btcusdc_perp: pd.DataFrame,
    binance_btcusdc_spot: pd.DataFrame,
    binance_usdcusdt_spot: pd.DataFrame | None = None,
    *,
    basis_window_s: int = 360,
    basis_min_periods: int = 30,
    max_dispersion_bps: float = 2.0,
    correction_beta: float = 1.0,
    tick_size: float = 0.1,
) -> pd.DataFrame:
    """Build the causal external consensus + Binance local bridge reference."""

    def consensus(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
        work = pd.DataFrame({"timestamp": _numeric(frame, "timestamp")})
        work[f"global_{prefix}_move_bps"] = (
            _numeric(frame, "consensus_ret_1s") * 10_000.0
        )
        work[f"fresh_{prefix}_venues"] = _numeric(
            frame, "available_venues", 0.0
        )
        work[f"{prefix}_confidence"] = _numeric(
            frame, "consensus_confidence", 0.0
        )
        work[f"{prefix}_dispersion_bps"] = _numeric(
            frame, "return_dispersion_bps"
        )
        work[f"{prefix}_majority_direction"] = _numeric(
            frame, "majority_direction", 0.0
        )
        work[f"{prefix}_outlier_venue"] = frame.get("outlier_venue", "")
        return (
            work.dropna(subset=["timestamp"])
            .drop_duplicates("timestamp", keep="last")
        )

    def price(frame: pd.DataFrame, column: str) -> pd.DataFrame:
        values: dict[str, object] = {
            "timestamp": _numeric(frame, "timestamp"),
            column: _numeric(frame, column if column in frame else "mid"),
        }
        if "source_age_ms" in frame:
            values[f"{column}_source_age_ms"] = _numeric(
                frame, "source_age_ms"
            )
        work = pd.DataFrame(values)
        return work.dropna().drop_duplicates("timestamp", keep="last")

    joined = consensus(perp_consensus, "perp").merge(
        consensus(spot_consensus, "spot"),
        on="timestamp",
        how="inner",
        validate="one_to_one",
    )
    for frame, column in (
        (binance_btcusdt_perp, "binance_btcusdt_perp_mid"),
        (execution_btcusdc_perp, "execution_btcusdc_perp_mid"),
        (binance_btcusdc_spot, "binance_btcusdc_spot_mid"),
    ):
        joined = joined.merge(
            price(frame, column),
            on="timestamp",
            how="inner",
            validate="one_to_one",
        )
    if binance_usdcusdt_spot is not None and not binance_usdcusdt_spot.empty:
        joined = joined.merge(
            price(binance_usdcusdt_spot, "binance_usdcusdt_spot_mid"),
            on="timestamp",
            how="left",
            validate="one_to_one",
        )
    joined = joined.sort_values("timestamp").reset_index(drop=True)
    if joined.empty:
        return joined

    consecutive_second = joined["timestamp"].diff().eq(1_000)
    joined["binance_local_move_bps"] = np.log(
        joined["binance_btcusdt_perp_mid"]
        / joined["binance_btcusdt_perp_mid"].shift(1)
    ).where(consecutive_second) * 10_000.0
    joined["perp_spot_divergence_bps"] = (
        joined["global_perp_move_bps"] - joined["global_spot_move_bps"]
    )
    spot_direction = np.sign(joined["global_spot_move_bps"])
    perp_direction = np.sign(joined["global_perp_move_bps"])
    direction_confirmed = spot_direction.eq(perp_direction) & spot_direction.ne(
        0.0
    )
    both_quiet = (
        joined["global_spot_move_bps"].abs().lt(0.05)
        & joined["global_perp_move_bps"].abs().lt(0.05)
    )
    dispersion = pd.concat(
        [joined["spot_dispersion_bps"], joined["perp_dispersion_bps"]], axis=1
    ).max(axis=1)
    joined["cross_venue_dispersion_bps"] = dispersion
    joined["consensus_confidence"] = pd.concat(
        [joined["spot_confidence"], joined["perp_confidence"]], axis=1
    ).min(axis=1)

    stablecoin_mid = joined.get(
        "binance_usdcusdt_spot_mid",
        pd.Series(math.nan, index=joined.index, dtype="float64"),
    )
    converted_bridge = joined["binance_btcusdt_perp_mid"] / stablecoin_mid
    stablecoin_valid = stablecoin_mid.gt(0.0) & np.isfinite(converted_bridge)
    joined["local_bridge_px_usdc"] = converted_bridge.where(
        stablecoin_valid, joined["binance_btcusdc_spot_mid"]
    )
    joined["bridge_source"] = np.where(
        stablecoin_valid,
        "binance_btcusdt_perp/usdcusdt",
        "binance_btcusdc_spot",
    )
    joined["spot_bridge_gap_bps"] = np.log(
        joined["binance_btcusdc_spot_mid"] / joined["local_bridge_px_usdc"]
    ) * 10_000.0
    current_basis = np.log(
        joined["execution_btcusdc_perp_mid"] / joined["local_bridge_px_usdc"]
    ) * 10_000.0
    basis_anchor = current_basis.rolling(
        max(2, int(basis_window_s)),
        min_periods=max(2, int(basis_min_periods)),
    ).median().shift(1)
    joined["bridge_basis_bps"] = basis_anchor
    valid = (
        joined["fresh_spot_venues"].ge(2)
        & joined["fresh_perp_venues"].ge(2)
        & joined["binance_local_move_bps"].notna()
        & (direction_confirmed | both_quiet)
        & dispersion.le(float(max_dispersion_bps))
        & basis_anchor.notna()
    )
    global_move = 0.5 * (
        joined["global_spot_move_bps"] + joined["global_perp_move_bps"]
    )
    joined["external_unabsorbed_bps"] = (
        global_move - joined["binance_local_move_bps"]
    )
    cap_bps = (
        abs(float(tick_size)) / joined["execution_btcusdc_perp_mid"] * 10_000.0
    )
    correction = (
        float(correction_beta)
        * joined["external_unabsorbed_bps"]
        * joined["consensus_confidence"]
    ).clip(lower=-cap_bps, upper=cap_bps)
    joined["external_correction_bps"] = correction.where(valid, 0.0)
    joined["ref_px_usdc"] = joined["local_bridge_px_usdc"] * np.exp(
        (basis_anchor.fillna(0.0) + joined["external_correction_bps"])
        / 10_000.0
    )
    joined["residual_bps"] = np.log(
        joined["ref_px_usdc"] / joined["execution_btcusdc_perp_mid"]
    ) * 10_000.0
    joined["consensus_direction"] = np.where(
        direction_confirmed & global_move.abs().ge(0.05),
        np.sign(global_move),
        0,
    ).astype("int8")
    joined["global_reference_valid"] = valid.astype("int8")
    joined["close"] = joined["ref_px_usdc"]
    return joined.replace([np.inf, -np.inf], np.nan)


def _causal_bbo(path: Path, day: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    raw = pd.DataFrame(
        {
            "event_ts": _numeric(frame, "timestamp"),
            "mid": 0.5
            * (_numeric(frame, "best_bid") + _numeric(frame, "best_ask")),
        }
    ).dropna()
    day_start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    grid = pd.DataFrame(
        {
            "timestamp": np.arange(
                day_start + 1000, day_start + 86_400_001, 1000
            )
        }
    )
    aligned = pd.merge_asof(
        grid,
        raw.sort_values("event_ts"),
        left_on="timestamp",
        right_on="event_ts",
        direction="backward",
        tolerance=2_000,
    )
    aligned["source_age_ms"] = aligned["timestamp"] - aligned["event_ts"]
    return aligned[["timestamp", "mid", "source_age_ms"]].dropna(
        subset=["mid"]
    )


def _causal_futures_trade_bar(
    path: Path,
    day: str,
    *,
    max_source_age_s: float = 2.0,
) -> pd.DataFrame:
    """Expose official Binance 1s trade bars only after their right edge.

    Existing Binance bars store their UTC-second left edge in the index. The
    close for ``[t, t+1s)`` therefore first becomes usable at ``t+1s``. Older
    bars do not contain the exact final trade timestamp, so their left edge is
    used as a conservative event-time lower bound for freshness. A future bar
    schema may provide ``last_event_ts_ms`` to remove that one-second bound.
    """

    frame = pd.read_parquet(path)
    bar_start_ms = pd.to_numeric(frame.index, errors="coerce")
    close = _numeric(frame, "close")
    if "trade_count" in frame:
        has_trade = _numeric(frame, "trade_count", 0.0).gt(0.0)
    else:
        has_trade = close.notna()
    if "last_event_ts_ms" in frame:
        event_ts_ms = _numeric(frame, "last_event_ts_ms")
    else:
        event_ts_ms = pd.Series(
            bar_start_ms,
            index=frame.index,
            dtype="float64",
        )
    raw = pd.DataFrame(
        {
            "visible_ts": np.asarray(bar_start_ms, dtype="float64") + 1_000.0,
            "event_ts": event_ts_ms.to_numpy(dtype="float64"),
            "mid": close.to_numpy(dtype="float64"),
            "has_trade": has_trade.to_numpy(dtype="bool"),
        }
    ).dropna(subset=["visible_ts", "event_ts", "mid"])
    raw = raw[
        raw["has_trade"]
        & raw["mid"].gt(0.0)
        & raw["event_ts"].le(raw["visible_ts"])
    ]
    if raw.empty:
        raise ValueError(f"{path}: no valid causal trade bars")
    raw["visible_ts"] = raw["visible_ts"].round().astype("int64")
    raw["event_ts"] = raw["event_ts"].round().astype("int64")
    raw = raw.sort_values("visible_ts").drop_duplicates(
        "visible_ts", keep="last"
    )

    day_start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    grid = pd.DataFrame(
        {
            "timestamp": np.arange(
                day_start + 1_000,
                day_start + 86_400_001,
                1_000,
                dtype="int64",
            )
        }
    )
    aligned = pd.merge_asof(
        grid,
        raw[["visible_ts", "event_ts", "mid"]],
        left_on="timestamp",
        right_on="visible_ts",
        direction="backward",
    )
    aligned["source_age_ms"] = aligned["timestamp"] - aligned["event_ts"]
    max_age_ms = max(1.0, float(max_source_age_s) * 1_000.0)
    valid = aligned["source_age_ms"].between(0.0, max_age_ms)
    return aligned.loc[valid, ["timestamp", "mid", "source_age_ms"]]


def _validate_individual_trade_bar_identity(
    path: Path,
    day: str,
) -> dict[str, object]:
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    if not meta_path.is_file():
        raise FileNotFoundError(f"{path}: missing individual-trade bar metadata")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if str(metadata.get("schema_version", "")) != BRIDGE_BAR_SCHEMA_VERSION:
        raise ValueError(f"{path}: unsupported trade-bar metadata schema")
    if not metadata.get("complete"):
        raise ValueError(f"{path}: incomplete trade-bar metadata")
    if str(metadata.get("utc_day", "")) != day:
        raise ValueError(f"{path}: trade-bar metadata UTC day mismatch")
    if str(metadata.get("source_data_type", "")) != "trades":
        raise ValueError(f"{path}: historical bridge requires individual trades")
    expected_sha256 = str(metadata.get("output_sha256", ""))
    if not expected_sha256:
        raise ValueError(f"{path}: missing output SHA256")
    if _sha256(path) != expected_sha256:
        raise ValueError(f"{path}: individual-trade bar SHA256 mismatch")
    return metadata


def _causal_spot(path: Path, day: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    raw = pd.DataFrame(
        {
            "timestamp": pd.to_numeric(
                frame.index, errors="raise"
            ).to_numpy(dtype="int64")
            + 1000,
            "mid": _numeric(frame, "close").to_numpy(dtype="float64"),
        }
    ).dropna()
    day_start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    grid = pd.DataFrame(
        {
            "timestamp": np.arange(
                day_start + 1000, day_start + 86_400_001, 1000
            )
        }
    )
    return pd.merge_asof(
        grid,
        raw.sort_values("timestamp"),
        on="timestamp",
        direction="backward",
        tolerance=2_000,
    ).dropna(subset=["mid"])


def _build_daily_hierarchical(
    day: str,
    perp_dir: Path,
    spot_dir: Path,
    futures_bar_dir: Path,
    execution_bbo_dir: Path,
    spot_bar_dir: Path,
    stablecoin_bar_dir: Path | None,
    out_dir: Path,
    basis_window_s: int,
    basis_min_periods: int,
    max_dispersion_bps: float,
    max_bridge_source_age_s: float,
) -> dict[str, object]:
    perp_path = _find_daily_feature(perp_dir, "BTCUSDT", day)
    spot_path = _find_daily_feature(spot_dir, "BTCUSDT", day)
    ref_trade_bar_path = futures_bar_dir / f"BTCUSDT-1s-{day}.parquet"
    exec_bbo_path = execution_bbo_dir / f"BTCUSDC-bbo-{day}.parquet"
    exec_spot_path = spot_bar_dir / f"BTCUSDC-1s-{day}.parquet"
    required_paths = [ref_trade_bar_path, exec_bbo_path, exec_spot_path]
    stablecoin_spot_path = (
        stablecoin_bar_dir / f"USDCUSDT-1s-{day}.parquet"
        if stablecoin_bar_dir is not None
        else None
    )
    if stablecoin_spot_path is not None:
        required_paths.append(stablecoin_spot_path)
    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(path)
    bridge_metadata = _validate_individual_trade_bar_identity(
        ref_trade_bar_path,
        day,
    )

    frame = build_hierarchical_reference_1s(
        pd.read_parquet(perp_path),
        pd.read_parquet(spot_path),
        _causal_futures_trade_bar(
            ref_trade_bar_path,
            day,
            max_source_age_s=max_bridge_source_age_s,
        ).rename(
            columns={"mid": "binance_btcusdt_perp_mid"}
        ),
        _causal_bbo(exec_bbo_path, day).rename(
            columns={"mid": "execution_btcusdc_perp_mid"}
        ),
        _causal_spot(exec_spot_path, day).rename(
            columns={"mid": "binance_btcusdc_spot_mid"}
        ),
        (
            _causal_spot(stablecoin_spot_path, day).rename(
                columns={"mid": "binance_usdcusdt_spot_mid"}
            )
            if stablecoin_spot_path is not None
            else None
        ),
        basis_window_s=basis_window_s,
        basis_min_periods=basis_min_periods,
        max_dispersion_bps=max_dispersion_bps,
    )
    if frame.empty:
        raise ValueError(f"{day}: empty hierarchical reference")
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"BTCUSDC-global-reference-1s-{day}.parquet"
    temp = target.with_suffix(target.suffix + ".tmp")
    frame.to_parquet(temp, index=False)
    os.replace(temp, target)
    bridge_metadata_path = ref_trade_bar_path.with_suffix(
        ref_trade_bar_path.suffix + ".meta.json"
    )

    def source_identity(path: Path) -> dict[str, object]:
        return {
            "path": str(path),
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }

    metadata = {
        "complete": True,
        "utc_day": day,
        "market_id": "global_reference:BTCUSDC",
        "rows": len(frame),
        "valid_rows": int(frame["global_reference_valid"].sum()),
        "perp_consensus": str(perp_path),
        "spot_consensus": str(spot_path),
        "binance_bridge": str(ref_trade_bar_path),
        "binance_bridge_source": "official_binance_individual_trade_bar_1s",
        "binance_bridge_artifact_sha256": bridge_metadata["output_sha256"],
        "binance_bridge_metadata": str(bridge_metadata_path),
        "binance_bridge_price": "last_trade_close",
        "binance_bridge_visibility": "bar [t,t+1s) visible at t+1s",
        "binance_bridge_max_source_age_s": max_bridge_source_age_s,
        "target_currency_anchor": str(
            stablecoin_spot_path or exec_spot_path
        ),
        "target_currency_anchor_fallback": str(exec_spot_path),
        "execution_market": str(exec_bbo_path),
        "basis_window_s": basis_window_s,
        "basis_min_periods": basis_min_periods,
        "max_dispersion_bps": max_dispersion_bps,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "causal": True,
        "policy_effect": "none",
        "output_sha256": _sha256(target),
        "source_identity": {
            "perp_consensus": source_identity(perp_path),
            "spot_consensus": source_identity(spot_path),
            "binance_bridge": source_identity(ref_trade_bar_path),
            "binance_bridge_metadata": source_identity(
                bridge_metadata_path
            ),
            "execution_market": source_identity(exec_bbo_path),
            "target_currency_anchor": source_identity(
                stablecoin_spot_path or exec_spot_path
            ),
            "target_currency_anchor_fallback": source_identity(
                exec_spot_path
            ),
        },
    }
    target.with_suffix(target.suffix + ".meta.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "day": day,
        "status": "built",
        "rows": len(frame),
        "valid_rows": metadata["valid_rows"],
        "path": str(target),
    }


def _write_build_status(
    results: list[dict[str, object]], status_path: Path
) -> int:
    status_path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in results for key in row})
    temp = status_path.with_suffix(status_path.suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(results, key=lambda row: str(row["day"])))
    os.replace(temp, status_path)
    return sum(row["status"] != "built" for row in results)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("auto", "consensus", "cross-instrument", "hierarchical"),
        default="auto",
    )
    parser.add_argument(
        "--venue-dir",
        action="append",
        nargs=2,
        metavar=("NAME", "FEATURE_DIR"),
        help="Daily causal 1s feature directory; use with --manifest/--out-dir",
    )
    parser.add_argument("--manifest", type=Path, help="Retained-day CSV for daily 1s consensus")
    parser.add_argument("--out-dir", type=Path, help="Daily 1s consensus output directory")
    parser.add_argument("--perp-consensus-dir", type=Path)
    parser.add_argument("--spot-consensus-dir", type=Path)
    parser.add_argument("--cross-out-dir", type=Path)
    parser.add_argument(
        "--binance-futures-bar-dir",
        type=Path,
        help="Official Binance futures 1s trade-bar directory for BTCUSDT",
    )
    parser.add_argument(
        "--binance-execution-bbo-dir",
        type=Path,
        help="BTCUSDC execution-market BBO directory",
    )
    parser.add_argument(
        "--binance-bbo-dir",
        type=Path,
        dest="legacy_binance_bbo_dir",
        help=(
            "Deprecated alias for --binance-execution-bbo-dir; BTCUSDT BBO "
            "is no longer accepted as the historical bridge"
        ),
    )
    parser.add_argument("--binance-spot-bar-dir", type=Path)
    parser.add_argument("--binance-stablecoin-bar-dir", type=Path)
    parser.add_argument("--basis-window-s", type=int, default=360)
    parser.add_argument("--basis-min-periods", type=int, default=30)
    parser.add_argument("--max-dispersion-bps", type=float, default=2.0)
    parser.add_argument("--max-bridge-source-age-s", type=float, default=2.0)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--instrument-type", choices=("perp", "spot"), default="perp")
    parser.add_argument("--min-venues", type=int, default=2)
    parser.add_argument("--max-source-age-s", type=float, default=2.0)
    parser.add_argument("--dispersion-scale-bps", type=float, default=1.0)
    parser.add_argument("--shock-threshold-bps", type=float, default=1.0)
    parser.add_argument("--quiet-threshold-bps", type=float, default=0.35)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-days", type=int, default=0)
    args = parser.parse_args()

    if args.mode == "hierarchical":
        execution_bbo_dir = (
            args.binance_execution_bbo_dir or args.legacy_binance_bbo_dir
        )
        required = (
            args.manifest,
            args.perp_consensus_dir,
            args.spot_consensus_dir,
            args.binance_futures_bar_dir,
            execution_bbo_dir,
            args.binance_spot_bar_dir,
            args.out_dir,
        )
        if not all(required):
            raise SystemExit(
                "hierarchical mode requires --manifest, --perp-consensus-dir, "
                "--spot-consensus-dir, --binance-futures-bar-dir, "
                "--binance-execution-bbo-dir, "
                "--binance-spot-bar-dir, and --out-dir"
            )
        days = _manifest_days(args.manifest)
        if args.max_days > 0:
            days = days[: args.max_days]
        results: list[dict[str, object]] = []
        with ProcessPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
            futures = {
                pool.submit(
                    _build_daily_hierarchical,
                    day,
                    args.perp_consensus_dir,
                    args.spot_consensus_dir,
                    args.binance_futures_bar_dir,
                    execution_bbo_dir,
                    args.binance_spot_bar_dir,
                    args.binance_stablecoin_bar_dir,
                    args.out_dir,
                    args.basis_window_s,
                    args.basis_min_periods,
                    args.max_dispersion_bps,
                    args.max_bridge_source_age_s,
                ): day
                for day in days
            }
            for index, future in enumerate(as_completed(futures), 1):
                day = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "day": day,
                        "status": "error",
                        "rows": 0,
                        "error": str(exc),
                    }
                results.append(result)
                print(
                    f"[{index:03d}/{len(days):03d}] {day} {result['status']}",
                    flush=True,
                )
        status_path = args.out_dir / "global_reference_build_status.csv"
        failures = _write_build_status(results, status_path)
        print(
            json.dumps(
                {
                    "days": len(days),
                    "failures": failures,
                    "status": str(status_path),
                }
            )
        )
        raise SystemExit(1 if failures else 0)

    cross_requested = args.mode == "cross-instrument" or (
        args.mode == "auto"
        and bool(
            args.perp_consensus_dir
            or args.spot_consensus_dir
            or args.cross_out_dir
        )
    )
    if cross_requested:
        if not all(
            (
                args.perp_consensus_dir,
                args.spot_consensus_dir,
                args.cross_out_dir,
                args.manifest,
            )
        ):
            raise SystemExit(
                "cross-instrument mode requires --perp-consensus-dir, "
                "--spot-consensus-dir, --cross-out-dir, and --manifest"
            )
        days = _manifest_days(args.manifest)
        if args.max_days > 0:
            days = days[: args.max_days]
        results: list[dict[str, object]] = []
        with ProcessPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
            futures = {
                pool.submit(
                    _build_daily_cross_instrument,
                    day,
                    args.perp_consensus_dir,
                    args.spot_consensus_dir,
                    args.cross_out_dir,
                    args.symbol,
                    args.max_source_age_s,
                    args.shock_threshold_bps,
                    args.quiet_threshold_bps,
                ): day
                for day in days
            }
            for index, future in enumerate(as_completed(futures), 1):
                day = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"day": day, "status": "error", "rows": 0, "error": str(exc)}
                results.append(result)
                print(f"[{index:03d}/{len(days):03d}] {day} {result['status']}", flush=True)
        status_path = args.cross_out_dir / "spot_perp_state_build_status.csv"
        fields = sorted({key for row in results for key in row})
        args.cross_out_dir.mkdir(parents=True, exist_ok=True)
        with status_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(sorted(results, key=lambda row: str(row["day"])))
        failures = [row for row in results if row["status"] != "built"]
        print(
            json.dumps(
                {
                    "days": len(days),
                    "failures": len(failures),
                    "status_out": str(status_path),
                }
            )
        )
        raise SystemExit(1 if failures else 0)

    consensus_requested = args.mode == "consensus" or (
        args.mode == "auto" and bool(args.venue_dir)
    )
    if consensus_requested:
        if args.manifest is None or args.out_dir is None:
            raise SystemExit("--venue-dir requires --manifest and --out-dir")
        venue_dirs = {name: Path(path).expanduser() for name, path in args.venue_dir}
        out_dir = args.out_dir.expanduser().resolve()
        if len(venue_dirs) < args.min_venues:
            raise SystemExit("not enough independent venue directories")
        days = _manifest_days(args.manifest)
        if args.max_days > 0:
            days = days[: args.max_days]
        if not days:
            raise SystemExit("empty retained-day manifest")
        results: list[dict[str, object]] = []
        with ProcessPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
            futures = {
                pool.submit(
                    _build_daily_consensus,
                    day,
                    venue_dirs,
                    out_dir,
                    args.symbol,
                    args.min_venues,
                    args.max_source_age_s,
                    args.instrument_type,
                    args.dispersion_scale_bps,
                ): day
                for day in days
            }
            for index, future in enumerate(as_completed(futures), 1):
                day = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"day": day, "status": "error", "rows": 0, "error": str(exc)}
                results.append(result)
                print(f"[{index:03d}/{len(days):03d}] {day} {result['status']}", flush=True)
        status_path = out_dir / "external_consensus_build_status.csv"
        fields = sorted({key for row in results for key in row})
        temp = status_path.with_suffix(status_path.suffix + ".tmp")
        with temp.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(sorted(results, key=lambda row: str(row["day"])))
        os.replace(temp, status_path)
        failures = [row for row in results if row["status"] != "built"]
        print(
            json.dumps(
                {
                    "days": len(days),
                    "complete": len(days) - len(failures),
                    "failures": len(failures),
                    "status_out": str(status_path),
                },
                sort_keys=True,
            )
        )
        raise SystemExit(1 if failures else 0)

    raise SystemExit(
        "choose --mode consensus, cross-instrument, or hierarchical "
        "(auto also infers consensus/cross-instrument from their arguments)"
    )


if __name__ == "__main__":
    main()
