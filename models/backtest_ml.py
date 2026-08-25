#!/usr/bin/env python3
"""
Step 4b: ML-Enhanced Backtest — Uses LightGBM predictions to improve AS quotes.

ML enhancements over pure AS:
  1. Volatility override: pred_vol → replaces rolling σ² for spread sizing
  2. Direction skew:      pred_dir → shifts reservation price toward predicted side
  3. Confidence gating:   only apply ML when prediction confidence is high

Usage:
  python models/backtest_ml.py                         # default: test set
  python models/backtest_ml.py --sweep                 # sweep ML params
  python models/backtest_ml.py --no-ml                 # pure AS baseline comparison
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from itertools import product
from multiprocessing import cpu_count
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

try:
    from numba import njit as _njit
    HAS_NUMBA = True
    def njit_opt(f):
        return _njit(cache=True)(f)
except ImportError:
    HAS_NUMBA = False
    def njit_opt(f):
        return f

try:
    from models.backtest_config import build_backtest_base_params, disable_ml_params
    from models.backtest_utils import attach_selection_scores as _attach_selection_scores, default_backtest_workers
    from models.queue_calibration import (
        calibration_path as _queue_calibration_path,
        load_daily_queue_calibration,
        build_daily_queue_arrays,
    )
    from models.symbol_paths import ROOT, DEFAULT_SYMBOL, data_root, update_symbol_globals
except ImportError:
    from backtest_config import build_backtest_base_params, disable_ml_params
    from backtest_utils import attach_selection_scores as _attach_selection_scores, default_backtest_workers
    from queue_calibration import (
        calibration_path as _queue_calibration_path,
        load_daily_queue_calibration,
        build_daily_queue_arrays,
    )
    from symbol_paths import ROOT, DEFAULT_SYMBOL, data_root, update_symbol_globals

from data_paths import cache_root, normalized_l2_root

try:
    from data_quality import filter_frame_for_orderbook_quality, filter_paths_for_orderbook_quality
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data_quality import filter_frame_for_orderbook_quality, filter_paths_for_orderbook_quality

DATA_ROOT = data_root(ROOT)
BARS_DIR = DATA_ROOT / "bars_1s"
L2_DIR = normalized_l2_root(ROOT) / "l2"

try:
    from models.backtest_config import DEFAULT_LIQ_BASELINE
except ImportError:
    from backtest_config import DEFAULT_LIQ_BASELINE

SYMBOL = DEFAULT_SYMBOL
MODEL_DIR: Path
RESULTS_DIR: Path
PREDICTIONS_PATH: Path
update_symbol_globals(
    globals(), SYMBOL,
    model_key="MODEL_DIR", results_key="RESULTS_DIR", predictions_key="PREDICTIONS_PATH",
)
MODEL_DIR = Path(globals()["MODEL_DIR"])
RESULTS_DIR = Path(globals()["RESULTS_DIR"])
PREDICTIONS_PATH = Path(globals()["PREDICTIONS_PATH"])


def configure_symbol(symbol=None):
    update_symbol_globals(
        globals(), symbol,
        model_key="MODEL_DIR", results_key="RESULTS_DIR", predictions_key="PREDICTIONS_PATH",
    )

TICK = 0.1


def _resolve_cap_params(params):
    max_spread_bps = float(params.get("max_spread_bps", 0.0))
    dynamic_cap_enabled = bool(params.get("dynamic_cap_enabled", False))
    dynamic_cap_base_bps = float(params.get("dynamic_cap_base_bps", max_spread_bps))
    if dynamic_cap_base_bps <= 0.0 and max_spread_bps > 0.0:
        dynamic_cap_base_bps = max_spread_bps
    dynamic_cap_alpha = float(params.get("dynamic_cap_alpha", 0.5))
    dynamic_cap_max_mult = float(params.get("dynamic_cap_max_mult", 2.0))
    if dynamic_cap_max_mult < 1.0:
        dynamic_cap_max_mult = 1.0
    dynamic_cap_var_baseline = float(params.get("dynamic_cap_var_baseline", 0.0))
    if dynamic_cap_var_baseline <= 0.0:
        vol_baseline = float(params.get("vol_baseline", 0.0))
        if vol_baseline > 0.0:
            dynamic_cap_var_baseline = vol_baseline * vol_baseline
    return (
        dynamic_cap_enabled,
        max_spread_bps,
        dynamic_cap_base_bps,
        dynamic_cap_alpha,
        dynamic_cap_max_mult,
        dynamic_cap_var_baseline,
    )


def _cap_label_from_fields(cap_mode, cap_bps):
    if cap_bps <= 0.0:
        return "off"
    if cap_mode == "dynamic":
        return f"D{cap_bps:.0f}"
    return f"{cap_bps:.0f}"


def _cap_cell(result):
    if "cap_label" in result:
        return str(result["cap_label"])
    cap_mode = result.get("cap_mode", "fixed")
    cap_bps = result.get("cap_base_bps", result.get("max_sprd_bps", 0.0))
    return _cap_label_from_fields(cap_mode, cap_bps)


# ═══════════════════════════════════════════════════════════════════
#  1. DATA — merge 1s bars with 10s ML predictions
# ═══════════════════════════════════════════════════════════════════

def _daily_tag_from_bar_path(path: Path) -> str | None:
    prefix = f"{SYMBOL}-1s-"
    tag = path.stem.removeprefix(prefix)
    return tag if len(tag) == 10 else None


def load_test_bars(day_start: str | None = None, day_end: str | None = None):
    """Load daily 1s bars for a requested UTC day range.

    This file is a legacy bar-level ML backtest.  It must not silently stitch
    hard-coded months together; if no day range is supplied, use the latest
    available quality-filtered day as a smoke window.
    """
    files = sorted(BARS_DIR.glob(f"{SYMBOL}-1s-*.parquet"))
    daily_files = []
    for path in files:
        tag = _daily_tag_from_bar_path(path)
        if tag is None:
            continue
        if day_start and tag < day_start:
            continue
        if day_end and tag > day_end:
            continue
        daily_files.append(path)
    files = daily_files
    files = filter_paths_for_orderbook_quality(files, SYMBOL, label="1s bar")
    if not files:
        raise SystemExit(
            f"No quality-approved daily bars found for {SYMBOL} "
            f"range {day_start or '*'}..{day_end or '*'}"
        )
    if day_start is None and day_end is None:
        files = [files[-1]]
        print(f"  [INFO] No --day-start/--day-end supplied; using latest daily smoke file {files[0].name}")
    dfs = []
    for f in files:
        available_cols = pq.ParquetFile(f).schema_arrow.names
        cols = [c for c in ["high", "low", "close", "trade_count"] if c in available_cols]
        dfs.append(pd.read_parquet(f, columns=cols))
        print(f"  {f.name}: {len(dfs[-1]):>10,} bars")
    bars = pd.concat(dfs).sort_index()
    bars = bars[~bars.index.duplicated(keep="first")]
    bars = filter_frame_for_orderbook_quality(bars, SYMBOL, label="1s bar")
    print(f"  Total: {len(bars):,} 1s bars\n")
    return bars


def load_predictions():
    """Load ML predictions (10s resolution)."""
    pred = pd.read_parquet(PREDICTIONS_PATH)
    pred = filter_frame_for_orderbook_quality(pred, SYMBOL, label="prediction")
    print(f"  Predictions: {len(pred):,} rows")
    # Keep only prediction columns + close for alignment
    pred_cols = [c for c in pred.columns if c.startswith("pred_")]
    print(f"  Prediction cols: {pred_cols}")
    keep_cols = pred_cols + (["close"] if "close" in pred.columns else [])
    return pred[keep_cols]


def _safe_divide(numerator, denominator):
    num = np.asarray(numerator, dtype=np.float64)
    den = np.asarray(denominator, dtype=np.float64)
    out = np.zeros_like(num, dtype=np.float64)
    np.divide(num, den, out=out, where=den > 0)
    return out


def _load_l2_execution_proxy(path):
    """Reduce raw historical L2 snapshots to 1s execution-aligned proxies."""
    try:
        parquet_file = pq.ParquetFile(path)
    except Exception as exc:
        print(f"  [WARN] Failed to open L2 file {path.name}: {exc}")
        return None

    state_frames = []
    bid_px_cols = [f"bid_px_{level}" for level in range(1, 11)]
    bid_qty_cols = [f"bid_qty_{level}" for level in range(1, 11)]
    ask_px_cols = [f"ask_px_{level}" for level in range(1, 11)]
    ask_qty_cols = [f"ask_qty_{level}" for level in range(1, 11)]

    try:
        for batch in parquet_file.iter_batches(batch_size=250_000):
            frame = batch.to_pandas()
            if frame.empty:
                continue

            required = ["timestamp", *bid_px_cols, *bid_qty_cols, *ask_px_cols, *ask_qty_cols]
            if any(col not in frame.columns for col in required):
                print(f"  [WARN] L2 file missing required columns, skipped: {path.name}")
                return None

            ts_ms = pd.to_numeric(frame["timestamp"], errors="coerce").fillna(0).astype("int64").to_numpy(copy=False)
            bid_px = frame[bid_px_cols].to_numpy(dtype=np.float64, copy=False)
            bid_qty = np.nan_to_num(frame[bid_qty_cols].to_numpy(dtype=np.float64, copy=False), nan=0.0)
            ask_px = frame[ask_px_cols].to_numpy(dtype=np.float64, copy=False)
            ask_qty = np.nan_to_num(frame[ask_qty_cols].to_numpy(dtype=np.float64, copy=False), nan=0.0)

            best_bid = bid_px[:, 0]
            best_ask = ask_px[:, 0]
            valid = (ts_ms > 0) & (best_bid > 0.0) & (best_ask > best_bid)
            if not valid.any():
                continue

            ts_ms = ts_ms[valid]
            bid_qty = bid_qty[valid]
            ask_qty = ask_qty[valid]

            bid_cum = np.cumsum(bid_qty, axis=1)
            ask_cum = np.cumsum(ask_qty, axis=1)
            book_imb = _safe_divide(
                bid_cum[:, 9] - ask_cum[:, 9],
                bid_cum[:, 9] + ask_cum[:, 9],
            )
            avg_top5_depth = 0.5 * (bid_cum[:, 4] + ask_cum[:, 4])

            out = pd.DataFrame({
                "bucket_ts": (ts_ms // 1000) * 1000,
                "book_imb": book_imb,
                "depth_near": avg_top5_depth,
            })
            state_frames.append(out.groupby("bucket_ts", sort=False).last())
    except Exception as exc:
        print(f"  [WARN] Failed to decode L2 execution proxy from {path.name}: {exc}")
        return None

    if not state_frames:
        return None

    reduced = pd.concat(state_frames, axis=0)
    reduced = reduced.groupby(level=0, sort=True).last()
    reduced.index = pd.to_datetime(reduced.index.astype(np.int64), unit="ms", utc=True)
    reduced.index.name = None
    return reduced[["book_imb", "depth_near"]]


def _load_book_imbalance(bars_1s, ts_ms):
    """
    Load historical L2 data, compute execution-aligned book imbalance /
    near-depth proxies, and forward-fill to 1s bar resolution.

    Returns: (book_imb, depth_near) tuple of np.ndarray shape (n,).
    """
    n = len(ts_ms)
    book_imb = np.zeros(n, dtype=np.float64)
    depth_near = np.zeros(n, dtype=np.float64)

    files = sorted(L2_DIR.glob(f"{SYMBOL}-l2-*.parquet"))
    files = filter_paths_for_orderbook_quality(files, SYMBOL, label="L2")
    if not files:
        print("  [WARN] No historical L2 files found, book_imb = 0")
        return book_imb, depth_near

    # Determine date range from bars
    ts_min = ts_ms[0]
    ts_max = ts_ms[-1]
    if ts_min > 1e15:  # nanoseconds
        ts_min_dt = pd.Timestamp(ts_min, unit="ns")
        ts_max_dt = pd.Timestamp(ts_max, unit="ns")
    else:  # milliseconds
        ts_min_dt = pd.Timestamp(ts_min, unit="ms")
        ts_max_dt = pd.Timestamp(ts_max, unit="ms")

    # Filter files to overlapping UTC days.
    dfs = []
    for f in files:
        tag = f.stem.replace(f"{SYMBOL}-l2-", "")
        if len(tag) == 10:
            start = pd.Timestamp(tag, tz="UTC")
            end = start + pd.Timedelta(days=1)
        else:
            continue
        if end < ts_min_dt.tz_localize("UTC") or start > ts_max_dt.tz_localize("UTC"):
            continue
        reduced = _load_l2_execution_proxy(f)
        if reduced is not None and not reduced.empty:
            dfs.append(reduced)

    if not dfs:
        print("  [WARN] No L2 files overlap with bars range, book_imb = 0")
        return book_imb, depth_near

    depth = pd.concat(dfs).sort_index()
    depth = depth[~depth.index.duplicated(keep="last")]
    depth = filter_frame_for_orderbook_quality(depth, SYMBOL, label="L2 execution proxy")
    print(f"  L2 execution proxy: {len(depth):,} seconds")

    # Convert explicitly instead of assuming pandas stores DatetimeIndex in ns;
    # recent pandas builds may preserve us/s resolution.
    depth_ts = depth.index.to_numpy(dtype="datetime64[ms]").astype(np.int64)

    # Forward-fill: for each 1s bar, use most recent depth snapshot
    depth_near = np.zeros(n, dtype=np.float64)
    j = 0
    n_depth = len(depth_ts)
    for i in range(n):
        while j < n_depth - 1 and depth_ts[j + 1] <= ts_ms[i]:
            j += 1
        if depth_ts[j] <= ts_ms[i]:
            book_imb[i] = depth["book_imb"].iloc[j]
            depth_near[i] = depth["depth_near"].iloc[j]

    return book_imb, depth_near


def build_ml_arrays(bars_1s, pred_10s, sigma_window=60):
    """
    For each 1s bar, find the most recent 10s prediction and forward-fill.
    Returns aligned numpy arrays for the Numba simulation core.
    """
    # Convert indices to int64 ms timestamps
    # Keep the legacy bar simulator on the same epoch-ms clock as prediction
    # timestamps, requote/timeout parameters, and metric normalization.
    ts = bars_1s.index.to_numpy(dtype="datetime64[ms]").astype(np.int64)

    # 1s bar arrays
    hi = bars_1s["high"].values.astype(np.float64)
    lo = bars_1s["low"].values.astype(np.float64)
    cl = bars_1s["close"].values.astype(np.float64)

    # Rolling variance from 1s bars
    diffs = np.empty_like(cl)
    diffs[0] = 0.0
    diffs[1:] = cl[1:] - cl[:-1]
    ssq = (pd.Series(diffs)
           .rolling(sigma_window, min_periods=max(10, sigma_window // 3))
           .var()
           .ffill().bfill()
           .values.astype(np.float64))
    ssq = np.maximum(ssq, 1e-6)

    # Prediction timestamps — convert from DatetimeIndex to int64 ms
    pred_ts_arr = pred_10s.index.to_numpy(dtype="datetime64[ms]").astype(np.int64)

    # Build prediction arrays aligned to 1s bars via forward-fill merge
    # pred_dir: direction probability (0.5 = neutral)
    # pred_vol: predicted volatility (used to scale σ²)
    # pred_ret: predicted return (used as reservation price offset)
    n = len(ts)
    pred_dir = np.full(n, 0.5, dtype=np.float64)
    pred_vol = np.zeros(n, dtype=np.float64)
    pred_ret = np.zeros(n, dtype=np.float64)

    # Get prediction values
    dir_col = "pred_dir_10s"
    vol_col = "pred_vol_10s"
    ret_col = "pred_ret_10s"
    has_dir = dir_col in pred_10s.columns
    has_vol = vol_col in pred_10s.columns
    has_ret = ret_col in pred_10s.columns

    if has_dir:
        pred_dir_vals = pred_10s[dir_col].values.astype(np.float64)
    if has_vol:
        pred_vol_vals = pred_10s[vol_col].values.astype(np.float64)
    if has_ret:
        pred_ret_vals = pred_10s[ret_col].values.astype(np.float64)

    # Forward-fill: for each 1s bar, copy most recent 10s prediction
    j = 0
    for i in range(n):
        while j < len(pred_ts_arr) - 1 and pred_ts_arr[j + 1] <= ts[i]:
            j += 1
        if pred_ts_arr[j] <= ts[i]:
            if has_dir:
                pred_dir[i] = pred_dir_vals[j]
            if has_vol:
                pred_vol[i] = pred_vol_vals[j]
            if has_ret:
                pred_ret[i] = pred_ret_vals[j]

    print(f"  ML arrays built: {n:,} bars")
    print(f"  pred_dir range: [{pred_dir.min():.4f}, {pred_dir.max():.4f}]")
    print(f"  pred_vol range: [{pred_vol.min():.6f}, {pred_vol.max():.6f}]")
    print(f"  pred_ret range: [{pred_ret.min():.8f}, {pred_ret.max():.8f}]")

    # ── Book imbalance from execution-aligned historical L2 ──
    book_imb, depth_near = _load_book_imbalance(bars_1s, ts)
    print(f"  book_imb range: [{book_imb.min():.4f}, {book_imb.max():.4f}]")
    print(f"  book_imb non-zero: {(np.abs(book_imb) > 1e-8).sum():,} / {n:,}")
    print(f"  depth_near range: [{depth_near.min():.1f}, {depth_near.max():.1f}]")

    # ── Trade intensity (60s rolling mean of 10s trade count) for liquidity γ layer ──
    # Live engine uses: feature_engineer → resample_to_10s → rolling(6).mean()
    # Equivalent on 1s bars: rolling(60).sum() / 6 = avg trades per 10s window
    tc = bars_1s["trade_count"].values.astype(np.float64) if "trade_count" in bars_1s.columns else np.ones(n, dtype=np.float64)
    trade_intensity = (pd.Series(tc)
                       .rolling(60, min_periods=10)
                       .sum()
                       .ffill().bfill()
                       .values.astype(np.float64) / 6.0)
    trade_intensity = np.maximum(trade_intensity, 1.0)
    print(f"  trade_intensity range: [{trade_intensity.min():.1f}, {trade_intensity.max():.1f}]")

    return ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret, book_imb, trade_intensity, depth_near


def build_ml_arrays_cached(bars_1s, pred_10s, sigma_window=60):
    def _index_ns(idx, pos):
        item = idx[pos]
        return int(getattr(item, "value", item))

    cache_dir = cache_root(ROOT) / "backtest_ml"
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": SYMBOL,
        "sigma_window": int(sigma_window),
        "bars_len": int(len(bars_1s)),
        "bars_start_ns": _index_ns(bars_1s.index, 0) if len(bars_1s) else 0,
        "bars_end_ns": _index_ns(bars_1s.index, -1) if len(bars_1s) else 0,
        "pred_len": int(len(pred_10s)),
        "pred_start_ns": _index_ns(pred_10s.index, 0) if len(pred_10s) else 0,
        "pred_end_ns": _index_ns(pred_10s.index, -1) if len(pred_10s) else 0,
        "pred_path": str(PREDICTIONS_PATH),
        "pred_mtime_ns": PREDICTIONS_PATH.stat().st_mtime_ns if PREDICTIONS_PATH.exists() else 0,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    cache_path = cache_dir / f"ml_arrays_{SYMBOL.lower()}_{digest}.npz"

    if cache_path.exists():
        data = np.load(cache_path)
        print(f"  ML array cache hit → {cache_path.name}")
        return (
            data["ts"], data["hi"], data["lo"], data["cl"], data["ssq"],
            data["pred_dir"], data["pred_vol"], data["pred_ret"],
            data["book_imb"], data["trade_intensity"], data["depth_near"],
        )

    arrays = build_ml_arrays(bars_1s, pred_10s, sigma_window=sigma_window)
    np.savez_compressed(
        cache_path,
        ts=arrays[0],
        hi=arrays[1],
        lo=arrays[2],
        cl=arrays[3],
        ssq=arrays[4],
        pred_dir=arrays[5],
        pred_vol=arrays[6],
        pred_ret=arrays[7],
        book_imb=arrays[8],
        trade_intensity=arrays[9],
        depth_near=arrays[10],
    )
    print(f"  ML array cache saved → {cache_path.name}")
    return arrays


def build_toxicity_arrays(ts_ms, pred_10s, pred_dir=None, toxicity_horizon_s=10):
    """Forward-fill side-specific toxicity probabilities onto 1s bars.

    If dedicated toxicity models are unavailable, fall back to the directional
    signal: down-probability is toxic for bids, up-probability is toxic for asks.
    """
    n = len(ts_ms)
    tox_bid = np.full(n, 0.5, dtype=np.float64)
    tox_ask = np.full(n, 0.5, dtype=np.float64)

    tox_bid_col = f"pred_tox_bid_{int(toxicity_horizon_s)}s"
    tox_ask_col = f"pred_tox_ask_{int(toxicity_horizon_s)}s"
    has_tox_bid = tox_bid_col in pred_10s.columns
    has_tox_ask = tox_ask_col in pred_10s.columns

    if has_tox_bid or has_tox_ask:
        pred_ts_arr = pred_10s.index.to_numpy(dtype="datetime64[ms]").astype(np.int64)
        tox_bid_vals = pred_10s[tox_bid_col].values.astype(np.float64) if has_tox_bid else None
        tox_ask_vals = pred_10s[tox_ask_col].values.astype(np.float64) if has_tox_ask else None

        j = 0
        for i in range(n):
            while j < len(pred_ts_arr) - 1 and pred_ts_arr[j + 1] <= ts_ms[i]:
                j += 1
            if pred_ts_arr[j] <= ts_ms[i]:
                if has_tox_bid:
                    tox_bid[i] = tox_bid_vals[j]
                if has_tox_ask:
                    tox_ask[i] = tox_ask_vals[j]

    if not has_tox_bid:
        if pred_dir is not None:
            tox_bid = np.clip(1.0 - pred_dir, 0.0, 1.0)
        else:
            tox_bid.fill(0.5)
    if not has_tox_ask:
        if pred_dir is not None:
            tox_ask = np.clip(pred_dir, 0.0, 1.0)
        else:
            tox_ask.fill(0.5)

    print(
        f"  toxicity_{int(toxicity_horizon_s)}s bid range: "
        f"[{tox_bid.min():.4f}, {tox_bid.max():.4f}]"
    )
    print(
        f"  toxicity_{int(toxicity_horizon_s)}s ask range: "
        f"[{tox_ask.min():.4f}, {tox_ask.max():.4f}]"
    )
    return tox_bid, tox_ask


# ═══════════════════════════════════════════════════════════════════
#  2. ML-ENHANCED SIMULATION CORE
# ═══════════════════════════════════════════════════════════════════

@njit_opt
def _simulate_ml_core(ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret,
                      book_imb, trade_intensity,
                      gamma, kappa, order_size, max_inv,
                      rq_ms, fee, taker_fee, tick, sample_rate,
                      skew_strength, vol_blend, dir_threshold,
                      asym_strength, gamma_dir_bonus,
                      regime_enabled, vol_baseline,
                      gamma_scale_min, gamma_scale_max,
                      ret_skew, max_spread_bps,
                      dynamic_cap_enabled, dynamic_cap_base_bps,
                      dynamic_cap_alpha, dynamic_cap_max_mult,
                      dynamic_cap_var_baseline,
                      position_timeout_ms,
                      kappa_ratio, queue_depth, eta, exit_urg_strength,
                      lot_size, book_imb_strength,
                      rq_min_ms, rq_max_ms,
                      liq_baseline, gamma_liq_min, gamma_liq_max,
                      p3_delta_star, inv_gamma_enabled,
                      inv_skew_strength,
                      fill_dist_decay, ret_shift_max_pct,
                      ret_demean_halflife,
                      maker_fill_prob, rng_seed,
                      p3_kappa_eff, depth_near, kappa_depth_baseline,
                      queue_base_arr, queue_decay_arr,
                      buy_fill_prob_arr, sell_fill_prob_arr,
                      ber_guard_thresh, ber_spread_mult,
                      vol_power, markout_ema_span_fills, markout_spread_scale,
                      urg_w_time, urg_w_pnl, urg_w_signal,
                      requote_threshold_bps,
                      fill_cooldown_ms,
                      symmetric_size,
                      direction_aware_fill, fill_directional_strength,
                      inventory_asym_strength,
                      inventory_signal_fade_strength):
    """
    ML-enhanced AS simulation with regime-adaptive gamma,
    fixed/dynamic max_spread_bps clamp, position_timeout, queue-aware fills,
    eta order size decay, exit urgency asymmetry, dynamic requote interval,
    lazy requote (Step 26B), and fill cooldown (Step 27).

    v1.2 enhancements (volatility scaling; Milionis et al. AMM LVR is only an analogy):
      vol_power > 1.0: superlinear vol scaling for Layer 1.
        This is an empirical CLOB arm, not a consequence of the CFMM/AMM LVR theorem.
        vol_spread_scale = (σ²/σ²_baseline)^(vol_power/2) instead of sqrt(σ²)/σ_baseline.
      markout_ema_span_fills > 0: online markout tracking with EMA.
        Tracks per-side 10s markout after each fill.
        markout_ema < 0 → widen spread; markout_ema > 0 → tighten.
        Scaling: spread *= (1 - markout_spread_scale * tanh(markout_ema / ref))
      markout_spread_scale > 0: per-side asymmetric spread from markout.
        When bid markout > ask markout → tighten bid, widen ask.

    v1.1 enhancements:
      P1 BER Guard (Zhao & Linetsky 2021):
        ber_guard_thresh > 0: when recent trade intensity spikes (proxy for
        book exhaustion), multiply spread by ber_spread_mult.

        Fill models:
      fill_dist_decay = 0:  touch-fill (legacy, 100% fill when price reaches limit)
      fill_dist_decay > 0:  distance-decay (P(fill|touch) = exp(-δ/fill_dist_decay))
        δ = distance from mid to limit price.  Closer orders fill more easily,
        naturally penalising strategies that create asymmetric bid/ask distances.
      maker_fill_prob < 1:  queue-position penalty — P(fill) *= maker_fill_prob.
        Simulates being behind other orders in the FIFO queue.
        Calibration: live cancel/fill ratio ≈ 120:1 → maker_fill_prob ≈ 0.01-0.10.
            direction_aware_fill > 0:  use realized within-bar pressure to avoid
                symmetric same-bar bid+ask fills in the bar backtest.
    Dynamic RQ: when rq_min_ms < rq_max_ms, requote interval adapts to
    recent volatility. High vol (fast σ > slow σ) → shorter RQ (rq_min).
    Low vol → longer RQ (rq_max). When rq_min_ms == rq_max_ms, static RQ.

        Inventory control layer:
            inventory_asym_strength: direct half-spread asymmetry toward reducing inventory.
            inventory_signal_fade_strength: fade exposure-increasing ML/markout asymmetry.
    """
    n = len(ts)
    ns = (n + sample_rate - 1) // sample_rate
    pnl_s = np.empty(ns, np.float64)
    inv_s = np.empty(ns, np.float64)
    ts_s  = np.empty(ns, np.int64)

    # Deterministic but configurable RNG for fill stochasticity.
    if fill_dist_decay > 0.0 or maker_fill_prob < 1.0:
        np.random.seed(rng_seed)

    q = 0.0; cash = 0.0
    bp = 0.0; ap = 0.0
    hb = False; ha = False
    lrt = ts[0] - rq_ms

    # Floor order_size to lot_size (matching live engine discretization)
    if lot_size > 0.0:
        order_size = np.floor(order_size / lot_size) * lot_size
        if order_size < lot_size:
            order_size = lot_size

    nfb = 0; nfa = 0; nrq = 0
    nto = 0  # timeout closes
    tsprd = 0.0; mx = 0.0; si_sum = 0.0
    si = 0

    # Position tracking for timeout
    pos_open_ts = ts[0]  # when position was opened

    # pred_ret EMA demeaning state
    ema_pr = 0.0
    alpha_dm = 2.0 / (ret_demean_halflife + 1.0) if ret_demean_halflife > 0 else 0.0

    # New state for realistic modeling
    entry_price = 0.0      # avg entry for unrealized PnL (exit urgency)
    bid_sz = order_size    # current bid order size (eta decay)
    ask_sz = order_size    # current ask order size (eta decay)

    # P3 effective kappa: use as base instead of raw config kappa (matching live)
    kappa_base = p3_kappa_eff if p3_kappa_eff > 0.0 else kappa
    kappa_eff = kappa_base * kappa_ratio

    # ── Dynamic RQ state ──
    dynamic_rq = rq_min_ms < rq_max_ms
    # EMA for fast volatility (10s half-life → α ≈ 0.067)
    ema_alpha_fast = 0.067
    # EMA for slow baseline volatility (60s half-life → α ≈ 0.011)
    ema_alpha_slow = 0.011
    ema_var_fast = 0.0  # fast EMA of squared returns
    ema_var_slow = 0.0  # slow EMA of squared returns
    prev_cl = cl[0]
    cur_rq_ms = rq_ms  # current effective RQ (static default)
    rq_sum = 0.0  # for reporting avg dynamic RQ

    # ── P1: BER guard state (trade intensity EMA) ──
    ema_ti_fast = 0.0  # fast EMA of trade intensity (5s half-life)
    ema_ti_slow = 0.0  # slow EMA of trade intensity (60s half-life)
    ber_alpha_fast = 0.13  # 5s half-life α
    ber_alpha_slow = 0.011  # 60s half-life α
    ber_active = False  # is BER guard currently widening spread?

    # ── v1.2: Markout tracking state ──
    mo_alpha = 2.0 / (markout_ema_span_fills + 1.0) if markout_ema_span_fills > 0 else 0.0
    mo_ema_bid = 0.0   # EMA of bid fill markout (price_10s_later - fill_price)
    mo_ema_ask = 0.0   # EMA of ask fill markout (fill_price - price_10s_later)
    mo_ema_all = 0.0   # combined markout EMA
    # Ring buffer for delayed markout computation (10s = 10 bars at 1s)
    MO_DELAY = 10
    mo_bid_pending_px = np.zeros(64, np.float64)  # circular buffer of bid fill prices
    mo_bid_pending_ts = np.zeros(64, np.int64)     # fill timestamps (bar index)
    mo_bid_head = 0
    mo_bid_tail = 0
    mo_ask_pending_px = np.zeros(64, np.float64)
    mo_ask_pending_ts = np.zeros(64, np.int64)
    mo_ask_head = 0
    mo_ask_tail = 0
    mo_ref = 50.0  # reference markout for tanh normalization ($50)
    mo_sum_all = 0.0
    mo_sum_bid = 0.0
    mo_sum_ask = 0.0
    mo_count_all = 0
    mo_count_bid = 0
    mo_count_ask = 0

    inventory_pnl = 0.0
    prev_mark_mid = cl[0]

    # ── Step 27: Fill cooldown state ──
    last_buy_fill_ts = ts[0] - 999999999  # last BUY fill timestamp (init to far past)
    last_sell_fill_ts = ts[0] - 999999999
    consec_buy = 0   # consecutive same-side BUY fills
    consec_sell = 0  # consecutive same-side SELL fills

    for i in range(n):
        mid = cl[i]

        if i > 0:
            inventory_pnl += q * (mid - prev_mark_mid)
        prev_mark_mid = mid

        # ── v1.2: resolve pending markouts (10s after fill) ──
        if markout_ema_span_fills > 0:
            # Bid fills: markout = mid_now - fill_price (positive = good)
            while mo_bid_head != mo_bid_tail:
                idx = mo_bid_pending_ts[mo_bid_head % 64]
                if i - idx >= MO_DELAY:
                    fpx = mo_bid_pending_px[mo_bid_head % 64]
                    mo_val = mid - fpx
                    mo_ema_bid = mo_alpha * mo_val + (1.0 - mo_alpha) * mo_ema_bid
                    mo_ema_all = mo_alpha * mo_val + (1.0 - mo_alpha) * mo_ema_all
                    mo_sum_bid += mo_val
                    mo_sum_all += mo_val
                    mo_count_bid += 1
                    mo_count_all += 1
                    mo_bid_head += 1
                else:
                    break
            # Ask fills: markout = fill_price - mid_now (positive = good)
            while mo_ask_head != mo_ask_tail:
                idx = mo_ask_pending_ts[mo_ask_head % 64]
                if i - idx >= MO_DELAY:
                    fpx = mo_ask_pending_px[mo_ask_head % 64]
                    mo_val = fpx - mid
                    mo_ema_ask = mo_alpha * mo_val + (1.0 - mo_alpha) * mo_ema_ask
                    mo_ema_all = mo_alpha * mo_val + (1.0 - mo_alpha) * mo_ema_all
                    mo_sum_ask += mo_val
                    mo_sum_all += mo_val
                    mo_count_ask += 1
                    mo_count_all += 1
                    mo_ask_head += 1
                else:
                    break

        # ── dynamic RQ: update EMAs of squared returns ──
        if dynamic_rq:
            ret_sq = (cl[i] - prev_cl) * (cl[i] - prev_cl)
            prev_cl = cl[i]
            if i == 0:
                ema_var_fast = ret_sq
                ema_var_slow = ret_sq
            else:
                ema_var_fast = ema_alpha_fast * ret_sq + (1.0 - ema_alpha_fast) * ema_var_fast
                ema_var_slow = ema_alpha_slow * ret_sq + (1.0 - ema_alpha_slow) * ema_var_slow

        # ── fills ──
        # Two fill models:
        # 1. touch-fill (fill_dist_decay=0): 100% fill when price reaches limit
        # 2. distance-decay (fill_dist_decay>0): P(fill|touch) = exp(-δ/λ)
        #    where δ = distance from mid, λ = fill_dist_decay.
        #    Closer orders fill more easily — penalises asymmetric quoting.
        filled_buy = False
        filled_sell = False
        buy_touched = hb and lo[i] <= bp - queue_depth
        sell_touched = ha and hi[i] >= ap + queue_depth
        q_base_cur = queue_base_arr[i]
        q_decay_cur = queue_decay_arr[i]
        queue_penalty = 1.0
        if q_base_cur > order_size and order_size > 0.0:
            queue_penalty = order_size / q_base_cur
            if queue_penalty < 0.01:
                queue_penalty = 0.01
        buy_gate_prob = maker_fill_prob * buy_fill_prob_arr[i] * queue_penalty
        sell_gate_prob = maker_fill_prob * sell_fill_prob_arr[i] * queue_penalty
        if direction_aware_fill > 0.5:
            up_exc = hi[i] - mid
            if up_exc < 0.0:
                up_exc = 0.0
            down_exc = mid - lo[i]
            if down_exc < 0.0:
                down_exc = 0.0
            total_exc = up_exc + down_exc
            bar_pressure = 0.0
            if total_exc > tick:
                bar_pressure = (up_exc - down_exc) / total_exc
            if i > 0:
                close_pressure = cl[i] - cl[i - 1]
                ref_sigma = np.sqrt(ssq[i])
                if ref_sigma > 1e-8:
                    close_pressure = close_pressure / ref_sigma
                    if close_pressure > 1.0:
                        close_pressure = 1.0
                    elif close_pressure < -1.0:
                        close_pressure = -1.0
                    bar_pressure = 0.7 * bar_pressure + 0.3 * close_pressure
            buy_mult = 1.0 - fill_directional_strength * bar_pressure
            sell_mult = 1.0 + fill_directional_strength * bar_pressure
            if buy_mult < 0.05:
                buy_mult = 0.05
            elif buy_mult > 2.0:
                buy_mult = 2.0
            if sell_mult < 0.05:
                sell_mult = 0.05
            elif sell_mult > 2.0:
                sell_mult = 2.0
            buy_gate_prob = maker_fill_prob * buy_mult
            sell_gate_prob = maker_fill_prob * sell_mult
            if buy_gate_prob > 1.0:
                buy_gate_prob = 1.0
            if sell_gate_prob > 1.0:
                sell_gate_prob = 1.0
            if buy_touched and sell_touched:
                if bar_pressure > 0.05:
                    buy_touched = False
                elif bar_pressure < -0.05:
                    sell_touched = False
                elif up_exc > down_exc:
                    buy_touched = False
                elif down_exc > up_exc:
                    sell_touched = False
                elif (ap - mid) <= (mid - bp):
                    buy_touched = False
                else:
                    sell_touched = False
        if buy_touched:
            do_fill_buy = True
            # Queue-position gate: even if touched, maker may not be first in queue
            if buy_gate_prob < 1.0:
                if np.random.random() >= buy_gate_prob:
                    do_fill_buy = False
            if do_fill_buy and fill_dist_decay > 0.0:
                bid_dist = mid - bp
                if bid_dist < tick:
                    bid_dist = tick
                p_fill = np.exp(-bid_dist / fill_dist_decay)
                if np.random.random() >= p_fill:
                    do_fill_buy = False
            if do_fill_buy and q_decay_cur > 0.0:
                bid_dist = mid - bp
                if bid_dist < tick:
                    bid_dist = tick
                p_fill = np.exp(-q_decay_cur * bid_dist)
                if np.random.random() >= p_fill:
                    do_fill_buy = False
            if do_fill_buy:
                q_before = q
                cash -= bp * bid_sz * (1.0 + fee)
                q += bid_sz
                hb = False; nfb += 1; filled_buy = True
                # Step 27: track consecutive same-side fills
                consec_buy += 1; consec_sell = 0; last_buy_fill_ts = ts[i]
                # Track entry price for unrealized PnL
                if q_before <= 0.0 and q > 0.0:
                    entry_price = bp
                elif q_before > 0.0:
                    entry_price = (entry_price * q_before + bp * bid_sz) / q
                if q < 1e-10 and q > -1e-10:
                    entry_price = 0.0
        if sell_touched:
            do_fill_sell = True
            # Queue-position gate
            if sell_gate_prob < 1.0:
                if np.random.random() >= sell_gate_prob:
                    do_fill_sell = False
            if do_fill_sell and fill_dist_decay > 0.0:
                ask_dist = ap - mid
                if ask_dist < tick:
                    ask_dist = tick
                p_fill = np.exp(-ask_dist / fill_dist_decay)
                if np.random.random() >= p_fill:
                    do_fill_sell = False
            if do_fill_sell and q_decay_cur > 0.0:
                ask_dist = ap - mid
                if ask_dist < tick:
                    ask_dist = tick
                p_fill = np.exp(-q_decay_cur * ask_dist)
                if np.random.random() >= p_fill:
                    do_fill_sell = False
            if do_fill_sell:
                q_before = q
                cash += ap * ask_sz * (1.0 - fee)
                q -= ask_sz
                ha = False; nfa += 1; filled_sell = True
                # Step 27: track consecutive same-side fills
                consec_sell += 1; consec_buy = 0; last_sell_fill_ts = ts[i]
                # Track entry price for unrealized PnL
                if q_before >= 0.0 and q < 0.0:
                    entry_price = ap
                elif q_before < 0.0:
                    aq_before = -q_before
                    entry_price = (entry_price * aq_before + ap * ask_sz) / (-q)
                if q < 1e-10 and q > -1e-10:
                    entry_price = 0.0

        # ── v1.2: enqueue fills for delayed markout ──
        if markout_ema_span_fills > 0:
            if filled_buy:
                mo_bid_pending_px[mo_bid_tail % 64] = bp
                mo_bid_pending_ts[mo_bid_tail % 64] = i
                mo_bid_tail += 1
            if filled_sell:
                mo_ask_pending_px[mo_ask_tail % 64] = ap
                mo_ask_pending_ts[mo_ask_tail % 64] = i
                mo_ask_tail += 1

        # Track position open time
        aq = q if q >= 0.0 else -q
        if filled_buy or filled_sell:
            if aq < 1e-10:
                pos_open_ts = ts[i]  # went flat, reset
            elif (filled_buy and q > 0.0 and q <= order_size + 1e-10) or \
                 (filled_sell and q < 0.0 and -q <= order_size + 1e-10):
                pos_open_ts = ts[i]  # new position opened
            # Cancel accumulating side if at max inventory
            if q >= max_inv:
                hb = False
            elif q <= -max_inv:
                ha = False

        # ── position timeout: force close at market (taker fee) ──
        if position_timeout_ms > 0 and aq > 1e-10:
            if ts[i] - pos_open_ts >= position_timeout_ms:
                if q > 0.0:
                    cash += mid * aq * (1.0 - taker_fee)
                else:
                    cash -= mid * aq * (1.0 + taker_fee)
                nto += 1
                q = 0.0
                hb = False; ha = False
                pos_open_ts = ts[i]

        # ── P1: BER Guard — update trade intensity EMAs ──
        if ber_guard_thresh > 0.0:
            ti_val = trade_intensity[i]
            if i == 0:
                ema_ti_fast = ti_val
                ema_ti_slow = ti_val
            else:
                ema_ti_fast = ber_alpha_fast * ti_val + (1.0 - ber_alpha_fast) * ema_ti_fast
                ema_ti_slow = ber_alpha_slow * ti_val + (1.0 - ber_alpha_slow) * ema_ti_slow
            # BER active when fast TI >> slow TI (aggressive sweep in progress)
            if ema_ti_slow > 1.0:
                ber_ratio = ema_ti_fast / ema_ti_slow
                ber_active = ber_ratio > ber_guard_thresh
            else:
                ber_active = False

        # ── requote ──
        # Dynamic RQ: compute effective interval from volatility ratio
        if dynamic_rq and i > 60:
            if ema_var_slow > 1e-12:
                vol_ratio = ema_var_fast / ema_var_slow
                # vol_ratio > 1 → high activity → shorter RQ
                # vol_ratio < 1 → low activity → longer RQ
                # Map: rq = rq_max * (rq_min/rq_max)^clamp(vol_ratio, 0, 2)
                vr_clamped = vol_ratio
                if vr_clamped > 2.0:
                    vr_clamped = 2.0
                if vr_clamped < 0.0:
                    vr_clamped = 0.0
                # Power mapping: ratio=0→rq_max, ratio=1→geometric_mean, ratio=2→rq_min
                log_ratio = np.log(rq_min_ms / rq_max_ms)  # negative
                cur_rq_ms_f = rq_max_ms * np.exp(log_ratio * vr_clamped)
                cur_rq_ms = int(cur_rq_ms_f)
                if cur_rq_ms < rq_min_ms:
                    cur_rq_ms = rq_min_ms
                if cur_rq_ms > rq_max_ms:
                    cur_rq_ms = rq_max_ms

        if ts[i] - lrt >= cur_rq_ms:
            # ── volatility: blend rolling with ML prediction ──
            vol_roll = ssq[i]
            vol_ml = pred_vol[i]
            if vol_ml < 1e-8:
                vol_ml = vol_roll
            s = (1.0 - vol_blend) * vol_roll + vol_blend * vol_ml

            # ── regime-adaptive: separate spread scaling from γ ──
            # Design: L0/L1 scale spread δ directly (always widens/tightens
            # correctly regardless of γ's relationship to the log term).
            # L3 scales γ for reservation price (inventory penalty).
            # This avoids the counterintuitive effect where multiplying γ
            # in the (2/γ)·ln(1+γ/κ) term REDUCES spread for small γ.
            regime_spread_scale = 1.0
            g_base = gamma
            if regime_enabled > 0.5:
                # Layer 0: Liquidity → spread scale (low liq → wider spread)
                if liq_baseline > 0.0:
                    ti = trade_intensity[i]
                    if ti < 1.0:
                        ti = 1.0
                    liq_ratio = ti / liq_baseline
                    liq_scale = 1.0 / np.sqrt(liq_ratio)
                    if liq_scale < gamma_liq_min:
                        liq_scale = gamma_liq_min
                    if liq_scale > gamma_liq_max:
                        liq_scale = gamma_liq_max
                    regime_spread_scale = regime_spread_scale * liq_scale

                # Layer 1: Vol-regime → spread scale (high vol → wider spread)
                # v1.2 empirical superlinear scaling when vol_power > 1.0.
                # AMM LVR motivates checking volatility sensitivity, but does not
                # derive this CLOB exponent or prove an optimum.
                # vol_power=1.0 → original linear: sqrt(σ²)/σ_base
                # vol_power=1.5 → (σ²/σ²_base)^0.75
                # vol_power=2.0 → σ²/σ²_base (quadratic-in-vol stress arm)
                vol_sq_ratio = s / (vol_baseline * vol_baseline)
                if vol_sq_ratio < 0.09:   # floor at 0.3²
                    vol_sq_ratio = 0.09
                vol_spread_scale = np.power(vol_sq_ratio, vol_power * 0.5)
                if vol_spread_scale < gamma_scale_min:
                    vol_spread_scale = gamma_scale_min
                if vol_spread_scale > gamma_scale_max:
                    vol_spread_scale = gamma_scale_max
                regime_spread_scale = regime_spread_scale * vol_spread_scale

            # Layer 3: Inventory escalation on γ for reservation price
            if inv_gamma_enabled > 0.5 and max_inv > 0.0:
                aq_g = q if q >= 0.0 else -q
                inv_ratio = aq_g / max_inv
                g_base = g_base * (1.0 + inv_ratio * inv_ratio)

            # ── direction signal ──
            dir_signal = pred_dir[i] - 0.5
            active_dir = (dir_signal > dir_threshold or
                          dir_signal < -dir_threshold)
            active_dir_quote = active_dir

            # ── gamma adjustment: align inventory with direction ──
            g_eff = g_base
            if active_dir_quote and gamma_dir_bonus > 0.0:
                # sign(q) * dir_signal > 0 → inventory aligns with prediction
                align = 0.0
                if q > 0.0:
                    align = dir_signal
                elif q < 0.0:
                    align = -dir_signal
                # align > 0 → reduce gamma (hold winner), < 0 → increase (cut loser)
                g_eff = g_base * (1.0 - gamma_dir_bonus * align * 2.0)
                if g_eff < g_base * 0.2:
                    g_eff = g_base * 0.2
                if g_eff > g_base * 3.0:
                    g_eff = g_base * 3.0

            # ── Spread: exponential GLFT ──
            # Dynamic kappa from depth (matching live estimate_kappa)
            cur_kappa_eff = kappa_eff  # static fallback
            if kappa_depth_baseline > 0.0:
                dn = depth_near[i]
                if dn > 0.0:
                    d_ratio = dn / kappa_depth_baseline
                    if d_ratio < 0.3:
                        d_ratio = 0.3
                    if d_ratio > 3.0:
                        d_ratio = 3.0
                    cur_kappa_eff = kappa_base * d_ratio * kappa_ratio

            r = mid - q * g_eff * s
            d = gamma * s + (2.0 / gamma) * np.log(1.0 + gamma / cur_kappa_eff)

            # Apply regime spread scaling (L0 liquidity + L1 vol)
            d = d * regime_spread_scale

            # ── P1: BER Guard spread widening ──
            # When book exhaustion is detected, widen spread to avoid
            # adverse selection from informed flow sweeps.
            if ber_active and ber_spread_mult > 1.0:
                d = d * ber_spread_mult

            # ── v1.2 Markout-based spread adjustment ──
            # When recent fills show adverse markout (mo_ema_all < 0),
            # widen spread; when fills are profitable, tighten.
            # d *= 1 - markout_spread_scale * tanh(mo_ema_all / mo_ref)
            if markout_spread_scale > 0.0 and mo_ema_all != 0.0:
                mo_ratio = mo_ema_all / mo_ref
                # tanh approx for Numba: use np.tanh
                mo_adj = 1.0 - markout_spread_scale * np.tanh(mo_ratio)
                if mo_adj < 0.5:
                    mo_adj = 0.5
                if mo_adj > 2.0:
                    mo_adj = 2.0
                d = d * mo_adj

            # Layer 2: P3 fill-probability floor (applied to δ directly)
            if regime_enabled > 0.5 and p3_delta_star > 0.0:
                min_spread = 2.0 * p3_delta_star
                if d < min_spread:
                    d = min_spread

            # Fee floor
            mn = 2.0 * fee * mid + tick
            if d < mn:
                d = mn

            # ── max spread clamp: fixed or volatility-scaled dynamic cap ──
            cap_bps = max_spread_bps
            if dynamic_cap_enabled > 0.5 and dynamic_cap_base_bps > 0.0:
                cap_mult = 1.0
                if dynamic_cap_var_baseline > 1e-12:
                    cap_ratio = s / dynamic_cap_var_baseline
                    if cap_ratio < 1.0:
                        cap_ratio = 1.0
                    cap_mult = np.power(cap_ratio, dynamic_cap_alpha)
                if cap_mult < 1.0:
                    cap_mult = 1.0
                if cap_mult > dynamic_cap_max_mult:
                    cap_mult = dynamic_cap_max_mult
                cap_bps = dynamic_cap_base_bps * cap_mult
            if cap_bps > 0.0:
                max_spread = mid * cap_bps / 10000.0
                if d > max_spread:
                    d = max_spread
            else:
                max_spread = 0.0

            hd = d * 0.5

            # ── CJP (2015) inventory skew: r -= φ·(q/q_max)·δ ──
            # Pure inventory-proportional reservation price shift, independent
            # of any ML signal. Ensures aggressive quote tightening toward
            # flat when position is large.
            if inv_skew_strength > 0.0 and max_inv > 1e-10:
                r = r - inv_skew_strength * (q / max_inv) * d

            # ── ML direction skew (reservation price shift) ──
            if active_dir_quote and skew_strength > 0.0:
                r = r + skew_strength * dir_signal * d

            # ── ret prediction skew (shifts r toward predicted return) ──
            # Matches live engine: clamp + inventory-aware fading
            if ret_skew > 0.0:
                # Demean pred_ret: subtract running EMA to remove momentum bias
                pr_raw = pred_ret[i]
                if ret_demean_halflife > 0:
                    ema_pr = alpha_dm * pr_raw + (1.0 - alpha_dm) * ema_pr
                    pr_use = pr_raw - ema_pr
                else:
                    pr_use = pr_raw
                rs = pr_use * ret_skew * mid
                # Clamp: limit r_shift to ret_shift_max_pct × half_spread
                rs_max = ret_shift_max_pct * hd
                if rs > rs_max:
                    rs = rs_max
                if rs < -rs_max:
                    rs = -rs_max
                # Inventory-aware fading: when shift adds exposure,
                # fade linearly with |q/q_max|. At max inventory the
                # pro-exposure component is zeroed out.
                if max_inv > 1e-10:
                    aq_rs = q if q >= 0.0 else -q
                    inv_r = aq_rs / max_inv
                    if inv_r > 1.0:
                        inv_r = 1.0
                    adds_exp = (q > 0.0 and rs > 0.0) or (q < 0.0 and rs < 0.0)
                    if adds_exp:
                        rs = rs * (1.0 - inv_r)
                r = r + rs

            # ── asymmetric spread ──
            asym = 0.0
            if active_dir_quote and asym_strength > 0.0:
                asym = asym_strength * dir_signal * 2.0

            # ── exit urgency (matching live engine) ──
            if exit_urg_strength > 0.0 and (q > 1e-8 or q < -1e-8):
                # time urgency
                if position_timeout_ms > 0:
                    hold = ts[i] - pos_open_ts
                    h_ratio = hold / position_timeout_ms
                    if h_ratio > 1.0:
                        h_ratio = 1.0
                    time_urg = h_ratio * h_ratio
                else:
                    time_urg = 0.0

                # PnL urgency
                pnl_urg = 0.0
                if s > 1e-10 and entry_price > 0.0:
                    aq_urg = q if q > 0.0 else -q
                    dollar_vol = np.sqrt(s) * mid * aq_urg
                    upnl = (mid - entry_price) * q
                    if dollar_vol > 1e-8 and upnl < 0.0:
                        pnl_urg = -upnl / dollar_vol
                        if pnl_urg > 3.0:
                            pnl_urg = 3.0

                # Signal urgency
                signal_urg = 0.0
                if q > 0.0 and dir_signal < 0.0:
                    signal_urg = (-dir_signal) * 2.0
                    if signal_urg > 1.0:
                        signal_urg = 1.0
                elif q < 0.0 and dir_signal > 0.0:
                    signal_urg = dir_signal * 2.0
                    if signal_urg > 1.0:
                        signal_urg = 1.0

                urgency = urg_w_time * time_urg + urg_w_pnl * pnl_urg + urg_w_signal * signal_urg
                if urgency > 1.0:
                    urgency = 1.0

                inv_sign = 1.0 if q > 0.0 else -1.0
                asym -= inv_sign * urgency * exit_urg_strength

            # ── book imbalance skew ──
            # bid heavier → book_imb > 0 → price likely up → tighten bid
            if book_imb_strength > 0.0:
                asym += book_imb[i] * book_imb_strength

            # ── v1.2 Per-side markout asymmetry ──
            # If bid markout > ask markout → bid side profit > ask side
            # → tighten bid (negative asym) to capture more bid fills
            # Conversely if ask markout better → tighten ask (positive asym)
            if markout_spread_scale > 0.0 and (mo_ema_bid != 0.0 or mo_ema_ask != 0.0):
                mo_diff = mo_ema_bid - mo_ema_ask  # positive = bid better
                # Map to asym: positive mo_diff → negative asym (tighten bid)
                mo_asym = -markout_spread_scale * np.tanh(mo_diff / mo_ref) * 0.5
                asym += mo_asym

            inv_ratio_ctrl = 0.0
            if max_inv > 1e-10:
                aq_ctrl = q if q >= 0.0 else -q
                inv_ratio_ctrl = aq_ctrl / max_inv
                if inv_ratio_ctrl > 1.0:
                    inv_ratio_ctrl = 1.0

            if inventory_signal_fade_strength > 0.0 and inv_ratio_ctrl > 0.0:
                adds_exp_asym = (q > 0.0 and asym > 0.0) or (q < 0.0 and asym < 0.0)
                if adds_exp_asym:
                    fade = 1.0 - inventory_signal_fade_strength * inv_ratio_ctrl
                    if fade < 0.0:
                        fade = 0.0
                    asym = asym * fade

            if inventory_asym_strength > 0.0 and inv_ratio_ctrl > 0.0:
                inv_sign_asym = 1.0 if q > 0.0 else -1.0
                asym = asym - inv_sign_asym * inventory_asym_strength * inv_ratio_ctrl

            # clamp asym
            if asym > 0.9:
                asym = 0.9
            if asym < -0.9:
                asym = -0.9
            hd_bid = hd * (1.0 - asym)
            hd_ask = hd * (1.0 + asym)

            nbid = np.floor((r - hd_bid) / tick) * tick
            nask = np.ceil((r + hd_ask) / tick) * tick

            # ── mid guard: bid must stay below mid, ask above mid ──
            # Crossing mid turns maker into taker (higher fees, different fill).
            if nbid >= mid:
                nbid = np.floor(mid / tick) * tick
                if nbid >= mid:
                    nbid -= tick
            if nask <= mid:
                nask = np.ceil(mid / tick) * tick
                if nask <= mid:
                    nask += tick

            # Match live's final recenter after all quote-side adjustments.
            if max_spread > 0.0 and nask - nbid > max_spread:
                nbid = np.floor((mid - max_spread * 0.5) / tick) * tick
                nask = np.ceil((mid + max_spread * 0.5) / tick) * tick

            # ── eta: inventory-weighted order size (matching live engine) ──
            bid_sz = order_size
            ask_sz = order_size
            if eta > 0.0 and max_inv > 1e-10:
                q_norm = q / max_inv
                if q > 0.0:
                    bid_sz = order_size * np.exp(-eta * q_norm)
                    bid_sz = np.floor(bid_sz / lot_size) * lot_size
                    if bid_sz < lot_size:
                        bid_sz = lot_size
                if q < 0.0:
                    ask_sz = order_size * np.exp(eta * q_norm)
                    ask_sz = np.floor(ask_sz / lot_size) * lot_size
                    if ask_sz < lot_size:
                        ask_sz = lot_size

            # ── symmetric size: mirror smaller side to keep buy/sell balanced ──
            if symmetric_size > 0.5:
                if bid_sz < ask_sz:
                    ask_sz = bid_sz
                elif ask_sz < bid_sz:
                    bid_sz = ask_sz

            # ── position room cap (prevent exceeding max_inv) ──
            if q > 0.0:
                room = max_inv - q
                room = np.floor(room / lot_size) * lot_size
                if room >= lot_size:
                    if bid_sz > room:
                        bid_sz = room
                else:
                    bid_sz = 0.0
            if q < 0.0:
                room = max_inv - (-q)
                room = np.floor(room / lot_size) * lot_size
                if room >= lot_size:
                    if ask_sz > room:
                        ask_sz = room
                else:
                    ask_sz = 0.0

            # ── anti-flip cap (matching live engine) ──
            if q < -lot_size:
                close_cap = np.floor((-q) / lot_size) * lot_size
                if bid_sz > close_cap:
                    bid_sz = close_cap
            if q > lot_size:
                close_cap = np.floor(q / lot_size) * lot_size
                if ask_sz > close_cap:
                    ask_sz = close_cap

            hb_new = q < max_inv and bid_sz >= lot_size
            ha_new = q > -max_inv and ask_sz >= lot_size

            # ── Step 27: fill cooldown — prevent same-side quoting ──
            # effective_cooldown = fill_cooldown × n_consecutive_same_side
            # Resets on opposite-side fill or timer expiry.
            if fill_cooldown_ms > 0.0:
                if consec_buy > 0:
                    cd_buy = fill_cooldown_ms * consec_buy
                    if ts[i] - last_buy_fill_ts >= cd_buy:
                        consec_buy = 0  # cooldown expired, reset
                    else:
                        hb_new = False  # buy side cooled
                if consec_sell > 0:
                    cd_sell = fill_cooldown_ms * consec_sell
                    if ts[i] - last_sell_fill_ts >= cd_sell:
                        consec_sell = 0
                    else:
                        ha_new = False  # sell side cooled

            # ── Step 26B: lazy requote ──
            # Keep existing order if price drift < threshold (saves cancel/replace)
            rq_thr = requote_threshold_bps / 10000.0
            bid_updated = True
            ask_updated = True
            if rq_thr > 0.0:
                if hb and hb_new and bp > 0.0:
                    drift_bid = (nbid - bp) if nbid > bp else (bp - nbid)
                    if drift_bid / bp <= rq_thr:
                        bid_updated = False
                if ha and ha_new and ap > 0.0:
                    drift_ask = (nask - ap) if nask > ap else (ap - nask)
                    if drift_ask / ap <= rq_thr:
                        ask_updated = False

            if bid_updated:
                hb = hb_new
                if hb:
                    bp = nbid
            else:
                hb = hb_new  # update active flag but keep price
            if ask_updated:
                ha = ha_new
                if ha:
                    ap = nask
            else:
                ha = ha_new

            tsprd += (nask - nbid); nrq += 1; lrt = ts[i]
            rq_sum += cur_rq_ms

        aq = q if q >= 0.0 else -q
        if aq > mx:
            mx = aq
        si_sum += aq

        if i % sample_rate == 0 and si < ns:
            pnl_s[si] = cash + q * mid
            inv_s[si] = q
            ts_s[si]  = ts[i]
            si += 1

    last_mid = cl[n - 1]
    # Window end is MTM only; taker_fee applies only to explicit taker exits.
    fp = cash + q * last_mid
    return (pnl_s[:si], inv_s[:si], ts_s[:si],
            fp, q, cash,
            nfb, nfa, nrq, nto, tsprd, mx, si_sum, n, rq_sum,
            inventory_pnl,
            mo_sum_all, mo_count_all,
            mo_sum_bid, mo_count_bid,
            mo_sum_ask, mo_count_ask)


def simulate_ml(ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret,
                params, book_imb=None, trade_intensity=None, depth_near=None,
                tox_bid=None, tox_ask=None):
    if book_imb is None:
        book_imb = np.zeros(len(ts), dtype=np.float64)
    if trade_intensity is None:
        trade_intensity = np.full(len(ts), 50.0, dtype=np.float64)
    if depth_near is None:
        depth_near = np.zeros(len(ts), dtype=np.float64)
    if tox_bid is None:
        tox_bid = np.clip(1.0 - pred_dir, 0.0, 1.0)
    if tox_ask is None:
        tox_ask = np.clip(pred_dir, 0.0, 1.0)
    gamma   = params["gamma"]
    kappa   = params["kappa"]
    osiz    = params["order_size"]
    maxinv  = params["max_inventory"]
    rq_ms   = int(params["requote_interval"] * 1000)
    fee     = params["maker_fee"]
    sr      = max(1, rq_ms // 1000)
    skew    = params["skew_strength"]
    vblend  = params["vol_blend"]
    dthr    = params["dir_threshold"]
    asym    = params.get("asym_strength", 0.0)
    gdir    = params.get("gamma_dir_bonus", 0.0)
    regime_en = 1.0 if params.get("regime_enabled", False) else 0.0
    vol_base = params.get("vol_baseline", 0.3)
    gs_min   = params.get("gamma_scale_min", 0.5)
    gs_max   = params.get("gamma_scale_max", 2.0)
    rskew   = params.get("ret_skew", 0.0)
    taker_fee = params.get("taker_fee", 0.0004)
    (dyn_cap_enabled,
     max_sprd_bps,
     dyn_cap_base_bps,
     dyn_cap_alpha,
     dyn_cap_max_mult,
     dyn_cap_var_baseline) = _resolve_cap_params(params)
    pos_timeout_ms = int(params.get("position_timeout", 0) * 1000)
    k_ratio = params.get("kappa_ratio", 1.0)
    qdepth  = params.get("queue_depth", 0.0)
    p_eta   = params.get("eta", 0.0)
    exit_urg = params.get("exit_urgency_strength", 0.0)
    p_lot   = params.get("lot_size", 0.001)
    bi_str  = params.get("book_imb_strength", 0.0)

    # Dynamic RQ: if rq_min < rq_max, enable adaptive requote interval
    rq_min_s = params.get("rq_min", params["requote_interval"])
    rq_max_s = params.get("rq_max", params["requote_interval"])
    rq_min_ms = int(rq_min_s * 1000)
    rq_max_ms = int(rq_max_s * 1000)
    # sample rate uses minimum RQ for proper PnL tracking
    if rq_min_ms < rq_max_ms:
        sr = max(1, rq_min_ms // 1000)

    # Layer 0/2/3 params
    liq_baseline = params.get("liq_baseline", DEFAULT_LIQ_BASELINE)
    gamma_liq_min = params.get("gamma_liq_scale_min", 0.5)
    gamma_liq_max = params.get("gamma_liq_scale_max", 3.0)
    p3_delta_star = params.get("p3_delta_star", 0.0)
    inv_gamma_en = 1.0 if params.get("inv_gamma_enabled", True) else 0.0

    inv_skew = params.get("inventory_skew_strength", 0.0)
    f_dist_decay = params.get("fill_dist_decay", 0.0)
    rs_max_pct = params.get("ret_shift_max_pct", 1.0)
    ret_dm_hl = params.get("ret_demean_halflife", 360)
    m_fill_prob = params.get("maker_fill_prob", 1.0)
    rng_seed = int(params.get("rng_seed", 42))
    if rng_seed <= 0:
        rng_seed = 42
    p3_ke = params.get("p3_kappa_eff", 0.0)
    k_depth_bl = params.get("kappa_depth_baseline", 0.0)
    queue_calibration = params.get("_queue_calibration")
    if queue_calibration:
        queue_base_arr, queue_decay_arr, buy_fill_prob_arr, sell_fill_prob_arr = build_daily_queue_arrays(
            ts,
            queue_calibration,
            default_queue_base=params.get("queue_base", 5.0),
            default_queue_decay=params.get("queue_decay", 0.0),
            default_buy_fill_prob=params.get("buy_fill_prob", 1.0),
            default_sell_fill_prob=params.get("sell_fill_prob", 1.0),
        )
    else:
        queue_base_arr = np.full(len(ts), params.get("queue_base", 5.0), dtype=np.float64)
        queue_decay_arr = np.full(len(ts), params.get("queue_decay", 0.0), dtype=np.float64)
        buy_fill_prob_arr = np.full(len(ts), params.get("buy_fill_prob", 1.0), dtype=np.float64)
        sell_fill_prob_arr = np.full(len(ts), params.get("sell_fill_prob", 1.0), dtype=np.float64)

    # P1: BER Guard params
    ber_gt = params.get("ber_guard_thresh", 0.0)
    ber_sm = params.get("ber_spread_mult", 2.0)

    # v1.2 empirical volatility/markout params (historically labeled LVR-inspired)
    v_power = params.get("vol_power", 1.0)
    mo_span = params.get("markout_ema_span_fills", 0)
    mo_sscale = params.get("markout_spread_scale", 0.0)

    # Step 26A: urgency weights
    urg_wt = params.get("urgency_time_weight", 0.3)
    urg_wp = params.get("urgency_pnl_weight", 0.3)
    urg_ws = params.get("urgency_signal_weight", 0.4)

    # Step 26B: lazy requote
    rq_thr_bps = params.get("requote_threshold_bps", 0.0)

    # Step 27: fill cooldown
    f_cooldown_ms = int(params.get("fill_cooldown", 0.0) * 1000)

    # Step 28a: symmetric order sizing
    sym_size = 1.0 if params.get("symmetric_size", False) else 0.0
    dir_fill = 1.0 if params.get("direction_aware_fill", False) else 0.0
    dir_fill_str = params.get("fill_directional_strength", 0.75)
    inv_asym_str = params.get("inventory_asym_strength", 0.0)
    inv_fade_str = params.get("inventory_signal_fade_strength", 0.0)

    raw = _simulate_ml_core(ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret,
                            book_imb, trade_intensity,
                            gamma, kappa, osiz, maxinv,
                            rq_ms, fee, taker_fee, TICK, sr,
                            skew, vblend, dthr,
                            asym, gdir,
                            regime_en, vol_base, gs_min, gs_max,
                            rskew, max_sprd_bps,
                            1.0 if dyn_cap_enabled and dyn_cap_base_bps > 0.0 else 0.0,
                            dyn_cap_base_bps, dyn_cap_alpha,
                            dyn_cap_max_mult, dyn_cap_var_baseline,
                            pos_timeout_ms,
                            k_ratio, qdepth, p_eta, exit_urg, p_lot,
                            bi_str,
                            rq_min_ms, rq_max_ms,
                            liq_baseline, gamma_liq_min, gamma_liq_max,
                            p3_delta_star, inv_gamma_en,
                            inv_skew,
                            f_dist_decay, rs_max_pct,
                            ret_dm_hl, m_fill_prob, rng_seed,
                            p3_ke, depth_near, k_depth_bl,
                            queue_base_arr, queue_decay_arr,
                            buy_fill_prob_arr, sell_fill_prob_arr,
                            ber_gt, ber_sm,
        v_power, mo_span, mo_sscale,
                            urg_wt, urg_wp, urg_ws,
                            rq_thr_bps,
                            f_cooldown_ms,
                            sym_size,
                            dir_fill, dir_fill_str,
                            inv_asym_str,
                            inv_fade_str)
    return _unpack(raw, params)


def _unpack(raw, params):
    (pnl_ts, inv_ts, ts_out,
     fp, q, cash,
    nfb, nfa, nrq, nto, tsprd, mx, si_sum, n, rq_sum,
    inventory_pnl,
    mo_sum_all, mo_count_all,
    mo_sum_bid, mo_count_bid,
    mo_sum_ask, mo_count_ask) = raw

    n_days = n / 86400.0
    nf = nfb + nfa

    # Keep average effective RQ for diagnostics/reporting.
    avg_rq_s = (rq_sum / nrq / 1000.0) if nrq > 0 else params["requote_interval"]
    pnl_diff = np.diff(pnl_ts)
    dt_s = np.diff(ts_out).astype(np.float64) / 1000.0
    valid = dt_s > 0.0
    if len(pnl_diff) > 1 and np.any(valid):
        dp = pnl_diff[valid]
        dt = dt_s[valid]
        mu_sec = dp.sum() / dt.sum()
        diff_norm = dp / np.sqrt(dt)
        sigma_sec = diff_norm.std()
        if sigma_sec > 0:
            sharpe = mu_sec / sigma_sec * math.sqrt(365.25 * 86400.0)
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0

    cum_max = np.maximum.accumulate(pnl_ts)
    dd = cum_max - pnl_ts
    max_dd = float(dd.max()) if len(dd) > 0 else 0.0

    avg_markout = mo_sum_all / mo_count_all if mo_count_all > 0 else 0.0
    avg_markout_bid = mo_sum_bid / mo_count_bid if mo_count_bid > 0 else 0.0
    avg_markout_ask = mo_sum_ask / mo_count_ask if mo_count_ask > 0 else 0.0
    inventory_adjusted_pnl = fp - inventory_pnl
    inventory_cost = -inventory_pnl
    (dyn_cap_enabled,
     fixed_cap_bps,
     dyn_cap_base_bps,
     dyn_cap_alpha,
     dyn_cap_max_mult,
     dyn_cap_var_baseline) = _resolve_cap_params(params)
    if dyn_cap_enabled and dyn_cap_base_bps > 0.0:
        cap_mode = "dynamic"
        cap_bps = dyn_cap_base_bps
    elif fixed_cap_bps > 0.0:
        cap_mode = "fixed"
        cap_bps = fixed_cap_bps
    else:
        cap_mode = "off"
        cap_bps = 0.0
    cap_label = params.get("cap_label", _cap_label_from_fields(cap_mode, cap_bps))

    return {
        "gamma": params["gamma"],
        "kappa": params["kappa"],
        "skew": params["skew_strength"],
        "vol_blend": params["vol_blend"],
        "dir_thr": params["dir_threshold"],
        "asym": params.get("asym_strength", 0.0),
        "gdir": params.get("gamma_dir_bonus", 0.0),
        "regime": params.get("regime_enabled", False),
        "ret_skew": params.get("ret_skew", 0.0),
        "rq_sec": params.get("requote_interval", 10.0),
        "rq_min": params.get("rq_min", params.get("requote_interval", 10.0)),
        "rq_max": params.get("rq_max", params.get("requote_interval", 10.0)),
        "avg_rq": avg_rq_s,
        "max_sprd_bps": cap_bps,
        "cap_mode": cap_mode,
        "cap_base_bps": cap_bps,
        "cap_alpha": dyn_cap_alpha,
        "cap_max_mult": dyn_cap_max_mult,
        "cap_var_baseline": dyn_cap_var_baseline,
        "cap_label": cap_label,
        "dynamic_cap_enabled": dyn_cap_enabled,
        "pos_timeout": params.get("position_timeout", 0.0),
        "kappa_ratio": params.get("kappa_ratio", 1.0),
        "queue_depth": params.get("queue_depth", 0.0),
        "eta": params.get("eta", 0.0),
        "exit_urg": params.get("exit_urgency_strength", 0.0),
        "book_imb_str": params.get("book_imb_strength", 0.0),
        "max_inv_cfg": params.get("max_inventory", 0.01),
        "inv_skew": params.get("inventory_skew_strength", 0.0),
        "inv_asym": params.get("inventory_asym_strength", 0.0),
        "inv_fade": params.get("inventory_signal_fade_strength", 0.0),
        "fill_dist_decay": params.get("fill_dist_decay", 0.0),
        "ret_shift_max_pct": params.get("ret_shift_max_pct", 1.0),
        "maker_fill_prob": params.get("maker_fill_prob", 1.0),
        "tox_h": params.get("toxicity_horizon_s", 10),
        "dir_fill": params.get("direction_aware_fill", False),
        "dir_fill_str": params.get("fill_directional_strength", 0.75),
        "selection_score": 0.0,
        "pnl": fp,
        "inventory_pnl": inventory_pnl,
        "inventory_cost": inventory_cost,
        "inventory_adjusted_pnl": inventory_adjusted_pnl,
        "pnl_per_day": fp / max(n_days, 0.01),
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "avg_markout": avg_markout,
        "avg_markout_bid": avg_markout_bid,
        "avg_markout_ask": avg_markout_ask,
        "markout_count": mo_count_all,
        "fills_bid": nfb,
        "fills_ask": nfa,
        "fills_total": nf,
        "fills_per_day": nf / max(n_days, 0.01),
        "fill_rate": nf / max(nrq, 1),
        "avg_spread": tsprd / max(nrq, 1),
        "max_inventory": mx,
        "avg_inventory": si_sum / max(n, 1),
        "final_inventory": q,
        "n_requotes": nrq,
        "timeout_closes": nto,
        "ber_guard_thresh": params.get("ber_guard_thresh", 0.0),
        "ber_spread_mult": params.get("ber_spread_mult", 2.0),
        "vol_power": params.get("vol_power", 1.0),
        "markout_ema_span_fills": params.get("markout_ema_span_fills", 0),
        "markout_spread_scale": params.get("markout_spread_scale", 0.0),
        "urg_w_time": params.get("urgency_time_weight", 0.3),
        "urg_w_pnl": params.get("urgency_pnl_weight", 0.3),
        "urg_w_signal": params.get("urgency_signal_weight", 0.4),
        "fill_cooldown": params.get("fill_cooldown", 0.0),
        "n_days": n_days,
        "_pnl_ts": pnl_ts,
        "_inv_ts": inv_ts,
        "_ts": ts_out,
    }


# ═══════════════════════════════════════════════════════════════════
#  3. SWEEP
# ═══════════════════════════════════════════════════════════════════

# v1.4 reachability-constrained sweep grid (2026-04-17)
# Root cause analysis: v1.x κ_eff=0.0024 → spread $300+, unreachable.
# Legacy note: raw κ is only a fallback in this path.
# NOTE: p3_kappa_eff (from fill_prob model) overrides grid κ when > 0.
#   Currently p3_kappa_eff ≈ 0.049 → grid κ is irrelevant; keep a generic placeholder.
# max_spread_bps caps spread within 10s excursion reachable range.
# maker_fill_prob < 1.0 models queue position (no free touch-fill).
# Fee floor: 2 × 0.018% × $85k ≈ $30.6 → need spread > $31 minimum.
SWEEP_GRID = {
    "gamma": [0.01, 0.02, 0.05, 0.1],
    "kappa": [0.05],               # overridden by p3_kappa_eff; fallback placeholder
    "skew_strength": [0.0],
    "vol_blend": [0.5],
    "dir_threshold": [0.05],
    "asym_strength": [0.1],
    "gamma_dir_bonus": [0.0],
    "regime_enabled": [True],
    "ret_skew": [0.0, 200.0],
    "requote_interval": [10.0],
    "rq_min": [5.0],
    "rq_max": [10.0],
    "max_spread_bps": [8.0, 12.0, 20.0],
    "position_timeout": [0.0],
    "max_inventory": [0.026],
    "kappa_ratio": [0.5, 1.0, 2.0],
    "queue_depth": [0.0],
    "eta": [0.5],
    "exit_urgency_strength": [0.5],
    "lot_size": [0.001],
    "book_imb_strength": [0.0],
    "fill_dist_decay": [0.0],
    "ret_shift_max_pct": [0.3],
    "maker_fill_prob": [0.05, 0.10, 0.20, 0.50, 1.0],
}

# Regime-aware sweep grid (v1.4 reachability-constrained)
SWEEP_GRID_REGIME = {
    "gamma": [0.01, 0.02, 0.05, 0.1],
    "kappa": [0.02, 0.05, 0.1],
    "skew_strength": [0.0],
    "vol_blend": [0.5],
    "dir_threshold": [0.05],
    "asym_strength": [0.1],
    "gamma_dir_bonus": [0.0],
    "regime_enabled": [True],
    "ret_skew": [0.0, 100.0, 200.0],
    "requote_interval": [10.0],
    "rq_min": [5.0],
    "rq_max": [10.0],
    "max_spread_bps": [5.0, 8.0, 12.0],
    "position_timeout": [0.0],
    "max_inventory": [0.026],
    "kappa_ratio": [0.5, 1.0],
    "queue_depth": [0.0],
    "eta": [0.5],
    "exit_urgency_strength": [0.5],
    "lot_size": [0.001],
    "book_imb_strength": [0.0],
    "fill_dist_decay": [0.0],
    "ret_shift_max_pct": [0.3],
    "maker_fill_prob": [0.10, 0.20],
}

# Live-structure-aligned sweep grid (v1.4 reachability-constrained)
# Includes a true no-ML control arm via vol_blend=0.0, asym_strength=0.0,
# ret_skew=0.0, skew_strength=0.0, gamma_dir_bonus=0.0.
SWEEP_GRID_LIVE = {
    "gamma": [0.01, 0.02, 0.05, 0.1],
    "kappa": [0.02, 0.05, 0.1],
    "skew_strength": [0.0],
    "vol_blend": [0.0, 0.5],
    "dir_threshold": [0.05],
    "asym_strength": [0.0, 0.1],
    "gamma_dir_bonus": [0.0],
    "regime_enabled": [True],
    "ret_skew": [0.0, 200.0],
    "requote_interval": [10.0],
    "rq_min": [5.0],
    "rq_max": [10.0],
    "max_spread_bps": [5.0, 8.0, 12.0],
    "position_timeout": [0.0],
    "max_inventory": [0.026],
    "kappa_ratio": [0.5, 1.0],
    "queue_depth": [0.0],
    "eta": [0.5],
    "exit_urgency_strength": [0.5],
    "lot_size": [0.001],
    "book_imb_strength": [0.0],
    "fill_dist_decay": [0.0],
    "ret_shift_max_pct": [0.3],
    "maker_fill_prob": [0.10, 0.20],
}

# ── v1.1 sweep: P1 BER (v1.4 reachability-constrained base) ──
SWEEP_GRID_V1_1 = {
    "gamma": [0.01, 0.02],
    "kappa": [0.05],
    "skew_strength": [0.0],
    "vol_blend": [0.5],
    "dir_threshold": [0.05],
    "asym_strength": [0.1],
    "gamma_dir_bonus": [0.0],
    "regime_enabled": [True],
    "ret_skew": [0.0, 200.0],
    "requote_interval": [10.0],
    "rq_min": [5.0],
    "rq_max": [10.0],
    "max_spread_bps": [8.0],
    "position_timeout": [0.0],
    "max_inventory": [0.026],
    "kappa_ratio": [1.0],
    "queue_depth": [0.0],
    "eta": [0.5],
    "exit_urgency_strength": [0.5],
    "lot_size": [0.001],
    "book_imb_strength": [0.0],
    "fill_dist_decay": [0.0],
    "ret_shift_max_pct": [0.3],
    "maker_fill_prob": [0.10, 0.20],
    # ── P1: BER Guard ──
    "ber_guard_thresh": [0.0, 1.2, 1.5, 2.0],
    "ber_spread_mult": [1.5, 2.0, 3.0],
}


def _dedup_v1_1_combos(params_list):
    """Remove redundant combos where disabled features have varying sub-params.

    When ber_guard_thresh=0, ber_spread_mult is irrelevant.
    """
    seen = set()
    deduped = []
    for p in params_list:
        norm = dict(p)
        if norm.get("ber_guard_thresh", 0.0) == 0.0:
            norm["ber_spread_mult"] = 2.0
        sig = tuple(sorted((k, v) for k, v in norm.items() if not k.startswith("_")))
        if sig not in seen:
            seen.add(sig)
            deduped.append(norm)
    return deduped


# ── v1.2 sweep: empirical volatility/markout scaling ──
SWEEP_GRID_V1_2 = {
    "gamma": [0.01, 0.02],
    "kappa": [0.05],
    "skew_strength": [0.0],
    "vol_blend": [0.5],
    "dir_threshold": [0.05],
    "asym_strength": [0.1],
    "gamma_dir_bonus": [0.0],
    "regime_enabled": [True],
    "ret_skew": [0.0, 200.0],
    "requote_interval": [10.0],
    "rq_min": [5.0],
    "rq_max": [10.0],
    "max_spread_bps": [8.0],
    "position_timeout": [0.0],
    "max_inventory": [0.026],
    "kappa_ratio": [1.0],
    "queue_depth": [0.0],
    "eta": [0.5],
    "exit_urgency_strength": [0.5],
    "lot_size": [0.001],
    "book_imb_strength": [0.0],
    "fill_dist_decay": [0.0],
    "ret_shift_max_pct": [0.3],
    "maker_fill_prob": [0.10, 0.20],
    # P1: BER
    "ber_guard_thresh": [0.0],
    "ber_spread_mult": [2.0],
    # v1.2 empirical volatility/markout scaling
    "vol_power": [1.0, 1.5, 2.0],
    "markout_ema_span_fills": [0, 10, 20, 50],
    "markout_spread_scale": [0.0, 0.2, 0.3, 0.5],
}

# Step 27: Fill cooldown sweep grid
# Sweeps fill_cooldown base seconds; effective cooldown = base × n_consecutive
SWEEP_GRID_COOLDOWN = {
    "gamma": [0.01],
    "kappa": [0.05],
    "skew_strength": [0.0],
    "vol_blend": [0.0, 0.5],
    "dir_threshold": [0.05],
    "asym_strength": [0.1],
    "gamma_dir_bonus": [0.0],
    "regime_enabled": [True],
    "ret_skew": [0.0],
    "requote_interval": [10.0],
    "rq_min": [5.0],
    "rq_max": [10.0],
    "max_spread_bps": [22.0],
    "position_timeout": [0.0],
    "max_inventory": [0.026],
    "kappa_ratio": [0.5],
    "queue_depth": [0.0],
    "eta": [0.5],
    "exit_urgency_strength": [0.0],
    "lot_size": [0.001],
    "book_imb_strength": [0.0],
    "fill_dist_decay": [0.0],
    "ret_shift_max_pct": [0.3],
    "maker_fill_prob": [0.05],
    "inventory_skew_strength": [0.0],
    "fill_cooldown": [41.0, 42.0, 43.0, 44.0, 45.0, 46.0],
}


def _dedup_v1_2_combos(params_list):
    """Remove redundant v1.2 combos.

    - When markout_ema_span_fills=0, markout_spread_scale is irrelevant.
    - When markout_spread_scale=0, markout_ema_span_fills is irrelevant.
    - When ber_guard_thresh=0, ber_spread_mult is irrelevant.
    """
    seen = set()
    deduped = []
    for p in params_list:
        norm = dict(p)
        if norm.get("ber_guard_thresh", 0.0) == 0.0:
            norm["ber_spread_mult"] = 2.0
        if norm.get("markout_ema_span_fills", 0) == 0:
            norm["markout_spread_scale"] = 0.0
        if norm.get("markout_spread_scale", 0.0) == 0.0:
            norm["markout_ema_span_fills"] = 0
        sig = tuple(sorted((k, v) for k, v in norm.items() if not k.startswith("_")))
        if sig not in seen:
            seen.add(sig)
            deduped.append(norm)
    return deduped

_G = {}

def _init_global(ts, hi, lo, cl, ssq, pd, pv, pr, tb, ta, bi, ti, dn=None):
    global _G
    _G = {"ts": ts, "hi": hi, "lo": lo, "cl": cl, "ssq": ssq,
           "pred_dir": pd, "pred_vol": pv, "pred_ret": pr,
           "tox_bid": tb, "tox_ask": ta,
           "book_imb": bi, "trade_intensity": ti,
           "depth_near": dn if dn is not None else np.zeros(len(ts), dtype=np.float64)}

def _worker(params):
    r = simulate_ml(
        _G["ts"], _G["hi"], _G["lo"], _G["cl"], _G["ssq"],
        _G["pred_dir"], _G["pred_vol"], _G["pred_ret"], params,
        book_imb=_G["book_imb"], trade_intensity=_G["trade_intensity"],
        depth_near=_G["depth_near"],
        tox_bid=_G["tox_bid"], tox_ask=_G["tox_ask"],
    )
    return {k: v for k, v in r.items() if not k.startswith("_")}


SORT_OBJECTIVES = {
    "selection_score": ("selection_score", "Selection"),
    "inventory_adjusted_pnl": ("inventory_adjusted_pnl", "InvAdjPnL"),
    "pnl": ("pnl", "PnL"),
    "avg_markout": ("avg_markout", "AvgMarkout"),
    "sharpe": ("sharpe", "Sharpe"),
    "pnl_per_day": ("pnl_per_day", "$/day"),
}

SORT_TIEBREAKERS = {
    "selection_score": ("inventory_adjusted_pnl", "pnl"),
    "inventory_adjusted_pnl": ("pnl", "avg_markout"),
    "pnl": ("inventory_adjusted_pnl", "avg_markout"),
    "avg_markout": ("inventory_adjusted_pnl", "pnl"),
    "sharpe": ("inventory_adjusted_pnl", "pnl"),
    "pnl_per_day": ("inventory_adjusted_pnl", "pnl"),
}


def _sort_key(result, sort_by):
    primary_field, _ = SORT_OBJECTIVES[sort_by]
    secondary_field, tertiary_field = SORT_TIEBREAKERS[sort_by]
    return (
        result.get(primary_field, float("-inf")),
        result.get(secondary_field, float("-inf")),
        result.get(tertiary_field, float("-inf")),
    )


def _sort_results(results, sort_by):
    return sorted(results, key=lambda r: _sort_key(r, sort_by), reverse=True)


def _best_result(results, sort_by):
    if not results:
        return None
    return _sort_results(results, sort_by)[0]


def run_sweep(ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret,
              book_imb, base, n_workers=None, trade_intensity=None,
              grid=None, depth_near=None, dedup_fn=None,
              sort_by="selection_score", tox_bid=None, tox_ask=None):
    import multiprocessing as mp

    if trade_intensity is None:
        trade_intensity = np.full(len(ts), 50.0, dtype=np.float64)
    if tox_bid is None:
        tox_bid = np.clip(1.0 - pred_dir, 0.0, 1.0)
    if tox_ask is None:
        tox_ask = np.clip(pred_dir, 0.0, 1.0)
    _init_global(
        ts, hi, lo, cl, ssq,
        pred_dir, pred_vol, pred_ret,
        tox_bid, tox_ask,
        book_imb, trade_intensity, depth_near,
    )

    sweep_grid = grid if grid is not None else SWEEP_GRID
    keys = list(sweep_grid.keys())
    vals = [sweep_grid[k] for k in keys]
    combos = list(product(*vals))

    params_list = []
    for idx, combo in enumerate(combos):
        p = dict(base)
        for k, v in zip(keys, combo, strict=True):
            p[k] = v
        if "rng_seed" not in p:
            p["rng_seed"] = 42 + (idx * 7919)
        params_list.append(p)

    # Deduplicate conditional combos (e.g. v1.1 BER sub-params)
    if dedup_fn is not None:
        n_before = len(params_list)
        params_list = dedup_fn(params_list)
        if len(params_list) < n_before:
            print(f"  Dedup: {n_before} → {len(params_list)} effective combos")

    nw = n_workers or default_backtest_workers()
    nc = len(params_list)
    print(f"Sweep: {nc} combos, {nw} workers\n")

    t0 = time.perf_counter()
    _worker(params_list[0])
    t_one = time.perf_counter() - t0

    ctx = mp.get_context("fork")
    t0 = time.perf_counter()
    with ctx.Pool(nw) as pool:
        results = pool.map(_worker, params_list)
    t_par = time.perf_counter() - t0

    speedup = t_one * nc / t_par if t_par > 0 else 0
    print(f"  Single: {t_one:.2f}s  "
          f"Total: {t_par:.1f}s  "
          f"Speedup: {speedup:.1f}×\n")

    return _sort_results(_attach_selection_scores(results), sort_by)


# ═══════════════════════════════════════════════════════════════════
#  4. OUTPUT
# ═══════════════════════════════════════════════════════════════════

_HDR = (f"{'Rk':>3s}  {'γ':>6s}  {'κ':>6s}  {'κR':>4s}  {'MI':>5s}  {'RQn':>3s}  {'RQx':>3s}  {'aRQ':>5s}  {'Cap':>4s}  {'Tout':>4s}  "
        f"{'Asy':>4s}  {'GD':>4s}  {'RS':>4s}  {'RSP':>4s}  {'QD':>4s}  {'η':>4s}  {'ExU':>4s}  {'BI':>4s}  {'FDD':>4s}  {'MFP':>4s}  "
        f"{'BER':>4s}  {'BSM':>3s}  "
        f"{'VP':>3s}  {'MHL':>3s}  {'MSS':>4s}  "
    f"{'Sel':>5s}  {'InvAdj':>10s}  {'Mko':>7s}  {'PnL($)':>10s}  {'$/day':>8s}  {'Sharpe':>7s}  "
        f"{'MaxDD':>8s}  {'F/d':>6s}  {'Fb':>5s}  {'Fa':>5s}  {'TO':>4s}  {'Sprd':>7s}  "
        f"{'AvgI':>7s}")


def _row(i, r):
    return (f"{i:3d}  {r['gamma']:6.3f}  {r['kappa']:6.3f}  "
            f"{r.get('kappa_ratio', 1.0):4.1f}  "
            f"{r.get('max_inv_cfg', 0.01):5.3f}  "
            f"{r.get('rq_min', r.get('rq_sec', 10)):3.0f}  "
            f"{r.get('rq_max', r.get('rq_sec', 10)):3.0f}  "
            f"{r.get('avg_rq', r.get('rq_sec', 10)):5.1f}  "
            f"{_cap_cell(r):>4s}  "
            f"{r.get('pos_timeout', 0.0):4.0f}  "
            f"{r.get('asym', 0.0):4.1f}  "
            f"{r.get('gdir', 0.0):4.1f}  {r.get('ret_skew', 0.0):4.0f}  "
            f"{r.get('ret_shift_max_pct', 1.0):4.1f}  "
            f"{r.get('queue_depth', 0.0):4.0f}  "
            f"{r.get('eta', 0.0):4.1f}  "
            f"{r.get('exit_urg', 0.0):4.1f}  "
            f"{r.get('book_imb_str', 0.0):4.1f}  "
            f"{r.get('fill_dist_decay', 0.0):4.0f}  "
            f"{r.get('maker_fill_prob', 1.0):4.2f}  "
            f"{r.get('ber_guard_thresh', 0.0):4.1f}  {r.get('ber_spread_mult', 2.0):3.1f}  "
            f"{r.get('vol_power', 1.0):3.1f}  {r.get('markout_ema_span_fills', 0):3d}  {r.get('markout_spread_scale', 0.0):4.2f}  "
            f"{r.get('selection_score', 0.0):5.2f}  {r.get('inventory_adjusted_pnl', 0.0):10.2f}  {r.get('avg_markout', 0.0):7.2f}  "
            f"{r['pnl']:10.2f}  {r['pnl_per_day']:8.2f}  "
            f"{r['sharpe']:7.2f}  {r['max_drawdown']:8.2f}  "
            f"{r['fills_per_day']:6.1f}  {r.get('fills_bid', 0):5d}  {r.get('fills_ask', 0):5d}  {r.get('timeout_closes', 0):4d}  {r['avg_spread']:7.2f}  "
            f"{r['avg_inventory']:7.4f}")


def _pick_robust_best(results, sort_by="selection_score", top_k=5):
    if not results:
        return None
    metric_field, _ = SORT_OBJECTIVES[sort_by]
    k = min(top_k, len(results))
    top = results[:k]
    metric_values = np.array([r.get(metric_field, 0.0) for r in top], dtype=np.float64)
    metric_median = float(np.median(metric_values))
    return min(
        top,
        key=lambda r: (
            abs(r.get(metric_field, 0.0) - metric_median),
            -r.get("inventory_adjusted_pnl", float("-inf")),
            -r["pnl"],
        ),
    )


def print_results(results, top_n=20, sort_by="selection_score"):
    metric_field, metric_label = SORT_OBJECTIVES[sort_by]
    print("=" * 130)
    print(f"Ranked by: {metric_label}")
    print(_HDR)
    print("-" * 130)
    for i, r in enumerate(results[:top_n], 1):
        print(_row(i, r))
    print("=" * 130)

    if results:
        k = min(5, len(results))
        top = results[:k]
        metric_values = np.array([r.get(metric_field, 0.0) for r in top], dtype=np.float64)
        pnl = np.array([r["pnl"] for r in top], dtype=np.float64)
        robust = _pick_robust_best(results, sort_by=sort_by, top_k=k)
        print(f"\n  Selection-robust (top-{k} by {metric_label}): median {metric_label}={np.median(metric_values):.2f}, "
              f"median PnL=${np.median(pnl):.2f}")
        print(f"  Robust pick: Cap={_cap_cell(robust)}, γ={robust['gamma']}, κ={robust['kappa']}, "
              f"{metric_label}={robust.get(metric_field, 0.0):.2f}, "
              f"InvAdj=${robust.get('inventory_adjusted_pnl', 0.0):.2f}, PnL=${robust['pnl']:.2f}")

    # ── compare ML vs baseline ──
    baseline = [r for r in results if r["skew"] == 0.0 and r["vol_blend"] == 0.0]
    ml_best = results[0] if results else None
    if baseline and ml_best:
        bl = _best_result(_attach_selection_scores(baseline), sort_by)
        print(f"\n  Baseline (no ML): InvAdj=${bl.get('inventory_adjusted_pnl', 0.0):.2f}  "
              f"PnL=${bl['pnl']:.2f}  AvgMarkout={bl.get('avg_markout', 0.0):.2f}")
        print(f"  ML Best:          InvAdj=${ml_best.get('inventory_adjusted_pnl', 0.0):.2f}  "
              f"PnL=${ml_best['pnl']:.2f}  AvgMarkout={ml_best.get('avg_markout', 0.0):.2f}")


def save_results(results):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(results)
    path = RESULTS_DIR / "ml_sweep_results.csv"
    df.to_csv(path, index=False, float_format="%.6f")
    print(f"\nResults saved → {path}")


def save_pnl_series(result, tag="ml_best"):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "timestamp": result["_ts"],
        "pnl": result["_pnl_ts"],
        "inventory": result["_inv_ts"],
    })
    path = RESULTS_DIR / f"{tag}_pnl_series.parquet"
    df.to_parquet(path)
    print(f"PnL series saved → {path}")


def _run_paired_experiment(ts, hi, lo, cl, ssq,
                           pred_dir, pred_vol, pred_ret,
                           tox_bid, tox_ask,
                           base, args, book_imb, trade_intensity, depth_near):
    """Run a strict paired ML on/off comparison with identical AS params."""
    ml_off = dict(base)
    ml_off.update({
        "skew_strength": 0.0,
        "vol_blend": 0.0,
        "asym_strength": 0.0,
        "gamma_dir_bonus": 0.0,
        "ret_skew": 0.0,
    })
    ml_on = dict(base)

    off_result = simulate_ml(
        ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret, ml_off,
        book_imb=book_imb, trade_intensity=trade_intensity, depth_near=depth_near,
        tox_bid=tox_bid, tox_ask=tox_ask,
    )
    off_result["variant"] = "ml_off"

    on_result = simulate_ml(
        ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret, ml_on,
        book_imb=book_imb, trade_intensity=trade_intensity, depth_near=depth_near,
        tox_bid=tox_bid, tox_ask=tox_ask,
    )
    on_result["variant"] = "ml_on"

    results = _sort_results(_attach_selection_scores([off_result, on_result]), args.sort_by)
    print("\nStrict paired experiment: AS params held fixed; only ML path changes.")
    print_results(results, top_n=2, sort_by=args.sort_by)

    delta_pnl = on_result["pnl"] - off_result["pnl"]
    delta_inv = on_result["inventory_adjusted_pnl"] - off_result["inventory_adjusted_pnl"]
    delta_mo = on_result["avg_markout"] - off_result["avg_markout"]
    delta_sel = on_result["selection_score"] - off_result["selection_score"]
    print(
        "\nPaired delta (ML ON - ML OFF): "
        f"PnL={delta_pnl:+.2f}  InvAdj={delta_inv:+.2f}  "
        f"AvgMarkout={delta_mo:+.2f}  Selection={delta_sel:+.4f}"
    )

    save_results(results)
    save_pnl_series(on_result, "paired_ml_on")
    save_pnl_series(off_result, "paired_ml_off")


# ═══════════════════════════════════════════════════════════════════
#  5. MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="ML-Enhanced AS Backtest")
    ap.add_argument("--symbol", default=None,
                    help=f"Symbol (default from config/MM_SYMBOL, fallback {DEFAULT_SYMBOL})")
    ap.add_argument("--config", type=str, default=None,
                    help="Path to live/config.yaml — overrides all default params with live config")
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--no-ml", action="store_true",
                    help="Run pure AS baseline for comparison")
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--paired-ml", action="store_true",
                    help="Strict paired comparison: hold AS params fixed and compare ML on/off only")
    ap.add_argument("--day-start", default=None,
                    help="UTC daily start YYYY-MM-DD for this legacy bar backtest")
    ap.add_argument("--day-end", default=None,
                    help="UTC daily end YYYY-MM-DD for this legacy bar backtest")
    ap.add_argument("--gamma", type=float, default=None)
    ap.add_argument("--kappa", type=float, default=None)
    ap.add_argument("--order-size", type=float, default=None)
    ap.add_argument("--max-inventory", type=float, default=None)
    ap.add_argument("--requote-interval", type=float, default=None)
    ap.add_argument("--maker-fee", type=float, default=None)
    ap.add_argument("--skew-strength", type=float, default=None)
    ap.add_argument("--vol-blend", type=float, default=None)
    ap.add_argument("--dir-threshold", type=float, default=None)
    ap.add_argument("--asym-strength", type=float, default=None)
    ap.add_argument("--gamma-dir-bonus", type=float, default=None)
    ap.add_argument("--regime", action="store_true", default=None,
                    help="Enable regime-adaptive gamma scaling")
    ap.add_argument("--no-regime", action="store_true", default=False,
                    help="Disable regime-adaptive gamma scaling")
    ap.add_argument("--liq-baseline", type=float, default=None,
                    help="Liquidity baseline for regime scaling (default 200)")
    ap.add_argument("--sweep-live", action="store_true",
                    help="Sweep with live-aligned grid (regime ON, no maker_fill_prob sweep)")
    ap.add_argument("--sweep-v1-1", action="store_true", dest="sweep_v1_1",
                    help="Sweep with v1.1 grid (P1 BER feature)")
    ap.add_argument("--vol-baseline", type=float, default=None)
    ap.add_argument("--gamma-scale-min", type=float, default=None)
    ap.add_argument("--gamma-scale-max", type=float, default=None)
    ap.add_argument("--ret-skew", type=float, default=None,
                    help="ret prediction skew factor")
    ap.add_argument("--taker-fee", type=float, default=None,
                    help="taker fee for timeout closes")
    ap.add_argument("--max-spread-bps", type=float, default=None,
                    help="max spread in bps (0=no cap)")
    ap.add_argument("--position-timeout", type=float, default=None,
                    help="position timeout in seconds (0=disabled)")
    ap.add_argument("--kappa-ratio", type=float, default=None,
                    help="kappa multiplier simulating dynamic depth (0.3=thin book, 1.0=static)")
    ap.add_argument("--queue-depth", type=float, default=None,
                    help="$ price must cross beyond limit for fill (0=trade-through)")
    ap.add_argument("--eta", type=float, default=None,
                    help="inventory order size decay coefficient (0=disabled, live=0.5)")
    ap.add_argument("--exit-urgency", type=float, default=None,
                    help="exit urgency asymmetry strength (0=disabled, live=0.5)")
    ap.add_argument("--inventory-skew-strength", type=float, default=None,
                    help="CJP (2015) inventory r-shift: r -= φ·(q/q_max)·δ (0=disabled, 0.3=live)")
    ap.add_argument("--lot-size", type=float, default=None,
                    help="minimum order size step (BTCUSDT=0.001)")
    ap.add_argument("--book-imb-strength", type=float, default=None,
                    help="book imbalance → spread asymmetry strength (0=disabled)")
    ap.add_argument("--rq-min", type=float, default=None,
                    help="dynamic RQ minimum (seconds). If set, enables dynamic RQ.")
    ap.add_argument("--rq-max", type=float, default=None,
                    help="dynamic RQ maximum (seconds). If set, enables dynamic RQ.")
    ap.add_argument("--fill-dist-decay", type=float, default=None,
                    help="distance-decay fill model: P(fill|touch)=exp(-δ/λ). 0=touch-fill (legacy)")
    ap.add_argument("--ret-shift-max-pct", type=float, default=None,
                    help="max ret_skew shift as fraction of half_spread (0.3=30%%, 1.0=100%%)")
    ap.add_argument("--ret-demean-halflife", type=int, default=None,
                    help="EMA halflife for pred_ret demeaning (10s bars; 360=1h; 0=off)")
    ap.add_argument("--maker-fill-prob", type=float, default=None,
                    help="Queue-position fill probability gate (0-1). 1.0=legacy touch-fill. "
                         "Live calibration: cancel/fill≈120:1 → try 0.02-0.10.")
    ap.add_argument("--toxicity-horizon", type=int, default=None,
                    help="Use 5s or 10s toxicity probabilities when available (default 10)")
    ap.add_argument("--direction-aware-fill", action="store_true",
                    help="Use within-bar directional pressure to avoid symmetric same-bar bid+ask fills")
    ap.add_argument("--fill-directional-strength", type=float, default=None,
                    help="Strength of the direction-aware fill bias (default 0.75)")
    # ── P1 args ──
    ap.add_argument("--ber-guard-thresh", type=float, default=None,
                    help="P1: BER guard fires when ema_fast/ema_slow > thresh (0=disabled)")
    ap.add_argument("--ber-spread-mult", type=float, default=None,
                    help="P1: spread multiplier when BER guard is active (default 2.0)")
    # ── v1.2 empirical volatility-scaling args ──
    ap.add_argument("--vol-power", type=float, default=None,
                    help="v1.2: empirical vol exponent (1.0=linear sqrt, 1.5=superlinear, 2.0=quadratic stress)")
    ap.add_argument("--markout-spread-scale", type=float, default=None,
                    help="v1.2: markout → spread scale factor (0=disabled, 0.3=moderate)")
    # ── Step 26A: urgency weight args ──
    ap.add_argument("--urgency-time-weight", type=float, default=None,
                    help="urgency: time component weight (default 0.3)")
    ap.add_argument("--urgency-pnl-weight", type=float, default=None,
                    help="urgency: PnL component weight (default 0.3)")
    ap.add_argument("--urgency-signal-weight", type=float, default=None,
                    help="urgency: ML signal component weight (default 0.4)")
    ap.add_argument("--sweep-v1-2", action="store_true", dest="sweep_v1_2",
                    help="Sweep with v1.2 grid (empirical vol_power + markout)")
    # ── Step 27: fill cooldown ──
    ap.add_argument("--fill-cooldown", type=float, default=None,
                    help="Same-side fill cooldown base (seconds). Effective CD = base × n_consecutive.")
    ap.add_argument("--sweep-cooldown", action="store_true", dest="sweep_cooldown",
                    help="Sweep with fill_cooldown grid (Step 27)")
    ap.add_argument("--sort-by", choices=sorted(SORT_OBJECTIVES.keys()),
                    default="selection_score",
                    help="Sweep ranking metric")
    args = ap.parse_args()

    # ── Load base params from config.yaml (default) or CLI defaults ──
    from backtest_config import load_live_config_as_params
    cfg_path = args.config
    if cfg_path is None:
        default_cfg = ROOT / "live" / "config.yaml"
        if default_cfg.exists():
            cfg_path = str(default_cfg)
    if cfg_path:
        live_params = load_live_config_as_params(cfg_path)
        print(f"  Config loaded from: {cfg_path}")
    else:
        # Fallback hardcoded defaults (legacy — should match config.yaml)
        live_params = {
            "gamma": 0.01, "kappa": 0.05, "order_size": 0.0026,
            "max_inventory": 0.026, "requote_interval": 10.0,
            "maker_fee": 0.0, "taker_fee": 0.00036,
            "skew_strength": 0.0, "vol_blend": 0.5,
            "dir_threshold": 0.05, "asym_strength": 0.1,
            "gamma_dir_bonus": 0.0, "regime_enabled": True,
            "vol_baseline": 3.0, "gamma_scale_min": 0.5,
            "gamma_scale_max": 2.0, "ret_skew": 200.0,
            "max_spread_bps": 8.0, "position_timeout": 0.0,
            "kappa_ratio": 1.0, "queue_depth": 0.0,
            "eta": 0.5, "exit_urgency_strength": 0.5,
            "inventory_skew_strength": 0.1, "lot_size": 0.001,
            "book_imb_strength": 0.0, "fill_dist_decay": 0.0,
            "ret_shift_max_pct": 0.3, "ret_demean_halflife": 0,
            "liq_baseline": 200.0, "gamma_liq_scale_min": 0.5,
            "gamma_liq_scale_max": 3.0,
            "rq_min": 5.0, "rq_max": 10.0,
            "ber_guard_thresh": 1.2, "ber_spread_mult": 2.0,
            "vol_power": 1.5, "markout_ema_span_fills": 50,
            "markout_spread_scale": 0.2,
            "urgency_time_weight": 0.3, "urgency_pnl_weight": 0.3,
            "urgency_signal_weight": 0.4,
            "toxicity_horizon_s": 10,
            "direction_aware_fill": True,
            "fill_directional_strength": 0.75,
        }
    configure_symbol(args.symbol or live_params.get("symbol"))

    # CLI args override config values (only if explicitly provided)
    _cli_map = {
        "gamma": "gamma", "kappa": "kappa", "order_size": "order_size",
        "max_inventory": "max_inventory", "requote_interval": "requote_interval",
        "maker_fee": "maker_fee", "taker_fee": "taker_fee",
        "skew_strength": "skew_strength", "vol_blend": "vol_blend",
        "dir_threshold": "dir_threshold", "asym_strength": "asym_strength",
        "gamma_dir_bonus": "gamma_dir_bonus", "vol_baseline": "vol_baseline",
        "gamma_scale_min": "gamma_scale_min", "gamma_scale_max": "gamma_scale_max",
        "ret_skew": "ret_skew", "max_spread_bps": "max_spread_bps",
        "position_timeout": "position_timeout", "kappa_ratio": "kappa_ratio",
        "queue_depth": "queue_depth", "eta": "eta",
        "exit_urgency": "exit_urgency_strength",
        "inventory_skew_strength": "inventory_skew_strength",
        "lot_size": "lot_size", "book_imb_strength": "book_imb_strength",
        "rq_min": "rq_min", "rq_max": "rq_max",
        "fill_dist_decay": "fill_dist_decay",
        "ret_shift_max_pct": "ret_shift_max_pct",
        "ret_demean_halflife": "ret_demean_halflife",
        "maker_fill_prob": "maker_fill_prob",
        "ber_guard_thresh": "ber_guard_thresh",
        "ber_spread_mult": "ber_spread_mult",
        "vol_power": "vol_power",
        "markout_spread_scale": "markout_spread_scale",
        "urgency_time_weight": "urgency_time_weight",
        "urgency_pnl_weight": "urgency_pnl_weight",
        "urgency_signal_weight": "urgency_signal_weight",
        "toxicity_horizon": "toxicity_horizon_s",
        "fill_directional_strength": "fill_directional_strength",
        "fill_cooldown": "fill_cooldown",
    }
    for cli_attr, param_key in _cli_map.items():
        v = getattr(args, cli_attr, None)
        if v is not None:
            live_params[param_key] = v
    # --regime flag override
    if args.regime is not None and args.regime:
        live_params["regime_enabled"] = True
    if args.no_regime:
        live_params["regime_enabled"] = False
    if args.liq_baseline is not None:
        live_params["liq_baseline"] = args.liq_baseline
    if args.direction_aware_fill:
        live_params["direction_aware_fill"] = True

    import platform
    print(f"\n{'='*60}")
    print(f"  {SYMBOL} ML-Enhanced Backtest")
    print(f"  Chip: {platform.processor() or 'Apple M4'}  "
          f"Cores: {cpu_count()}  Numba: {'✓' if HAS_NUMBA else '✗'}")
    print(f"{'='*60}\n")

    # ── Load data ──
    print("Loading 1s bars (test set) …")
    bars = load_test_bars(day_start=args.day_start, day_end=args.day_end)

    print("Loading ML predictions …")
    pred = load_predictions()

    print("\nBuilding aligned arrays …")
    ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret, book_imb, trade_intensity, depth_near = \
        build_ml_arrays_cached(bars, pred)
    tox_horizon = int(live_params.get("toxicity_horizon_s", 10))
    tox_bid, tox_ask = build_toxicity_arrays(
        ts, pred, pred_dir=pred_dir, toxicity_horizon_s=tox_horizon,
    )
    del bars, pred  # free memory

    # Auto-load P3 fill probability model for delta_star + effective kappa
    p3_delta_star = 0.0
    p3_kappa_eff = 0.0
    try:
        from research.families.f02_empirical_p3_touch.fill_probability import FillProbabilityModel
        fp_path = MODEL_DIR / "fill_prob_params.json"
        if fp_path.exists():
            fp_model = FillProbabilityModel.load(fp_path)
            p3_delta_star = fp_model.optimal_delta()
            p3_kappa_eff = fp_model.effective_kappa()
            print(f"  P3 δ* = {p3_delta_star:.2f} USDT, κ_eff = {p3_kappa_eff:.6f}")
    except Exception as e:
        print(f"  P3 model not loaded: {e}")

    queue_calibration = load_daily_queue_calibration(symbol=SYMBOL, path=_queue_calibration_path(SYMBOL))
    if queue_calibration.get("days"):
        print(f"  Queue calibration loaded: {len(queue_calibration['days'])} days")
    else:
        queue_calibration = None

    live_params["toxicity_horizon_s"] = tox_horizon
    base = build_backtest_base_params(
        live_params,
        p3_delta_star=p3_delta_star,
        p3_kappa_eff=p3_kappa_eff,
        queue_calibration=queue_calibration,
    )
    if args.no_ml:
        disable_ml_params(base)

    if args.paired_ml:
        _run_paired_experiment(
            ts, hi, lo, cl, ssq,
            pred_dir, pred_vol, pred_ret,
            tox_bid, tox_ask,
            base, args, book_imb, trade_intensity, depth_near,
        )
        return

    if args.sweep:
        if args.sweep_cooldown:
            sweep_grid = SWEEP_GRID_COOLDOWN
            dedup = None
        elif args.sweep_v1_2:
            sweep_grid = SWEEP_GRID_V1_2
            dedup = _dedup_v1_2_combos
        elif args.sweep_v1_1:
            sweep_grid = SWEEP_GRID_V1_1
            dedup = _dedup_v1_1_combos
        elif args.sweep_live:
            sweep_grid = SWEEP_GRID_LIVE
            dedup = None
        elif args.regime:
            sweep_grid = SWEEP_GRID_REGIME
            dedup = None
        else:
            sweep_grid = SWEEP_GRID
            dedup = None
        results = run_sweep(ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret,
                            book_imb, base, args.workers, trade_intensity,
                            tox_bid=tox_bid, tox_ask=tox_ask,
                            grid=sweep_grid, depth_near=depth_near,
                            dedup_fn=dedup, sort_by=args.sort_by)
        print_results(results, sort_by=args.sort_by)
        save_results(results)

        # Re-run best with full PnL series
        best_p = dict(base)
        best_r = _pick_robust_best(results, sort_by=args.sort_by, top_k=5)
        best_p["gamma"] = best_r["gamma"]
        best_p["kappa"] = best_r["kappa"]
        best_p["skew_strength"] = best_r["skew"]
        best_p["vol_blend"] = best_r["vol_blend"]
        best_p["dir_threshold"] = best_r["dir_thr"]
        best_p["asym_strength"] = best_r.get("asym", 0.0)
        best_p["gamma_dir_bonus"] = best_r.get("gdir", 0.0)
        best_p["regime_enabled"] = best_r.get("regime", False)
        best_p["ret_skew"] = best_r.get("ret_skew", 0.0)
        best_p["kappa_ratio"] = best_r.get("kappa_ratio", 1.0)
        best_p["book_imb_strength"] = best_r.get("book_imb_str", 0.0)
        best_p["rq_min"] = best_r.get("rq_min", best_r.get("rq_sec", 10.0))
        best_p["rq_max"] = best_r.get("rq_max", best_r.get("rq_sec", 10.0))
        best_p["fill_dist_decay"] = best_r.get("fill_dist_decay", 0.0)
        best_p["ret_shift_max_pct"] = best_r.get("ret_shift_max_pct", 1.0)
        best_p["maker_fill_prob"] = best_r.get("maker_fill_prob", 1.0)
        best_p["ber_guard_thresh"] = best_r.get("ber_guard_thresh", 0.0)
        best_p["ber_spread_mult"] = best_r.get("ber_spread_mult", 2.0)
        best_p["vol_power"] = best_r.get("vol_power", 1.0)
        best_p["markout_ema_span_fills"] = best_r.get(
            "markout_ema_span_fills",
            0,
        )
        best_p["markout_spread_scale"] = best_r.get("markout_spread_scale", 0.0)
        best_result = simulate_ml(ts, hi, lo, cl, ssq,
                                  pred_dir, pred_vol, pred_ret, best_p,
                                  book_imb=book_imb, trade_intensity=trade_intensity,
                                  depth_near=depth_near,
                                  tox_bid=tox_bid, tox_ask=tox_ask)
        save_pnl_series(best_result, "ml_best")

        # Also save baseline PnL
        bl_p = dict(base)
        bl_p["skew_strength"] = 0.0
        bl_p["vol_blend"] = 0.0
        bl_p["asym_strength"] = 0.0
        bl_p["gamma_dir_bonus"] = 0.0
        bl_result = simulate_ml(ts, hi, lo, cl, ssq,
                                pred_dir, pred_vol, pred_ret, bl_p,
                                book_imb=book_imb, trade_intensity=trade_intensity,
                                depth_near=depth_near,
                                tox_bid=tox_bid, tox_ask=tox_ask)
        save_pnl_series(bl_result, "baseline")
    else:
        tag = "baseline" if args.no_ml else "ml"
        print(f"Running {'baseline' if args.no_ml else 'ML-enhanced'} backtest …")
        t0 = time.perf_counter()
        result = simulate_ml(ts, hi, lo, cl, ssq, pred_dir, pred_vol, pred_ret,
                             base, book_imb=book_imb, trade_intensity=trade_intensity,
                             depth_near=depth_near,
                             tox_bid=tox_bid, tox_ask=tox_ask)
        t_sim = time.perf_counter() - t0
        print(f"  {t_sim:.2f}s  ({len(ts)/t_sim/1e6:.1f}M bars/s)\n")
        print_results([{k: v for k, v in result.items()
                        if not k.startswith("_")}], sort_by=args.sort_by)
        save_pnl_series(result, tag)

    print()


if __name__ == "__main__":
    main()
