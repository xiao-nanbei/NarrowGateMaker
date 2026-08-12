#!/usr/bin/env python3
"""
Legacy 1s-bar Avellaneda-Stoikov diagnostic on BTCUSDC daily bars.

This file is intentionally not the promotion-grade replay path.  It does not
model exact L2 queue ahead, maker fill gate, latency, TTL, or guard lifecycle.
Use it only for quick AS-shape sanity checks; live candidates must go through
daily tick replay and live/replay mechanism gates.

Apple M4 optimisations
──────────────────────
  ✦  Numba @njit   → hot loop compiled to native ARM64 + NEON  (~50-100× vs CPython)
  ✦  multiprocessing.fork  → parameter sweep across all 10 P-cores (zero-copy data)
  ✦  Contiguous numpy arrays + pandas rolling  → M4 Accelerate / NEON SIMD

Usage
─────
  python models/backtest.py --day 2026-03-01           # single UTC day
  python models/backtest.py --day 2026-03-01 --sweep   # daily diagnostic sweep
  python models/backtest.py --day 2026-03-01 --gamma 0.1 --kappa 0.05
"""

import argparse
import math
import sys
import time
from itertools import product
from multiprocessing import cpu_count
from pathlib import Path

import numpy as np
import pandas as pd

# ── Optional Numba JIT ─────────────────────────────────────────────
try:
    from numba import njit as _njit
    HAS_NUMBA = True
    def njit_opt(f):
        return _njit(cache=True)(f)
except ImportError:
    HAS_NUMBA = False
    def njit_opt(f):
        return f

# ── Paths ──────────────────────────────────────────────────────────
try:
    from models.backtest_utils import default_backtest_workers
    from models.symbol_paths import ROOT, DEFAULT_SYMBOL, data_root, update_symbol_globals
except ImportError:
    from backtest_utils import default_backtest_workers
    from symbol_paths import ROOT, DEFAULT_SYMBOL, data_root, update_symbol_globals

try:
    from data_quality import filter_frame_for_orderbook_quality, filter_paths_for_orderbook_quality
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data_quality import filter_frame_for_orderbook_quality, filter_paths_for_orderbook_quality

BARS_DIR = data_root(ROOT) / "bars_1s"

SYMBOL = DEFAULT_SYMBOL
update_symbol_globals(globals(), SYMBOL, results_key="RESULTS_DIR")


def configure_symbol(symbol=None):
    update_symbol_globals(globals(), symbol, results_key="RESULTS_DIR")

TICK = 0.1  # legacy bar diagnostic tick; tick replay/live config is authoritative.

# ── Legacy bar-level sweep grids (v2.0 reachability-constrained) ────
# These grids are archival diagnostics.  The current tick/live path uses
# p3_kappa_eff when available, so strategy.kappa is not an active tuning axis.
SWEEP_COARSE = {
    "gamma": [0.01, 0.05, 0.1, 0.2, 0.5, 1.0],
    "kappa": [0.02, 0.05, 0.1, 0.5],
}
SWEEP_REFINE = {
    "gamma": [0.01, 0.02, 0.05, 0.1, 0.2],
    "kappa": [0.02, 0.05, 0.1, 0.2],
    "max_inventory": [0.01, 0.02, 0.026],
    "order_size": [0.001, 0.0026],
}


# ═══════════════════════════════════════════════════════════════════
#  1. DATA
# ═══════════════════════════════════════════════════════════════════

def load_bars(split="test", day=None):
    if not day or len(str(day)) != 10:
        raise SystemExit("Legacy bar backtest requires --day YYYY-MM-DD; implicit ranges are disabled.")
    files = sorted(BARS_DIR.glob(f"{SYMBOL}-1s-*.parquet"))
    # Daily-only: avoid implicit train/val/test ranges here.  The bar model is
    # too coarse for promotion, so every run should name its UTC day explicitly.
    files = [f for f in files if day in f.name]
    files = filter_paths_for_orderbook_quality(files, SYMBOL, label="1s bar")
    if not files:
        print(f"Error: no files for split='{split}', day='{day}'")
        sys.exit(1)

    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        dfs.append(df)
        print(f"  {f.name}: {len(df):>10,} bars")
    bars = pd.concat(dfs).sort_index()
    bars = bars[~bars.index.duplicated(keep="first")]
    bars = filter_frame_for_orderbook_quality(bars, SYMBOL, label="1s bar")
    print(f"  {'Total':<30s} {len(bars):>10,} bars  "
          f"({len(bars)/86400:.1f} days)\n")
    return bars


def prepare_arrays(bars, sigma_window=60):
    """Contiguous arrays + rolling variance (vectorised, M4 SIMD)."""
    # Simulation clocks, requote intervals, and reporting all use epoch ms.
    # DatetimeIndex exposes ns, so passing it through unchanged makes a 1s bar
    # look one million times longer than it is.
    ts = bars.index.to_numpy(dtype="datetime64[ms]").astype(np.int64)
    hi = bars["high"].values.astype(np.float64)
    lo = bars["low"].values.astype(np.float64)
    cl = bars["close"].values.astype(np.float64)

    diffs = np.empty_like(cl)
    diffs[0] = 0.0
    diffs[1:] = cl[1:] - cl[:-1]
    ssq = (pd.Series(diffs)
           .rolling(sigma_window, min_periods=max(10, sigma_window // 3))
           .var()
           .ffill().bfill()
           .values.astype(np.float64))
    ssq = np.maximum(ssq, 1e-6)
    return ts, hi, lo, cl, ssq


# ═══════════════════════════════════════════════════════════════════
#  2. SIMULATION CORE  (Numba-compiled if available)
# ═══════════════════════════════════════════════════════════════════

@njit_opt
def _simulate_core(ts, hi, lo, cl, ssq,
                   gamma, kappa, order_size, max_inv,
                   rq_ms, fee, taker_fee, tick, sample_rate):
    """Hot loop — compiled to ARM64 by Numba on M4, or runs as CPython."""
    n = len(ts)
    ns = (n + sample_rate - 1) // sample_rate
    pnl_s = np.empty(ns, np.float64)
    inv_s = np.empty(ns, np.float64)
    ts_s  = np.empty(ns, np.int64)

    q = 0.0;  cash = 0.0
    bp = 0.0; ap = 0.0
    hb = False; ha = False
    lrt = ts[0] - rq_ms

    nfb = 0; nfa = 0; nrq = 0
    tsprd = 0.0; mx = 0.0; si_sum = 0.0
    si = 0

    spread_const = (2.0 / gamma) * np.log(1.0 + gamma / kappa)

    for i in range(n):
        mid = cl[i]

        # ── fills (touch-fill: price reaches limit price) ──
        if hb and lo[i] <= bp:
            cash -= bp * order_size * (1.0 + fee)
            q += order_size
            hb = False; nfb += 1
        if ha and hi[i] >= ap:
            cash += ap * order_size * (1.0 - fee)
            q -= order_size
            ha = False; nfa += 1

        # ── requote ──
        if ts[i] - lrt >= rq_ms:
            s = ssq[i]
            r = mid - q * gamma * s
            d = gamma * s + spread_const
            mn = 2.0 * fee * mid + tick
            if d < mn:
                d = mn
            hd = d * 0.5
            nbid = np.floor((r - hd) / tick) * tick
            nask = np.ceil((r + hd) / tick) * tick

            hb = q < max_inv
            ha = q > -max_inv
            if hb:
                bp = nbid
            if ha:
                ap = nask

            tsprd += d; nrq += 1; lrt = ts[i]

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
    # Window end is MTM only; taker_fee applies only when an actual taker exit
    # is executed inside the simulation.
    fp = cash + q * last_mid
    return (pnl_s[:si], inv_s[:si], ts_s[:si],
            fp, q, cash,
            nfb, nfa, nrq, tsprd, mx, si_sum, n)


def simulate(ts, hi, lo, cl, ssq, params):
    """Run single backtest and return metrics dict."""
    gamma   = params["gamma"]
    kappa   = params["kappa"]
    osiz    = params["order_size"]
    maxinv  = params["max_inventory"]
    rq_ms   = int(params["requote_interval"] * 1000)
    fee     = params["maker_fee"]
    taker_fee = params.get("taker_fee", 0.0004)
    tick    = TICK
    sr      = max(1, rq_ms // 1000)

    raw = _simulate_core(ts, hi, lo, cl, ssq,
                         gamma, kappa, osiz, maxinv,
                         rq_ms, fee, taker_fee, tick, sr)
    return _unpack(raw, params)


def _unpack(raw, params):
    (pnl_ts, inv_ts, ts_out,
     fp, q, cash,
     nfb, nfa, nrq, tsprd, mx, si_sum, n) = raw

    n_days = n / 86400.0
    nf = nfb + nfa

    # ── PnL metrics ──
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

    return {
        "gamma": params["gamma"],
        "kappa": params["kappa"],
        "pnl": fp,
        "pnl_per_day": fp / max(n_days, 0.01),
        "sharpe": sharpe,
        "max_drawdown": max_dd,
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
        "n_days": n_days,
        # time series (only kept for single-run, dropped in sweep)
        "_pnl_ts": pnl_ts,
        "_inv_ts": inv_ts,
        "_ts": ts_out,
    }


# ═══════════════════════════════════════════════════════════════════
#  3. PARAMETER SWEEP  (multiprocessing on M4)
# ═══════════════════════════════════════════════════════════════════

# shared read-only data, set by parent before fork
_G = {}


def _init_data(ts, hi, lo, cl, ssq):
    global _G
    _G = {"ts": ts, "hi": hi, "lo": lo, "cl": cl, "ssq": ssq}


def _worker(params):
    r = simulate(_G["ts"], _G["hi"], _G["lo"], _G["cl"], _G["ssq"], params)
    # drop time series to save IPC bandwidth, keep param info
    out = {k: v for k, v in r.items() if not k.startswith("_")}
    out["order_size"] = params["order_size"]
    out["max_inv_param"] = params["max_inventory"]
    return out


def run_sweep(ts, hi, lo, cl, ssq, base_params,
              n_workers=None, refine=False):
    import multiprocessing as mp

    _init_data(ts, hi, lo, cl, ssq)

    grid = SWEEP_REFINE if refine else SWEEP_COARSE
    # build all combinations from grid
    keys = list(grid.keys())
    vals = [grid[k] for k in keys]
    combos = list(product(*vals))

    params_list = []
    for combo in combos:
        p = dict(base_params)
        for k, v in zip(keys, combo):
            p[k] = v
        params_list.append(p)

    n_combos = len(params_list)
    nw = n_workers or default_backtest_workers()

    print(f"Sweep: {n_combos} combos × {len(ts):,} bars, {nw} workers\n")

    # ── single-core benchmark (1 run) ──
    t0 = time.perf_counter()
    _worker(params_list[0])
    t_one = time.perf_counter() - t0
    est_seq = t_one * n_combos

    # ── parallel sweep ──
    ctx = mp.get_context("fork")
    t0 = time.perf_counter()
    with ctx.Pool(nw) as pool:
        results = pool.map(_worker, params_list)
    t_par = time.perf_counter() - t0

    speedup = est_seq / t_par if t_par > 0 else 0

    print("┌─── Apple M4 Performance ──────────────────────────────┐")
    print(f"│  Single run:      {t_one:7.2f}s                          │")
    print(f"│  Sequential est:  {est_seq:7.1f}s  ({n_combos} runs)             │")
    print(f"│  Parallel:        {t_par:7.1f}s  ({nw} cores)              │")
    print(f"│  Speedup:         {speedup:7.1f}×                          │")
    jit = "✓ Numba ARM64" if HAS_NUMBA else "✗ CPython (pip3 install numba)"
    print(f"│  JIT:             {jit:<38s}│")
    print("└───────────────────────────────────────────────────────┘\n")

    return sorted(results, key=lambda r: r["pnl"], reverse=True)


# ═══════════════════════════════════════════════════════════════════
#  4. OUTPUT
# ═══════════════════════════════════════════════════════════════════

_HDR = (f"{'Rank':>4s}  {'γ':>7s}  {'κ':>7s}  {'OrdSz':>6s}  {'MaxPos':>6s}  "
        f"{'PnL($)':>10s}  {'$/day':>9s}  {'Sharpe':>7s}  {'MaxDD($)':>10s}  "
        f"{'Fills/d':>8s}  {'Fill%':>6s}  {'Spread':>7s}  "
        f"{'MaxInv':>7s}  {'AvgInv':>7s}")


def _row(i, r):
    return (f"{i:4d}  {r['gamma']:7.3f}  {r['kappa']:7.3f}  "
            f"{r.get('order_size',0.01):6.3f}  "
            f"{r.get('max_inventory',0.1):6.2f}  "
            f"{r['pnl']:10.2f}  {r['pnl_per_day']:9.2f}  "
            f"{r['sharpe']:7.2f}  {r['max_drawdown']:10.2f}  "
            f"{r['fills_per_day']:8.1f}  "
            f"{r['fill_rate']*100:5.1f}%  "
            f"{r['avg_spread']:7.2f}  "
            f"{r['max_inventory']:7.4f}  "
            f"{r['avg_inventory']:7.4f}")


def print_results(results, top_n=15):
    print("=" * 105)
    print(_HDR)
    print("-" * 105)
    for i, r in enumerate(results[:top_n], 1):
        print(_row(i, r))
    print("=" * 105)


def save_sweep_csv(results):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(results)
    path = RESULTS_DIR / "sweep_results.csv"
    df.to_csv(path, index=False, float_format="%.6f")
    print(f"\nSweep results saved → {path}")


def save_pnl_series(result):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "timestamp": result["_ts"],
        "pnl": result["_pnl_ts"],
        "inventory": result["_inv_ts"],
    })
    path = RESULTS_DIR / "best_pnl_series.parquet"
    df.to_parquet(path)
    print(f"PnL series saved → {path}")


# ═══════════════════════════════════════════════════════════════════
#  5. MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="AS Backtest on 1s bars")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL,
                    help=f"Symbol (default {DEFAULT_SYMBOL}; MM_SYMBOL also supported)")
    ap.add_argument("--split", default="test",
                    choices=["train", "val", "test", "all"])
    ap.add_argument("--day", required=True, help="UTC day, e.g. 2026-03-01")
    ap.add_argument("--sweep", action="store_true", help="Coarse sweep")
    ap.add_argument("--refine", action="store_true", help="Refined sweep")
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--sigma-window", type=int, default=60)
    ap.add_argument("--gamma", type=float, default=0.1)
    ap.add_argument("--kappa", type=float, default=0.05)
    ap.add_argument("--order-size", type=float, default=0.01)
    ap.add_argument("--max-inventory", type=float, default=0.1)
    ap.add_argument("--requote-interval", type=float, default=10)
    ap.add_argument("--maker-fee", type=float, default=0.0)
    ap.add_argument("--taker-fee", type=float, default=0.00036)
    args = ap.parse_args()
    configure_symbol(args.symbol)

    # ── hardware info ──
    import platform
    ncpu = cpu_count()
    print(f"\n{'='*60}")
    print(f"  {SYMBOL} Avellaneda-Stoikov Backtest")
    print(f"  Chip: {platform.processor() or 'Apple M4'}  "
          f"Cores: {ncpu}  Numba: {'✓' if HAS_NUMBA else '✗'}")
    print(f"{'='*60}\n")

    # ── load data ──
    print("Loading 1s bars …")
    bars = load_bars(split=args.split, day=args.day)

    print("Pre-computing rolling volatility …")
    t0 = time.perf_counter()
    ts, hi, lo, cl, ssq = prepare_arrays(bars, args.sigma_window)
    t_prep = time.perf_counter() - t0
    print(f"  σ² computed in {t_prep:.2f}s  "
          f"(median σ²={np.median(ssq):.2f}, "
          f"mean spread_min=${2*args.maker_fee*np.mean(cl)+TICK:.1f})\n")

    base = {
        "gamma": args.gamma,
        "kappa": args.kappa,
        "order_size": args.order_size,
        "max_inventory": args.max_inventory,
        "requote_interval": args.requote_interval,
        "maker_fee": args.maker_fee,
        "taker_fee": args.taker_fee,
    }

    if args.sweep or args.refine:
        # ── parallel parameter sweep ──
        results = run_sweep(ts, hi, lo, cl, ssq, base,
                            args.workers, refine=args.refine)
        print_results(results)
        save_sweep_csv(results)

        # re-run best with time series
        best_p = dict(base)
        best_p["gamma"] = results[0]["gamma"]
        best_p["kappa"] = results[0]["kappa"]
        best_result = simulate(ts, hi, lo, cl, ssq, best_p)
        save_pnl_series(best_result)
    else:
        # ── single run ──
        print(f"Running AS backtest  γ={args.gamma}  κ={args.kappa}  "
              f"order={args.order_size} BTC …")
        t0 = time.perf_counter()
        result = simulate(ts, hi, lo, cl, ssq, base)
        t_sim = time.perf_counter() - t0

        bars_per_sec = len(ts) / t_sim
        print(f"  Simulation: {t_sim:.2f}s  "
              f"({bars_per_sec/1e6:.1f}M bars/s"
              f"{' — Numba JIT' if HAS_NUMBA else ' — CPython'})\n")

        print_results([{k: v for k, v in result.items()
                        if not k.startswith("_")}
                       | {"order_size": args.order_size,
                          "max_inv_param": args.max_inventory}])
        save_pnl_series(result)

    print()


if __name__ == "__main__":
    main()
