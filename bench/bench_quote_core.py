#!/usr/bin/env python3
"""Benchmark Python quote core vs the pybind11 C++ quote core.

Scalar results mostly measure pybind/dataclass boundary cost; batch results are
offline throughput evidence for trace/label generation, not live scalar proof.

Run from the repo root with the extension build on PYTHONPATH, for example:

  PYTHONPATH=/tmp/narrowgate_btcusdc_cpp_build:. python3 bench/bench_quote_core.py --n 200000
"""

from __future__ import annotations

import argparse
import time

import numpy as np

import narrowgate_cpp
from strategy import quote_core as qc


def _cfg() -> qc.QuoteCoreConfig:
    return qc.QuoteCoreConfig(
        gamma=0.01,
        kappa=1.0,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.02,
        ml_enabled=True,
        vol_blend=0.2,
        skew_strength=0.15,
        asym_strength=0.20,
        ret_skew=0.05,
        dynamic_cap_enabled=True,
        dynamic_cap_base_bps=20.0,
        dynamic_cap_alpha=0.5,
        dynamic_cap_var_baseline=1.0,
        max_spread_bps=20.0,
        adverse_guard_enabled=True,
        adverse_markout_threshold=5.0,
        adverse_pause=False,
    )


def _cpp_cfg(cfg: qc.QuoteCoreConfig):
    out = narrowgate_cpp.QuoteCoreConfig()
    for name in qc._CPP_CFG_FIELDS:
        if hasattr(out, name):
            setattr(out, name, getattr(cfg, name))
    return out


def _data(n: int):
    idx = np.arange(n, dtype=np.float64)
    mid = 100.0 + np.sin(idx / 100.0) * 3.0
    inventory = np.sin(idx / 71.0) * 0.006
    sigma_sq = 0.5 + (np.cos(idx / 53.0) + 1.0) * 0.75
    trade_intensity = 80.0 + (np.sin(idx / 37.0) + 1.0) * 80.0
    best_bid = mid - 0.1
    best_ask = mid + 0.1
    dir_10s = 0.5 + np.sin(idx / 29.0) * 0.08
    vol_10s = 0.5 + (np.cos(idx / 43.0) + 1.0) * 0.5
    ret_10s = np.sin(idx / 97.0) * 2e-5
    tox_bid = 0.5 + np.maximum(0.0, np.sin(idx / 31.0)) * 0.2
    tox_ask = 0.5 + np.maximum(0.0, np.cos(idx / 31.0)) * 0.2
    return mid, inventory, sigma_sq, trade_intensity, best_bid, best_ask, dir_10s, vol_10s, ret_10s, tox_bid, tox_ask


def _timeit(label: str, fn, n: int):
    start = time.perf_counter()
    checksum = fn()
    elapsed = time.perf_counter() - start
    rate = n / elapsed if elapsed > 0.0 else float("inf")
    print(f"{label:24s} {elapsed:9.4f}s  {rate:12,.0f} quotes/s  checksum={checksum:.6f}")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=200_000)
    args = parser.parse_args()

    n = int(args.n)
    cfg = _cfg()
    cpp_cfg = _cpp_cfg(cfg)
    arrays = _data(n)
    mid, inventory, sigma_sq, trade_intensity, best_bid, best_ask, dir_10s, vol_10s, ret_10s, tox_bid, tox_ask = arrays

    def python_loop():
        total = 0.0
        for i in range(n):
            result = qc._compute_quote_core_py(
                qc.QuoteState(
                    mid=float(mid[i]),
                    inventory=float(inventory[i]),
                    sigma_sq=float(sigma_sq[i]),
                    trade_intensity=float(trade_intensity[i]),
                    best_bid=float(best_bid[i]),
                    best_ask=float(best_ask[i]),
                    mo_ema_bid=-1.0,
                    mo_ema_ask=-1.0,
                ),
                cfg,
                qc.QuotePrediction(
                    dir_10s=float(dir_10s[i]),
                    vol_10s=float(vol_10s[i]),
                    ret_10s=float(ret_10s[i]),
                    tox_bid=float(tox_bid[i]),
                    tox_ask=float(tox_ask[i]),
                ),
            )
            total += result.bid_price + result.ask_price
        return total

    def cpp_scalar_loop():
        total = 0.0
        for i in range(n):
            state = narrowgate_cpp.QuoteState()
            state.mid = float(mid[i])
            state.inventory = float(inventory[i])
            state.sigma_sq = float(sigma_sq[i])
            state.trade_intensity = float(trade_intensity[i])
            state.best_bid = float(best_bid[i])
            state.best_ask = float(best_ask[i])
            state.mo_ema_bid = -1.0
            state.mo_ema_ask = -1.0
            pred = narrowgate_cpp.QuotePrediction()
            pred.dir_10s = float(dir_10s[i])
            pred.vol_10s = float(vol_10s[i])
            pred.ret_10s = float(ret_10s[i])
            pred.tox_bid = float(tox_bid[i])
            pred.tox_ask = float(tox_ask[i])
            result = narrowgate_cpp.compute_quote_core(state, cpp_cfg, pred)
            total += result.bid_price + result.ask_price
        return total

    def cpp_batch():
        out = narrowgate_cpp.compute_quote_core_batch(
            mid,
            inventory,
            sigma_sq,
            trade_intensity,
            best_bid,
            best_ask,
            dir_10s,
            vol_10s,
            ret_10s,
            tox_bid,
            tox_ask,
            cpp_cfg,
        )
        return float(np.sum(out["bid_price"]) + np.sum(out["ask_price"]))

    print(f"Benchmarking {n:,} quote decisions")
    t_py = _timeit("python scalar", python_loop, n)
    t_cpp_scalar = _timeit("c++ scalar binding", cpp_scalar_loop, n)
    t_cpp_batch = _timeit("c++ batch", cpp_batch, n)
    print(f"speedup c++ scalar / python: {t_py / t_cpp_scalar:.2f}x")
    print(f"speedup c++ batch  / python: {t_py / t_cpp_batch:.2f}x")


if __name__ == "__main__":
    main()
