#!/usr/bin/env python3
"""Benchmark the simplified tick replay state machine in Python vs C++.

This synthetic benchmark is for loop/ABI throughput only.  Formal daily
candidate selection must still use the Python-authoritative replay until the
C++ engine has decision-trace and queue-calibration parity for the same window.

Run from the repo root with the extension build on PYTHONPATH, for example:

  PYTHONPATH=/tmp/narrowgate_btcusdc_cpp_build:. python3 bench/bench_tick_replay.py --n 500000
"""

from __future__ import annotations

import argparse
import math
import time

import numpy as np

import narrowgate_cpp
from strategy import quote_core as qc


def _make_params():
    params = narrowgate_cpp.TickReplayParams()
    params.order_size = 0.001
    params.max_inventory = 0.01
    params.requote_interval_s = 1.0
    params.maker_fee = 0.0
    params.queue_base = 0.0
    params.queue_decay = 0.0
    params.maker_fill_prob = 1.0
    params.initial_inventory = 0.0
    params.initial_entry_price = 0.0
    params.initial_sigma_sq = 1.0
    params.quote.gamma = 0.01
    params.quote.kappa = 1.0
    params.quote.tick_size = 0.1
    params.quote.lot_size = 0.001
    params.quote.order_size = params.order_size
    params.quote.max_inventory = params.max_inventory
    params.quote.maker_fee = params.maker_fee
    params.quote.max_spread_bps = 20.0
    params.quote.dynamic_cap_enabled = True
    params.quote.dynamic_cap_base_bps = 20.0
    return params


def _data(n: int):
    ts = np.arange(n, dtype=np.int64) * 100
    idx = np.arange(n, dtype=np.float64)
    price = 100.0 + np.sin(idx / 8.0) * 0.6 + np.sin(idx / 97.0) * 0.2
    price = np.round(price, 1).astype(np.float64)
    qty = np.full(n, 0.003, dtype=np.float64)
    is_buyer_maker = ((np.arange(n) % 2) == 0).astype(np.uint8)
    return ts, price, qty, is_buyer_maker


def _python_replay(ts, price, qty, is_buyer_maker, params):
    tick = params.quote.tick_size
    lot = params.quote.lot_size
    order_size = params.order_size
    requote_ms = int(params.requote_interval_s * 1000.0)
    quote_cfg = qc.QuoteCoreConfig(
        gamma=params.quote.gamma,
        kappa=params.quote.kappa,
        tick_size=tick,
        lot_size=lot,
        maker_fee=params.maker_fee,
        order_size=order_size,
        max_inventory=params.max_inventory,
        max_spread_bps=params.quote.max_spread_bps,
        dynamic_cap_enabled=params.quote.dynamic_cap_enabled,
        dynamic_cap_base_bps=params.quote.dynamic_cap_base_bps,
    )
    cash = 0.0
    inv = params.initial_inventory
    bid_orders = []
    ask_orders = []
    last_requote = int(ts[0]) - requote_ms
    fills_bid = 0
    fills_ask = 0
    n_requotes = 0
    spread_sum = 0.0
    abs_inventory_time_s = 0.0

    def floor_lot(value):
        return math.floor((value + 1e-12) / lot) * lot

    for i in range(len(ts)):
        t = int(ts[i])
        p = float(price[i])
        q = max(0.0, float(qty[i]))
        if i > 0:
            dt_s = max(0.0, (t - int(ts[i - 1])) / 1000.0)
            abs_inventory_time_s += abs(inv) * dt_s

        if is_buyer_maker[i]:
            remaining = q
            kept = []
            for order_price, remaining_order in bid_orders:
                if remaining >= lot and remaining_order >= lot and p <= order_price:
                    fill_qty = floor_lot(min(remaining_order, remaining))
                    if fill_qty >= lot:
                        cash -= order_price * fill_qty
                        inv += fill_qty
                        remaining_order -= fill_qty
                        remaining -= fill_qty
                        fills_bid += 1
                if remaining_order >= lot:
                    kept.append((order_price, remaining_order))
            bid_orders = kept
        else:
            remaining = q
            kept = []
            for order_price, remaining_order in ask_orders:
                if remaining >= lot and remaining_order >= lot and p >= order_price:
                    fill_qty = floor_lot(min(remaining_order, remaining))
                    if fill_qty >= lot:
                        cash += order_price * fill_qty
                        inv -= fill_qty
                        remaining_order -= fill_qty
                        remaining -= fill_qty
                        fills_ask += 1
                if remaining_order >= lot:
                    kept.append((order_price, remaining_order))
            ask_orders = kept

        if t - last_requote < requote_ms:
            continue
        last_requote = t
        quote = qc._compute_quote_core_py(
            qc.QuoteState(
                mid=p,
                inventory=inv,
                sigma_sq=params.initial_sigma_sq,
                best_bid=p - tick,
                best_ask=p + tick,
            ),
            quote_cfg,
            qc.QuotePrediction(),
        )
        bid_orders.clear()
        ask_orders.clear()
        if inv < params.max_inventory:
            bid_orders.append((quote.bid_price, order_size))
        if inv > -params.max_inventory:
            ask_orders.append((quote.ask_price, order_size))
        spread_sum += quote.spread
        n_requotes += 1

    final_price = float(price[-1])
    return {
        "pnl": cash + inv * final_price,
        "final_inventory": inv,
        "fills_total": fills_bid + fills_ask,
        "n_requotes": n_requotes,
        "avg_spread": spread_sum / n_requotes if n_requotes else 0.0,
        "abs_inventory_time_s": abs_inventory_time_s,
    }


def _timeit(label: str, fn, n: int):
    start = time.perf_counter()
    summary = fn()
    elapsed = time.perf_counter() - start
    rate = n / elapsed if elapsed > 0.0 else float("inf")
    print(
        f"{label:18s} {elapsed:9.4f}s  {rate:12,.0f} trades/s  "
        f"pnl={summary['pnl']:.8f} fills={summary['fills_total']}"
    )
    return elapsed, summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=500_000)
    args = parser.parse_args()

    n = int(args.n)
    params = _make_params()
    ts, price, qty, is_buyer_maker = _data(n)
    print(f"Benchmarking {n:,} synthetic trades")

    t_py, py_summary = _timeit(
        "python replay",
        lambda: _python_replay(ts, price, qty, is_buyer_maker, params),
        n,
    )

    def cpp_run():
        summary = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params).summary
        return {
            "pnl": float(summary.pnl),
            "final_inventory": float(summary.final_inventory),
            "fills_total": int(summary.fills_total),
            "n_requotes": int(summary.n_requotes),
            "avg_spread": float(summary.avg_spread),
            "abs_inventory_time_s": float(summary.abs_inventory_time_s),
        }

    t_cpp, cpp_summary = _timeit("c++ replay", cpp_run, n)
    print(f"speedup c++ / python: {t_py / t_cpp:.2f}x")
    print(
        "parity deltas: "
        f"pnl={cpp_summary['pnl'] - py_summary['pnl']:+.12f}, "
        f"inventory={cpp_summary['final_inventory'] - py_summary['final_inventory']:+.12f}, "
        f"fills={cpp_summary['fills_total'] - py_summary['fills_total']:+d}"
    )


if __name__ == "__main__":
    main()
