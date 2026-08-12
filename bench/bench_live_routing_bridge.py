#!/usr/bin/env python3
"""Measure the compact tuple live-routing binding.

This is a pybind/materialization microbenchmark.  Treat it as boundary-cost
evidence only, not as end-to-end live latency or expected trading improvement.
"""

from __future__ import annotations

import argparse
import gc
import statistics
import time
from dataclasses import dataclass

import narrowgate_cpp


@dataclass
class Policy:
    side: str
    mode: str = "normal"
    allow_post: bool = True
    allow_exposure_increase: bool = True
    spread_mult: float = 1.0
    size_mult: float = 1.0
    reason_mask: int = 0
    reason_text: str = "none"
    inventory_ratio: float = 0.0
    toxicity: float = 0.5
    markout_ema: float = 0.0
    depth_age_s: float = 0.0
    microprice_shift_bps: float = 0.0
    l2_quote_flip_rate: float = 0.0
    l2_book_refresh_ratio: float = 0.0
    l2_book_cancel_ratio: float = 0.0
    l2_near_depth_total: float = 0.0
    bid_quote_ev_30s: float = 0.0
    bid_quote_toxic_30s: float = 0.0
    bid_quote_fill_prob: float = 0.0
    bid_quote_fill_markout_30s: float = 0.0
    order_ttl_ms: int = 0


INPUT_VALUES = (
    65_000.0, 0.002, 64_998.0, 65_002.0, 64_999.9, 65_000.1,
    0.1, 0.001, 0.001, 5.0, 0.001, 0.02,
    1.5, False, 1.0, 20.0,
    True, 64_998.0, 250.0, True, 65_002.0, 250.0,
)


def percentile(values: list[float], q: float) -> float:
    return values[min(len(values) - 1, int((len(values) - 1) * q))]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=100_000)
    args = parser.parse_args()

    bid = Policy("BUY", allow_exposure_increase=False, spread_mult=1.25, size_mult=0.7, order_ttl_ms=1_000)
    ask = Policy("SELL", spread_mult=1.10, size_mult=0.8, order_ttl_ms=1_000)

    def invoke():
        result = narrowgate_cpp.compute_live_routing_decision(
            INPUT_VALUES,
            (bid.allow_post, bid.allow_exposure_increase, bid.spread_mult, bid.size_mult, bid.order_ttl_ms),
            (ask.allow_post, ask.allow_exposure_increase, ask.spread_mult, ask.size_mult, ask.order_ttl_ms),
        )
        return result[0] + result[1] + result[9] + result[10]

    for _ in range(2_000):
        invoke()
    gc.disable()
    samples = []
    checksum = 0.0
    for _ in range(args.n):
        start = time.perf_counter_ns()
        checksum += invoke()
        samples.append((time.perf_counter_ns() - start) / 1_000.0)
    gc.enable()
    samples.sort()

    print(f"module={narrowgate_cpp.__file__}")
    print(f"api=compact n={args.n} checksum={checksum:.3f}")
    print(
        f"mean_us={statistics.fmean(samples):.3f} "
        f"p50_us={percentile(samples, 0.50):.3f} "
        f"p95_us={percentile(samples, 0.95):.3f} "
        f"p99_us={percentile(samples, 0.99):.3f} "
        f"p999_us={percentile(samples, 0.999):.3f}"
    )


if __name__ == "__main__":
    main()
