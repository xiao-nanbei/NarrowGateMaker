#!/usr/bin/env python3
"""Benchmark Python and fixed-array native cross-venue flow ingestion.

The signal-path rows include the same NumPy normalization, market source-state
update, 1s bar rollover, and global-flow update used by venue callbacks.  No
worker or dispatcher thread is created.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategy.global_flow import GlobalFlowEngine  # noqa: E402
from strategy.signal import SignalEngine  # noqa: E402


def _native_module():
    try:
        import narrowgate_cpp
    except ImportError as exc:
        raise SystemExit(f"narrowgate_cpp is required: {exc}") from exc
    required = ("NativeGlobalFlowEngine", "TradeBarAggregator")
    missing = [name for name in required if not hasattr(narrowgate_cpp, name)]
    if missing:
        raise SystemExit(f"narrowgate_cpp missing APIs: {', '.join(missing)}")
    if not hasattr(narrowgate_cpp.TradeBarAggregator(False), "update_batch"):
        raise SystemExit("narrowgate_cpp missing TradeBarAggregator.update_batch")
    return narrowgate_cpp


def _frames(frame_size: int, count: int):
    start_ms = 1_800_000_000_000
    output = []
    for frame_index in range(count):
        frame_start = start_ms + frame_index * 5
        offsets = np.arange(frame_size, dtype=np.int64) % 5
        ts_ms = np.ascontiguousarray(frame_start + offsets, dtype=np.int64)
        phase = frame_index * frame_size + np.arange(frame_size)
        prices = np.ascontiguousarray(
            60_000.0 + np.sin(phase / 17.0) * 0.5, dtype=np.float64
        )
        sizes = np.ascontiguousarray(
            0.001 + (phase % 9) * 0.0001, dtype=np.float64
        )
        maker = np.ascontiguousarray(phase % 2, dtype=np.uint8)
        receive_ns = int(ts_ms[-1]) * 1_000_000 + 2_000_000
        output.append((ts_ms, prices, sizes, maker, receive_ns))
    return output


def _measure(label: str, frame_size: int, frames, consume_factory, rounds: int) -> dict:
    samples = []
    accepted = 0
    for _ in range(rounds):
        consume = consume_factory()
        start = time.perf_counter_ns()
        round_accepted = 0
        for ts_ms, prices, sizes, maker, receive_ns in frames:
            round_accepted += int(
                consume(ts_ms, prices, sizes, maker, receive_ns) or 0
            )
        elapsed_s = (time.perf_counter_ns() - start) / 1_000_000_000.0
        samples.append(elapsed_s)
        accepted = round_accepted
    median_s = statistics.median(samples)
    event_count = len(frames) * frame_size
    return {
        "path": label,
        "frame_size": frame_size,
        "frames": len(frames),
        "events": event_count,
        "accepted_last_round": accepted,
        "median_ms": median_s * 1_000.0,
        "us_per_frame": median_s * 1_000_000.0 / max(1, len(frames)),
        "ns_per_event": median_s * 1_000_000_000.0 / max(1, event_count),
        "events_per_s": event_count / max(median_s, 1e-12),
    }


def _flow_consumer(native: bool, cpp):
    backend = cpp.NativeGlobalFlowEngine(2_000, 1_000.0, 1_000.0) if native else None
    engine = GlobalFlowEngine(
        horizons_ms=(10, 25, 50, 100, 250, 500),
        native_backend=backend,
    )
    market_id = "bybit:perp:BTCUSDT"

    def consume(ts_ms, prices, sizes, maker, receive_ns):
        return engine.on_trade_batch(
            market_id,
            receive_ts_ns=receive_ns,
            exchange_ts_ns=np.ascontiguousarray(ts_ms * 1_000_000),
            prices=prices,
            sizes=sizes,
            is_buyer_maker=maker,
        )

    return consume


def _signal_consumer(native: bool):
    flag = "NARROWGATE_CPP_GLOBAL_FLOW"
    prior_flag = os.environ.get(flag)
    prior_strict = os.environ.get("NARROWGATE_CPP_STRICT")
    try:
        if native:
            os.environ[flag] = "1"
            os.environ["NARROWGATE_CPP_STRICT"] = "1"
        else:
            os.environ.pop(flag, None)
            os.environ.pop("NARROWGATE_CPP_STRICT", None)
        signal = SignalEngine(
            enable_ml=False, symbol="BTCUSDC", reference_symbol="BTCUSDT"
        )
    finally:
        if prior_flag is None:
            os.environ.pop(flag, None)
        else:
            os.environ[flag] = prior_flag
        if prior_strict is None:
            os.environ.pop("NARROWGATE_CPP_STRICT", None)
        else:
            os.environ["NARROWGATE_CPP_STRICT"] = prior_strict

    def consume(ts_ms, prices, sizes, maker, receive_ns):
        signal.on_cross_trade_arrays(
            "BTCUSDT",
            ts_ms,
            prices,
            sizes,
            maker,
            market_type="perp",
            venue="bybit",
            receive_ts_ns=receive_ns,
        )
        return 0

    return consume


def run(args) -> list[dict]:
    cpp = _native_module()
    rows = []
    for frame_size in args.frame_sizes:
        frames = _frames(frame_size, args.frames)
        rows.append(
            _measure(
                "python-global-flow",
                frame_size,
                frames,
                lambda: _flow_consumer(False, cpp),
                args.rounds,
            )
        )
        rows.append(
            _measure(
                "native-global-flow",
                frame_size,
                frames,
                lambda: _flow_consumer(True, cpp),
                args.rounds,
            )
        )
        rows.append(
            _measure(
                "python-signal-ingress",
                frame_size,
                frames,
                lambda: _signal_consumer(False),
                args.rounds,
            )
        )
        rows.append(
            _measure(
                "native-signal-ingress",
                frame_size,
                frames,
                lambda: _signal_consumer(True),
                args.rounds,
            )
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--frames", type=int, default=500)
    parser.add_argument("--frame-sizes", type=int, nargs="+", default=[1, 8, 32])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    rows = run(args)
    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return
    print(
        f"{'path':28s} {'frame':>7s} {'us/frame':>12s} "
        f"{'ns/event':>12s} {'events/s':>13s}"
    )
    for row in rows:
        print(
            f"{row['path']:28s} {row['frame_size']:7d} "
            f"{row['us_per_frame']:12.2f} {row['ns_per_event']:12.1f} "
            f"{row['events_per_s']:13.0f}"
        )


if __name__ == "__main__":
    main()
