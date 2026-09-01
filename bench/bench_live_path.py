#!/usr/bin/env python3
"""Benchmark live-layer CPU paths without connecting to exchange or placing orders.

This drives the same SignalEngine and MakerEngine quote/policy methods used by
live/main.py with synthetic market events.  It is meant to answer whether a C++
quote-core migration materially improves the live loop after SignalEngine and
policy overhead are included.

Examples:
    python3 bench/bench_live_path.py --n 5000
    python3 bench/bench_live_path.py --n 5000 --ml
    python3 bench/bench_live_path.py --n 5000 --engine cpp --strict-cpp
    python3 bench/bench_live_path.py --n 2000 --cprofile
"""

from __future__ import annotations

import argparse
import contextlib
import cProfile
import gc
import io
import logging
import math
import os
import pstats
import sys
import time
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap_cpp_module_path() -> Optional[Path]:
    """Make local benchmark builds importable without requiring PYTHONPATH."""
    repo_token = ROOT.name.lower()
    candidates: list[Path] = []
    env_build = os.environ.get("NARROWGATE_CPP_BUILD_DIR")
    if env_build:
        candidates.append(Path(env_build).expanduser())
    candidates.extend([
        ROOT / "cpp" / "build",
        ROOT / "build" / "cpp",
        ROOT / "build",
        Path("/tmp") / f"{repo_token.lower()}_cpp_build",
        Path("/tmp") / f"{repo_token.replace('_', '-').lower()}_cpp_build",
        Path("/tmp") / "narrowgate_cpp_build",
    ])
    for candidate in candidates:
        if not candidate.exists():
            continue
        if any(candidate.glob("narrowgate_cpp*.so")):
            resolved = candidate.resolve()
            if str(resolved) not in sys.path:
                sys.path.insert(0, str(resolved))
            return resolved
    return None


_CPP_BOOTSTRAP_DIR = _bootstrap_cpp_module_path()

from live.config import load_config  # noqa: E402
from strategy.inventory_manager import InventoryManager  # noqa: E402
from strategy.maker_engine import MakerEngine, Side, _resolve_model_dir  # noqa: E402
from strategy.signal import (  # noqa: E402
    Prediction,
    SIGNAL_COMPUTE_PHASE_FIELDS,
    SignalEngine,
)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    idx = (len(values) - 1) * pct
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return values[lo]
    weight = idx - lo
    return values[lo] * (1.0 - weight) + values[hi] * weight


def _time_samples(label: str, iterations: int, fn: Callable[[int], object]) -> dict[str, float]:
    samples: list[float] = []
    checksum = 0.0
    for idx in range(iterations):
        start = time.perf_counter_ns()
        result = fn(idx)
        elapsed_us = (time.perf_counter_ns() - start) / 1000.0
        samples.append(elapsed_us)
        if isinstance(result, (int, float)):
            checksum += float(result)
    samples.sort()
    total_us = sum(samples)
    mean_us = total_us / len(samples) if samples else 0.0
    row = {
        "label": label,
        "n": float(iterations),
        "mean_us": mean_us,
        "p50_us": _percentile(samples, 0.50),
        "p95_us": _percentile(samples, 0.95),
        "p99_us": _percentile(samples, 0.99),
        "p999_us": _percentile(samples, 0.999),
        "total_ms": total_us / 1000.0,
        "checksum": checksum,
    }
    return row


def _signal_span_rows(
    label: str,
    samples: dict[str, list[float]],
) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for name in (
        *SIGNAL_COMPUTE_PHASE_FIELDS,
        "signal_compute_accounted_us",
        "signal_compute_residual_us",
        "signal_compute_us",
    ):
        values = sorted(samples.get(name, []))
        if not values:
            continue
        total_us = sum(values)
        span_label = (
            "total_us"
            if name == "signal_compute_us"
            else name.removeprefix("signal_compute_")
        )
        rows.append(
            {
                "label": f"{label} {span_label}",
                "n": float(len(values)),
                "mean_us": total_us / len(values),
                "p50_us": _percentile(values, 0.50),
                "p95_us": _percentile(values, 0.95),
                "p99_us": _percentile(values, 0.99),
                "p999_us": _percentile(values, 0.999),
                "total_ms": total_us / 1000.0,
                "checksum": 0.0,
            }
        )
    return rows


def _record_signal_spans(
    timings: dict[str, object],
    samples: dict[str, list[float]],
) -> None:
    for name in (
        *SIGNAL_COMPUTE_PHASE_FIELDS,
        "signal_compute_accounted_us",
        "signal_compute_residual_us",
        "signal_compute_us",
    ):
        samples.setdefault(name, []).append(float(timings[name]))


def _compute_signal_with_wall_timing(
    signal: SignalEngine,
    timings: dict[str, object],
) -> Prediction:
    start_ns = time.perf_counter_ns()
    try:
        return signal.compute_signal(perf_timings=timings)
    finally:
        timings["signal_compute_us"] = (
            time.perf_counter_ns() - start_ns
        ) / 1_000.0
        timings["signal_compute_residual_us"] = max(
            0.0,
            float(timings["signal_compute_us"])
            - float(timings["signal_compute_accounted_us"]),
        )


def _print_table(rows: list[dict[str, float]]) -> None:
    print(
        f"{'path':32s} {'n':>8s} {'mean_us':>10s} {'p50_us':>10s} "
        f"{'p95_us':>10s} {'p99_us':>10s} {'p999_us':>10s} {'total_ms':>10s}"
    )
    for row in rows:
        print(
            f"{str(row['label']):32s} "
            f"{int(row['n']):8d} "
            f"{row['mean_us']:10.2f} "
            f"{row['p50_us']:10.2f} "
            f"{row['p95_us']:10.2f} "
            f"{row['p99_us']:10.2f} "
            f"{row['p999_us']:10.2f} "
            f"{row['total_ms']:10.2f}"
        )


def _depth_event(ts_ms: int, mid: float, width: float = 0.1, levels: int = 20) -> dict:
    bids = []
    asks = []
    for level in range(levels):
        px_offset = width * (level + 1)
        qty_base = 1.0 + 0.05 * level
        bids.append([f"{mid - px_offset:.1f}", f"{qty_base + 0.02 * math.sin(level):.6f}"])
        asks.append([f"{mid + px_offset:.1f}", f"{qty_base + 0.02 * math.cos(level):.6f}"])
    return {"T": ts_ms, "b": bids, "a": asks}


def _trade_event(ts_ms: int, mid: float, idx: int) -> dict:
    price = mid + math.sin(idx / 7.0) * 0.8 + math.cos(idx / 17.0) * 0.2
    qty = 0.001 + (idx % 9) * 0.0002
    return {
        "s": "BTCUSDC",
        "T": ts_ms,
        "p": f"{price:.1f}",
        "q": f"{qty:.6f}",
        "m": bool(idx % 2),
    }


def _feed_second(signal: SignalEngine, ts_ms: int, idx: int, trades_per_second: int) -> None:
    mid = 65000.0 + math.sin(idx / 23.0) * 12.0 + idx * 0.01
    signal.on_depth(_depth_event(ts_ms, mid))
    for trade_idx in range(trades_per_second):
        signal.on_agg_trade(_trade_event(ts_ms + trade_idx, mid, idx * trades_per_second + trade_idx))


def _seed_signal(signal: SignalEngine, seconds: int, trades_per_second: int) -> int:
    now_ms = int(time.time() * 1000)
    start_ms = ((now_ms // 1000) - seconds - 30) * 1000
    for idx in range(seconds):
        _feed_second(signal, start_ms + idx * 1000, idx, trades_per_second)
    _feed_second(signal, start_ms + seconds * 1000, seconds, trades_per_second)
    signal.compute_signal()
    return start_ms + (seconds + 1) * 1000


def _make_signal(
    cfg,
    enable_ml: bool,
    trades_per_second: int,
    model_dir_override: str = "",
) -> tuple[SignalEngine, int]:
    model_dir = (
        Path(model_dir_override).expanduser().resolve()
        if model_dir_override
        else _resolve_model_dir(cfg)
    )
    signal = SignalEngine(
        model_dir=model_dir,
        enable_ml=enable_ml,
        rest_client=None,
        symbol=cfg.symbol,
        reference_symbol=getattr(getattr(cfg, "multi_market", None), "reference_symbol", None),
        ret_demean_halflife=cfg.ml.ret_demean_halflife,
        bad_trade_log_every=cfg.logging.bad_trade_log_every,
    )
    next_ts = _seed_signal(signal, seconds=420, trades_per_second=trades_per_second)
    return signal, next_ts


def _make_engine_shell(cfg, signal: SignalEngine) -> MakerEngine:
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = cfg
    engine.rest = None
    engine.signal = signal
    engine.inventory = InventoryManager(
        max_inventory=cfg.strategy.max_inventory,
        position_timeout=cfg.strategy.position_timeout,
        trade_log_path=None,
    )
    engine._model_dir = _resolve_model_dir(cfg)
    engine._requote_count = 1
    engine._best_bid = 64999.9
    engine._best_ask = 65000.1
    engine._ber_active = False
    engine._mo_ema_bid = -0.2
    engine._mo_ema_ask = -0.1
    engine._mo_ema_all = -0.15
    engine._mo_ref = 50.0
    engine._mo_pending = []
    engine._last_quote_context = {}
    engine._last_quote_diagnostics = {}
    engine._order_policy_context = {}
    engine._fill_cooldown_until = {"BUY": 0.0, "SELL": 0.0}
    engine._flat_unilateral_started = {"BUY": 0.0, "SELL": 0.0, "BOTH": 0.0}
    engine._last_flat_unilateral_release_log = {"BUY": 0.0, "SELL": 0.0, "BOTH": 0.0}
    engine._sync_adjust_degrade_until = 0.0
    engine._last_sync_adjust_degrade_log = 0.0
    engine._last_seen_sync_adjust_seq = 0
    engine._last_sync_adjust_user_reconnect = 0.0
    engine._ws_handler = None
    return engine


@contextlib.contextmanager
def _quote_core_engine(engine: str, strict_cpp: bool):
    old_quote = os.environ.get("NARROWGATE_CPP_QUOTE_CORE")
    old_strict = os.environ.get("NARROWGATE_CPP_STRICT")
    if engine == "cpp":
        os.environ["NARROWGATE_CPP_QUOTE_CORE"] = "1"
        if strict_cpp:
            os.environ["NARROWGATE_CPP_STRICT"] = "1"
    else:
        os.environ.pop("NARROWGATE_CPP_QUOTE_CORE", None)
        if strict_cpp:
            os.environ.pop("NARROWGATE_CPP_STRICT", None)
    try:
        yield
    finally:
        if old_quote is None:
            os.environ.pop("NARROWGATE_CPP_QUOTE_CORE", None)
        else:
            os.environ["NARROWGATE_CPP_QUOTE_CORE"] = old_quote
        if old_strict is None:
            os.environ.pop("NARROWGATE_CPP_STRICT", None)
        else:
            os.environ["NARROWGATE_CPP_STRICT"] = old_strict


def _cpp_status() -> str:
    try:
        import narrowgate_cpp
    except Exception as exc:
        return f"unavailable ({type(exc).__name__}: {exc})"
    return f"available ({getattr(narrowgate_cpp, '__file__', '<unknown>')})"


def run(args: argparse.Namespace) -> list[dict[str, float]]:
    logging.basicConfig(level=logging.ERROR)
    cfg = load_config(ROOT / "live" / "config.yaml")
    cfg.ml.enabled = bool(args.ml)
    cfg.depth_execution.shadow_enabled = False
    cfg.strategy.bid_quote_ev_enabled = False
    cfg.strategy.ask_quote_ev_enabled = False

    signal, next_ts = _make_signal(
        cfg,
        enable_ml=bool(args.ml),
        trades_per_second=args.trades_per_second,
        model_dir_override=args.model_dir,
    )
    engine = _make_engine_shell(cfg, signal)
    pred = signal.compute_signal()
    if not args.ml:
        pred = Prediction(
            ts=time.time(),
            dir_10s=0.53,
            dir_30s=0.52,
            dir_60s=0.51,
            vol_10s=1.2,
            vol_30s=1.1,
            vol_60s=1.0,
            ret_10s=2e-5,
            ret_30s=1e-5,
            ret_60s=0.0,
            tox_bid_10s=0.55,
            tox_ask_10s=0.54,
        )

    mid = signal.mid_price or 65000.0
    q = float(args.inventory)
    if not args.signal_only:
        with _quote_core_engine(args.engine, args.strict_cpp):
            engine._compute_quotes(mid, q, pred)
            engine._build_side_policy(Side.BUY, mid, q, pred)
            engine._build_side_policy(Side.SELL, mid, q, pred)

    gc.disable()
    rows: list[dict[str, float]] = []

    def ingest_trade_depth(idx: int) -> float:
        nonlocal next_ts
        _feed_second(signal, next_ts, idx, args.trades_per_second)
        next_ts += 1000
        return signal.mid_price

    rows.append(_time_samples("ingest 1s ws events", args.n, ingest_trade_depth))

    signal.compute_signal()
    rows.append(_time_samples("signal cached", args.n, lambda _idx: signal.compute_signal().vol_10s))

    cached_spans: dict[str, list[float]] = {}

    def signal_cached_telemetry(_idx: int) -> float:
        timings: dict[str, object] = {}
        value = _compute_signal_with_wall_timing(signal, timings).vol_10s
        if timings["signal_compute_path"] != "cached_no_new_bucket":
            raise RuntimeError(f"expected cached signal path, got {timings}")
        _record_signal_spans(timings, cached_spans)
        return value

    rows.append(
        _time_samples("signal cached + telemetry", args.n, signal_cached_telemetry)
    )
    rows.extend(_signal_span_rows("cached span", cached_spans))

    def signal_10s(idx: int) -> float:
        nonlocal next_ts, pred
        for step in range(10):
            _feed_second(signal, next_ts, idx * 10 + step, args.trades_per_second)
            next_ts += 1000
        pred = signal.compute_signal()
        return pred.vol_10s + pred.ret_10s

    rows.append(_time_samples("signal 10s features", max(1, args.signal_n), signal_10s))

    new_spans: dict[str, list[float]] = {}

    def signal_10s_telemetry(idx: int) -> float:
        nonlocal next_ts, pred
        for step in range(10):
            _feed_second(signal, next_ts, idx * 10 + step, args.trades_per_second)
            next_ts += 1000
        timings: dict[str, object] = {}
        pred = _compute_signal_with_wall_timing(signal, timings)
        if timings["signal_compute_path"] != "new_bucket":
            raise RuntimeError(f"expected new signal path, got {timings}")
        _record_signal_spans(timings, new_spans)
        return pred.vol_10s + pred.ret_10s

    rows.append(
        _time_samples(
            "signal 10s ingest + compute + telemetry",
            max(1, args.signal_n),
            signal_10s_telemetry,
        )
    )
    rows.extend(_signal_span_rows("new span", new_spans))

    catch_up_spans: dict[str, list[float]] = {}

    def signal_catch_up_telemetry(idx: int) -> float:
        nonlocal next_ts, pred
        for step in range(30):
            _feed_second(signal, next_ts, idx * 30 + step, args.trades_per_second)
            next_ts += 1000
        timings: dict[str, object] = {}
        pred = _compute_signal_with_wall_timing(signal, timings)
        if timings["signal_compute_path"] != "catch_up":
            raise RuntimeError(f"expected catch_up signal path, got {timings}")
        _record_signal_spans(timings, catch_up_spans)
        return pred.vol_10s + pred.ret_10s

    rows.append(
        _time_samples(
            "signal 30s ingest + catch-up telemetry",
            max(1, args.catch_up_n),
            signal_catch_up_telemetry,
        )
    )
    rows.extend(_signal_span_rows("catch-up span", catch_up_spans))

    if not args.signal_only:
        with _quote_core_engine(args.engine, args.strict_cpp):
            rows.append(_time_samples("live _compute_quotes", args.n, lambda idx: sum(engine._compute_quotes(mid + math.sin(idx / 29.0), q, pred))))

            def policy_pair(idx: int) -> float:
                policy_bid = engine._build_side_policy(Side.BUY, mid + math.sin(idx / 31.0), q, pred)
                policy_ask = engine._build_side_policy(Side.SELL, mid + math.sin(idx / 31.0), q, pred)
                return policy_bid.spread_mult + policy_ask.spread_mult + policy_bid.size_mult + policy_ask.size_mult

            rows.append(_time_samples("side policy pair", args.n, policy_pair))

            def quote_policy(idx: int) -> float:
                quote_mid = mid + math.sin(idx / 37.0)
                bid, ask, spread = engine._compute_quotes(quote_mid, q, pred)
                policy_bid = engine._build_side_policy(Side.BUY, quote_mid, q, pred)
                policy_ask = engine._build_side_policy(Side.SELL, quote_mid, q, pred)
                return bid + ask + spread + policy_bid.spread_mult + policy_ask.spread_mult

            rows.append(_time_samples("quote + policy", args.n, quote_policy))

    gc.enable()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=5000, help="iterations for hot live-path methods")
    parser.add_argument("--signal-n", type=int, default=500, help="10s signal feature iterations")
    parser.add_argument("--catch-up-n", type=int, default=50, help="30s catch-up signal iterations")
    parser.add_argument("--trades-per-second", type=int, default=4, help="synthetic aggTrade events per second")
    parser.add_argument("--inventory", type=float, default=0.0)
    parser.add_argument("--engine", choices=("python", "cpp"), default="python", help="quote core engine used inside live _compute_quotes")
    parser.add_argument("--strict-cpp", action="store_true", help="raise instead of fallback when --engine cpp cannot load narrowgate_cpp")
    parser.add_argument("--ml", action="store_true", help="load and run saved LightGBM models during SignalEngine prediction")
    parser.add_argument("--model-dir", default="", help="explicit validated 13-head model bundle for --ml")
    parser.add_argument("--signal-only", action="store_true", help="run only signal ingestion, cadence, and telemetry benchmarks")
    parser.add_argument("--cprofile", action="store_true", help="print cProfile top cumulative functions")
    parser.add_argument("--profile-lines", type=int, default=30)
    args = parser.parse_args()

    print(f"repo={ROOT}")
    print(f"python={sys.version.split()[0]} executable={sys.executable}")
    if _CPP_BOOTSTRAP_DIR is not None:
        print(f"cpp_bootstrap_dir={_CPP_BOOTSTRAP_DIR}")
    print(f"quote_core_engine={args.engine} strict_cpp={args.strict_cpp} cpp_status={_cpp_status()}")
    print(f"ml={args.ml} n={args.n} signal_n={args.signal_n} trades_per_second={args.trades_per_second}")

    if args.cprofile:
        profiler = cProfile.Profile()
        rows = profiler.runcall(run, args)
        stream = io.StringIO()
        pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats("cumtime").print_stats(args.profile_lines)
        _print_table(rows)
        print("\n[cProfile cumulative]")
        print(stream.getvalue().rstrip())
    else:
        rows = run(args)
        _print_table(rows)


if __name__ == "__main__":
    main()
