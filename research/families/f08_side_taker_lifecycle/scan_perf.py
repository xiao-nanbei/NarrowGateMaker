"""Explicit bounded market-scan oracle/benchmark; no strategy execution.

Run each measurement in a fresh process. Supply the same private configuration
and interval to reference/candidate/candidate/reference, without a profiler.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import json
from pathlib import Path
import resource
import sys
import time


class ScanPerf:
    """Opt-in coarse timers. Do not use a context manager per price lookup."""

    def __init__(self):
        self.counters = Counter()

    @contextmanager
    def measure(self, name):
        start = time.perf_counter_ns()
        try:
            yield
        finally:
            self.counters[name] += time.perf_counter_ns() - start


def run(config, *, seconds=900, offset_seconds=0, timed=False, fast=False,
        top_cache=False, response_state=False, range_block_size=None, fact_batch_size=None,
        scan_no_features=False, scan_skip_book_view=False, compact_pending=False,
        parquet_path=None, columnar_output=False, compression="zstd"):
    from data.runtime import ObservationProfile, PublicInputStream
    from data.tardis_input import CONTRACT
    from research.families.f08_side_taker_lifecycle.joint_trade_book_response import MarketPanels

    start = config["start_ns"]
    end = start + (offset_seconds + seconds) * 10**9
    profile = ObservationProfile(**config["observation_profile"])
    # Earlier events must be consumed to retain continuity in the stable window.
    panel_kwargs = {}
    if fast or response_state:
        panel_kwargs["fast_response_state"] = True
    if range_block_size:
        panel_kwargs["range_block_size"] = range_block_size
    if compact_pending:
        panel_kwargs["compact_pending"] = True
    panels = MarketPanels(start_ns=start, end_ns=end, profile=profile, **panel_kwargs)
    roots = (list(config["facts_by_day"].values()) if "facts_by_day" in config else
             [str(Path(config["facts_root"]) / config.get("benchmark_day", "2025-08-01"))])
    kwargs = {"incremental_top_cache": True} if fast or top_cache else {}
    if fact_batch_size:
        kwargs.update(fast_fact_decode=True, fact_batch_size=fact_batch_size)
    if scan_no_features:
        kwargs["consumer_mode"] = "market_response_scan"
    if scan_skip_book_view:
        kwargs["scan_skip_book_view"] = True
    stream = PublicInputStream(roots, profile=profile, start_ns=start, end_ns=end,
        market_id=config["market_id"], input_contract_id=CONTRACT, verify=False,
        capture_source_books=True, **kwargs)
    perf = ScanPerf() if timed else None
    wall, cpu, ticks = time.perf_counter(), time.process_time(), 0
    full_wall, full_cpu = wall, cpu
    measuring = offset_seconds == 0
    rows = {}
    iterator = iter(stream)
    while True:
        try:
            if perf:
                with perf.measure("runtime.input_ns"):
                    tick = next(iterator)
            else:
                tick = next(iterator)
        except StopIteration:
            break
        if not measuring and tick.now_ns >= start + offset_seconds * 10**9:
            wall, cpu, ticks = time.perf_counter(), time.process_time(), 0
            measuring = True
            if perf:
                perf.counters.clear()
        if perf:
            with perf.measure("response.advance_ns"):
                panels.advance(tick)
        else:
            panels.advance(tick)
        for day, selected in panels.complete_days(tick.now_ns):
            rows[day] = selected
        ticks += 1
    panels.flush(end)
    rows.update({day: sample.selected() for day, sample in panels.samples.items()})
    if parquet_path:
        import pyarrow as pa
        import pyarrow.parquet as pq
        from research.families.f08_side_taker_lifecycle.joint_trade_book_response import HORIZONS
        flattened = [row for selected in rows.values() for row in selected]
        strings = {"panel", "day", "side"} | {f"{kind}_{h}" for h in HORIZONS for kind in ("missing", "recovery_event")}
        bools = {"response_conservation_qualified"} | {f"price_covered_{h}" for h in HORIZONS}
        ints = {"now_ns", "source_asof_ns", "book_version", "eligible_day_rows", "continuity_epoch", "recovery_start_ns"} | {f"reference_age_ns_{h}" for h in HORIZONS}
        schema = pa.schema([(name, pa.string() if name in strings else pa.bool_() if name in bools
                            else pa.int64() if name in ints else pa.float64()) for name in flattened[0]])
        table = (pa.Table.from_pydict({name: [row[name] for row in flattened] for name in schema.names}, schema=schema)
                 if columnar_output else pa.Table.from_pylist(flattened, schema=schema))
        pq.write_table(table, parquet_path, compression="zstd" if compression == "zstd1" else compression,
                       **({"compression_level": 1} if compression == "zstd1" else {}))
        assert pq.read_table(parquet_path).equals(table)
    elapsed, used = time.perf_counter() - wall, time.process_time() - cpu
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    cost = dict(wall_seconds=elapsed, cpu_seconds=used,
        peak_rss_bytes=rss if sys.platform == "darwin" else rss * 1024,
        ticks=ticks, ticks_per_second=ticks / elapsed,
        market_seconds_per_wall_second=seconds / elapsed,
        measured_interval_seconds=seconds,
        total_wall_seconds=time.perf_counter() - full_wall,
        total_cpu_seconds=time.process_time() - full_cpu,
        requested_stable_offset_seconds=offset_seconds,
        counters=dict(perf.counters) if perf else {})
    evidence = dict(rows=rows, daily_counters=panels.days,
        state_counters={k: dict(v.stats) for k, v in panels.states.items()},
        delivery_counters=stream.stats, idle_witness=stream.delivery_idle_ns)
    return evidence, cost


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=900)
    parser.add_argument("--offset-seconds", type=int, default=0)
    parser.add_argument("--timers", action="store_true")
    parser.add_argument("--fast", action="store_true")
    parser.add_argument("--top-cache", action="store_true")
    parser.add_argument("--response-state", action="store_true")
    parser.add_argument("--range-block-size", type=int, choices=(64, 128, 256))
    parser.add_argument("--fact-batch-size", type=int, choices=(8192, 16384, 32768, 65536))
    parser.add_argument("--scan-no-features", action="store_true")
    parser.add_argument("--scan-skip-book-view", action="store_true")
    parser.add_argument("--compact-pending", action="store_true")
    parser.add_argument("--parquet-output", action="store_true")
    parser.add_argument("--columnar-output", action="store_true")
    parser.add_argument("--compression", choices=("zstd", "zstd1", "snappy"), default="zstd")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence, cost = run(json.loads(args.config.read_text()), seconds=args.seconds,
                        offset_seconds=args.offset_seconds, timed=args.timers, fast=args.fast,
                        top_cache=args.top_cache, response_state=args.response_state,
                        range_block_size=args.range_block_size, fact_batch_size=args.fact_batch_size,
                        scan_no_features=args.scan_no_features,
                        scan_skip_book_view=args.scan_skip_book_view, compact_pending=args.compact_pending,
                        parquet_path=args.output.with_suffix(".parquet") if args.parquet_output else None,
                        columnar_output=args.columnar_output, compression=args.compression)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, separators=(",", ":"), allow_nan=False))
    cost["output_bytes"] = args.output.stat().st_size
    args.output.with_suffix(".cost.json").write_text(json.dumps(cost, indent=2))
    print(json.dumps(cost))


if __name__ == "__main__":
    main()
