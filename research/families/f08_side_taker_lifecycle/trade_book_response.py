"""Read-only clock admission for trade/book response research.

Uses the existing source/observation producer; this is not a strategy runner,
feature fit, alternative parser, or evidence of native/live clock equivalence.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, replace
import json
from pathlib import Path
import resource
import sys
import time

from data.observation import TradeContribution
from data.runtime import ConsumerBundle, ObservationProfile, PublicInputStream
from data.tardis_input import BookMessage, BookView, CONTRACT, TradeExecution


class ClockAdmission:
    """Bounded diagnostics, preserving delivery order and missing clocks."""

    def __init__(self):
        self.counts = Counter()
        self.delays = {}
        self.previous_tick = None
        self.previous_ready = None
        self.book_watermark = None
        self.source_watermarks = {}

    def _delay(self, name, value):
        if value < 0:
            raise ValueError(f"negative {name}")
        row = self.delays.setdefault(name, {"count": 0, "sum_ns": 0, "max_ns": 0})
        row["count"] += 1
        row["sum_ns"] += value
        row["max_ns"] = max(row["max_ns"], value)

    def advance(self, tick):
        if self.previous_tick is not None and tick.now_ns < self.previous_tick:
            raise ValueError("input scheduler time moved backwards")
        self.previous_tick = tick.now_ns
        self.counts["input_ticks"] += 1
        for event in tick.exchange_events:
            kind = "book" if isinstance(event, BookMessage) else "trade"
            if not isinstance(event, (BookMessage, TradeExecution)):
                raise TypeError("unsupported source event")
            self.counts[f"source_{kind}"] += 1
            self.counts[f"{kind}_clock:{event.source_clock_kind}"] += 1
            self.counts[f"{kind}_native_exchange_missing"] += event.exchange_ts_ns is None
            source = event.source_timestamp_us * 1000
            previous = self.source_watermarks.get(kind)
            if previous is not None:
                self.counts[f"{kind}_source_regressions"] += source < previous
                self.counts[f"{kind}_same_source_time"] += source == previous
            self.source_watermarks[kind] = source
            if kind == "book":
                self.counts["source_book_levels"] += len(event.levels)
                self.counts[f"book_kind:{event.kind}"] += 1
        for observation in tick.observations:
            if observation.ready_ns > tick.now_ns:
                raise ValueError("observation consumed before ready")
            if self.previous_ready is not None and observation.ready_ns < self.previous_ready:
                raise ValueError("delivery order moved backwards")
            self.previous_ready = observation.ready_ns
            kind = "trade" if isinstance(observation.payload, TradeContribution) else "book"
            if not isinstance(observation.payload, (TradeContribution, BookView)):
                raise TypeError("unsupported observation payload")
            self.counts[f"visible_{kind}"] += 1
            self.counts[f"{kind}_coverage:{observation.coverage}"] += 1
            for name, value in (
                ("source_to_publish", observation.publish_ns - observation.source_asof_ns),
                ("publish_to_receive", observation.receive_ns - observation.publish_ns),
                ("receive_to_ready", observation.ready_ns - observation.receive_ns),
                ("ready_to_read", tick.now_ns - observation.ready_ns),
            ):
                self._delay(f"{kind}_{name}", value)
            if kind == "book":
                view = observation.payload
                key = (observation.source_asof_ns, view.version)
                stale_replacement = self.book_watermark is not None and key < self.book_watermark
                self.counts["late_replacement_book_observations"] += stale_replacement
                # This is a diagnostic watermark, not mutation of the input or
                # existing consumer. A later feature adapter must reject these.
                if not stale_replacement:
                    self.book_watermark = key
                self.counts["invalid_visible_books"] += not view.valid

    def report(self):
        return {"counts": dict(self.counts), "delays": self.delays,
                "units": {"delays": "nanoseconds", "source_book_levels": "level rows"},
                "clock_claim": "modeled delivery; native/live parity not established"}


def audit_bundle(bundle, *, start_ns, end_ns, output, transport_order="bound"):
    """Replay only the original market-input adapter, never the strategy.

    The selected interval must remain inside the original bundle contract.
    Source hashes are not recomputed: existing schema/source metadata checks
    remain in read_facts. The output explicitly records this verification scope.
    """
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    consumer = ConsumerBundle(bundle)
    manifest = consumer.manifest
    plan = manifest["plan"]
    if not plan["start_ns"] <= start_ns < end_ns <= plan["end_ns"]:
        raise ValueError("diagnostic interval outside the bound consumer interval")
    if start_ns % 1_000_000_000:
        raise ValueError("start must use an existing whole-second boundary")
    profile = ObservationProfile(**plan["observation_profile"])
    if transport_order == "separate_ws":
        profile = replace(profile, depth_connection_id="depth:epoch0", trade_connection_id="trade:epoch0")
    elif transport_order == "shared_ws":
        profile = replace(profile, depth_connection_id="market:epoch0", trade_connection_id="market:epoch0")
    elif transport_order != "bound":
        raise ValueError("unknown transport-order scenario")
    roots = consumer.source_paths()
    # Preserve original warmup/publication phase and serial delivery backlog.
    # Cutting the producer directly at start_ns would change ready times.
    stream = PublicInputStream(roots, profile=profile, start_ns=plan["start_ns"],
        end_ns=end_ns, market_id=plan["market_id"], input_contract_id=CONTRACT, verify=False)
    audit = ClockAdmission()
    wall, cpu = time.monotonic(), time.process_time()
    warmup_ticks = 0
    for tick in stream:
        if tick.now_ns < start_ns:
            warmup_ticks += 1
            continue
        audit.advance(tick)
    elapsed, cpu_seconds = time.monotonic() - wall, time.process_time() - cpu
    result = {
        "visibility": "local_only_do_not_publish", "stage": "input_clock_admission",
        "status": "complete", "market_research_complete": False,
        "strategy_replays": 0, "strategy_speed_multiple": None,
        "interval": {"start_ns": start_ns, "end_ns": end_ns},
        "producer_start_ns": plan["start_ns"], "warmup_ticks_excluded": warmup_ticks,
        "profile": asdict(profile), "bound_profile": plan["observation_profile"],
        "transport_order_scenario": transport_order,
        "scenario_override_is_not_historical_B0": transport_order != "bound",
        "adapter_stats": stream.stats,
        **audit.report(),
        "cost": {"wall_seconds": elapsed, "cpu_seconds": cpu_seconds,
                 "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                 * (1 if sys.platform == "darwin" else 1024),
                 "input_only_interval_wall_multiple": (end_ns - start_ns) / 1e9 / elapsed},
        "verification": "original fact schema/source metadata; no repeated content hashes",
        "limitations": ["input throughput is not strategy replay throughput",
                        "no market prediction, action labels or economic comparison executed",
                        "counts exclude pre-interval warmup; cost includes it"],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--start-ns", type=int, required=True)
    parser.add_argument("--end-ns", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--transport-order", choices=("bound", "separate_ws", "shared_ws"), default="bound",
                        help="explicit diagnostic override only; never rewrites the bound bundle")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; original diagnostics are not overwritten")
    report = audit_bundle(args.bundle, start_ns=args.start_ns, end_ns=args.end_ns,
                          output=args.output, transport_order=args.transport_order)
    print(json.dumps({"status": report["status"], "cost": report["cost"],
                      "strategy_replays": 0}))


if __name__ == "__main__":
    main()
