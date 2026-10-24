"""Deep-book observation engineering and coverage, not economic evaluation.

Use the bound original publication/latency profile. No model is fitted or
invoked, and no strategy, order gateway or account state is imported here.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import fields, is_dataclass, replace
from decimal import Decimal
import hashlib
from itertools import zip_longest
import json
from pathlib import Path
import resource
import sys
import time

from data.depth_observation import DepthObservationTap
from data.facts import _digest, save_private_json
from data.runtime import ConsumerBundle, ObservationProfile, PublicInputStream
from data.tardis_input import CONTRACT


def canonical_value(value):
    """All typed values, independent of incidental Python object aliasing."""
    if is_dataclass(value):
        return [type(value).__module__ + "." + type(value).__qualname__,
                [[f.name, canonical_value(getattr(value, f.name))] for f in fields(value)]]
    if isinstance(value, Decimal):
        return ["decimal", str(value)]
    if isinstance(value, float):
        return ["float", value.hex()]
    if isinstance(value, (tuple, list)):
        return [type(value).__name__, [canonical_value(v) for v in value]]
    if isinstance(value, dict):
        return ["dict", [[canonical_value(k), canonical_value(v)] for k, v in sorted(value.items())]]
    if value is None or type(value) in (str, int, bool):
        return value
    raise TypeError(f"unhandled observation value: {type(value).__name__}")


def canonical_tick(tick):
    return json.dumps(canonical_value(tick), separators=(",", ":"), allow_nan=False).encode()


def admitted_observation(config):
    """Resolve a complete account from its existing ConsumerBundle owner.

    Physical calibration relocation is explicit and content checked. In
    particular it cannot change a seed, observation cadence or delivery rule.
    No features/predictions or a second prepared market view are generated.
    """
    if config.get('schema') != 'f08.depth-price-source.v1':
        raise ValueError('unknown full-account depth source')
    bundle = ConsumerBundle(config['bundle'])
    if bundle.input_manifest_id != config['input_manifest_id']:
        raise ValueError('depth source differs from accepted observation input')
    plan = bundle.manifest['plan']
    if not plan['start_ns'] <= config['start_ns'] < config['end_ns'] <= plan['end_ns']:
        raise ValueError('depth source must cover the complete original account')
    profile = ObservationProfile(**plan['observation_profile'])
    location = config.get('measured_latency_location')
    if location is not None:
        if (not profile.measured_latency_path or
                _digest(Path(location)) != profile.measured_latency_sha256):
            raise ValueError('relocated measured latency differs from original')
        profile = replace(profile, measured_latency_path=location)
    # Validate the selected path before economics, not on first capture.
    if (profile.measured_latency_path and
            _digest(Path(profile.measured_latency_path)) != profile.measured_latency_sha256):
        raise ValueError('original measured latency changed')
    roots = bundle.source_paths()
    if sys.platform == 'darwin' and any(str(p).startswith('/Volumes/') for p in roots):
        raise ValueError('external disk full-depth replay is not admitted')
    return plan, profile, roots


def streams(config, tap, verified=None):
    """Original engineering binding or full-account ConsumerBundle binding."""
    if config.get('schema') == 'f08.depth-price-source.v1':
        original, profile, roots = admitted_observation(config)
        return PublicInputStream(roots, profile=profile,
            # Keep the original observation preroll (some accounts have 120s).
            # A new 30-minute reference is not backfilled before this origin.
            start_ns=original['start_ns'], end_ns=config['end_ns'],
            market_id=original['market_id'], input_contract_id=CONTRACT,
            verified_files=verified, incremental_top_cache=True,
            fast_fact_decode=True, research_depth_tap=tap)
    source = Path(config["parent_config"])
    if _digest(source) != config["parent_config_sha256"]:
        raise ValueError("original observation configuration changed")
    original = json.loads(source.read_text())["observation"]
    start, end = config["start_ns"], config["end_ns"]
    if not original["start_ns"] <= start < end <= original["end_ns"]:
        raise ValueError("engineering interval outside bound original parent")
    profile = ObservationProfile(**original["observation_profile"])
    roots = []
    locations = config.get("source_locations", {})
    for entry in original["source_bundles"]:
        root = Path(locations.get(entry["path"], entry["path"]))
        if _digest(root / "manifest.json") != entry["sha256"]:
            raise ValueError("original source manifest changed")
        roots.append(root)
    return PublicInputStream(roots, profile=profile, start_ns=start, end_ns=end,
                             market_id=original["market_id"], input_contract_id=CONTRACT,
                             verified_files=verified, incremental_top_cache=True,
                             fast_fact_decode=True, research_depth_tap=tap)


def run(config, output, *, compare=True):
    """Stream bounded output; comparison covers every original tick field."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    tap = DepthObservationTap(levels=config["levels"], sample_period_ns=config["sample_period_ns"])
    verified = {}
    expanded = streams(config, tap, verified)
    reference = streams(config, None, verified) if compare else None
    counters = Counter()
    hashes = [hashlib.sha256(), hashlib.sha256()]
    wall, cpu = time.monotonic(), time.process_time()
    progress_at, last_ns = wall, config["start_ns"]
    reference_seconds = expanded_seconds = 0.0
    status = "RUNNING"
    error = None
    writer = None
    rows = []

    def report():
        return dict(schema="narrowgate.depth-observation-engineering.v1", status=status,
                    interval=[config["start_ns"], config["end_ns"]], last_ns=last_ns,
                    comparison_executed=compare,
                    original_stream_hashes=[h.hexdigest() for h in hashes] if compare else None,
                    counters=dict(counters), deep_pending=len(tap.pending),
                    deep_peak_pending=tap.peak_pending, deep_published=tap.published,
                    deep_delivered=tap.delivered, original_stream_stats=expanded.stats,
                    reference_iterator_seconds=reference_seconds, expanded_iterator_seconds=expanded_seconds,
                    wall_seconds=time.monotonic()-wall, cpu_seconds=time.process_time()-cpu,
                    peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss *
                    (1 if sys.platform == "darwin" else 1024), error=error,
                    economic_replays=0, strategy_parity="not_executed",
                    information_increment="not_estimated", native_sequence_parity="not_proven",
                    note="Sparse publication sampling for coverage/cost only; not optimal-depth or PnL evidence.")

    def timed(iterator, which):
        nonlocal reference_seconds, expanded_seconds
        while True:
            begin = time.monotonic()
            value = next(iterator, None)
            elapsed = time.monotonic()-begin
            if which == 0:
                reference_seconds += elapsed
            else:
                expanded_seconds += elapsed
            if value is None:
                return
            yield value

    def flush():
        nonlocal writer
        if rows:
            table = pa.Table.from_pylist(rows)
            if writer is None:
                writer = pq.ParquetWriter(output / "depth-coverage.parquet", table.schema, compression="zstd")
            writer.write_table(table)
            rows.clear()

    save_private_json(output / "config.json", config)
    save_private_json(output / "progress.json", report())
    try:
        observed = timed(iter(expanded), 1)
        pairs = zip_longest(timed(iter(reference), 0), observed) if compare else ((None, t) for t in observed)
        for old, new in pairs:
            if new is None or (compare and old is None):
                raise ValueError("observation stream length changed")
            if compare:
                left = canonical_tick(old)
                right = canonical_tick(replace(new, research_depth_observations=()))
                if left != right:
                    raise ValueError(f"original observation first diverged at {new.now_ns}")
                hashes[0].update(left)
                hashes[1].update(right)
            last_ns = new.now_ns
            counters["ticks"] += 1
            for item in new.research_depth_observations:
                original, snapshot = item.original, item.snapshot
                stale = original.ready_ns-original.source_asof_ns > expanded.profile.max_book_age_ns
                for side, levels in (("bid", snapshot.book.bids), ("ask", snapshot.book.asks)):
                    counters["sample_side_rows"] += 1
                    counters["valid_fresh_rows"] += snapshot.book.valid and not stale
                    counters["fewer_than_requested"] += len(levels) < tap.levels
                    bounds = snapshot.bid_initialized_range if side == "bid" else snapshot.ask_initialized_range
                    row = dict(event_id=original.event_id, side=side, publish_ns=original.publish_ns,
                               receive_ns=original.receive_ns, ready_ns=original.ready_ns,
                               source_asof_ns=original.source_asof_ns, version=snapshot.book.version,
                               source_ordinal=snapshot.source_ordinal, snapshot_version=snapshot.snapshot_version,
                               valid=snapshot.book.valid, stale=stale, levels=len(levels),
                               initialized_min=float(bounds[0]) if bounds else float("nan"),
                               initialized_max=float(bounds[1]) if bounds else float("nan"))
                    for count in (10, 20, tap.levels):
                        selected = levels[:count]
                        row[f"span_{count}"] = float(abs(selected[0][0]-selected[-1][0])) if selected else float("nan")
                        row[f"qty_{count}"] = float(sum(q for _, q in selected))
                        row[f"continuous_coverage_{count}"] = bool(selected and snapshot.covers(
                            side, min(selected[0][0], selected[-1][0]), max(selected[0][0], selected[-1][0])))
                    rows.append(row)
                if len(rows) >= 2048:
                    flush()
            if time.monotonic()-progress_at >= 20:
                flush()
                save_private_json(output / "progress.json", report())
                progress_at = time.monotonic()
        if compare and reference.stats != expanded.stats:
            raise ValueError("original observation counters changed")
        flush()
        status = "OBSERVATION_PARITY_PASSED" if compare else "COVERAGE_SCAN_COMPLETE"
    except BaseException as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if writer is not None:
            writer.close()
        save_private_json(output / "progress.json", report())
    save_private_json(output / "result.json", report())
    return report()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--coverage-only", action="store_true")
    args = parser.parse_args()
    result = run(json.loads(Path(args.config).read_text()), args.output, compare=not args.coverage_only)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
