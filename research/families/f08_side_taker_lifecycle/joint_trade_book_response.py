"""Read-only independent market panels, streamed once before any strategy fit.

No simulator, quote engine, account, private default path, or live connection.
Source attribution and modeled ready state are separate statistical panels.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from functools import lru_cache
import hashlib
import heapq
import json
from pathlib import Path
import resource
import sys
import time

from data.observation import TradeContribution, historical_trade
from data.runtime import ObservationProfile, PublicInputStream
from data.tardis_input import BookMessage, BookView, ObservableBook, CONTRACT
from features.trade_book_response import ResponseState, price_quantity, SECOND
from research.families.f03_causal_13_head.time_weighted_evaluation import CALENDAR

HORIZONS = (100_000_000, 500_000_000, SECOND, 5 * SECOND, 30 * SECOND)
DAY = 86400 * SECOND


def day_of(ts):
    return _day_name(ts // DAY)


@lru_cache(maxsize=8)
def _day_name(day_number):
    return datetime.fromtimestamp(day_number * 86400, UTC).date().isoformat()


@lru_cache(maxsize=8)
def _day_end(day):
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()) * SECOND + DAY


class DailySample:
    """Bottom-k uniform hash sample across both panels and sides together."""

    def __init__(self, limit=3000):
        self.limit, self.count, self.heap, self.rows = limit, 0, [], {}

    def offer(self, identity, row):
        if not self.admit(identity):
            return False
        self.rows[identity] = row
        return True

    def admit(self, identity):
        self.count += 1
        rank = int.from_bytes(hashlib.blake2b(identity.encode(), digest_size=16).digest(), "big")
        key = (rank, identity)
        if len(self.rows) >= self.limit:
            worst = (-self.heap[0][0], self.heap[0][1])
            if key >= worst:
                return False
            _, old = heapq.heappop(self.heap)
            del self.rows[old]
        heapq.heappush(self.heap, (-rank, identity))
        self.rows[identity] = None
        return True

    def selected(self):
        probability = min(1.0, self.limit / self.count) if self.count else 0.0
        return [dict(row, inclusion_probability=probability, eligible_day_rows=self.count)
                for _, row in sorted(self.rows.items())]


class MarketPanels:
    def __init__(self, *, start_ns, end_ns, profile, limit=3000, fast_response_state=False,
                 range_block_size=None, compact_pending=False):
        self.start, self.end, self.limit = start_ns, end_ns, limit
        self.states = {panel: ResponseState(max_book_age_ns=profile.max_book_age_ns,
                       trade_coverage=profile.trade_coverage, fast_response_state=fast_response_state,
                       range_block_size=range_block_size)
                       for panel in ("source", "visible")}
        self.source_book = ObservableBook()
        self.source_view = None
        self.reset_version = 0
        self.visible_reset_version = 0
        self.samples, self.pending, self.days = {}, [], {}
        self.sequence = 0
        self.last_now = start_ns
        self.last_source_time = None
        self.compact_pending = compact_pending
        self.sample_meta = {}

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.__dict__.setdefault("compact_pending", False)
        self.__dict__.setdefault("sample_meta", {})

    def _daily(self, ts):
        day = day_of(ts)
        return self.days.setdefault(day, Counter())

    def flush(self, now, *, finish=False):
        """Resolve due outcomes from the last state at/before due, never next book.

        Same-time input is included: due == now waits until the next timestamp.
        End is exclusive, so unresolved targets at/after end remain censored.
        """
        while self.pending and self.pending[0][0] < now:
            item = heapq.heappop(self.pending)
            if self.compact_pending:
                due, seq, h_index = item
                day, identity, exact_price, remaining = self.sample_meta[seq]
                horizon = HORIZONS[h_index]
                if remaining == 1:
                    del self.sample_meta[seq]
                else:
                    self.sample_meta[seq][3] -= 1
            else:
                due, seq, day, identity, horizon = item
                exact_price = None
            sample = self.samples.get(day)
            row = sample.rows.get(identity) if sample else None
            if row is None:
                continue
            if due >= self.end:
                row[f"missing_{horizon}"] = "accounting_free_market_end"
                continue
            state = self.states[row["panel"]]
            if state.epoch != row["continuity_epoch"]:
                row[f"missing_{horizon}"] = "reset_or_gap"
                continue
            valid_book = (state.book is not None and state.book.valid and
                          due - state.source_ns <= state.max_age and state.last_book_ready <= due)
            if not valid_book:
                row[f"missing_{horizon}"] = "invalid_or_stale_reference"
                continue
            view = state.book
            mid = float((view.bids[0][0] + view.asks[0][0]) / 2)
            sign = row["side_sign"]
            row[f"mid_move_{horizon}"] = sign * (mid - row["mid"])
            quantity = price_quantity(view, row["side"], exact_price if exact_price is not None else row["price"])
            row[f"fixed_qty_{horizon}"] = quantity
            row[f"price_covered_{horizon}"] = quantity is not None
            touch = view.bids[0][0] if sign > 0 else view.asks[0][0]
            row[f"still_best_{horizon}"] = float(touch == (exact_price if exact_price is not None else Decimal(str(row["price"]))))
            if quantity is not None and row["recovery_target"] is not None:
                row[f"recovered_{horizon}"] = float(quantity >= row["recovery_target"])
            anchor_key = (row["side"], exact_price if exact_price is not None else Decimal(str(row["price"])), row["recovery_start_ns"])
            outcome = state.recovery_outcomes.get(anchor_key)
            if row["recovery_start_ns"] is not None:
                if outcome is not None and outcome[0] <= due:
                    row[f"recovery_event_{horizon}"] = outcome[1]
                    if outcome[1] == "recovered":
                        row[f"first_recovery_s_{horizon}"] = (outcome[0] - row["recovery_start_ns"]) / SECOND
                else:
                    row[f"recovery_event_{horizon}"] = "not_yet_recovered"
            row[f"same_aggressor_qty_{horizon}"] = state.cumulative[1 if sign > 0 else 0] - row["initial_aggressor_qty"]
            row[f"net_aggressor_qty_{horizon}"] = -sign * (state.cumulative[0] - state.cumulative[1] - row["initial_net_qty"])
            if state.range_index is not None:
                bounds = state.range_index.range_minmax(row["now_ns"], due)
                row[f"execution_range_{horizon}"] = bounds[1] - bounds[0] if bounds else None
            else:
                prices = [p for t, p in state.trade_prices if row["now_ns"] < t <= due]
                row[f"execution_range_{horizon}"] = max(prices) - min(prices) if prices else None
            row[f"reference_age_ns_{horizon}"] = due - state.source_ns
            row[f"missing_{horizon}"] = None
        self.last_now = now

    def offer(self, panel, frame):
        if frame.now_ns < self.start:
            return
        counter = self._daily(frame.now_ns)
        counter[f"{panel}:frames"] += 1
        if not frame.valid:
            counter[f"{panel}:missing:{frame.reason}"] += 1
            return
        counter[f"{panel}:eligible"] += 1
        day = day_of(frame.now_ns)
        # No label may cross into a different split date. Keep the row and
        # independently censor each crossing horizon rather than drop it.
        # Calendar-local identity is invariant to preceding warmup length and
        # absolute book-version offsets. It does not use outcome information.
        identity = f"{panel}:{frame.now_ns}:{frame.side}:{counter[f'{panel}:eligible']}"
        self.sequence += 1
        sample = self.samples.setdefault(day, DailySample(self.limit))
        if not sample.admit(identity):
            return
        if not frame.values:
            frame = self.states[panel].frame(frame.side)
        row = dict(frame.values)
        anchor = self.states[panel].anchor(frame.side, Decimal(str(frame.price)))
        row.update(panel=panel, day=day, side=frame.side, now_ns=frame.now_ns,
                   source_asof_ns=frame.source_asof_ns, book_version=frame.book_version,
                   price=frame.price, quantity=frame.quantity, mid=frame.mid,
                   recovery_target=frame.recovery_target,
                   continuity_epoch=self.states[panel].epoch,
                   recovery_start_ns=anchor[0] if anchor else None,
                   initial_aggressor_qty=self.states[panel].cumulative[1 if frame.side == "bid" else 0],
                   initial_net_qty=self.states[panel].cumulative[0] - self.states[panel].cumulative[1],
                   response_conservation_qualified=False)
        for h in HORIZONS:
            for key in ("mid_move", "fixed_qty", "recovered", "same_aggressor_qty", "reference_age_ns", "still_best", "first_recovery_s", "net_aggressor_qty", "execution_range"):
                row[f"{key}_{h}"] = None
            row[f"recovery_event_{h}"] = "no_anchor" if anchor is None else "not_yet_resolved"
            row[f"price_covered_{h}"] = None
            row[f"missing_{h}"] = "not_yet_resolved"
        sample.rows[identity] = row
        remaining = 0
        for h_index, h in enumerate(HORIZONS):
            if day_of(frame.now_ns + h) != day or frame.now_ns + h >= self.end:
                row[f"missing_{h}"] = "date_or_sample_end"
            else:
                if self.compact_pending:
                    heapq.heappush(self.pending, (frame.now_ns + h, self.sequence, h_index))
                    remaining += 1
                else:
                    heapq.heappush(self.pending, (frame.now_ns + h, self.sequence, day, identity, h))
        if remaining:
            # Preserve the reference's float-to-Decimal label price exactly;
            # do not silently fix/reinterpret it as a different raw price.
            self.sample_meta[self.sequence] = [day, identity, Decimal(str(row["price"])), remaining]

    def advance(self, tick):
        self.flush(tick.now_ns)
        for state in self.states.values():
            state.advance(tick.now_ns)
        counters = self._daily(tick.now_ns)
        source_views = iter(tick.source_book_views) if tick.source_book_views is not None else None
        for event in tick.exchange_events:
            source_ns = event.exchange_ts_ns if event.exchange_ts_ns is not None else event.source_timestamp_us * 1000
            if self.last_source_time is not None and source_ns < self.last_source_time:
                counters["source:timestamp_regressions"] += 1
            self.last_source_time = source_ns
            if isinstance(event, BookMessage):
                counters["source:complete_book_messages"] += 1
                counters["source:level_rows"] += len(event.levels)
                if source_views is None:
                    self.source_book.apply(event)
                    view = self.source_book.view(20)
                else:
                    view = next(source_views)
                if event.kind == "snapshot":
                    self.reset_version = view.version
                # Whole logical message is applied before any level is read.
                frames = self.states["source"].observe_book(tick.now_ns, source_ns, view,
                                                            reset=event.kind == "snapshot", include_values=False)
                for frame in frames:
                    self.offer("source", frame)
            else:
                counters["source:individual_trades"] += 1
                timed = event if event.exchange_ts_ns is not None else replace(event, exchange_ts_ns=source_ns)
                self.states["source"].observe_trade(tick.now_ns, historical_trade(timed))
        for observation in tick.observations:
            if observation.ready_ns > tick.now_ns:
                raise ValueError("future visible observation")
            if isinstance(observation.payload, TradeContribution):
                counters["visible:trade_contributions"] += 1
                self.states["visible"].observe_trade(tick.now_ns, observation.payload)
            elif isinstance(observation.payload, BookView):
                counters["visible:book_publications"] += 1
                reset = observation.payload.version >= self.reset_version > self.visible_reset_version
                if reset:
                    self.visible_reset_version = self.reset_version
                frames = self.states["visible"].observe_book(tick.now_ns, observation.source_asof_ns,
                                                           observation.payload, reset=reset, include_values=False)
                for frame in frames:
                    self.offer("visible", frame)

    def complete_days(self, now):
        for day in list(self.samples):
            end = _day_end(day)
            if now >= end:
                yield day, self.samples.pop(day).selected()


def scan_market(config, output):
    import pyarrow as pa
    import pyarrow.parquet as pq

    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    panel_path = output / "market-panel.parquet"
    status_path = output / "market-status.json"
    if panel_path.exists() or status_path.exists():
        raise FileExistsError("existing market work must be inspected, not automatically restarted")
    profile = ObservationProfile(**config["observation_profile"])
    if profile.depth_connection_id is None or profile.trade_connection_id is None:
        raise ValueError("ordered connection mapping required for this market study")
    start, end = config["start_ns"], config["end_ns"]
    warmup = config.get("warmup_start_ns", start)
    legal_start = int(datetime.fromisoformat(CALENDAR[0]).replace(tzinfo=UTC).timestamp()) * SECOND
    legal_end = int(datetime.fromisoformat(CALENDAR[300]).replace(tzinfo=UTC).timestamp()) * SECOND
    if not legal_start <= start < end <= legal_end:
        raise ValueError("market study cannot read Final")
    if not legal_start <= warmup <= start or (warmup != start and start - warmup < DAY):
        raise ValueError("parallel block needs a complete preceding warmup day")
    roots = [Path(config["facts_by_day"][day]) if "facts_by_day" in config else Path(config["facts_root"]) / day for day in CALENDAR[:300]
             if int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()) * SECOND < end
             and int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()) * SECOND + DAY > warmup]
    fast = config.get("market_scan_fast", {})
    allowed = {"incremental_top_cache", "fast_response_state", "range_block_size",
               "fast_fact_decode", "fact_batch_size", "scan_lightweight_runtime",
               "scan_skip_book_view", "compact_pending"}
    if not isinstance(fast, dict) or set(fast) - allowed:
        raise ValueError("unknown market scan optimization option")
    state = MarketPanels(start_ns=start, end_ns=end, profile=profile,
                        fast_response_state=fast.get("fast_response_state", False),
                        range_block_size=fast.get("range_block_size"),
                        compact_pending=fast.get("compact_pending", False))
    stream = PublicInputStream(roots, profile=profile, start_ns=warmup, end_ns=end,
                              market_id=config["market_id"], input_contract_id=CONTRACT, verify=False,
                              capture_source_books=True,
                              incremental_top_cache=fast.get("incremental_top_cache", False),
                              fast_fact_decode=fast.get("fast_fact_decode", False),
                              fact_batch_size=fast.get("fact_batch_size", 8192),
                              consumer_mode="market_response_scan" if fast.get("scan_lightweight_runtime") else "default",
                              scan_skip_book_view=fast.get("scan_skip_book_view", False))
    writer = None
    written = 0
    wall, cpu, update = time.monotonic(), time.process_time(), time.monotonic()

    def status(stage, error=None):
        row = {"visibility": "local_only_do_not_publish", "status": stage,
               "start_ns": start, "end_ns": end, "last_market_ns": state.last_now,
               "written_sample_rows": written, "days": {d: dict(c) for d, c in state.days.items() if d >= day_of(start)},
               "warmup_days": {d: dict(c) for d, c in state.days.items() if d < day_of(start)},
               "delivery_idle_ns": stream.delivery_idle_ns,
               "state_counts": {p: dict(s.stats) for p, s in state.states.items()},
               "wall_seconds": time.monotonic() - wall, "cpu_seconds": time.process_time() - cpu,
               "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024),
               "strategy_replays": 0, "error": error}
        row["input_contract"] = config
        row["output_bytes"] = panel_path.stat().st_size if panel_path.exists() else 0
        status_path.write_text(json.dumps(row, indent=2) + "\n")

    def write_rows(rows):
        nonlocal writer, written
        if not rows:
            return
        # Explicit nullable schema avoids first-day nulls freezing a null type.
        string_names = {"panel", "day", "side"} | {f"{kind}_{h}" for h in HORIZONS for kind in ("missing", "recovery_event")}
        bool_names = {"response_conservation_qualified"} | {f"price_covered_{h}" for h in HORIZONS}
        int_names = {"now_ns", "source_asof_ns", "book_version", "eligible_day_rows", "continuity_epoch", "recovery_start_ns"} | {f"reference_age_ns_{h}" for h in HORIZONS}
        schema = pa.schema([(name, pa.string() if name in string_names else pa.bool_() if name in bool_names
                             else pa.int64() if name in int_names else pa.float64()) for name in rows[0]])
        table = pa.Table.from_pylist(rows, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(panel_path, schema, compression="zstd")
        writer.write_table(table)
        written += len(rows)

    status("running")
    admitted = warmup == start
    warmup_idle_witness = None
    try:
        for tick in stream:
            if tick.now_ns <= start - 40 * SECOND and stream.delivery_idle_ns is not None:
                warmup_idle_witness = stream.delivery_idle_ns
            if not admitted and tick.now_ns >= start:
                # Observe actual queue drainage well before scoring. No
                # artificial reconnect, flush, clock advance or RNG redraw.
                idle = warmup_idle_witness
                if idle is None or idle < warmup:
                    raise ValueError("warmup transport drainage not established before finite-history horizon")
                admitted = True
            state.advance(tick)
            for _, rows in state.complete_days(tick.now_ns):
                write_rows(rows)
            if time.monotonic() - update > 30:
                status("running")
                update = time.monotonic()
        state.flush(end)
        for _, sample in list(state.samples.items()):
            write_rows(sample.selected())
        if writer:
            writer.close()
            writer = None
        status("market_panel_complete_not_economic_validation")
    except BaseException as exc:
        if writer:
            writer.close()
        status("failed", f"{type(exc).__name__}: {exc}")
        raise
    return panel_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    if args.plan:
        if args.config or args.output or args.workers < 1:
            parser.error("plan uses positive workers, without config/output")
        from concurrent.futures import ProcessPoolExecutor
        from multiprocessing import get_context
        jobs = json.loads(args.plan.read_text())["jobs"]
        ranges = sorted((job["config"]["start_ns"], job["config"]["end_ns"]) for job in jobs)
        if any(b[0] < a[1] for a, b in zip(ranges, ranges[1:], strict=False)):
            raise ValueError("overlapping scoring blocks")
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context("spawn")) as executor:
            for result in executor.map(_scan_job, jobs):
                print(result, flush=True)
        return
    if not args.config or not args.output:
        parser.error("config and output required without plan")
    config = json.loads(args.config.read_text())
    print(scan_market(config, args.output))


def _scan_job(job):
    return str(scan_market(job["config"], job["output"]))


if __name__ == "__main__":
    main()
