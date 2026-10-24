"""Opt-in research views on the existing depth publication/delivery clock.

This is not an additional feed or queue. The original Top20 observation and
latency draw remain unchanged. Only selected publications retain a deep view.
"""

from dataclasses import dataclass
from decimal import Decimal

from data.observation import Observation
from data.tardis_input import BookView


def depth_bucket(side, price, count=64):
    """Numeric Decimal hashes are process-stable; string hashes are salted.

    Saved copy-on-write states must retain the same bucket lookup in a new
    interpreter. This is storage indexing only, never publication ordering.
    """
    if side not in {'bid', 'ask'}:
        raise ValueError('invalid depth side')
    return (hash(price)*2 + (side == 'ask')) % count


@dataclass(frozen=True)
class DepthSnapshot:
    book: BookView
    requested_levels: int
    snapshot_version: int
    source_ordinal: int
    bid_initialized_range: tuple[Decimal, Decimal] | None
    ask_initialized_range: tuple[Decimal, Decimal] | None

    def covers(self, side, low, high):
        """Known continuous interval, not the extrema of isolated deltas."""
        if side not in {"bid", "ask"} or low > high:
            raise ValueError("invalid depth region")
        levels = self.book.bids if side == "bid" else self.book.asks
        bounds = self.bid_initialized_range if side == "bid" else self.ask_initialized_range
        if not self.book.valid or not levels or bounds is None:
            return False
        projected_low = min(levels[0][0], levels[-1][0])
        projected_high = max(levels[0][0], levels[-1][0])
        # A valid top-of-book snapshot also establishes zero bid quantity above
        # its best bid, and zero ask quantity below its best ask. Subsequent
        # better prices therefore do not open an unknown interval. Only the
        # outer truncation boundary limits the initialized side of the book.
        known_low = max(bounds[0], projected_low) if side == "bid" else projected_low
        known_high = projected_high if side == "bid" else min(bounds[1], projected_high)
        return known_low <= low <= high <= known_high

    def quantity(self, side, price):
        """Exact known positive point; zero only in a proven projected interval."""
        if side not in {"bid", "ask"}:
            raise ValueError("invalid book side")
        levels = self.book.bids if side == "bid" else self.book.asks
        if not self.book.valid:
            return None
        for candidate, quantity in levels:
            if candidate == price:
                return quantity
        return Decimal(0) if self.covers(side, price, price) else None


@dataclass(frozen=True)
class DepthObservation:
    original: Observation
    snapshot: DepthSnapshot

    def __post_init__(self):
        old, deep = self.original.payload, self.snapshot.book
        if (not isinstance(old, BookView) or old.version != deep.version
                or old.bids != deep.bids[:20] or old.asks != deep.asks[:20]
                or old.source_asof_us != deep.source_asof_us):
            raise ValueError("research depth must share the original publication")


class DepthObservationTap:
    """Sparse immutable snapshots, released only with their original message.

    Snapshot bounds are deliberately conservative. Outside delta points do not
    establish continuity between themselves and the initialized range. Native
    sequence continuity is unavailable in this source contract, not certified.
    """

    def __init__(self, *, levels=800, sample_period_ns=1_000_000_000):
        if type(levels) is not int or levels < 20:
            raise ValueError("research depth must retain the unchanged Top20")
        if type(sample_period_ns) is not int or sample_period_ns <= 0:
            raise ValueError("positive research sampling period required")
        self.levels, self.period = levels, sample_period_ns
        self.bounds = {"bid": None, "ask": None}
        self.snapshot_version = 0
        self.source_ordinal = -1
        self.pending = {}
        self.published = self.delivered = self.peak_pending = 0

    def source_applied(self, message, book):
        self.source_ordinal = message.source_ordinal
        if message.kind == "snapshot":
            self.snapshot_version = book.version
            # Whole-message last-write-wins, including zero quantities.
            points = {(side, price): quantity for side, price, quantity in message.levels}
            for side in self.bounds:
                prices = [price for (s, price), qty in points.items() if s == side and qty > 0]
                self.bounds[side] = (min(prices), max(prices)) if prices else None

    def publish(self, event_id, book, now_ns, phase_ns):
        if (now_ns - phase_ns) % self.period:
            return
        if event_id in self.pending:
            raise ValueError("duplicate research publication")
        self.pending[event_id] = DepthSnapshot(
            book.view(self.levels), self.levels, self.snapshot_version,
            self.source_ordinal, self.bounds["bid"], self.bounds["ask"],
        )
        self.published += 1
        self.peak_pending = max(self.peak_pending, len(self.pending))

    def deliver(self, observations):
        result = []
        for observation in observations:
            snapshot = self.pending.pop(observation.event_id, None)
            if snapshot is not None:
                result.append(DepthObservation(observation, snapshot))
                self.delivered += 1
        return tuple(result)


@dataclass(frozen=True)
class PublishedDepthState:
    """Copy-on-write publication: unchanged price buckets are shared, never edited."""

    buckets: tuple
    book: BookView
    snapshot_version: int
    source_ordinal: int
    bounds: tuple

    def points(self, side):
        for bucket in self.buckets:
            for (s, price), quantity in bucket.items():
                if s == side:
                    yield price, quantity

    def quantity(self, side, price):
        value = self.buckets[depth_bucket(side, price, len(self.buckets))].get((side, price))
        if value is not None:
            return value
        return Decimal(0) if self.covers(side, price, price) else None

    def covers(self, side, low, high):
        if side not in {"bid", "ask"} or low > high:
            raise ValueError("invalid depth region")
        bounds = dict(self.bounds)[side]
        if not self.book.valid or bounds is None:
            return False
        return low >= bounds[0] if side == "bid" else high <= bounds[1]


@dataclass(frozen=True)
class DepthStateObservation:
    original: Observation
    snapshot: PublishedDepthState


class DepthStateTap(DepthObservationTap):
    """Full known-price state at every original publication, not source-time flow.

    No second parser, random draw or delivery queue. Only changed buckets are
    copied at publication; a source snapshot rebases rather than inventing flow.
    The private dictionaries are immutable after publication by construction.
    """

    def __init__(self, *, sample_period_ns=100_000_000):
        super().__init__(sample_period_ns=sample_period_ns)
        self.buckets = tuple({} for _ in range(64))
        self.dirty = {}
        self.rebase = False

    def source_applied(self, message, book):
        super().source_applied(message, book)
        if message.kind == "snapshot":
            self.dirty.clear()
            self.rebase = True
        for side, price, quantity in message.levels:
            self.dirty[side, price] = quantity

    def publish(self, event_id, book, now_ns, phase_ns):
        # Every original publication is represented, even if no price changed.
        if event_id in self.pending:
            raise ValueError("duplicate research publication")
        buckets = list(tuple({} for _ in range(64)) if self.rebase else self.buckets)
        changed = set()
        for key, quantity in self.dirty.items():
            slot = depth_bucket(*key, len(buckets))
            if slot not in changed:
                buckets[slot] = dict(buckets[slot])
                changed.add(slot)
            if quantity:
                buckets[slot][key] = quantity
            else:
                buckets[slot].pop(key, None)
        self.buckets = tuple(buckets)
        self.dirty.clear()
        self.rebase = False
        self.pending[event_id] = PublishedDepthState(
            self.buckets, book.view(20), self.snapshot_version,
            self.source_ordinal, tuple(self.bounds.items()))
        self.published += 1
        self.peak_pending = max(self.peak_pending, len(self.pending))

    def deliver(self, observations):
        result = []
        for observation in observations:
            snapshot = self.pending.pop(observation.event_id, None)
            if snapshot is not None:
                if snapshot.book != observation.payload:
                    raise ValueError("deep publication differs from original Top20")
                result.append(DepthStateObservation(observation, snapshot))
                self.delivered += 1
        return tuple(result)


@dataclass(frozen=True)
class CostedDepthStateObservation:
    original: Observation
    snapshot: PublishedDepthState
    maintenance_work: tuple
    measured_maintenance_ns: int | None


class MeasuredDepthStateTap(DepthStateTap):
    """Same publication semantics with causal per-publication work counters.

    Actual CPU sampling is opt-in engineering telemetry, never replay latency.
    Source work is attached to its publication and charged only on delivery;
    lookahead by the offline reader cannot charge a future publication early.
    """

    def __init__(self, *, measure=False):
        super().__init__()
        from collections import Counter
        self.measure = measure
        self.work = Counter()
        self.offline_bootstrap_work = Counter()
        self.offline_bootstrap_cpu_ns = 0
        self.work_start_ns = 0
        self.source_clock = -1
        self.cpu_ns = 0
        self.publication_work = {}

    def source_applied(self, message, book):
        import time
        begin = time.process_time_ns() if self.measure else 0
        super().source_applied(message, book)
        source = message.exchange_ts_ns
        if source is None:
            source = message.source_timestamp_us * 1000
        self.source_clock = max(self.source_clock, source)
        # The owner scans earlier facts to reconstruct the original initial
        # book. Preserve that state, but do not charge offline reconstruction
        # as if all those old events arrived during the first capture.
        bootstrap = self.source_clock < self.work_start_ns
        work = self.offline_bootstrap_work if bootstrap else self.work
        work['source_events'] += 1
        work['source_levels'] += len(message.levels)
        if self.measure:
            elapsed = time.process_time_ns() - begin
            if bootstrap:
                self.offline_bootstrap_cpu_ns += elapsed
            else:
                self.cpu_ns += elapsed

    def publish(self, event_id, book, now_ns, phase_ns):
        import time
        begin = time.process_time_ns() if self.measure else 0
        slots = {depth_bucket(*key, len(self.buckets)) for key in self.dirty}
        self.work['copied_points'] += sum(len(self.buckets[s]) for s in slots) if not self.rebase else 0
        self.work['published_levels'] += len(self.dirty)
        super().publish(event_id, book, now_ns, phase_ns)
        if self.measure:
            self.cpu_ns += time.process_time_ns() - begin
        self.publication_work[event_id] = (tuple(self.work.items()), self.cpu_ns if self.measure else None)
        self.work.clear()
        self.cpu_ns = 0

    def deliver(self, observations):
        import time
        begin = time.process_time_ns() if self.measure else 0
        rows = super().deliver(observations)
        elapsed = time.process_time_ns() - begin if self.measure else 0
        result = []
        for row in rows:
            work, measured = self.publication_work.pop(row.original.event_id)
            result.append(CostedDepthStateObservation(row.original, row.snapshot, work,
                          measured + elapsed // len(rows) if measured is not None else None))
        return tuple(result)
