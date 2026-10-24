"""Ready-clock fixed-price dynamics; L2 decreases are not identified cancels."""

from collections import deque
from bisect import bisect_left, bisect_right
from decimal import Decimal


class VisibleDepthHistory:
    def __init__(self, *, windows_ns=(10**9, 5*10**9, 10*10**9), max_age_ns=10**9):
        if not windows_ns or any(type(w) is not int or w <= 0 for w in windows_ns):
            raise ValueError("positive integer windows required")
        self.windows = tuple(sorted(set(windows_ns)))
        self.max_age = max_age_ns
        self.current = None
        self.clock = -1
        self.started = None
        self.changes = deque()
        self.coverage = deque()
        self.resets = self.regressions = 0

    def advance(self, observation):
        now = observation.original.ready_ns
        if now < self.clock:
            raise ValueError("observations must retain delivery order")
        old = self.current
        new = observation.snapshot
        reset = old is None or new.snapshot_version != old.snapshot.snapshot_version
        regression = old is not None and new.book.version < old.snapshot.book.version
        if reset or regression or not new.book.valid:
            self.changes.clear()
            self.coverage.clear()
            self.started = now
            self.resets += int(reset)
            self.regressions += int(regression)
        elif old is not None:
            for left, right in zip(old.snapshot.buckets, new.buckets, strict=True):
                if left is right:
                    continue
                for side, price in left.keys() | right.keys():
                    before = old.snapshot.quantity(side, price)
                    after = new.quantity(side, price)
                    if before is not None and after is not None and before != after:
                        self.changes.append((now, side, price, after-before))
        self.current, self.clock = observation, now
        self.coverage.append((now, new, observation.original.source_asof_ns))
        self._prune(now)

    def _prune(self, now):
        cutoff = now-self.windows[-1]
        while self.changes and self.changes[0][0] <= cutoff:
            self.changes.popleft()
        # Keep the state at the left boundary as well as later publications.
        while len(self.coverage) > 1 and self.coverage[1][0] <= cutoff:
            self.coverage.popleft()

    def query(self, *, side, low, high, read_ns):
        if side not in {"bid", "ask"}:
            raise ValueError("invalid side")
        low, high = Decimal(str(low)), Decimal(str(high))
        if not low.is_finite() or not high.is_finite() or low > high:
            raise ValueError("invalid price region")
        if self.current is None:
            return {"available": False, "reason": "no_delivered_depth", "windows": {}}
        if read_ns < self.clock:
            raise ValueError("cannot read future depth")
        self._prune(read_ns)
        snap = self.current.snapshot
        fresh = read_ns-self.current.original.source_asof_ns <= self.max_age
        covered = fresh and snap.covers(side, low, high)
        result = dict(available=covered, low=str(low), high=str(high),
                      reason=None if covered else "stale_or_unknown_region",
                      quantity=str(sum((q for p, q in snap.points(side) if low <= p <= high), Decimal(0)))
                      if covered else None, windows={})
        for window in self.windows:
            begin = read_ns-window
            history = [(t, s, source) for t, s, source in self.coverage if t <= read_ns]
            anchor = [i for i, (t, _, _) in enumerate(history) if t <= begin]
            available = bool(covered and anchor and self.started <= begin)
            if available:
                history = history[anchor[-1]:]
                for index, (_t, state, source) in enumerate(history):
                    until = history[index+1][0] if index+1 < len(history) else read_ns
                    if not state.covers(side, low, high) or until-source > self.max_age:
                        available = False
                        break
            selected = [(t, p, delta) for t, s, p, delta in self.changes
                        if begin < t <= read_ns and s == side and low <= p <= high]
            changes = [delta for _, _, delta in selected]
            declines, recoveries, recovery_delays = {}, set(), []
            positive_after_decline = Decimal(0)
            for t, price, delta in selected:
                if delta < 0:
                    count, _ = declines.get(price, (0, t))
                    declines[price] = (count+1, t)
                elif price in declines:
                    positive_after_decline += delta
                    if price not in recoveries:
                        recovery_delays.append(t-declines[price][1])
                    recoveries.add(price)
            result["windows"][str(window)] = dict(
                available=available, reason=None if available else "incomplete_visible_history",
                positive_qty=str(sum((d for d in changes if d > 0), Decimal(0))) if available else None,
                negative_qty=str(-sum((d for d in changes if d < 0), Decimal(0))) if available else None,
                changed_price_publications=len(changes) if available else None,
                positive_change_events=sum(d > 0 for d in changes) if available else None,
                negative_change_events=sum(d < 0 for d in changes) if available else None,
                distinct_changed_prices=len({p for _, p, _ in selected}) if available else None,
                decline_then_increase_prices=len(recoveries) if available else None,
                repeated_decline_prices=sum(count > 1 for count, _ in declines.values()) if available else None,
                positive_after_observed_decline_qty=str(positive_after_decline) if available else None,
                first_increase_after_latest_decline_delays_ns=recovery_delays if available else None,
                semantics="net_fixed_price_changes_between_delivered_publications_not_order_add_cancel")
        return result


class IndexedDepthState:
    """A bounded, immutable publication index, shared across all queries.

    Reuse an unchanged COW book instead of sorting it again on each capture.
    Unknown support is checked on the actual snapshot, never inferred from
    the extrema of isolated price updates.
    """

    def __init__(self, observation, previous=None):
        self.observation = observation
        self.snapshot = observation.snapshot
        self.indexed_points = 0
        if previous is not None and all(a is b for a, b in zip(
                self.snapshot.buckets, previous.snapshot.buckets, strict=True)):
            self.index = previous.index
            return
        self.index = {}
        for side in ('bid', 'ask'):
            levels = sorted(self.snapshot.points(side))
            prices, totals = [], [0.0]
            for price, quantity in levels:
                prices.append(price)
                totals.append(totals[-1] + float(quantity))
            self.index[side] = (tuple(prices), tuple(totals))
            self.indexed_points += len(prices)

    def quantity(self, side, low, high):
        if not self.snapshot.covers(side, low, high):
            return None
        prices, totals = self.index[side]
        return max(0.0, totals[bisect_right(prices, high)] - totals[bisect_left(prices, low)])


class VisibleDepthEndpoints:
    """As-of endpoint differences, NOT continuous flow or cancellation counts.

    Unlike VisibleDepthHistory, a rebase does not erase a trustworthy old
    endpoint. Gap/rebase crossings are reported and never called continuity.
    """

    def __init__(self, *, window_ns=5_000_000_000, max_age_ns=1_000_000_000):
        if any(type(v) is not int or v <= 0 for v in (window_ns, max_age_ns)):
            raise ValueError('positive endpoint durations required')
        self.window, self.max_age = window_ns, max_age_ns
        self.rows = deque()
        self.clock = -1
        self.epoch = 0

    @property
    def current(self):
        return self.rows[-1][1] if self.rows else None

    def advance(self, observation):
        now = observation.original.ready_ns
        if now < self.clock or observation.original.source_asof_ns > now:
            raise ValueError('endpoint observation clock is not causal')
        old = self.current
        if old is not None and (
                now - old.observation.original.source_asof_ns > self.max_age
                or observation.snapshot.snapshot_version != old.snapshot.snapshot_version
                or observation.snapshot.book.version < old.snapshot.book.version
                or not observation.snapshot.book.valid or not old.snapshot.book.valid):
            self.epoch += 1
        index = IndexedDepthState(observation, old)
        self.rows.append((now, index, self.epoch))
        self.clock = now
        self.prune(now)
        return index

    def prune(self, read_ns):
        cutoff = read_ns - self.window
        while len(self.rows) > 1 and self.rows[1][0] <= cutoff:
            self.rows.popleft()

    def query(self, *, regions, read_ns):
        if read_ns < self.clock:
            raise ValueError('cannot read future endpoints')
        self.prune(read_ns)
        target = read_ns - self.window
        chosen = next((row for row in reversed(self.rows) if row[0] <= target), None)
        if chosen is None:
            return dict(available=False, reason='endpoint_warmup', quantities=None)
        ready, index, epoch = chosen
        source = index.observation.original.source_asof_ns
        quantities = [index.quantity(*region) for region in regions]
        fresh = 0 <= target - source <= self.max_age
        ok = fresh and all(q is not None for q in quantities)
        return dict(available=ok, reason=None if ok else ('endpoint_stale' if not fresh else 'endpoint_region_unknown'),
                    quantities=quantities if ok else None, endpoint_ready_ns=ready,
                    endpoint_source_ns=source, endpoint_read_ns=target,
                    endpoint_lag_ns=target - ready, crossed_gap_or_rebase=epoch != self.epoch,
                    semantics='fixed_absolute_interval_endpoint_net_change_not_order_flow')


class VisibleTradeHistory:
    def __init__(self, *, start_ns, windows_ns, coverage):
        self.start, self.windows, self.coverage = start_ns, tuple(windows_ns), coverage
        self.rows = deque()
        self.clock = -1

    def observe(self, observation):
        from data.observation import TradeContribution
        if isinstance(observation.payload, TradeContribution):
            if observation.ready_ns < self.clock:
                raise ValueError('trade ready clock regressed')
            self.clock = observation.ready_ns
            self.rows.append((observation.ready_ns, observation.payload, observation.coverage))

    def query(self, *, low, high, read_ns):
        if read_ns < self.clock:
            raise ValueError('cannot read future trade')
        low, high = Decimal(str(low)), Decimal(str(high))
        while self.rows and self.rows[0][0] <= read_ns-max(self.windows):
            self.rows.popleft()
        result = {}
        for w in self.windows:
            rows = [r for r in self.rows if read_ns-w < r[0] <= read_ns]
            valid = self.coverage == 'observed' and read_ns-w >= self.start and all(r[2] == 'observed' for r in rows)
            selected = [trade for _, trade, _ in rows if low <= trade.price <= high]
            result[str(w)] = dict(available=valid,
                buy_qty=str(sum((t.quantity for t in selected if t.side == 'buy'), Decimal(0))) if valid else None,
                sell_qty=str(sum((t.quantity for t in selected if t.side == 'sell'), Decimal(0))) if valid else None,
                observed_events=len(selected) if valid else None,
                semantics='delivered_trade_window_not_source_interval_depth_consumption')
        return result
