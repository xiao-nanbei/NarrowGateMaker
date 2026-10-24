"""First NarrowGate depth-quoting policy: symmetric geometry and partial support.

Explicit new arm names never select the retired adaptive policy. All five
new arms maintain and calculate the same four alternatives on their own path.
"""

from bisect import bisect_left, bisect_right
from collections import Counter
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from research.families.f08_side_taker_lifecycle.depth_dynamics import VisibleDepthEndpoints
from research.families.f08_side_taker_lifecycle.depth_price_reference import (
    GeometryEwma, component_corrections, radius_grid,
)


class DepthQuotingPolicy:
    SCHEMA = 'narrowgate.depth-quoting-policy.v1'
    ARMS = {'OBSERVE_ZERO', 'LOCAL_STATIC', 'LOCAL_DYNAMIC', 'QUOTE_STATIC', 'QUOTE_DYNAMIC'}

    def __init__(self, *, arm, tick_size):
        if arm not in self.ARMS:
            raise ValueError('unknown depth-quoting arm')
        self.tick = Decimal(str(tick_size))
        if not self.tick.is_finite() or self.tick <= 0:
            raise ValueError('positive tick required')
        self.arm = arm
        self.reference = GeometryEwma()
        self.history = VisibleDepthEndpoints()
        self.radii = [0, 1]
        self.counts = Counter()
        self.work = Counter()
        self.last_query_ns = -1
        self.last_audit = None

    def _regions(self, k):
        book = self.history.current.snapshot.book
        return [('bid', book.bids[0][0] - k * self.tick, book.bids[0][0]),
                ('ask', book.asks[0][0], book.asks[0][0] + k * self.tick)]

    def _quantities(self, k):
        current = self.history.current
        book = current.snapshot.book
        if not book.valid or not book.bids or not book.asks:
            return [None, None]
        self.work['region_queries'] += 2
        return [current.quantity(*r) for r in self._regions(k)]

    def _integrate(self, now):
        if self.reference.clock is not None and now != self.reference.clock:
            self.work['radius_integrations'] += len(self.reference.mass)
        self.reference.integrate_to(now)

    def advance(self, observation):
        now = observation.original.ready_ns
        if now < self.last_query_ns or observation.original.source_asof_ns > now:
            raise ValueError('late or future observation cannot revise a capture')
        old = self.history.current
        if old is not None and observation.snapshot.book.version < old.snapshot.book.version:
            # A proven logical version regression invalidates the estimator;
            # an ordinary later snapshot/rebase alone does not do this.
            self.reference = GeometryEwma()
            self.counts['reference_version_regressions'] += 1
        self._integrate(now)
        current = self.history.advance(observation)
        self.work['indexed_points'] += current.indexed_points
        self.work['publications'] += 1
        self.counts['publications'] += 1
        book = current.snapshot.book
        bounds = dict(current.snapshot.bounds)
        if book.valid and book.bids and book.asks and all(bounds.values()):
            width = min((book.bids[0][0] - bounds['bid'][0]) / self.tick,
                        (bounds['ask'][1] - book.asks[0][0]) / self.tick)
            if width >= 0:
                self.radii = radius_grid(max(self.radii[-1], int(width.to_integral_value(rounding=ROUND_FLOOR))))
        # Establish all currently supported grid radii at this event, not at a
        # future query and not by filling their histories from later states.
        densities = {}
        for k in self.radii:
            qb, qa = self._quantities(k)
            densities[k] = (qb + qa) / (2 * (k + 1)) if qb is not None and qa is not None else None
        self.reference.publish(now=now, expiry=observation.original.source_asof_ns + self.history.max_age,
                               densities=densities)

    def begin_capture(self, read_ns):
        if read_ns < max(self.last_query_ns, self.history.clock):
            raise ValueError('capture clock regressed')
        self._integrate(read_ns)
        self.history.prune(read_ns)

    def _components(self, k, read_ns, invalid_reason=None):
        current = self.history.current
        reference = self.reference.query(k) if k is not None else dict(available=False, reason='geometry_unavailable')
        qb, qa = self._quantities(k) if k is not None and not invalid_reason else (None, None)
        endpoint = (self.history.query(regions=self._regions(k), read_ns=read_ns)
                    if current is not None and k is not None and not invalid_reason
                    else dict(available=False, reason=invalid_reason or 'geometry_unavailable'))
        self.work['endpoint_queries'] += int(k is not None and not invalid_reason)
        prior = endpoint.get('quantities') or (None, None)
        row = component_corrections(qb=qb, qa=qa, grid_points=2 * (k + 1) if k is not None else 2,
                                   reference=reference, previous_qb=prior[0], previous_qa=prior[1],
                                   endpoint_reason=endpoint['reason'])
        if invalid_reason:
            row['reasons']['I'] = invalid_reason
        return dict(row, radius_ticks=k, current_quantity=[qb, qa], reference=reference, endpoint=endpoint)

    def correction(self, *, bid, ask, state, depth, read_ns):
        self.begin_capture(read_ns)
        self.last_query_ns = read_ns
        self.work['correction_queries'] += 1
        self.counts['decisions'] += 1
        bid, ask = Decimal(str(bid)), Decimal(str(ask))
        if not bid.is_finite() or not ask.is_finite() or not 0 < bid < ask:
            raise ValueError('invalid original raw quote')
        current = self.history.current
        reason, local, quote, required = None, None, None, None
        if current is None:
            reason = 'no_delivered_depth'
        else:
            book = current.snapshot.book
            if not book.valid or not book.bids or not book.asks:
                reason = 'invalid_book'
            elif read_ns - current.observation.original.source_asof_ns > self.history.max_age:
                reason = 'current_source_stale'
            else:
                bb, ba = book.bids[0][0], book.asks[0][0]
                if (float(bb) != state.best_bid or float(ba) != state.best_ask
                        or not depth.bids or not depth.asks
                        or float(bb) != depth.bids[0][0] or float(ba) != depth.asks[0][0]):
                    reason = 'book_alignment_mismatch'
                else:
                    if len(book.bids) >= 20 and len(book.asks) >= 20:
                        width = min((bb - book.bids[19][0]) / self.tick,
                                    (book.asks[19][0] - ba) / self.tick)
                        if width < 0 or width != width.to_integral_value():
                            raise ValueError('unaligned local geometry')
                        local = self.radii[bisect_right(self.radii, int(width)) - 1]
                    radius = max(bb - bid, ask - ba, Decimal(0)) + Decimal('.1') * (ask - bid) + self.tick
                    required = int((radius / self.tick).to_integral_value(rounding=ROUND_CEILING))
                    need = max(required, local or 0)
                    if need > self.radii[-1]:
                        self.radii = radius_grid(need)
                    quote = self.radii[bisect_left(self.radii, need)]
                    for k in (local, quote):
                        if k is not None and k not in self.reference.mass:
                            qb, qa = self._quantities(k)
                            self.reference.establish(k, (qb + qa) / (2 * (k + 1))
                                                     if qb is not None and qa is not None else None)
        local_row = self._components(local, read_ns, reason or ('local_fewer_than_20' if local is None else None))
        quote_row = self._components(quote, read_ns, reason)
        options = dict(LOCAL_STATIC=local_row['static'], LOCAL_DYNAMIC=local_row['dynamic'],
                       QUOTE_STATIC=quote_row['static'], QUOTE_DYNAMIC=quote_row['dynamic'])
        for region, row in (('LOCAL', local_row), ('QUOTE', quote_row)):
            for component, available in row['masks'].items():
                self.counts[f'{region}:{component}:available'] += available
                if not available:
                    self.counts[f'{region}:{component}:missing:{row["reasons"][component]}'] += 1
        selected = dict(u=0., v=0.) if self.arm == 'OBSERVE_ZERO' else options[self.arm]
        available = not reason and (quote_row if self.arm.startswith('QUOTE') else local_row)['masks']['I']
        self.last_audit = dict(schema=self.SCHEMA, arm=self.arm, read_ns=read_ns,
                              **selected, available=bool(available), reason=reason,
                              local=local_row, quote=quote_row, options=options,
                              required_quote_radius_ticks=required,
                              quote_radius_rounding_ticks=quote - required if quote is not None else None,
                              original_raw_bid=float(bid), original_raw_ask=float(ask),
                              visible_best_bid=state.best_bid, visible_best_ask=state.best_ask,
                              inventory=state.inventory, variance_rate=state.sigma_sq,
                              event_id=current.observation.original.event_id if current else None,
                              source_asof_ns=current.observation.original.source_asof_ns if current else None)
        return self.last_audit
