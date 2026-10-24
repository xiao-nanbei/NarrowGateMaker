"""Per-account causal state for the adaptive depth-price experiment.

Only delivered observations enter this owner. It neither reads exchange truth
nor sends orders. The caller must advance to its actual capture timestamp and
charge maintenance/query costs before admitting the resulting order request.
"""

from collections import Counter
from dataclasses import replace
from decimal import Decimal, ROUND_CEILING

from data.depth_observation import PublishedDepthState, depth_bucket
from research.families.f08_side_taker_lifecycle.depth_dynamics import VisibleDepthHistory
from research.families.f08_side_taker_lifecycle.depth_price_policy import (
    GeometryMatchedReference, ReferenceSample, SideProfile, bounded_correction,
)


class AdaptiveDepthPricePolicy:
    ARMS = {'NEAR_STATIC', 'NEAR_DYNAMIC', 'DEEP_STATIC', 'DEEP_DYNAMIC'}

    def __init__(self, *, arm, tick_size, reference_window_ns=1_800_000_000_000,
                 sample_period_ns=1_000_000_000, maximum_gap_ns=2_000_000_000,
                 dynamic_window_ns=5_000_000_000, max_source_age_ns=1_000_000_000):
        if arm not in self.ARMS:
            raise ValueError('unknown adaptive arm')
        self.tick = Decimal(str(tick_size))
        if not self.tick.is_finite() or self.tick <= 0:
            raise ValueError('positive tick required')
        for duration in (sample_period_ns, dynamic_window_ns, max_source_age_ns):
            if type(duration) is not int or duration <= 0:
                raise ValueError('positive integer duration required')
        self.arm, self.period, self.max_age = arm, sample_period_ns, max_source_age_ns
        self.dynamic_window = dynamic_window_ns
        self.reference = GeometryMatchedReference(window_ns=reference_window_ns,
                                                   maximum_gap_ns=maximum_gap_ns)
        self.history = VisibleDepthHistory(windows_ns=(dynamic_window_ns,),
                                           max_age_ns=max_source_age_ns)
        self.epoch = 0
        self.last_sample = None
        self.last_query_ns = -1
        self.counts = Counter()
        self.last_audit = None

    def _projection(self, observation):
        if self.arm.startswith('DEEP'):
            return observation
        # The near control owns ONLY the original Top20, including in the past.
        source = observation.snapshot
        buckets = tuple({} for _ in range(64))
        bounds = []
        for side in ('bid', 'ask'):
            levels = getattr(source.book, side+'s')[:20]
            for price, qty in levels:
                buckets[depth_bucket(side, price)][side, price] = qty
            bounds.append((side, (min(p for p, _ in levels), max(p for p, _ in levels))
                           if levels else None))
        return replace(observation, snapshot=PublishedDepthState(
            buckets, source.book, source.snapshot_version, source.source_ordinal, tuple(bounds)))

    def advance(self, observation):
        now = observation.original.ready_ns
        if observation.original.source_asof_ns > now:
            raise ValueError('source observation is not yet causally available')
        if now < self.last_query_ns:
            raise ValueError('late observation cannot revise an already evaluated decision')
        observation = self._projection(observation)
        old, new = self.history.current, observation.snapshot
        broken = (old is None or new.snapshot_version != old.snapshot.snapshot_version
                  or new.book.version < old.snapshot.book.version or not new.book.valid
                  or now-observation.original.source_asof_ns > self.max_age
                  or (old is not None and now-old.original.source_asof_ns > self.max_age))
        if broken:
            self.epoch += 1
        self.history.advance(observation)
        self.counts['publications'] += 1
        if self.last_sample is not None and now-self.last_sample < self.period:
            return
        profiles = []
        fresh = now-observation.original.source_asof_ns <= self.max_age
        for side in ('bid', 'ask'):
            bounds = dict(new.bounds)[side]
            levels = sorted(new.points(side), reverse=(side == 'bid'))
            if not fresh or not new.book.valid or not bounds or not levels:
                profiles.append(None)
                continue
            best = levels[0][0]
            width = (best-bounds[0] if side == 'bid' else bounds[1]-best)/self.tick
            if width < 0 or width != width.to_integral_value():
                profiles.append(None)
                continue
            profiles.append(SideProfile.from_levels(levels, side=side, tick_size=self.tick,
                                                     covered_ticks=int(width)))
        self.reference.observe(ReferenceSample(now, *profiles, self.epoch))
        self.last_sample = now
        self.counts['reference_samples'] += 1

    def correction(self, *, bid, ask, state, depth, read_ns):
        if read_ns < self.last_query_ns or read_ns < self.history.clock:
            raise ValueError('decision cannot read future or regress its capture clock')
        self.last_query_ns = read_ns
        self.counts['decisions'] += 1

        def fallback(reason, **details):
            self.counts['fallback:'+reason] += 1
            self.last_audit = dict(u=0., v=0., available=False, reason=reason,
                                   read_ns=read_ns, arm=self.arm, **details)
            return self.last_audit

        current = self.history.current
        if current is None:
            return fallback('no_delivered_depth')
        book = current.snapshot.book
        if not book.valid or not book.bids or not book.asks:
            return fallback('invalid_book')
        bb, ba = book.bids[0][0], book.asks[0][0]
        # Both paths must refer to the SAME captured BBO, not a later deep feed.
        if (float(bb) != state.best_bid or float(ba) != state.best_ask
                or not depth.bids or not depth.asks
                or float(bb) != depth.bids[0][0] or float(ba) != depth.asks[0][0]):
            return fallback('book_alignment_mismatch')
        if self.arm.startswith('NEAR'):
            widths = ((bb-book.bids[-1][0])/self.tick,
                      (book.asks[-1][0]-ba)/self.tick)
        else:
            bid, ask = Decimal(str(bid)), Decimal(str(ask))
            if not (bid.is_finite() and ask.is_finite() and 0 < bid < ask):
                raise ValueError('invalid unrounded B0 pair')
            half = (ask-bid)/2
            radius = max(bb-bid, ask-ba, Decimal(0))+Decimal('.2')*half+self.tick
            width = (radius/self.tick).to_integral_value(rounding=ROUND_CEILING)
            widths = (width, width)
        if any(w < 0 or w != w.to_integral_value() for w in widths):
            raise ValueError('region geometry is not tick aligned')
        bw, aw = map(int, widths)
        reference = self.reference.query(read_ns=read_ns, bid_width_ticks=bw, ask_width_ticks=aw)
        if not reference['available']:
            return fallback(reference['reason'], reference=reference)
        if reference['epoch'] != self.epoch:
            return fallback('reference_recent_discontinuity')
        regions = [('bid', bb-bw*self.tick, bb), ('ask', ba, ba+aw*self.tick)]
        quantities, previous = [], []
        for side, low, high in regions:
            row = self.history.query(side=side, low=low, high=high, read_ns=read_ns)
            if not row['available']:
                return fallback(row['reason'])
            quantity = Decimal(row['quantity'])
            quantities.append(float(quantity))
            if self.arm.endswith('DYNAMIC'):
                window = row['windows'][str(self.dynamic_window)]
                if not window['available']:
                    return fallback(window['reason'])
                prior = quantity-Decimal(window['positive_qty'])+Decimal(window['negative_qty'])
                if prior < 0:
                    raise ValueError('fixed-price history reconstructed a negative quantity')
                previous.append(float(prior))
        result = bounded_correction(qb=quantities[0], qa=quantities[1],
            grid_points=bw+aw+2, rho_ref=reference['rho_ref'],
            dynamic=self.arm.endswith('DYNAMIC'),
            previous_qb=previous[0] if previous else None,
            previous_qa=previous[1] if previous else None)
        self.last_audit = dict(result, read_ns=read_ns, arm=self.arm,
            event_id=current.original.event_id, source_asof_ns=current.original.source_asof_ns,
            bid_width_ticks=bw, ask_width_ticks=aw, rho_ref=reference['rho_ref'],
            region_coordinates='current BBO-relative reference; same absolute prices for dynamics')
        self.counts['available' if result['available'] else 'fallback:'+result['reason']] += 1
        return self.last_audit
