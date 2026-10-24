"""Account-owned adaptive quote policy on original delivered observations.

The executor owns request timing. This extension returns a deterministic cost
from a frozen measured profile; host wall time is never sampled as economic
latency. Reopening a source after a checkpoint is IO recovery, not a new warmup.
"""

from collections import Counter
from copy import deepcopy
import math
import json

from data.depth_observation import DepthStateTap, MeasuredDepthStateTap
from research.families.f08_side_taker_lifecycle.depth_information import streams
from research.families.f08_side_taker_lifecycle.depth_price_runtime import AdaptiveDepthPricePolicy
from research.families.f08_side_taker_lifecycle.depth_quote_runtime import DepthQuotingPolicy


class DeliveredDepthCursor:
    def __init__(self, config, *, work_accounting=False):
        self.config = deepcopy(config)
        self.work_accounting = work_accounting
        self.clock = -1
        self.consumed_ticks = 0
        self.source_updates = self.publications = 0
        self._iterator = self._next = self._tap = None

    def __getstate__(self):
        return {k: v for k, v in vars(self).items()
                if k not in {'_iterator', '_next', '_tap'}}

    def __setstate__(self, state):
        vars(self).update(state)
        self._iterator = self._next = self._tap = None

    def _open(self):
        self._tap = MeasuredDepthStateTap() if self.work_accounting else DepthStateTap()
        stream = streams(self.config, self._tap)
        if self.work_accounting:
            self._tap.work_start_ns = getattr(stream, 'start_ns', 0)
        self._iterator = iter(stream)
        # Reconstruct only the cursor, never re-apply observations to the saved
        # account policy or charge them twice. No future tick is consumed here.
        for _ in range(self.consumed_ticks):
            next(self._iterator)

    def advance_to(self, read_ns):
        if type(read_ns) is not int or read_ns < self.clock:
            raise ValueError('delivered-depth capture clock regressed')
        if self._iterator is None:
            self._open()
        self.clock = read_ns
        while True:
            if self._next is None:
                self._next = next(self._iterator, None)
            if self._next is None or self._next.now_ns > read_ns:
                break
            tick, self._next = self._next, None
            self.consumed_ticks += 1
            # Cost follows actual delivered work, not lookahead performed by
            # the offline iterator to discover the next event timestamp.
            for observation in tick.research_depth_observations:
                if observation.original.ready_ns > read_ns:
                    raise ValueError('future depth publication at capture')
                yield observation


class DepthPriceReplayRuntime:
    def __init__(self, *, arm, tick_size, cost_profile, source_config=None,
                 constant=None, policy_options=None):
        costs = deepcopy(cost_profile)
        self.geometry_policy = arm in DepthQuotingPolicy.ARMS
        if (not isinstance(costs, dict) or not costs.get('measurement_id')
                or (not self.geometry_policy and any(type(costs.get(k)) is not int or costs[k] < 0
                                                     for k in ('publication_ns', 'query_ns')))):
            raise ValueError('bound measured publication/query nanosecond costs required')
        if self.geometry_policy:
            required = {'source_events', 'source_levels', 'copied_points', 'published_levels',
                        'publications', 'indexed_points', 'radius_integrations', 'region_queries',
                        'endpoint_queries', 'correction_queries', 'audit_bytes'}
            coefficients = costs.get('ns_per_unit', {})
            if (set(coefficients) != required or any(type(v) is not int or v < 0 for v in coefficients.values())
                    or costs.get('schema') != 'depth-quoting-work-cost.v1'):
                raise ValueError('complete frozen depth-quoting work-cost profile required')
        options = dict(policy_options or {})
        self.policy = None
        self.source = None
        if arm == 'PRICE_ONLY':
            if source_config is not None or options or constant is None:
                raise ValueError('constant price control must not consume deep state')
            u, v = constant
            if any(not math.isfinite(x) or abs(x) > .1 for x in (u, v)):
                raise ValueError('constant correction outside frozen envelope')
            self.constant = (float(u), float(v))
        elif self.geometry_policy:
            if constant is not None or source_config is None or options:
                raise ValueError('new quoting policy requires its fixed source and specification')
            self.policy = DepthQuotingPolicy(arm=arm, tick_size=tick_size)
            self.source = DeliveredDepthCursor(source_config, work_accounting=True)
            self.constant = None
        elif arm in AdaptiveDepthPricePolicy.ARMS:
            if constant is not None or source_config is None:
                raise ValueError('adaptive policy requires original bound observations')
            self.policy = AdaptiveDepthPricePolicy(arm=arm, tick_size=tick_size, **options)
            self.source = DeliveredDepthCursor(source_config)
            self.constant = None
        else:
            raise ValueError('unknown price policy arm; B0 omits the extension')
        self.contract = dict(schema='quote-price-replay-v2' if self.geometry_policy else 'quote-price-replay-v1', arm=arm,
            tick_size=tick_size, cost_profile=costs, source_config=deepcopy(source_config),
            constant=self.constant, policy_options=options,
            timing=('delivered maintenance plus all queries; combined ns rounded up once per capture to ms'
                    if self.geometry_policy else
                    'delivered maintenance plus query charged after original capture; ceil per call to ms'))
        self.counts = Counter()
        self.read_ns = -1
        self.pending_ns = 0
        self.capture_open = False
        self.last_audit = None
        self.work = Counter()
        self.capture_work = Counter()
        self.capture_rows = []

    def begin_capture(self, read_ns):
        if self.capture_open or type(read_ns) is not int or read_ns < self.read_ns:
            raise ValueError('overlapping or regressing price-policy capture')
        self.read_ns, self.capture_open = read_ns, True
        self.pending_ns = 0
        self.capture_work.clear()
        self.capture_rows.clear()
        self.counts['captures'] += 1
        if self.source is not None:
            for observation in self.source.advance_to(read_ns):
                self.policy.advance(observation)
                self.counts['publications'] += 1
                if self.geometry_policy:
                    self.capture_work.update(dict(observation.maintenance_work))
                else:
                    self.pending_ns += self.contract['cost_profile']['publication_ns']
        if self.geometry_policy:
            self.policy.begin_capture(read_ns)

    def correction(self, **kwargs):
        if not self.capture_open:
            raise ValueError('price correction outside captured decision')
        self.counts['queries'] += 1
        if not self.geometry_policy:
            self.pending_ns += self.contract['cost_profile']['query_ns']
        if self.policy is None:
            row = dict(u=self.constant[0], v=self.constant[1], available=True,
                       reason='frozen_constant', read_ns=self.read_ns)
        else:
            row = self.policy.correction(**kwargs, read_ns=self.read_ns)
        self.last_audit = deepcopy(row)
        if self.geometry_policy:
            self.capture_work['audit_bytes'] += len(json.dumps(row, separators=(',', ':'), allow_nan=False))
            self.capture_rows.append(self.last_audit)
        self.counts['nonzero_raw_corrections'] += bool(row['u'] or row['v'])
        self.counts['available_queries'] += bool(row['available'])
        self.counts['reason:'+str(row['reason'])] += 1
        return row

    def finish_capture(self):
        if not self.capture_open:
            raise ValueError('price-policy cost already charged')
        self.capture_open = False
        if self.geometry_policy:
            self.capture_work.update(self.policy.work)
            self.policy.work.clear()
            self.work.update(self.capture_work)
            self.pending_ns = sum(self.capture_work[k] * ns for k, ns in
                                  self.contract['cost_profile']['ns_per_unit'].items())
        added_ms = (self.pending_ns+999_999)//1_000_000
        self.counts['charged_ns_before_quantization'] += self.pending_ns
        self.counts['charged_ms'] += added_ms
        self.pending_ns = 0
        return added_ms

    def audit(self):
        result = dict(contract=self.contract, counts=dict(self.counts), last=self.last_audit,
                      adaptive_counts=dict(self.policy.counts) if self.policy else {})
        if self.geometry_policy:
            result['work_units'] = dict(self.work)
        return result

    def capture_evidence(self):
        """Journal-only evidence; never feeds the policy or request clock."""
        if not self.geometry_policy or self.capture_open:
            raise ValueError('completed depth-quoting capture required')
        current = self.policy.history.current
        bounds = dict(current.snapshot.bounds) if current is not None else {}
        return dict(capture_id=self.counts['captures'], read_ns=self.read_ns,
                    corrections=self.capture_rows, work_units=dict(self.capture_work),
                    publication_count=self.counts['publications'],
                    known_depth_bounds={side: [str(x) for x in bounds[side]]
                                        if bounds.get(side) is not None else None for side in ('bid', 'ask')},
                    depth_valid=bool(current is not None and current.snapshot.book.valid),
                    source_ready_ns=current.observation.original.ready_ns if current is not None else None)
