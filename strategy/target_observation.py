"""Output-only target publications; never schedules work or samples latency.

Replay's enqueue-ready boundary is a modeled proxy, not a measured per-side
CPU completion. Pending publications enter the estimator only at an existing
executor event at/after that boundary. No R or trading consumer is involved.
"""
from collections import Counter
from dataclasses import dataclass, field
import math

from strategy.target_variance import TargetVarianceState


@dataclass
class TargetObservation:
    contract: str = "target_pre_throttle_enqueue_proxy.v1"
    sample_limit: int = 256
    sequence: int = 0
    call_sequence: int = 0
    applied_sequence: int = 0
    sides: dict = field(default_factory=lambda: {
        "BUY": TargetVarianceState(), "SELL": TargetVarianceState()})
    pending: list = field(default_factory=list)
    samples: list = field(default_factory=list)
    counts: Counter = field(default_factory=Counter)
    last_call: dict = field(default_factory=dict)
    last_time_ns: int | None = None

    def begin_call(self):
        self.call_sequence += 1
        return self.call_sequence

    def publish(self, *, side, call_id, price, observation_ns, logical_ns,
                trigger_kind, stages, source_version=None):
        if side not in self.sides:
            raise ValueError("invalid target side")
        if not math.isfinite(price) or price <= 0:
            raise ValueError("invalid target price")
        if any(type(t) is not int or t < 0 for t in (observation_ns, logical_ns)):
            raise ValueError("invalid target clock")
        if not call_id:
            raise ValueError("target call identity required")
        identity = (call_id, price, observation_ns)
        previous = self.last_call.get(side)
        if previous and previous[0] == call_id:
            if previous != identity:
                raise ValueError("target publication identity conflict")
            return None
        # The executor is serial; recording a future target does not expose it.
        if self.last_time_ns is not None and observation_ns < self.last_time_ns:
            raise ValueError("target publication clock regression")
        self.sequence += 1
        row = dict(side=side, publication_seq=self.sequence, call_id=call_id,
                   target_price=float(price), observation_ns=observation_ns,
                   logical_ns=logical_ns, trigger_kind=trigger_kind,
                   source_version=source_version, valid=True, **stages)
        self.last_call[side] = identity
        self.last_time_ns = observation_ns
        self.pending.append(row)
        self.counts[f"published:{side}:{trigger_kind}"] += 1
        self.counts[f"compute:{stages['compute_mode']}"] += 1
        if len(self.samples) < self.sample_limit:
            self.samples.append(dict(row))
        return row

    def advance(self, logical_ns):
        """Called by existing events only; no future advance at checkpoint/end."""
        while self.pending and self.pending[0]['observation_ns'] <= logical_ns:
            row = self.pending.pop(0)
            state = self.sides[row['side']]
            disposition = state.observe(price=row['target_price'],
                ready_ns=row['observation_ns'], version=row['publication_seq'])
            self.counts[f"estimate:{row['side']}:{disposition}"] += 1
            self.counts[f"coverage:{row['side']}:{'ready' if state.ready else 'warmup'}"] += 1
            self.applied_sequence = row['publication_seq']

    def snapshot(self):
        return dict(contract=self.contract, publication_seq=self.sequence,
                    call_sequence=self.call_sequence,
                    applied_sequence=self.applied_sequence,
                    pending=[dict(x) for x in self.pending],
                    sides={k: v.snapshot() for k, v in self.sides.items()},
                    counts=dict(self.counts), samples=[dict(x) for x in self.samples],
                    sample_limit=self.sample_limit)
