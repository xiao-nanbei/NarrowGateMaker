"""Causal, geometry-matched reference and v3 price-policy mathematics.

This module does not place orders or bypass the shared quote protections.
Reference geometry is BBO-relative; the five-second dynamic term must instead
be supplied for the SAME absolute price interval at both endpoints.
"""

from bisect import bisect_right
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
import math


@dataclass(frozen=True)
class SideProfile:
    """One legally visible side, indexed by integer ticks away from its BBO."""

    offsets: tuple[int, ...]
    cumulative_qty: tuple[float, ...]
    covered_ticks: int

    @classmethod
    def from_levels(cls, levels, *, side, tick_size, covered_ticks):
        if side not in {"bid", "ask"}:
            raise ValueError("invalid side")
        tick = Decimal(str(tick_size))
        if not tick.is_finite() or tick <= 0:
            raise ValueError("positive tick required")
        if type(covered_ticks) is not int or covered_ticks < 0 or not levels:
            raise ValueError("explicit initialized coverage and nonempty levels required")
        best = Decimal(str(levels[0][0]))
        offsets, totals = [], []
        total = 0.0
        previous = -1
        for price, quantity in levels:
            price, quantity = Decimal(str(price)), Decimal(str(quantity))
            if not (price.is_finite() and quantity.is_finite() and price > 0 and quantity > 0):
                raise ValueError("invalid displayed level")
            distance = (best-price if side == "bid" else price-best)/tick
            if distance != distance.to_integral_value() or distance <= previous:
                raise ValueError("levels must be sorted, distinct and tick aligned")
            previous = int(distance)
            if previous <= covered_ticks:
                offsets.append(previous)
                total += float(quantity)
                if not math.isfinite(total):
                    raise ValueError("quantity overflow")
                totals.append(total)
        return cls(tuple(offsets), tuple(totals), covered_ticks)

    def quantity(self, width_ticks):
        if type(width_ticks) is not int or width_ticks < 0:
            raise ValueError("nonnegative integer region width required")
        if width_ticks > self.covered_ticks:
            return None
        index = bisect_right(self.offsets, width_ticks)
        return self.cumulative_qty[index-1] if index else 0.0


@dataclass(frozen=True)
class ReferenceSample:
    ready_ns: int
    bid: SideProfile | None
    ask: SideProfile | None
    epoch: int


class GeometryMatchedReference:
    """Trailing time-weighted mean, querying the SAME widths in every sample.

    Input is an explicit fixed-cadence sample of delivered book states. Read
    excludes all samples at the read timestamp; evaluate before observing the
    current sample. Unknown coverage/gaps/rebases invalidate the window, never
    contribute zero. No price bins, future fit, or per-candidate PnL selection.
    """

    def __init__(self, *, window_ns=1_800_000_000_000,
                 maximum_gap_ns=2_000_000_000):
        if any(type(x) is not int or x <= 0 for x in (window_ns, maximum_gap_ns)):
            raise ValueError("positive integer reference durations required")
        self.window_ns = window_ns
        self.maximum_gap_ns = maximum_gap_ns
        self.samples = deque()
        self.last_clock = -1

    def observe(self, sample):
        if type(sample.ready_ns) is not int or sample.ready_ns < 0 or sample.ready_ns <= self.last_clock:
            raise ValueError("reference clock must strictly advance; coalesce same-time publications first")
        self.last_clock = sample.ready_ns
        self.samples.append(sample)
        cutoff = sample.ready_ns-self.window_ns
        while len(self.samples) > 1 and self.samples[1].ready_ns <= cutoff:
            self.samples.popleft()

    def query(self, *, read_ns, bid_width_ticks, ask_width_ticks):
        if type(read_ns) is not int or read_ns < self.last_clock:
            raise ValueError("cannot query a future-updated reference")
        if any(type(x) is not int or x < 0 for x in (bid_width_ticks, ask_width_ticks)):
            raise ValueError("integer region widths required")
        # Closed price intervals contain width+1 grid points, including BBO.
        grid_points = bid_width_ticks+ask_width_ticks+2
        rows = [s for s in self.samples if s.ready_ns < read_ns]
        begin = read_ns-self.window_ns
        if not rows or rows[0].ready_ns > begin:
            return {"available": False, "reason": "reference_warmup"}
        epoch, total = rows[-1].epoch, 0.0
        for index, sample in enumerate(rows):
            end = rows[index+1].ready_ns if index+1 < len(rows) else read_ns
            if end <= begin:
                continue
            if sample.epoch != epoch or end-sample.ready_ns > self.maximum_gap_ns:
                return {"available": False, "reason": "reference_gap_or_rebase"}
            if sample.bid is None or sample.ask is None:
                return {"available": False, "reason": "reference_unknown",
                        "sample_ready_ns": sample.ready_ns}
            qb = sample.bid.quantity(bid_width_ticks)
            qa = sample.ask.quantity(ask_width_ticks)
            if qb is None or qa is None:
                return {"available": False, "reason": "reference_geometry_uncovered",
                        "sample_ready_ns": sample.ready_ns,
                        "requested_width_ticks": [bid_width_ticks, ask_width_ticks],
                        "covered_width_ticks": [sample.bid.covered_ticks, sample.ask.covered_ticks]}
            total += (qb+qa)/grid_points * (end-max(begin, sample.ready_ns))
        return {"available": True, "reason": None,
                "rho_ref": total/self.window_ns,
                "epoch": epoch,
                "window_start_ns": begin, "window_end_exclusive_ns": read_ns,
                "bid_width_ticks": bid_width_ticks, "ask_width_ticks": ask_width_ticks}


def bounded_correction(*, qb, qa, grid_points, rho_ref,
                       dynamic=False, previous_qb=None, previous_qa=None, g=0.10):
    """Return v3 u/v, with explicit missingness and zero-denominator fallback."""
    if not math.isfinite(g) or not 0 < g < 1:
        raise ValueError("correction bound must be positive and below one")
    values = [qb, qa, rho_ref]
    if dynamic:
        values += [previous_qb, previous_qa]
    if any(x is None for x in values):
        return {"u": 0.0, "v": 0.0, "available": False, "reason": "missing_input"}
    if any(not math.isfinite(x) or x < 0 for x in values):
        raise ValueError("finite nonnegative quantities and reference required")
    if type(grid_points) is not int or grid_points <= 0:
        raise ValueError("positive integer grid size required")
    q = qb+qa
    rho = q/grid_points
    if q == 0 or rho+rho_ref == 0:
        return {"u": 0.0, "v": 0.0, "available": False, "reason": "zero_denominator"}
    imbalance, thinness = (qb-qa)/q, (rho_ref-rho)/(rho_ref+rho)
    flow = depletion = 0.0
    if dynamic:
        qprev = previous_qb+previous_qa
        flow = ((qb-previous_qb)-(qa-previous_qa))/(q+qprev)
        depletion = (qprev-q)/(qprev+q)
    clip = lambda x: min(1.0, max(-1.0, x))
    return {"u": g*clip(imbalance+flow), "v": g*clip(thinness+depletion),
            "available": True, "reason": None,
            "I": imbalance, "T": thinness, "F": flow, "E": depletion}


def corrected_raw_pair(bid, ask, *, u, v):
    """Unprotected research targets; caller MUST use the original projection."""
    if not all(math.isfinite(x) for x in (bid, ask, u, v)):
        raise ValueError("finite prices and correction required")
    if not 0 < bid < ask or abs(u) > 0.10 or abs(v) > 0.10:
        raise ValueError("invalid pair or correction outside frozen bound")
    if u == v == 0:
        return bid, ask
    center, half = (bid+ask)/2, (ask-bid)/2
    return center+half*u-half*(1+v), center+half*u+half*(1+v)
