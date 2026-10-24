"""Small, serializable work ledger for optional visible-state quote scheduling.

This module owns no market data, strategy rules, clock, or execution loop.
Adapters publish only committed dependencies and keep existing safety checks.
Times are integer nanoseconds in the adapter's causal clock, never wall time
elapsed while a replay checkpoint is stopped.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Iterable


SIDES = ("BUY", "SELL")


def quote_schedule_mode(value: str) -> str:
    if value not in ("existing", "state_event"):
        raise ValueError("quote_schedule_mode must be existing or state_event")
    return value


def strict_ready_ms(ready_ns: int) -> int:
    """First representable replay millisecond satisfying original ready < now.

    This is a projection onto the existing millisecond executor, not a new
    latency sample or an epsilon added to the underlying publication time.
    """
    return int(ready_ns) // 1_000_000 + 1


@dataclass
class QuoteWork:
    sequence: int
    sides: tuple[str, ...]
    reason: str
    claimed: dict[str, int]
    reads: dict[str, dict[str, int]] = field(default_factory=dict)
    ticket: tuple[str, str] | None = None
    terminal_versions: dict[str, str] = field(default_factory=dict)


@dataclass
class QuoteSchedule:
    """One quote owner; callers synchronize live mutations with their wake lock.

    Claim is not an input read. A resumed calculation retains the same work
    object; only actual reads may absorb versions published after claim.
    """

    published: dict[str, int] = field(default_factory=dict)
    handled: dict[str, dict[str, int]] = field(
        default_factory=lambda: {side: {} for side in SIDES})
    terminals: dict[str, str] = field(default_factory=dict)
    deadlines: dict[str, tuple[int, str, str]] = field(default_factory=dict)
    inflight: QuoteWork | None = None
    busy_until_ns: int = 0
    sequence: int = 0
    counters: dict[str, int] = field(default_factory=dict)

    def count(self, key: str, n: int = 1) -> None:
        self.counters[key] = self.counters.get(key, 0) + n

    def publish(self, domain: str, version: int) -> bool:
        if not domain or isinstance(version, bool) or not isinstance(version, int):
            raise ValueError("committed domain and integer version required")
        if version <= self.published.get(domain, -1):
            self.count(f"duplicate:{domain}")
            return False
        if any(self.published.get(domain, -1) > self.handled[s].get(domain, -1)
               for s in SIDES):
            self.count(f"coalesced:{domain}")
        self.published[domain] = version
        self.count(f"published:{domain}")
        return True

    def change(self, domain: str) -> None:
        """Commit a state transition that has no external sequence number."""
        self.publish(domain, self.published.get(domain, -1) + 1)

    def terminal(self, side: str, ticket: str) -> None:
        if side not in SIDES or not ticket:
            raise ValueError("side and original terminal ticket required")
        if self.terminals.get(side) != ticket:
            self.terminals[side] = ticket
            self.count("terminal_published")

    def deadline(self, key: str, due_ns: int, side: str, identity: str) -> None:
        if side not in SIDES or not identity or not key:
            raise ValueError("deadline requires side and original state identity")
        self.deadlines[key] = (int(due_ns), side, identity)

    def expire(self, now_ns: int, valid: Iterable[tuple[str, str]]) -> None:
        """Adapter supplies currently valid (side, order/risk identity) tokens."""
        identities = set(valid)
        for key, (due, side, identity) in list(self.deadlines.items()):
            if (side, identity) not in identities:
                del self.deadlines[key]
                self.count("deadline_invalidated")
            elif due <= now_ns:
                del self.deadlines[key]
                domain = f"deadline:{key}"
                self.publish(domain, self.published.get(domain, -1) + 1)
                self.count("deadline_ready")

    def unhandled(self, sides: Iterable[str] = SIDES) -> bool:
        return any(v > self.handled[s].get(d, -1)
                   for s in sides for d, v in self.published.items())

    def claim(self, now_ns: int) -> QuoteWork | None:
        if self.inflight is not None or now_ns < self.busy_until_ns:
            return None
        ticket = next(iter(self.terminals.items()), None)
        if ticket is None and not self.unhandled():
            return None
        self.sequence += 1
        self.inflight = QuoteWork(
            self.sequence, (ticket[0],) if ticket else SIDES,
            "terminal" if ticket else "visible_state", dict(self.published),
            ticket=ticket,
            terminal_versions=dict(self.terminals),
        )
        self.count(f"started:{self.inflight.reason}")
        return self.inflight

    def read(self, sequence: int, domain: str, version: int,
             sides: Iterable[str]) -> None:
        work = self._work(sequence)
        if version > self.published.get(domain, -1):
            raise ValueError("read watermark was not published")
        routed_sides = tuple(sides)
        if any(side not in work.sides for side in routed_sides):
            raise ValueError("read side outside original route")
        for side in routed_sides:
            reads = work.reads.setdefault(side, {})
            reads[domain] = max(reads.get(domain, -1), version)

    def claim_route(self, now_ns: int, sides: Iterable[str], reason: str) -> QuoteWork:
        """Bind an already-authorized continuation, without a new dirty event.

        Its original order lifecycle remains the sole owner of the ticket.
        """
        routed = tuple(sides)
        if not routed or any(side not in SIDES for side in routed):
            raise ValueError("nonempty valid route required")
        if self.inflight is not None or now_ns < self.busy_until_ns:
            raise RuntimeError("quote computation is still occupied")
        self.sequence += 1
        self.inflight = QuoteWork(self.sequence, routed, reason, dict(self.published))
        self.count(f"started:{reason}")
        return self.inflight

    def finish(self, sequence: int, *, busy_until_ns: int,
               blocked: bool = False) -> None:
        work = self._work(sequence)
        for side in work.sides:
            # A claim acknowledges a trigger only when no concrete input read
            # supersedes it (for example a blocked prequote). Entry-frozen
            # predictions can be older than the publication at claim time.
            # Do not consume that newer publication merely by claiming work.
            acknowledged = dict(work.claimed)
            acknowledged.update(work.reads.get(side, {}))
            for domain, version in work.reads.get(side, {}).items():
                self.count("absorbed_versions", max(
                    0, version - work.claimed.get(domain, -1)))
            for domain, version in acknowledged.items():
                self.handled[side][domain] = max(
                    self.handled[side].get(domain, -1), version)
        for side, ticket in work.terminal_versions.items():
            if side in work.sides and self.terminals.get(side) == ticket:
                del self.terminals[side]
        self.busy_until_ns = max(self.busy_until_ns, int(busy_until_ns))
        self.inflight = None
        self.count(f"completed:{work.reason}")
        self.count("prequote_blocked", int(blocked))
        self.count("completed_with_unhandled", int(self.unhandled()))

    def _work(self, sequence: int) -> QuoteWork:
        if self.inflight is None or self.inflight.sequence != sequence:
            raise ValueError("not the current quote call")
        return self.inflight


class QuoteScheduleWakeup:
    """Synchronization only: no quote, market processing, I/O or RNG under lock.

    The runtime checkpoint owns ``state``, not this thread-local adapter. The
    existing main-loop Event may be supplied so terminal/shutdown and visible
    dependencies share one notification channel. Those facts remain in their
    respective authoritative state objects.
    """

    def __init__(self, state: QuoteSchedule, event: threading.Event):
        self.state = state
        self.event = event
        self.lock = threading.Lock()

    def publish(self, domain: str, version: int) -> bool:
        with self.lock:
            changed = self.state.publish(domain, version)
            if changed:
                self.event.set()
            return changed

    def claim(self, now_ns: int) -> QuoteWork | None:
        with self.lock:
            return self.state.claim(now_ns)

    def change(self, domain: str) -> None:
        with self.lock:
            self.state.change(domain)
            self.event.set()

    def terminal(self, side: str, ticket: str) -> None:
        with self.lock:
            self.state.terminal(side, ticket)
            self.event.set()

    def route(self, sides: Iterable[str]) -> None:
        routed = tuple(sides)
        if not routed or any(side not in SIDES for side in routed):
            raise ValueError("nonempty valid route required")
        with self.lock:
            if self.state.inflight is None:
                raise RuntimeError("route requires claimed quote work")
            self.state.inflight.sides = routed

    def expire(self, now_ns: int, valid: Iterable[tuple[str, str]]) -> None:
        with self.lock:
            self.state.expire(now_ns, valid)
            if self.state.unhandled():
                self.event.set()

    def deadline(self, key: str, due_ns: int, side: str, identity: str) -> None:
        with self.lock:
            self.state.deadline(key, due_ns, side, identity)

    def remaining_wait_s(self, now_ns: int, maximum_s: float) -> float:
        with self.lock:
            deadlines = [item[0] for item in self.state.deadlines.values()]
            if not deadlines:
                return maximum_s
            return min(maximum_s, max(0.0, (min(deadlines) - now_ns) / 1e9))

    def read(self, sequence: int, domain: str, version: int,
             sides: Iterable[str]) -> None:
        with self.lock:
            self.state.read(sequence, domain, version, sides)

    def finish(self, sequence: int, *, busy_until_ns: int,
               blocked: bool = False) -> None:
        with self.lock:
            self.state.finish(sequence, busy_until_ns=busy_until_ns, blocked=blocked)
            if self.state.unhandled() or self.state.terminals:
                self.event.set()

    def prepare_wait(self, now_ns: int) -> bool:
        """Atomically check and clear; return false if eligible work exists.

        The caller performs Event.wait *after* this method returns. A producer
        arriving after the clear sets the event and cannot be lost before wait.
        """
        with self.lock:
            if (self.state.inflight is None and now_ns >= self.state.busy_until_ns
                    and (self.state.unhandled() or self.state.terminals)):
                return False
            self.event.clear()
            return True
