"""Local candidate adapter for the frozen trade/book response price choice.

No transport, simulated latency, fitting, or deployment authority lives here.
Missing/stale inputs preserve the existing price gate; risk and age gates remain
owned by MakerEngine. Live clocks are observed, never replay latency samples.
"""
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from data.observation import TradeContribution
from data.tardis_input import BookView
from features.trade_book_response import ResponseState
from strategy.response_action_value import ResponseActionValue


class LiveResponseHistory:
    """Called only under the signal lock, in actual local commit order."""

    def __init__(self):
        self.state = ResponseState(trade_coverage="unknown", fast_response_state=True)
        self.sequence = 0
        self.last_trade_id = None

    def _clock(self, now):
        self.sequence += 1
        if now < self.state.clock:
            # Wall clock steps are not repaired by sorting or clamping events.
            self.state = ResponseState(trade_coverage="unknown", fast_response_state=True)
            self.last_trade_id = None

    def trade(self, event, *, now_ns):
        self._clock(now_ns)
        first = last = event.get("t")
        if (event.get("e") != "trade" or "f" in event or "l" in event
                or type(first) is not int or first < 0
                or type(event.get("m")) is not bool):
            self.state.invalidate(now_ns, "unverified_trade_packet")
            self.state.trade_coverage = "unknown"
            self.last_trade_id = None
            return
        if self.last_trade_id is not None and first != self.last_trade_id + 1:
            self.state.invalidate(now_ns, "trade_gap_or_duplicate")
        self.last_trade_id = last
        self.state.trade_coverage = "observed"
        self.state.observe_trade(now_ns, TradeContribution(
            str(last), int(event["T"]) * 1_000_000,
            Decimal(str(event["p"])), Decimal(str(event["q"])),
            "sell" if event["m"] else "buy", last-first+1, 1,
            "native_individual_trade", self.sequence))

    def book(self, event, *, now_ns, version):
        self._clock(now_ns)
        source_ns = int(event.get("T", event.get("E", 0))) * 1_000_000
        bids = tuple((Decimal(str(p)), Decimal(str(q))) for p, q in event.get("b", ()))
        asks = tuple((Decimal(str(p)), Decimal(str(q))) for p, q in event.get("a", ()))
        valid = bool(bids and asks
                     and all(p.is_finite() and q.is_finite() and p > 0 and q > 0
                             for p, q in bids + asks)
                     and bids[0][0] < asks[0][0]
                     and all(a[0] > b[0] for a, b in zip(bids, bids[1:], strict=False))
                     and all(a[0] < b[0] for a, b in zip(asks, asks[1:], strict=False)))
        if not valid or not 0 < source_ns <= now_ns:
            self.state.invalidate(now_ns, "invalid_book_clock_or_levels")
            return
        self.state.observe_book(now_ns, source_ns,
            BookView(version, bids, asks, None, None, "live_committed_depth", True),
            emit_frames=False)

    def frame(self, *, sequence, capture_ns, side, price):
        if sequence != self.sequence or capture_ns < self.state.clock:
            return None
        self.state.advance(capture_ns)
        return self.state.order_frame("bid" if side == "BUY" else "ask", price)


class LiveResponsePolicy:
    def __init__(self, artifact):
        self.model = ResponseActionValue.from_fitted(artifact, "TRADE_BOOK_RESPONSE_VALUE")
        self.counts = Counter()

    @classmethod
    def load(cls, path, expected_sha256):
        raw = Path(path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise ValueError("response action model identity mismatch")
        return cls(json.loads(raw))

    def price_blocked(self, *, frame, side, inventory, order, target_price,
                      tick_size, age_ms, context, reasons, baseline_blocked):
        self.counts["calls"] += 1
        if (frame is None or not frame.valid or frame.price != order.price
                or frame.side != ("bid" if side == "BUY" else "ask")):
            self.counts["missing_or_stale_frame"] += 1
            return baseline_blocked
        row = self.action_observation(frame=frame, side=side, inventory=inventory,
            order=order, target_price=target_price, tick_size=tick_size,
            age_ms=age_ms, context=context, reasons=reasons)
        selected, value, reason = self.model.choose(row, baseline=not baseline_blocked)
        self.counts[reason] += 1
        if value is None:
            return baseline_blocked
        self.counts["scored"] += 1
        self.counts["changed_price_intent"] += int(selected == baseline_blocked)
        return not selected

    @staticmethod
    def action_observation(*, frame, side, inventory, order, target_price,
                           tick_size, age_ms, context, reasons):
        """Inspectable input to the actual scorer, without changing clocks/state.

        Offline acceptance can compare this exact consumer boundary; constructing
        a row is not evidence of eligibility, scoring or a submitted request.
        """
        role = "opener" if inventory == 0 else "add" if (side == "BUY") == (inventory > 0) else "reducing"
        action_context = {name: context.get(name) for name in (
            "pred_dir", "pred_ret", "tox_bid", "tox_ask", "book_imb",
            "raw_half_spread", "raw_mid_shift")}
        action_context.update(order_age_s=age_ms/1000., confirmed_fill_qty=order.filled_qty,
            local_inventory=inventory,
            signed_target_delta_ticks=(1 if side == "BUY" else -1)*(target_price-order.price)/tick_size,
            **{"role_"+name: float(role == name) for name in ("opener", "add", "reducing")})
        return dict(role=role, features=dict(frame.values), action_context=action_context,
            optional_update_admission=dict(decision_admissible=not reasons, reasons=reasons))
