"""Finite, causal trade/book history. No orders, labels, model or I/O.

Fixed-price changes are observations, not identified adds/cancels. Trade
windows use delivery time; they are never added to a different source interval
and described as a conservation equation.
"""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from decimal import Decimal
import math

from data.observation import TradeContribution
from data.tardis_input import BookView

SECOND = 1_000_000_000
WINDOWS = (1, 5, 10)


@dataclass(frozen=True)
class ResponseFrame:
    now_ns: int
    source_asof_ns: int
    book_version: int
    side: str
    price: float
    quantity: float
    mid: float
    values: tuple[tuple[str, float | None], ...]
    valid: bool
    reason: str | None
    recovery_target: float | None = None


def price_quantity(view, side, price):
    """Absence is zero only inside the visible side's price interval."""
    price = Decimal(str(price)) if not isinstance(price, Decimal) else price
    levels = view.bids if side == "bid" else view.asks
    if not view.valid or not levels:
        return None
    for p, q in levels:
        if p == price:
            return float(q)
    if min(levels[0][0], levels[-1][0]) <= price <= max(levels[0][0], levels[-1][0]):
        return 0.0
    return None


class ResponseState:
    """One path's state; checkpoint by ordinary serialization, never global.

    Windows are (now-w, now] in the chosen panel clock. The caller must label
    source attribution separately from strategy-ready observations. Explicit
    source reset and lost input invalidate continuity; no midnight reset.
    """

    def __init__(self, *, max_book_age_ns=SECOND, trade_coverage="unknown", max_window_events=200_000,
                 fast_response_state=False, range_block_size=None):
        if max_book_age_ns < 0 or max_window_events < 1:
            raise ValueError("invalid bounded state limits")
        self.max_age = max_book_age_ns
        self.trade_coverage = trade_coverage
        self.max_events = max_window_events
        self.clock = -1
        self.started_ns = None
        self.book = None
        self.book_key = None
        self.source_ns = None
        self.last_book_ready = None
        self.trades = {w: deque() for w in WINDOWS}
        self.volumes = {w: [0.0, 0.0] for w in WINDOWS}
        self.cumulative = [0.0, 0.0]
        self.trade_prices = deque()
        from features.trade_price_range import TradePriceRangeIndex
        self.range_index = TradePriceRangeIndex(range_block_size) if range_block_size else None
        self.prices = deque()
        self.fast_response_state = fast_response_state
        self.anchors = {"bid": {}, "ask": {}} if fast_response_state else {}
        self.delta = {"bid": {}, "ask": {}} if fast_response_state else {}
        self.stats = Counter()
        self.epoch = 0
        self.recovery_outcomes = {}
        self.recovery_expiry = deque()

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.__dict__.setdefault("fast_response_state", False)
        self.__dict__.setdefault("range_index", None)

    def advance(self, now):
        if now < self.clock:
            raise ValueError("response clock moved backwards")
        if self.started_ns is None:
            self.started_ns = now
        self.clock = now
        for w in WINDOWS:
            events = self.trades[w]
            while events and events[0][0] <= now - w * SECOND:
                _, side, qty = events.popleft()
                self.volumes[w][side] -= qty
        while len(self.prices) > 1 and self.prices[1][0] <= now - 10 * SECOND:
            self.prices.popleft()
        while self.trade_prices and self.trade_prices[0][0] <= now - 30 * SECOND:
            self.trade_prices.popleft()
        if self.range_index is not None:
            self.range_index.expire(now - 30 * SECOND)
        while self.recovery_expiry and self.recovery_expiry[0][0] < now - 40 * SECOND:
            _, key = self.recovery_expiry.popleft()
            self.recovery_outcomes.pop(key, None)

    def _end_anchor(self, key, now, reason):
        anchor = (self.anchors[key[0]].pop(key[1]) if self.fast_response_state else self.anchors.pop(key))
        identity = (*key, anchor[0])
        self.recovery_outcomes[identity] = (now, reason)
        self.recovery_expiry.append((now, identity))

    def anchor(self, side, price):
        return self.anchors[side].get(price) if self.fast_response_state else self.anchors.get((side, price))

    def _clear_anchors(self):
        count = sum(map(len, self.anchors.values())) if self.fast_response_state else len(self.anchors)
        self.anchors = {"bid": {}, "ask": {}} if self.fast_response_state else {}
        return count

    def invalidate(self, now, reason):
        self.advance(now)
        self.stats[f"invalid:{reason}"] += 1
        self.epoch += 1
        self.stats[f"recovery_censored:{reason}"] += self._clear_anchors()
        for w in WINDOWS:
            self.trades[w].clear()
            self.volumes[w] = [0.0, 0.0]
        self.prices.clear()
        self.trade_prices.clear()
        if self.range_index is not None:
            self.range_index.clear()
        self.started_ns = now
        self.book = self.book_key = self.source_ns = self.last_book_ready = None

    def observe_trade(self, now, trade):
        self.advance(now)
        if not isinstance(trade, TradeContribution):
            raise TypeError("expected shared trade contribution")
        side, qty = (0 if trade.side == "buy" else 1), float(trade.quantity)
        if not math.isfinite(qty) or qty <= 0:
            raise ValueError("invalid trade quantity")
        if len(self.trades[10]) >= self.max_events or len(self.trade_prices) >= self.max_events:
            self.invalidate(now, "overflow")
        self.cumulative[side] += qty
        self.trade_prices.append((now, float(trade.price)))
        if self.range_index is not None:
            self.range_index.append(now, float(trade.price))
        for w in WINDOWS:
            self.trades[w].append((now, side, qty))
            self.volumes[w][side] += qty
        self.stats["trade_contributions"] += 1

    def observe_book(self, now, source_ns, view, *, reset=False, include_values=True, emit_frames=True):
        self.advance(now)
        if not isinstance(view, BookView) or source_ns > now:
            raise ValueError("invalid/future book")
        key = (source_ns, view.version)
        if self.last_book_ready == now:
            self.stats["same_clock_book_publications"] += 1
        if not reset and self.book_key is not None and key < self.book_key:
            self.stats["older_replacement_ignored"] += 1
            return ()
        if reset or (self.last_book_ready is not None and now - self.last_book_ready > self.max_age):
            self.epoch += 1
            self.stats["recovery_censored:reset_or_gap"] += self._clear_anchors()
            self.book = None
        old = self.book
        if self.fast_response_state:
            self._fast_book_changes(now, old, view)
        else:
            self._reference_book_changes(now, old, view)
        self.book, self.book_key, self.source_ns, self.last_book_ready = view, key, source_ns, now
        self.stats["book_publications"] += 1
        if not view.valid or not view.bids or not view.asks:
            self.stats["invalid_books"] += 1
            return ()
        mid = float((view.bids[0][0] + view.asks[0][0]) / 2)
        self.prices.append((now, mid))
        if not emit_frames:
            return ()
        return tuple(self.frame(side, include_values=include_values) for side in ("bid", "ask"))

    def _reference_book_changes(self, now, old, view):
        self.delta = {}
        # Each visible quantity is reused by both recovery anchors and the
        # fixed-price delta walk. Convert once, preserving Decimal price keys.
        mappings = {"bid": {p: float(q) for p, q in view.bids},
                    "ask": {p: float(q) for p, q in view.asks}}
        bounds = {side: (min(levels[0][0], levels[-1][0]), max(levels[0][0], levels[-1][0]))
                  for side, levels in (("bid", view.bids), ("ask", view.asks)) if levels}

        def quantity_at(side, price):
            if not view.valid or side not in bounds:
                return None
            value = mappings[side].get(price)
            if value is not None:
                return value
            low, high = bounds[side]
            return 0.0 if low <= price <= high else None

        for anchor_key, anchor in list(self.anchors.items()):
            side, price = anchor_key
            quantity = quantity_at(side, price)
            if quantity is None:
                self.stats["recovery_censored:outside_depth_or_invalid"] += 1
                self._end_anchor(anchor_key, now, "outside_depth_or_invalid")
            elif quantity >= anchor[1]:
                self.stats["recovery_completed"] += 1
                self._end_anchor(anchor_key, now, "recovered")
            elif now - anchor[0] > 10 * SECOND:
                self.stats["recovery_censored:history_limit"] += 1
                self._end_anchor(anchor_key, now, "history_limit")
            else:
                anchor[2] = min(anchor[2], quantity)
        if old is not None and old.valid and view.valid:
            for side, levels in (("bid", old.bids), ("ask", old.asks)):
                for price, qty in levels:
                    after = quantity_at(side, price)
                    if after is None:
                        self.stats["fixed_price_unknown_after"] += 1
                        continue
                    change = after - float(qty)
                    self.delta[side, price] = change
                    if change < 0:
                        self.stats["fixed_price_declines"] += 1
                        self.anchors.setdefault((side, price), [now, float(qty), after])
    def _fast_book_changes(self, now, old, view):
        self.delta = {"bid": {}, "ask": {}}
        for side, levels in (("bid", view.bids), ("ask", view.asks)):
            mapping = {p: float(q) for p, q in levels}
            covered = bool(view.valid and levels)
            low, high = ((min(levels[0][0], levels[-1][0]), max(levels[0][0], levels[-1][0]))
                         if levels else (None, None))
            anchors = self.anchors[side]
            ended = []
            for price, anchor in anchors.items():
                quantity = mapping.get(price) if covered else None
                if quantity is None and covered and low <= price <= high:
                    quantity = 0.0
                if quantity is None:
                    reason = "outside_depth_or_invalid"
                    self.stats["recovery_censored:outside_depth_or_invalid"] += 1
                elif quantity >= anchor[1]:
                    reason = "recovered"
                    self.stats["recovery_completed"] += 1
                elif now - anchor[0] > 10 * SECOND:
                    reason = "history_limit"
                    self.stats["recovery_censored:history_limit"] += 1
                else:
                    anchor[2] = min(anchor[2], quantity)
                    continue
                ended.append((price, reason))
            for price, reason in ended:
                self._end_anchor((side, price), now, reason)
            if old is not None and old.valid and view.valid:
                delta = self.delta[side]
                for price, qty in (old.bids if side == "bid" else old.asks):
                    after = mapping.get(price)
                    if after is None and covered and low <= price <= high:
                        after = 0.0
                    if after is None:
                        self.stats["fixed_price_unknown_after"] += 1
                        continue
                    change = after - float(qty)
                    delta[price] = change
                    if change < 0:
                        self.stats["fixed_price_declines"] += 1
                        anchors.setdefault(price, [now, float(qty), after])

    def frame(self, side, *, include_values=True):
        view, now = self.book, self.clock
        if view is None or not view.valid:
            return None
        levels = view.bids if side == "bid" else view.asks
        opposite = view.asks if side == "bid" else view.bids
        price, qty = levels[0]
        sign = 1 if side == "bid" else -1
        mid = float((view.bids[0][0] + view.asks[0][0]) / 2)
        age = now - self.source_ns
        ready = now - self.started_ns >= 10 * SECOND
        valid = ready and age <= self.max_age and self.trade_coverage == "observed"
        reason = None if valid else ("warmup" if not ready else "book_age" if age > self.max_age else "trade_coverage")
        anchor = self.anchor(side, price)
        if not include_values:
            return ResponseFrame(now, self.source_ns, view.version, side, float(price), float(qty), mid,
                                 (), valid, reason, anchor[1] if anchor else None)
        values = {
            "spread": float(view.asks[0][0] - view.bids[0][0]),
            "own_depth": sum(float(q) for _, q in levels),
            "opposite_depth": sum(float(q) for _, q in opposite),
            "own_touch_qty": float(qty), "side_sign": sign,
            "book_age_s": age / SECOND,
            "touch_delta_qty": (self.delta[side].get(price) if self.fast_response_state else self.delta.get((side, price))),
            "neighbor_delta_qty": ((sum(self.delta[side][p] for p, _ in levels[1:])
                                    if all(p in self.delta[side] for p, _ in levels[1:]) else None)
                                   if self.fast_response_state else
                                   (sum(self.delta[side, p] for p, _ in levels[1:])
                                    if all((side, p) in self.delta for p, _ in levels[1:]) else None)),
            "utc_time_sin": math.sin(2 * math.pi * (now % (86400 * SECOND)) / (86400 * SECOND)),
            "utc_time_cos": math.cos(2 * math.pi * (now % (86400 * SECOND)) / (86400 * SECOND)),
        }
        history = list(self.prices)
        values["past_price_variation_10s"] = sum((b[1] - a[1]) ** 2 for a, b in zip(history, history[1:], strict=False)) / 10
        for w in WINDOWS:
            buy, sell = self.volumes[w]
            values[f"trade_pressure_{w}s"] = -sign * (buy - sell) / w if valid else None
            values[f"trade_activity_{w}s"] = (buy + sell) / w if valid else None
            prior = next((p for t, p in reversed(self.prices) if t <= now - w * SECOND), None)
            values[f"past_mid_move_{w}s"] = sign * (mid - prior) if prior is not None else None
        anchor = self.anchor(side, price)
        values["recovery_age_s"] = (now - anchor[0]) / SECOND if anchor else None
        values["observed_recovery_fraction"] = ((float(qty) - anchor[2]) / (anchor[1] - anchor[2]) if anchor and anchor[1] > anchor[2] else None)
        pressure = values["trade_pressure_1s"]
        change = values["touch_delta_qty"]
        values["pressure_x_depth_change"] = pressure * change if pressure is not None and change is not None else None
        values["pressure_acceleration"] = (pressure - values["trade_pressure_5s"] if pressure is not None else None)
        return ResponseFrame(now, self.source_ns, view.version, side, float(price), float(qty), mid,
                             tuple(values.items()), valid, reason, anchor[1] if anchor else None)

    def order_frame(self, side, order_price):
        """Touch context plus history at the actual old order's fixed price.

        Outside visible depth is unsupported, never zero and never replaced by
        the touch's recovery history. Does not mutate history or create events.
        """
        if side not in ('bid', 'ask'):
            raise ValueError('unknown book side')
        price = Decimal(str(order_price))
        if not price.is_finite() or price <= 0:
            raise ValueError('invalid old order price')
        if self.book is None or not self.book.valid:
            return None
        qty = price_quantity(self.book, side, price)
        if qty is None:
            return None
        touch = self.frame(side)
        anchor = self.anchor(side, price)
        values = dict(touch.values)
        values.update(
            order_price_qty=qty,
            order_price_delta_qty=(self.delta[side].get(price) if self.fast_response_state
                                   else self.delta.get((side, price))),
            order_distance_from_touch=float(price - Decimal(str(touch.price))),
            order_recovery_age_s=(self.clock - anchor[0]) / SECOND if anchor else None,
            order_observed_recovery_fraction=((qty - anchor[2]) / (anchor[1] - anchor[2])
                if anchor and anchor[1] > anchor[2] else None),
        )
        return ResponseFrame(touch.now_ns, touch.source_asof_ns, touch.book_version, side,
                             float(price), qty, touch.mid, tuple(values.items()), touch.valid,
                             touch.reason, anchor[1] if anchor else None)
