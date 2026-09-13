"""Immutable public observations and causal windows shared by both adapters.

This is an input contract, not a live deployment or an exchange queue model.
The source adapter is kept separate from publication/latency scenarios.
"""

from __future__ import annotations

import heapq
from collections import OrderedDict, deque
from dataclasses import dataclass
from decimal import Decimal
import math

from data.tardis_input import BookView, TradeExecution

CONTRACT = "data.observation.v1"
ZERO = Decimal(0)


@dataclass(frozen=True)
class TradeContribution:
    event_id: str
    exchange_ts_ns: int
    price: Decimal
    quantity: Decimal
    side: str
    individual_count: int | None
    native_packet_count: int | None
    count_origin: str
    source_ordinal: int

    def __post_init__(self):
        if (not self.event_id or self.exchange_ts_ns < 0 or self.side not in {"buy", "sell"}
                or not self.price.is_finite() or self.price <= 0
                or not self.quantity.is_finite() or self.quantity <= 0):
            raise ValueError("invalid trade contribution")
        if any(value is not None and (type(value) is not int or value < 1)
               for value in (self.individual_count, self.native_packet_count)):
            raise ValueError("invalid observed count")


def historical_trade(trade: TradeExecution) -> TradeContribution:
    if trade.exchange_ts_ns is None:
        raise ValueError("trade requires an explicit exchange-clock evidence contract")
    return TradeContribution(f"{trade.market_id}:{trade.trade_id}", trade.exchange_ts_ns,
        trade.price, trade.quantity, trade.aggressor_side, 1, None, "source_trade_event", trade.source_ordinal)


def live_aggregate(*, event_id, exchange_ts_ns, price, quantity, side,
                   first_id=None, last_id=None, source_ordinal=0):
    """One observed packet, never fabricated individually timed children."""
    count = None
    if first_id is not None or last_id is not None:
        if type(first_id) is not int or type(last_id) is not int or first_id < 0 or last_id < first_id:
            raise ValueError("invalid aggregate ID range")
        count = last_id - first_id + 1
    return TradeContribution(event_id, exchange_ts_ns, Decimal(str(price)), Decimal(str(quantity)), side,
                             count, 1, "derived_id_range" if count is not None else "unavailable", source_ordinal)


class LiveAggregateAdapter:
    """Per-market ordered packet identity/range validation with bounded memory.

    Ancient duplicates outside the retained identity window fail rather than
    silently re-entering statistics. Gaps are reported, never filled from IDs.
    """

    def __init__(self, *, identity_window=10000):
        if identity_window < 1:
            raise ValueError("positive identity window required")
        self.identity_window = identity_window
        self.seen = OrderedDict()
        self.last_packet = self.last_child = None
        self.id_gaps = 0

    def adapt(self, *, packet_id, first_id=None, last_id=None, **fields):
        if type(packet_id) is not int or packet_id < 0:
            raise ValueError("invalid packet identity")
        contribution = live_aggregate(event_id=str(packet_id), first_id=first_id, last_id=last_id, **fields)
        content = (contribution.exchange_ts_ns, contribution.price, contribution.quantity,
                   contribution.side, first_id, last_id)
        if packet_id in self.seen:
            if self.seen[packet_id] != content:
                raise ValueError("conflicting live packet identity")
            return None
        if self.last_packet is not None and packet_id <= self.last_packet:
            raise ValueError("out-of-order or expired duplicate packet identity")
        if first_id is not None and self.last_child is not None:
            if first_id <= self.last_child:
                raise ValueError("overlapping live child ID ranges")
            if first_id != self.last_child + 1:
                self.id_gaps += 1
        self.seen[packet_id] = content
        self.last_packet, self.last_child = packet_id, last_id
        while len(self.seen) > self.identity_window:
            self.seen.popitem(last=False)
        return contribution


@dataclass(frozen=True)
class Observation:
    event_id: str
    market_id: str
    input_contract_id: str
    origin: str
    source_asof_ns: int
    publish_ns: int
    receive_ns: int
    ready_ns: int
    payload: BookView | TradeContribution
    coverage: str = "unknown"
    observation_contract_id: str = CONTRACT

    def __post_init__(self):
        if not (0 <= self.source_asof_ns <= self.publish_ns <= self.receive_ns <= self.ready_ns):
            raise ValueError("observation clocks violate causal order")
        if not self.event_id or not self.market_id or not self.input_contract_id:
            raise ValueError("observation identity and input contract required")
        if self.coverage not in {"observed", "partial", "missing", "unknown"}:
            raise ValueError("invalid observation coverage")


@dataclass(frozen=True)
class LatencyProfile:
    market_delay_ns: int
    processing_ns: int
    profile_id: str

    def __post_init__(self):
        if self.market_delay_ns < 0 or self.processing_ns < 0 or not self.profile_id:
            raise ValueError("explicit nonnegative latency profile required")


class DeliveryQueue:
    """Receive-order processing, immutable payloads, no provider clock input."""

    def __init__(self, profile: LatencyProfile):
        self.profile = profile
        self._received, self._ready = [], []
        self._ordinal = self._available = 0
        self._clock = -1

    def publish(self, *, event_id, market_id, input_contract_id, origin,
                source_asof_ns, publish_ns, payload, coverage="unknown", market_delay_ns=None,
                processing_ns=None):
        delay = self.profile.market_delay_ns if market_delay_ns is None else market_delay_ns
        service = self.profile.processing_ns if processing_ns is None else processing_ns
        if min(delay, service) < 0 or source_asof_ns > publish_ns or publish_ns < self._clock:
            raise ValueError("future source or retroactive publication")
        if not isinstance(payload, (BookView, TradeContribution)):
            raise TypeError("only immutable contract payloads may cross the visibility boundary")
        expected = payload.exchange_ts_ns if isinstance(payload, TradeContribution) else (
            payload.source_asof_us * 1000 if payload.source_asof_us is not None else None)
        if expected != source_asof_ns:
            raise ValueError("observation source clock does not match immutable payload")
        receive = publish_ns + delay
        observation = Observation(event_id, market_id, input_contract_id, origin, source_asof_ns,
                                  publish_ns, receive, receive, payload, coverage)
        heapq.heappush(self._received, (receive, self._ordinal, observation, service))
        self._ordinal += 1

    def advance(self, now_ns):
        from dataclasses import replace
        if now_ns < self._clock:
            raise ValueError("visibility clock cannot run backwards")
        self._clock = now_ns
        while self._received and self._received[0][0] <= now_ns:
            receive, ordinal, observation, service = heapq.heappop(self._received)
            ready = max(receive, self._available) + service
            self._available = ready
            heapq.heappush(self._ready, (ready, ordinal, replace(observation, ready_ns=ready)))
        result = []
        while self._ready and self._ready[0][0] <= now_ns:
            result.append(heapq.heappop(self._ready)[2])
        return tuple(result)


class DepthPublisher:
    """Explicit derived cadence. Caller supplies state established by cutoff."""

    def __init__(self, period_ns=100_000_000, phase_ns=0):
        if period_ns <= 0 or not 0 <= phase_ns < period_ns:
            raise ValueError("invalid depth cadence/phase")
        self.period_ns, self.phase_ns = period_ns, phase_ns
        self.last_publication_ns = None

    def publish(self, queue, view, *, now_ns, market_id, input_contract_id,
                source_clock_evidence, coverage="unknown"):
        if (now_ns - self.phase_ns) % self.period_ns:
            raise ValueError("publication must follow the configured grid")
        if self.last_publication_ns is not None and now_ns <= self.last_publication_ns:
            raise ValueError("duplicate or regressing depth publication")
        if source_clock_evidence not in {"exchange_event_E", "exchange_snapshot_anchor"}:
            raise ValueError("strict publisher requires explicit source-clock evidence")
        if view.source_asof_us is None or view.source_asof_us * 1000 > now_ns:
            raise ValueError("future or unavailable book observation")
        queue.publish(event_id=f"{market_id}:depth:{now_ns}", market_id=market_id,
            input_contract_id=input_contract_id, origin="derived_partial_depth20",
            source_asof_ns=view.source_asof_us * 1000, publish_ns=now_ns, payload=view, coverage=coverage)
        self.last_publication_ns = now_ns


@dataclass(frozen=True)
class Bar:
    start_ns: int
    end_ns: int
    ready_ns: int
    coverage: str
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None
    turnover: Decimal | None
    buy_volume: Decimal | None
    sell_volume: Decimal | None
    individual_count: int | None
    native_packet_count: int | None
    count_origins: tuple[str, ...]
    last_trade_ns: int | None
    market_id: str
    input_contract_id: str | None
    observation_contract_id: str = CONTRACT
    buy_turnover: Decimal | None = None
    sell_turnover: Decimal | None = None
    buy_count: int | None = None
    sell_count: int | None = None
    observed_event_count: int = 0


class VisibleTradeWindows:
    """Timer-closed ready-time windows; no retroactive modification of bars.

    Observations at the deadline are included before timer closure (tie rule).
    Advance with all ready observations up to that cutoff as one ordered batch.
    Unknown capture remains unknown even when some contributions are observed.
    """

    def __init__(self, *, start_ns, period_ns=1_000_000_000, allowed_lateness_ns,
                 market_id, coverage="unknown", input_contract_id=None):
        if period_ns <= 0 or allowed_lateness_ns < 0 or start_ns % period_ns:
            raise ValueError("invalid window clock contract")
        if coverage not in {"observed", "partial", "missing", "unknown"}:
            raise ValueError("invalid coverage status")
        self.next_start = start_ns
        self.period, self.lateness = period_ns, allowed_lateness_ns
        self.market_id, self.coverage = market_id, coverage
        self.input_contract_id = input_contract_id
        self.pending = {}
        self.clock = -1
        self.late_count = 0
        self.last_trade = None

    def advance(self, now_ns, observations=()):
        if now_ns < self.clock:
            raise ValueError("window clock cannot regress")
        for observation in observations:
            if observation.market_id != self.market_id or not isinstance(observation.payload, TradeContribution):
                raise ValueError("window market/payload mismatch")
            if observation.ready_ns > now_ns or observation.ready_ns < self.clock:
                raise ValueError("future or retroactively delivered observation")
            if observation.observation_contract_id != CONTRACT:
                raise ValueError("mixed observation contracts")
            if self.input_contract_id is None:
                self.input_contract_id = observation.input_contract_id
            elif observation.input_contract_id != self.input_contract_id:
                raise ValueError("mixed input contracts")
            trade = observation.payload
            if trade.side not in {"buy", "sell"} or trade.quantity <= 0 or trade.price <= 0:
                raise ValueError("invalid trade contribution")
            if trade.exchange_ts_ns > observation.publish_ns:
                raise ValueError("future trade contribution")
            start = trade.exchange_ts_ns // self.period * self.period
            if start < self.next_start or observation.ready_ns > start + self.period + self.lateness:
                self.late_count += 1
                continue
            self.pending.setdefault(start, []).append(trade)
            if self.last_trade is None or (trade.exchange_ts_ns, trade.source_ordinal) >= (self.last_trade.exchange_ts_ns, self.last_trade.source_ordinal):
                self.last_trade = trade
        self.clock = now_ns
        bars = []
        while self.next_start + self.period + self.lateness <= now_ns:
            start = self.next_start
            end = start + self.period
            trades = sorted(self.pending.pop(start, []), key=lambda t: (t.exchange_ts_ns, t.source_ordinal))
            prices = [t.price for t in trades]
            observed = self.coverage == "observed"
            def total(field, trades=trades, observed=observed):
                values = [getattr(t, field) for t in trades]
                return sum(values) if observed and all(v is not None for v in values) else None
            # The timer is serviced now, not at an oracle event-time deadline
            # in the past. A coarse/blocked processor cannot backdate readiness.
            bars.append(Bar(start, end, now_ns, self.coverage,
                prices[0] if prices else None, max(prices) if prices else None,
                min(prices) if prices else None, prices[-1] if prices else None,
                sum((t.quantity for t in trades), ZERO) if observed else None,
                sum((t.quantity * t.price for t in trades), ZERO) if observed else None,
                sum((t.quantity for t in trades if t.side == "buy"), ZERO) if observed else None,
                sum((t.quantity for t in trades if t.side == "sell"), ZERO) if observed else None,
                total("individual_count"), total("native_packet_count") if trades else None,
                tuple(sorted({t.count_origin for t in trades})), trades[-1].exchange_ts_ns if trades else None,
                self.market_id, self.input_contract_id, CONTRACT,
                sum((t.quantity * t.price for t in trades if t.side == "buy"), ZERO) if observed else None,
                sum((t.quantity * t.price for t in trades if t.side == "sell"), ZERO) if observed else None,
                sum(t.individual_count for t in trades if t.side == "buy") if observed and all(t.individual_count is not None for t in trades if t.side == "buy") else None,
                sum(t.individual_count for t in trades if t.side == "sell") if observed and all(t.individual_count is not None for t in trades if t.side == "sell") else None,
                len(trades)))
            self.next_start = end
        return tuple(bars)


@dataclass(frozen=True)
class FeatureFrame:
    input_contract_id: str
    observation_contract_id: str
    feature_contract_id: str
    cutoff_ns: int
    max_dependency_ready_ns: int | None
    values: tuple[tuple[str, Decimal | None], ...]
    source_ages_ns: tuple[tuple[str, int], ...]
    validity_mask: tuple[tuple[str, bool], ...]


class SharedFeatures:
    """Small common feature contract, NOT the retired 13-head feature schema."""

    def __init__(self, input_contract_id, *, window_ns=10_000_000_000):
        if window_ns <= 0:
            raise ValueError("positive feature lookback required")
        self.input_contract_id, self.window_ns = input_contract_id, window_ns
        self.books, self.bars = {}, {}
        self.clock = -1

    def advance(self, now_ns, observations=(), closed_bars=()):
        if now_ns < self.clock:
            raise ValueError("features cannot rewrite past time")
        for observation in observations:
            if observation.ready_ns > now_ns or observation.ready_ns < self.clock:
                raise ValueError("future or retroactive feature dependency")
            if (observation.input_contract_id != self.input_contract_id
                    or observation.observation_contract_id != CONTRACT):
                raise ValueError("mixed input contracts")
            if isinstance(observation.payload, BookView):
                self.books[observation.market_id] = observation
        for market, bar in closed_bars:
            if bar.ready_ns > now_ns or bar.ready_ns < self.clock:
                raise ValueError("bar is not causally delivered")
            if (bar.input_contract_id != self.input_contract_id or bar.market_id != market
                    or bar.observation_contract_id != CONTRACT):
                raise ValueError("bar market/input contract mismatch")
            self.bars.setdefault(market, deque()).append(bar)
        for bars in self.bars.values():
            while bars and bars[0].end_ns <= now_ns - self.window_ns:
                bars.popleft()
        self.clock = now_ns

    def frame(self, cutoff_ns):
        if cutoff_ns != self.clock:
            raise ValueError("frame must use the current delivered cutoff")
        values, ages, dependencies = [], [], []
        for market, observation in sorted(self.books.items()):
            book = observation.payload
            mid = (book.bids[0][0] + book.asks[0][0]) / 2 if book.valid else None
            values.append((market + ":mid", mid))
            ages.append((market, cutoff_ns - observation.source_asof_ns))
            dependencies.append(observation.ready_ns)
        for market, bars in sorted(self.bars.items()):
            volume = sum((b.volume for b in bars), ZERO) if all(b.volume is not None for b in bars) else None
            values.append((market + ":volume", volume))
            dependencies.extend(b.ready_ns for b in bars)
        return FeatureFrame(self.input_contract_id, CONTRACT, "data.features.v1", cutoff_ns,
                            max(dependencies, default=None), tuple(values), tuple(ages),
                            tuple((name, value is not None) for name, value in values))


FEATURE_CONTRACT = "data.execution_features.v1"
EXECUTION_FEATURE_NAMES = (
    "mid", "spread", "spread_bps", "weighted_mid_proxy", "book_age_s",
    "depth_bid_5", "depth_ask_5", "depth_bid_20", "depth_ask_20", "depth_imbalance_5",
    "depth_imbalance_20", "volume_10s", "turnover_10s", "buy_volume_10s", "sell_volume_10s",
    "clock_volume_imbalance_10s", "vwap_10s", "observed_trade_count_10s",
    "individual_count_10s", "native_packet_count_10s", "close", "last_trade_age_s",
    "return_10s", "return_30s", "return_60s", "realized_vol_60s",
    "volume_60s", "trade_intensity_burst_guard", "utc_hour_sin", "utc_hour_cos",
)


class ExecutionFeatures(SharedFeatures):
    """Complete supported execution-only schema for new model consumers.

    This does not impersonate the 173-column retired model. Missing metrics,
    reference markets and native packet statistics are not silently fabricated.
    Both live and historical adapters feed this exact calculator.
    """

    def __init__(self, input_contract_id, *, market_id, max_book_age_ns):
        super().__init__(input_contract_id, window_ns=61_000_000_000)
        if max_book_age_ns < 0:
            raise ValueError("explicit nonnegative book-age limit required")
        self.market_id, self.max_book_age_ns = market_id, max_book_age_ns
        self.last_valuation = None

    def advance(self, now_ns, observations=(), closed_bars=()):
        observations, closed_bars = tuple(observations), tuple(closed_bars)
        if any(x.market_id != self.market_id for x in observations) or any(m != self.market_id for m, _ in closed_bars):
            raise ValueError("execution-only contract cannot silently admit a reference market")
        super().advance(now_ns, observations, closed_bars)
        for _, bar in closed_bars:
            if bar.close is not None and bar.last_trade_ns is not None:
                self.last_valuation = bar.close, bar.last_trade_ns

    def frame(self, cutoff_ns):
        if cutoff_ns != self.clock:
            raise ValueError("frame must use current delivered cutoff")
        values = dict.fromkeys(EXECUTION_FEATURE_NAMES)
        dependencies, ages = [], []
        observation = self.books.get(self.market_id)
        if observation:
            book = observation.payload
            age = cutoff_ns - observation.source_asof_ns
            values["book_age_s"] = Decimal(age) / 1_000_000_000
            dependencies.append(observation.ready_ns)
            ages.append((self.market_id, age))
            if book.valid and age <= self.max_book_age_ns:
                (bid, bq), (ask, aq) = book.bbo
                mid = (bid + ask) / 2
                values.update(mid=mid, spread=ask-bid, spread_bps=(ask-bid)/mid*10000,
                              weighted_mid_proxy=(ask*bq+bid*aq)/(bq+aq))
                for depth in (5, 20):
                    if len(book.bids) >= depth and len(book.asks) >= depth:
                        bv = sum((q for _, q in book.bids[:depth]), ZERO)
                        av = sum((q for _, q in book.asks[:depth]), ZERO)
                        values[f"depth_bid_{depth}"] = bv
                        values[f"depth_ask_{depth}"] = av
                        values[f"depth_imbalance_{depth}"] = (bv-av)/(bv+av)
        bars = list(self.bars.get(self.market_id, ()))
        dependencies.extend(b.ready_ns for b in bars)
        # Full-window support is explicit: a sparse or unknown window is not
        # made dense by dropping missing seconds from the sums.
        windows = {}
        window_end = bars[-1].end_ns if bars else cutoff_ns
        for seconds in (10, 30, 60):
            selected = [b for b in bars if window_end-seconds*1_000_000_000 < b.end_ns <= window_end]
            complete = (len(selected) == seconds and all(b.coverage == "observed" for b in selected)
                        and all(b.end_ns == c.start_ns for b, c in zip(selected, selected[1:], strict=False)))
            windows[seconds] = selected if complete else []
            if complete:
                first, last = selected[0].open, selected[-1].close
                if first is not None and last is not None:
                    values[f"return_{seconds}s"] = last/first - 1
        ten, sixty = windows[10], windows[60]
        if ten:
            for name, field in (("volume_10s", "volume"), ("turnover_10s", "turnover"),
                                ("buy_volume_10s", "buy_volume"), ("sell_volume_10s", "sell_volume"),
                                ("individual_count_10s", "individual_count"),
                                ("native_packet_count_10s", "native_packet_count")):
                entries = [getattr(b, field) for b in ten]
                if all(x is not None for x in entries):
                    values[name] = sum(entries, ZERO)
            values["observed_trade_count_10s"] = Decimal(sum(b.observed_event_count for b in ten))
            volume = values["volume_10s"]
            if volume:
                values["vwap_10s"] = values["turnover_10s"]/volume
                values["clock_volume_imbalance_10s"] = (values["buy_volume_10s"]-values["sell_volume_10s"])/volume
        if sixty:
            values["volume_60s"] = sum((b.volume for b in sixty), ZERO)
            closes = [b.close for b in sixty]
            if all(x is not None for x in closes):
                returns = [math.log(float(b/a)) for a, b in zip(closes, closes[1:], strict=False)]
                values["realized_vol_60s"] = Decimal(str(math.sqrt(sum(r*r for r in returns)/len(returns))))
            slow = sum(b.observed_event_count for b in sixty)
            if ten and slow:
                values["trade_intensity_burst_guard"] = values["observed_trade_count_10s"]*6/slow
        if self.last_valuation:
            price, ts = self.last_valuation
            values["close"] = price
            values["last_trade_age_s"] = Decimal(cutoff_ns-ts)/1_000_000_000
        angle = (cutoff_ns % 86_400_000_000_000) / 86_400_000_000_000 * 2 * math.pi
        values["utc_hour_sin"], values["utc_hour_cos"] = Decimal(str(math.sin(angle))), Decimal(str(math.cos(angle)))
        return FeatureFrame(self.input_contract_id, CONTRACT, FEATURE_CONTRACT, cutoff_ns,
            max(dependencies, default=None), tuple(values.items()), tuple(ages),
            tuple((name, value is not None) for name, value in values.items()))


def model_row(frame, metadata, *, decision_ns):
    """The one model-input boundary used by offline, signal and replay."""
    if (frame.cutoff_ns > decision_ns or (frame.max_dependency_ready_ns is not None
            and frame.max_dependency_ready_ns > frame.cutoff_ns)):
        raise ValueError("model attempted to consume a future feature dependency")
    for key in ("input_contract_id", "observation_contract_id", "feature_contract_id"):
        if metadata.get(key) != getattr(frame, key):
            raise ValueError(f"model {key} mismatch; old model fallback forbidden")
    names = metadata.get("feature_cols")
    if not isinstance(names, list) or not names or len(set(names)) != len(names):
        raise ValueError("explicit unique model feature schema required")
    values, mask = dict(frame.values), dict(frame.validity_mask)
    if any(n not in values for n in names):
        raise ValueError("unsupported model feature; cannot substitute another source")
    missing = [n for n in names if not mask.get(n, False) or values[n] is None
               or not math.isfinite(float(values[n]))]
    if missing and metadata.get("missing_policy") != "native_nan":
        raise ValueError("model missing feature policy does not admit this frame")
    return [float(values[n]) if n not in missing else math.nan for n in names]
