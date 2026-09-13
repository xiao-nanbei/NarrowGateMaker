"""Receive/ready-time adapter for the shared execution feature protocol.

This boundary does not create deployment authority. In particular, aggregate
packets retain their packet cardinality; ID ranges are not synthetic trades.
"""

from collections import OrderedDict
from decimal import Decimal

from data.observation import (
    ExecutionFeatures, LiveAggregateAdapter, Observation, TradeContribution, VisibleTradeWindows,
)
from data.tardis_input import BookView


LIVE_INPUT_CONTRACT = "binance_usdm_live_observations.v1"
PACKET_DEPENDENT_FEATURES = frozenset({
    "observed_trade_count_10s", "trade_intensity_burst_guard",
})


def validate_live_feature_support(model_manifest, *, trade_source="aggregate"):
    """Reject a historical event-count model on an aggregate-only transport.

    An explicit successor model/transport contract is required. Renaming a
    metadata version or replacing a count with ID-range weight cannot prove it.
    """
    heads = model_manifest.get("heads", {})
    columns = {name for head in heads.values() for name in head.get("feature_cols", ())}
    incompatible = sorted(columns & PACKET_DEPENDENT_FEATURES)
    if trade_source not in {"aggregate", "individual"}:
        raise ValueError("explicit trade source required")
    if incompatible and trade_source != "individual":
        raise ValueError(
            "individual-source event counts are not live aggregate packet counts: "
            + ", ".join(incompatible)
        )
    from data.tardis_input import CONTRACT as HISTORICAL_INPUT
    if model_manifest.get("input_contract_id") not in {LIVE_INPUT_CONTRACT, HISTORICAL_INPUT}:
        raise ValueError("explicit live input/model compatibility contract required")


class LiveExecutionFeatures:
    """Single-owner adapter; callers serialize callbacks and decision timers.

    Times are actual UTC nanoseconds, never exchange timestamps relabelled as
    local readiness. Disconnect invalidates rolling coverage and the book;
    reconnect begins a fresh window, without REST backfill into past decisions.
    """

    def __init__(self, *, symbol, start_ns, allowed_lateness_ns, max_book_age_ns):
        if symbol != "BTCUSDC":
            raise ValueError("execution-only BTCUSDC contract required")
        self.symbol = symbol
        self.market_id = "binance-futures:" + symbol
        self.lateness = allowed_lateness_ns
        self.max_book_age = max_book_age_ns
        self.trades = LiveAggregateAdapter()
        self.individual_seen = OrderedDict()
        self.last_individual_id = None
        self.trade_source = None
        if type(start_ns) is not int or start_ns < 0:
            raise ValueError("nonnegative integer start clock required")
        self.clock = start_ns
        self.connected = False
        self.depth_id = None
        self.depth_content = None
        self.generation = 0
        self._reset(start_ns)

    def _reset(self, now_ns):
        self.generation += 1
        # Discard the incomplete starting second instead of claiming full
        # observed coverage for a connection established inside that second.
        start = (now_ns + 999_999_999) // 1_000_000_000 * 1_000_000_000
        self.windows = VisibleTradeWindows(
            start_ns=start, allowed_lateness_ns=self.lateness,
            market_id=self.market_id, coverage="observed" if self.connected else "unknown",
            input_contract_id=LIVE_INPUT_CONTRACT,
        )
        self.features = ExecutionFeatures(
            LIVE_INPUT_CONTRACT, market_id=self.market_id,
            max_book_age_ns=self.max_book_age,
        )
        self.depth_id = self.depth_content = None

    def connection(self, *, connected, now_ns):
        if type(now_ns) is not int or now_ns < self.clock:
            raise ValueError("live clock regressed")
        self.connected = bool(connected)
        self.clock = now_ns
        self._reset(now_ns)

    def _observation(self, event, *, receive_ns, ready_ns, payload, event_id):
        if event.get("s") != self.symbol:
            raise ValueError("wrong execution symbol")
        if not self.connected:
            raise ValueError("disconnected input cannot be admitted")
        if any(type(x) is not int or x < 0 for x in (event["T"], event["E"], receive_ns, ready_ns)):
            raise ValueError("integer exchange and local clocks required")
        source = event["T"] * 1_000_000
        published = event["E"] * 1_000_000
        if ready_ns < self.clock:
            raise ValueError("retroactive live readiness")
        if ready_ns - self.clock > 120_000_000_000:
            raise ValueError("unserviced live timer: connection/coverage reset required")
        return Observation(
            str(event_id), self.market_id, LIVE_INPUT_CONTRACT,
            "native_live_packet", source, published, receive_ns, ready_ns,
            payload, "observed",
        )

    def aggregate_trade(self, event, *, receive_ns, ready_ns):
        if self.trade_source not in (None, "aggregate"):
            raise ValueError("cannot mix individual and aggregate trade sources")
        if event.get("e") != "aggTrade" or type(event.get("m")) is not bool:
            raise ValueError("native aggregate trade packet required")
        # Validate causal clocks before mutating packet identity state.
        self._observation(event, receive_ns=receive_ns, ready_ns=ready_ns,
                          payload=None, event_id=event["a"])
        gaps_before = self.trades.id_gaps
        previous_packet = self.trades.last_packet
        trade = self.trades.adapt(
            packet_id=event["a"], first_id=event.get("f"), last_id=event.get("l"),
            exchange_ts_ns=int(event["T"]) * 1_000_000,
            price=event["p"], quantity=event["q"],
            side="sell" if event["m"] else "buy",
        )
        if trade is None:
            return False
        self.trade_source = "aggregate"
        if (self.trades.id_gaps != gaps_before
                or (previous_packet is not None and event["a"] != previous_packet + 1)):
            self._reset(ready_ns)
        observation = self._observation(event, receive_ns=receive_ns, ready_ns=ready_ns,
                                        payload=trade, event_id=event["a"])
        self._advance(ready_ns, trades=(observation,))
        return True

    def individual_trade(self, event, *, receive_ns, ready_ns):
        """Consume a real @trade message; never synthesize it from aggTrade."""
        if self.trade_source not in (None, "individual"):
            raise ValueError("cannot mix individual and aggregate trade sources")
        if (event.get("e") != "trade" or type(event.get("t")) is not int
                or event["t"] < 0 or type(event.get("m")) is not bool
                or "f" in event or "l" in event):
            raise ValueError("native individual trade identity required")
        identity = event["t"]
        self._observation(event, receive_ns=receive_ns, ready_ns=ready_ns,
                          payload=None, event_id=identity)
        contribution = TradeContribution(
            str(identity), event["T"] * 1_000_000,
            Decimal(event["p"]), Decimal(event["q"]),
            "sell" if event["m"] else "buy", 1, 1, "native_individual_trade", identity,
        )
        observation = self._observation(
            event, receive_ns=receive_ns, ready_ns=ready_ns,
            payload=contribution, event_id=identity,
        )
        content = (event["T"], contribution.price, contribution.quantity, contribution.side)
        if identity in self.individual_seen:
            if self.individual_seen[identity] != content:
                raise ValueError("conflicting individual trade identity")
            return False
        if self.last_individual_id is not None:
            if identity <= self.last_individual_id:
                raise ValueError("regressing or expired individual trade identity")
            if identity != self.last_individual_id + 1:
                self._reset(ready_ns)
        self._advance(ready_ns, trades=(observation,))
        self.individual_seen[identity] = content
        while len(self.individual_seen) > 10000:
            self.individual_seen.popitem(last=False)
        self.last_individual_id = identity
        self.trade_source = "individual"
        return True

    def partial_depth(self, event, *, receive_ns, ready_ns):
        if event.get("e") != "depthUpdate":
            raise ValueError("native partial depth packet required")
        version = event["u"]
        if type(version) is not int or version < 0:
            raise ValueError("invalid book update identity")
        bids = tuple((Decimal(p), Decimal(q)) for p, q in event["b"])
        asks = tuple((Decimal(p), Decimal(q)) for p, q in event["a"])
        if not bids or not asks or any(
            not p.is_finite() or not q.is_finite() or p <= 0 or q <= 0
            for p, q in bids + asks
        ):
            raise ValueError("invalid partial book levels")
        if (any(a[0] <= b[0] for a, b in zip(bids, bids[1:], strict=False))
                or any(a[0] >= b[0] for a, b in zip(asks, asks[1:], strict=False))
                or bids[0][0] >= asks[0][0]):
            raise ValueError("unsorted or crossed partial book")
        content = (event["T"], bids, asks)
        view = BookView(version, bids, asks, int(event["T"])*1000, None,
                        "live_partial_snapshot_no_state_change_claim", True)
        observation = self._observation(event, receive_ns=receive_ns, ready_ns=ready_ns,
                                        payload=view, event_id=f"depth:{version}")
        if self.depth_id is not None:
            if version == self.depth_id and content == self.depth_content:
                return False
            if version <= self.depth_id:
                raise ValueError("conflicting or regressing book identity")
        self._advance(ready_ns, books=(observation,))
        self.depth_id, self.depth_content = version, content
        return True

    def _advance(self, now_ns, *, trades=(), books=()):
        if type(now_ns) is not int or now_ns < self.clock:
            raise ValueError("live clock regressed")
        if now_ns - self.clock > 120_000_000_000:
            raise ValueError("unserviced live timer: connection/coverage reset required")
        bars = self.windows.advance(now_ns, trades)
        self.features.advance(now_ns, books, ((self.market_id, b) for b in bars))
        self.clock = now_ns

    def frame(self, now_ns):
        self._advance(now_ns)
        return self.features.frame(now_ns)
