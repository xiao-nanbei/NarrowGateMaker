"""Receive-time cross-venue flow state for shadow maker research.

This module deliberately uses local receive time and event windows.  It does
not feed the active quote policy.  Top-of-book changes provide an L1
OFI/depletion/refill proxy; they are not exact-L2 cancel attribution.
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from market_fusion import (
    BINANCE_VENUE,
    BITGET_VENUE,
    BYBIT_VENUE,
    OKX_VENUE,
    PERP_MARKET,
    SPOT_MARKET,
    market_key,
    normalize_symbol,
)

GLOBAL_FLOW_SCHEMA_VERSION = "global_flow.v1"
DEFAULT_FLOW_HORIZONS_MS = (10, 25, 50, 100, 250, 500)
EXTERNAL_VENUES = (BITGET_VENUE, BYBIT_VENUE, OKX_VENUE)


@dataclass(frozen=True)
class _BookEvent:
    receive_ns: int
    bid: float
    bid_size: float
    ask: float
    ask_size: float
    mid: float
    l1_ofi: float
    bid_depletion: float
    bid_refill: float
    ask_depletion: float
    ask_refill: float
    gap_flag: bool | None


@dataclass(frozen=True)
class _TradeEvent:
    receive_ns: int
    price: float
    size: float
    aggressor_side: str


@dataclass
class _MarketBuffer:
    books: deque[_BookEvent]
    trades: deque[_TradeEvent]
    last_book: _BookEvent | None = None
    last_receive_ns: int = 0
    out_of_order_events: int = 0
    stale_trade_events: int = 0


@dataclass(frozen=True)
class GlobalFlowState:
    schema_version: str
    as_of_receive_ts_ns: int
    windows: dict[int, dict[str, Any]]

    def window(self, horizon_ms: int) -> dict[str, Any]:
        return self.windows.get(int(horizon_ms), {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "as_of_receive_ts_ns": self.as_of_receive_ts_ns,
            "windows": {str(key): value for key, value in self.windows.items()},
        }


def _finite_median(values: Iterable[float]) -> float:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    return float(statistics.median(finite)) if finite else math.nan


def _direction_agreement(values: Iterable[float], *, epsilon: float = 1e-9) -> float:
    signs = [1 if value > epsilon else -1 for value in values if abs(value) > epsilon]
    if not signs:
        return 0.0
    positives = sum(value > 0 for value in signs)
    return max(positives, len(signs) - positives) / len(signs)


class GlobalFlowEngine:
    """Maintain event-driven BBO/trade flow over maker-relevant horizons."""

    def __init__(
        self,
        *,
        execution_symbol: str = "BTCUSDC",
        reference_symbol: str = "BTCUSDT",
        horizons_ms: Iterable[int] = DEFAULT_FLOW_HORIZONS_MS,
        retention_ms: int = 2_000,
        max_source_age_ms: float = 1_000.0,
        max_trade_event_age_ms: float = 1_000.0,
        native_backend: Any | None = None,
    ):
        self.execution_symbol = normalize_symbol(execution_symbol, "BTCUSDC")
        self.reference_symbol = normalize_symbol(reference_symbol, "BTCUSDT")
        self.horizons_ms = tuple(sorted({max(1, int(value)) for value in horizons_ms}))
        self.retention_ns = max(
            int(retention_ms * 1_000_000),
            max(self.horizons_ms, default=500) * 2_000_000,
        )
        self.max_source_age_ms = max(1.0, float(max_source_age_ms))
        self.max_trade_event_age_ms = max(1.0, float(max_trade_event_age_ms))
        self._native = native_backend
        self._markets: dict[str, _MarketBuffer] = {}

    @property
    def native_enabled(self) -> bool:
        return self._native is not None

    def set_symbols(self, *, execution_symbol: str, reference_symbol: str) -> None:
        execution = normalize_symbol(execution_symbol, "BTCUSDC")
        reference = normalize_symbol(reference_symbol, "BTCUSDT")
        if execution == self.execution_symbol and reference == self.reference_symbol:
            return
        self.execution_symbol = execution
        self.reference_symbol = reference
        if self._native is not None:
            self._native.clear()
        else:
            self._markets.clear()

    def on_book(
        self,
        market_id: str,
        *,
        receive_ts_ns: int,
        bid: float,
        bid_size: float,
        ask: float,
        ask_size: float,
        gap_flag: bool | None = None,
    ) -> bool:
        if self._native is not None:
            native_gap = -1 if gap_flag is None else int(bool(gap_flag))
            return bool(
                self._native.on_book(
                    str(market_id),
                    int(receive_ts_ns),
                    float(bid),
                    float(bid_size),
                    float(ask),
                    float(ask_size),
                    native_gap,
                )
            )
        receive_ns = int(receive_ts_ns)
        bid = float(bid)
        ask = float(ask)
        bid_size = max(0.0, float(bid_size))
        ask_size = max(0.0, float(ask_size))
        if receive_ns <= 0 or bid <= 0.0 or ask <= bid:
            return False

        buffer = self._buffer(market_id)
        if receive_ns < buffer.last_receive_ns:
            buffer.out_of_order_events += 1
            return False
        previous = buffer.last_book
        l1_ofi = 0.0
        bid_depletion = bid_refill = ask_depletion = ask_refill = 0.0
        if previous is not None:
            if bid > previous.bid:
                bid_component = bid_size
                bid_refill = bid_size
            elif bid < previous.bid:
                bid_component = -previous.bid_size
                bid_depletion = previous.bid_size
            else:
                bid_component = bid_size - previous.bid_size
                bid_depletion = max(0.0, previous.bid_size - bid_size)
                bid_refill = max(0.0, bid_size - previous.bid_size)

            if ask < previous.ask:
                ask_component = -ask_size
                ask_refill = ask_size
            elif ask > previous.ask:
                ask_component = previous.ask_size
                ask_depletion = previous.ask_size
            else:
                ask_component = previous.ask_size - ask_size
                ask_depletion = max(0.0, previous.ask_size - ask_size)
                ask_refill = max(0.0, ask_size - previous.ask_size)
            l1_ofi = bid_component + ask_component

        event = _BookEvent(
            receive_ns=receive_ns,
            bid=bid,
            bid_size=bid_size,
            ask=ask,
            ask_size=ask_size,
            mid=0.5 * (bid + ask),
            l1_ofi=l1_ofi,
            bid_depletion=bid_depletion,
            bid_refill=bid_refill,
            ask_depletion=ask_depletion,
            ask_refill=ask_refill,
            gap_flag=gap_flag,
        )
        buffer.books.append(event)
        buffer.last_book = event
        buffer.last_receive_ns = receive_ns
        self._prune(buffer, receive_ns)
        return True

    def on_trade(
        self,
        market_id: str,
        *,
        receive_ts_ns: int,
        price: float,
        size: float,
        aggressor_side: str,
        exchange_ts_ns: int = 0,
    ) -> bool:
        if self._native is not None:
            side = str(aggressor_side).strip().lower()
            if side not in {"buy", "sell"}:
                return False
            return bool(
                self._native.on_trade(
                    str(market_id),
                    int(receive_ts_ns),
                    int(exchange_ts_ns or 0),
                    float(price),
                    float(size),
                    side == "sell",
                )
            )
        receive_ns = int(receive_ts_ns)
        price = float(price)
        size = float(size)
        side = str(aggressor_side).strip().lower()
        if receive_ns <= 0 or price <= 0.0 or size <= 0.0 or side not in {"buy", "sell"}:
            return False
        buffer = self._buffer(market_id)
        exchange_ns = int(exchange_ts_ns or 0)
        if (
            exchange_ns > 0
            and receive_ns >= exchange_ns
            and receive_ns - exchange_ns
            > int(self.max_trade_event_age_ms * 1_000_000.0)
        ):
            buffer.stale_trade_events += 1
            return False
        if receive_ns < buffer.last_receive_ns:
            buffer.out_of_order_events += 1
            return False
        buffer.trades.append(_TradeEvent(receive_ns, price, size, side))
        buffer.last_receive_ns = receive_ns
        self._prune(buffer, receive_ns)
        return True

    def on_trade_batch(
        self,
        market_id: str,
        *,
        receive_ts_ns: int,
        exchange_ts_ns,
        prices,
        sizes,
        is_buyer_maker,
    ) -> int:
        """Consume one normalized venue frame without per-trade Python objects."""
        count = len(prices)
        if not (
            len(exchange_ts_ns) == count
            and len(sizes) == count
            and len(is_buyer_maker) == count
        ):
            raise ValueError("global-flow trade arrays must have equal length")
        if self._native is not None:
            return int(
                self._native.on_trade_batch(
                    str(market_id),
                    int(receive_ts_ns),
                    exchange_ts_ns,
                    prices,
                    sizes,
                    is_buyer_maker,
                )
            )
        accepted = 0
        for index in range(count):
            accepted += int(
                self.on_trade(
                    market_id,
                    receive_ts_ns=receive_ts_ns,
                    exchange_ts_ns=exchange_ts_ns[index],
                    price=prices[index],
                    size=sizes[index],
                    aggressor_side="sell" if is_buyer_maker[index] else "buy",
                )
            )
        return accepted

    def market_window(
        self,
        market_id: str,
        *,
        now_ns: int,
        horizon_ms: int,
    ) -> dict[str, Any]:
        if self._native is not None:
            return dict(
                self._native.market_window(
                    str(market_id), int(now_ns), int(horizon_ms)
                )
            )
        buffer = self._markets.get(market_id)
        if buffer is None:
            return self._empty_market_window(market_id, horizon_ms)
        now_ns = int(now_ns)
        cutoff_ns = now_ns - int(horizon_ms) * 1_000_000
        books = [event for event in buffer.books if cutoff_ns < event.receive_ns <= now_ns]
        trades = [event for event in buffer.trades if cutoff_ns < event.receive_ns <= now_ns]

        prior_book = None
        latest_book = None
        for event in buffer.books:
            if event.receive_ns <= cutoff_ns:
                prior_book = event
            if event.receive_ns <= now_ns:
                latest_book = event
            else:
                break
        if prior_book is None and books:
            prior_book = books[0]
        if latest_book is None and books:
            latest_book = books[-1]

        buy_volume = sum(event.size for event in trades if event.aggressor_side == "buy")
        sell_volume = sum(event.size for event in trades if event.aggressor_side == "sell")
        total_volume = buy_volume + sell_volume
        trade_imbalance = (
            (buy_volume - sell_volume) / total_volume if total_volume > 0.0 else 0.0
        )
        l1_ofi = sum(event.l1_ofi for event in books)
        average_top_depth = (
            sum(event.bid_size + event.ask_size for event in books) / len(books)
            if books
            else (
                latest_book.bid_size + latest_book.ask_size
                if latest_book is not None
                else 0.0
            )
        )
        l1_ofi_normalized = l1_ofi / max(average_top_depth, 1e-12)
        flow_pressure = 0.5 * trade_imbalance + 0.5 * math.tanh(l1_ofi_normalized)
        mid_move_bps = math.nan
        if prior_book is not None and latest_book is not None and prior_book.mid > 0.0:
            mid_move_bps = math.log(latest_book.mid / prior_book.mid) * 10_000.0
        book_age_ms = (
            (now_ns - latest_book.receive_ns) / 1_000_000.0
            if latest_book is not None
            else math.inf
        )
        latest_trade = None
        for event in buffer.trades:
            if event.receive_ns <= now_ns:
                latest_trade = event
            else:
                break
        trade_age_ms = (
            (now_ns - latest_trade.receive_ns) / 1_000_000.0
            if latest_trade is not None
            else math.inf
        )
        return {
            "market_id": market_id,
            "horizon_ms": int(horizon_ms),
            "book_events": len(books),
            "trade_events": len(trades),
            "book_age_ms": max(0.0, book_age_ms),
            "trade_age_ms": max(0.0, trade_age_ms),
            "book_fresh": int(book_age_ms <= self.max_source_age_ms),
            "aggressive_buy_volume": buy_volume,
            "aggressive_sell_volume": sell_volume,
            "trade_imbalance": trade_imbalance,
            "l1_ofi": l1_ofi,
            "l1_ofi_normalized": l1_ofi_normalized,
            "bid_depletion": sum(event.bid_depletion for event in books),
            "bid_refill": sum(event.bid_refill for event in books),
            "ask_depletion": sum(event.ask_depletion for event in books),
            "ask_refill": sum(event.ask_refill for event in books),
            "mid_move_bps": mid_move_bps,
            "flow_pressure": flow_pressure,
            "gap_events": sum(event.gap_flag is True for event in books),
            "gap_known_events": sum(event.gap_flag is not None for event in books),
            "out_of_order_events": buffer.out_of_order_events,
            "stale_trade_events": buffer.stale_trade_events,
            "book_overflow_events": 0,
            "trade_overflow_events": 0,
        }

    def backend_stats(self) -> dict[str, Any]:
        if self._native is not None:
            return {"native": 1, **dict(self._native.stats())}
        return {
            "native": 0,
            "market_count": len(self._markets),
            "out_of_order_events": sum(
                market.out_of_order_events for market in self._markets.values()
            ),
            "stale_trade_events": sum(
                market.stale_trade_events for market in self._markets.values()
            ),
            "book_overflow_events": 0,
            "trade_overflow_events": 0,
        }

    def snapshot(self, *, now_ns: int) -> GlobalFlowState:
        now_ns = int(now_ns)
        windows: dict[int, dict[str, Any]] = {}
        for horizon_ms in self.horizons_ms:
            spot = self._factor(SPOT_MARKET, now_ns=now_ns, horizon_ms=horizon_ms)
            perp = self._factor(PERP_MARKET, now_ns=now_ns, horizon_ms=horizon_ms)
            binance_bridge = self.market_window(
                market_key(BINANCE_VENUE, PERP_MARKET, self.reference_symbol),
                now_ns=now_ns,
                horizon_ms=horizon_ms,
            )
            execution = self.market_window(
                market_key(BINANCE_VENUE, PERP_MARKET, self.execution_symbol),
                now_ns=now_ns,
                horizon_ms=horizon_ms,
            )
            spot_move = (
                float(spot["mid_move_bps"]) if spot["valid"] else math.nan
            )
            perp_move = (
                float(perp["mid_move_bps"]) if perp["valid"] else math.nan
            )
            spot_pressure = (
                float(spot["flow_pressure"]) if spot["valid"] else math.nan
            )
            perp_pressure = (
                float(perp["flow_pressure"]) if perp["valid"] else math.nan
            )
            global_mid_move = _finite_median(
                [spot_move, perp_move]
            )
            global_pressure = _finite_median(
                [spot_pressure, perp_pressure]
            )
            bridge_move = float(binance_bridge["mid_move_bps"])
            execution_move = float(execution["mid_move_bps"])
            windows[horizon_ms] = {
                "horizon_ms": horizon_ms,
                "spot": spot,
                "perp": perp,
                "local_bridge": binance_bridge,
                "execution": execution,
                "global_mid_move_bps": global_mid_move,
                "global_flow_pressure": global_pressure,
                "perp_minus_spot_move_bps": (
                    perp_move - spot_move
                    if math.isfinite(perp_move)
                    and math.isfinite(spot_move)
                    else math.nan
                ),
                "local_bridge_move_bps": bridge_move,
                "execution_move_bps": execution_move,
                "global_minus_bridge_bps": (
                    global_mid_move - bridge_move
                    if math.isfinite(global_mid_move) and math.isfinite(bridge_move)
                    else math.nan
                ),
                "global_minus_execution_bps": (
                    global_mid_move - execution_move
                    if math.isfinite(global_mid_move) and math.isfinite(execution_move)
                    else math.nan
                ),
                "valid": int(spot["valid"] or perp["valid"]),
            }
        return GlobalFlowState(GLOBAL_FLOW_SCHEMA_VERSION, now_ns, windows)

    def _factor(self, market_type: str, *, now_ns: int, horizon_ms: int) -> dict[str, Any]:
        markets = [
            self.market_window(
                market_key(venue, market_type, self.reference_symbol),
                now_ns=now_ns,
                horizon_ms=horizon_ms,
            )
            for venue in EXTERNAL_VENUES
        ]
        fresh = [market for market in markets if market["book_fresh"]]
        moves = [market["mid_move_bps"] for market in fresh]
        pressures = [market["flow_pressure"] for market in fresh]
        median_move = _finite_median(moves)
        finite_moves = [float(value) for value in moves if math.isfinite(float(value))]
        dispersion = (
            _finite_median(abs(value - median_move) for value in finite_moves)
            if finite_moves and math.isfinite(median_move)
            else math.nan
        )
        return {
            "market_type": market_type,
            "valid": int(len(fresh) >= 2),
            "fresh_venues": len(fresh),
            "venue_agreement": _direction_agreement(pressures),
            "mid_move_bps": median_move,
            "dispersion_bps": dispersion,
            "flow_pressure": _finite_median(pressures),
            "trade_imbalance": _finite_median(
                market["trade_imbalance"] for market in fresh
            ),
            "l1_ofi_normalized": _finite_median(
                market["l1_ofi_normalized"] for market in fresh
            ),
            "aggressive_buy_volume": sum(
                market["aggressive_buy_volume"] for market in fresh
            ),
            "aggressive_sell_volume": sum(
                market["aggressive_sell_volume"] for market in fresh
            ),
            "bid_depletion": sum(market["bid_depletion"] for market in fresh),
            "bid_refill": sum(market["bid_refill"] for market in fresh),
            "ask_depletion": sum(market["ask_depletion"] for market in fresh),
            "ask_refill": sum(market["ask_refill"] for market in fresh),
            "markets": markets,
        }

    def _buffer(self, market_id: str) -> _MarketBuffer:
        key = str(market_id)
        buffer = self._markets.get(key)
        if buffer is None:
            buffer = _MarketBuffer(deque(), deque())
            self._markets[key] = buffer
        return buffer

    def _prune(self, buffer: _MarketBuffer, now_ns: int) -> None:
        cutoff = int(now_ns) - self.retention_ns
        # Keep one pre-window book anchor for return calculations.
        while len(buffer.books) > 1 and buffer.books[1].receive_ns < cutoff:
            buffer.books.popleft()
        while buffer.trades and buffer.trades[0].receive_ns < cutoff:
            buffer.trades.popleft()

    @staticmethod
    def _empty_market_window(market_id: str, horizon_ms: int) -> dict[str, Any]:
        return {
            "market_id": market_id,
            "horizon_ms": int(horizon_ms),
            "book_events": 0,
            "trade_events": 0,
            "book_age_ms": math.inf,
            "trade_age_ms": math.inf,
            "book_fresh": 0,
            "aggressive_buy_volume": 0.0,
            "aggressive_sell_volume": 0.0,
            "trade_imbalance": 0.0,
            "l1_ofi": 0.0,
            "l1_ofi_normalized": 0.0,
            "bid_depletion": 0.0,
            "bid_refill": 0.0,
            "ask_depletion": 0.0,
            "ask_refill": 0.0,
            "mid_move_bps": math.nan,
            "flow_pressure": 0.0,
            "gap_events": 0,
            "gap_known_events": 0,
            "out_of_order_events": 0,
            "stale_trade_events": 0,
            "book_overflow_events": 0,
            "trade_overflow_events": 0,
        }
