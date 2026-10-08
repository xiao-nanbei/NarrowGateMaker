"""Causal 100ms market-window extraction for cooldown-duration v2.

The extractor implements only formulas with an existing, auditable source
meaning.  It does not read rewards, choose predicates, or invent values for
deferred M2 channels.  Provider-local book time and Binance exchange trade
time remain separate identities; provider rows therefore cannot expose joint
trade/book state to an action-grade policy.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd

from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    IDENTITY,
    PRICE_TICK_SIZE_USDC_PER_BTC,
    TOP_K_DEPTH_LEVELS,
    CausalWindowObservation,
    FeatureContractError,
    fit_cumulative_depth_shape,
)

STRICT_EXCHANGE_TIME_PROFILE = "strict_native_exchange_time_joint"
PROVIDER_BOOK_PROFILE = "provider_local_receive_book_only"
SOURCE_CLOCK_PROFILES = (
    STRICT_EXCHANGE_TIME_PROFILE,
    PROVIDER_BOOK_PROFILE,
)

TRADE_CHANNELS = frozenset(
    {
        "aggressive_buy_qty_btc_per_s",
        "aggressive_sell_qty_btc_per_s",
        "signed_flow_imbalance",
        "trade_count_per_s",
        "buy_run_length",
        "sell_run_length",
        "last_aggressive_buy_age_s",
        "last_aggressive_sell_age_s",
    }
)
DEPTH_CHANNELS = frozenset(
    {
        "topk_bid_depth_btc",
        "topk_ask_depth_btc",
        "depth_imbalance",
        "bid_depth_slope_btc_per_tick",
        "ask_depth_slope_btc_per_tick",
        "bid_depth_convexity_btc_per_tick2",
        "ask_depth_convexity_btc_per_tick2",
        "topk_bid_displayed_depth_increase_btc_per_s",
        "topk_bid_displayed_depth_decrease_btc_per_s",
        "topk_ask_displayed_depth_increase_btc_per_s",
        "topk_ask_displayed_depth_decrease_btc_per_s",
    }
)
BBO_CHANNELS = frozenset(
    {
        "mid_usdc_per_btc",
        "spread_bps",
        "best_bid_qty_btc",
        "best_ask_qty_btc",
        "bbo_imbalance",
        "microprice_deviation_bps",
    }
)


class WindowExtractionError(FeatureContractError):
    """Raised when source arrays cannot satisfy the frozen window contract."""


@dataclass(frozen=True, slots=True)
class WindowExtractionContract:
    block: Literal["R0", "M1", "M2"]
    source_clock_profile: str
    left_ts_ns: int
    right_ts_ns: int
    top_k_depth_levels: int = TOP_K_DEPTH_LEVELS

    def __post_init__(self) -> None:
        if self.block not in CHANNELS_BY_BLOCK:
            raise WindowExtractionError(f"unsupported block: {self.block}")
        if self.source_clock_profile not in SOURCE_CLOCK_PROFILES:
            raise WindowExtractionError("source clock profile drifted")
        if self.left_ts_ns <= 0 or self.right_ts_ns <= self.left_ts_ns:
            raise WindowExtractionError("window extraction interval is invalid")
        if self.left_ts_ns % BASE_WINDOW_WIDTH_NS:
            raise WindowExtractionError("left boundary is not on the 100ms grid")
        if self.right_ts_ns % BASE_WINDOW_WIDTH_NS:
            raise WindowExtractionError("right boundary is not on the 100ms grid")
        if self.top_k_depth_levels != TOP_K_DEPTH_LEVELS:
            raise WindowExtractionError("top-k depth identity drifted")


@dataclass(frozen=True, slots=True)
class WindowExtractionStats:
    window_count: int
    missing_bbo_windows: int
    missing_depth_windows: int
    trade_windows_observed: int
    trade_windows_unobserved: int
    boundary_trade_exclusion_count: int
    economic_outcomes_read: bool = False


@dataclass(slots=True)
class WindowExtractionAccumulator:
    """Mutable audit carried beside the streaming extractor."""

    window_count: int = 0
    missing_bbo_windows: int = 0
    missing_depth_windows: int = 0
    trade_windows_observed: int = 0
    trade_windows_unobserved: int = 0
    boundary_trade_exclusion_count: int = 0

    def freeze(self) -> WindowExtractionStats:
        return WindowExtractionStats(
            window_count=int(self.window_count),
            missing_bbo_windows=int(self.missing_bbo_windows),
            missing_depth_windows=int(self.missing_depth_windows),
            trade_windows_observed=int(self.trade_windows_observed),
            trade_windows_unobserved=int(self.trade_windows_unobserved),
            boundary_trade_exclusion_count=int(
                self.boundary_trade_exclusion_count
            ),
            economic_outcomes_read=False,
        )


def _canonical_bucket_right_ms(values: np.ndarray, *, role: str) -> np.ndarray:
    """Map one last-event timestamp per normalized bucket to its right edge."""

    timestamps = np.asarray(values, dtype=np.int64)
    if timestamps.ndim != 1:
        raise WindowExtractionError(f"{role} timestamps must be one-dimensional")
    if timestamps.size and np.any(np.diff(timestamps) <= 0):
        raise WindowExtractionError(f"{role} timestamps must be unique and increasing")
    bucket_width_ms = BASE_WINDOW_WIDTH_NS // 1_000_000
    bucket_ids = timestamps // bucket_width_ms
    if bucket_ids.size and np.any(np.diff(bucket_ids) <= 0):
        raise WindowExtractionError(
            f"{role} has more than one normalized row in a 100ms bucket"
        )
    return (bucket_ids + 1) * bucket_width_ms


def _finite_positive(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _depth_shape(
    prices: np.ndarray,
    quantities: np.ndarray,
    *,
    side: Literal["bid", "ask"],
) -> tuple[float, float] | None:
    """Fit cumulative displayed depth against side distance in price ticks."""

    price = np.asarray(prices[:TOP_K_DEPTH_LEVELS], dtype=float)
    quantity = np.asarray(quantities[:TOP_K_DEPTH_LEVELS], dtype=float)
    if price.size != TOP_K_DEPTH_LEVELS or quantity.size != TOP_K_DEPTH_LEVELS:
        return None
    return fit_cumulative_depth_shape(
        price / PRICE_TICK_SIZE_USDC_PER_BTC,
        quantity,
        side=side,
    )


def _book_values(
    bbo: HistoricalBBOData,
    bbo_index: int | None,
    l2: HistoricalL2Data | None,
    l2_index: int | None,
) -> tuple[dict[str, float | None], bool, bool]:
    values: dict[str, float | None] = {name: None for name in BBO_CHANNELS | DEPTH_CHANNELS}
    bbo_missing = bbo_index is None
    depth_missing = l2 is None or l2_index is None
    if bbo_index is not None:
        bid = _finite_positive(bbo.best_bid[bbo_index])
        ask = _finite_positive(bbo.best_ask[bbo_index])
        bid_qty = _finite_positive(bbo.bid_qty[bbo_index])
        ask_qty = _finite_positive(bbo.ask_qty[bbo_index])
        if (
            bid is not None
            and ask is not None
            and bid_qty is not None
            and ask_qty is not None
            and bid < ask
        ):
            mid = 0.5 * (bid + ask)
            quantity_sum = bid_qty + ask_qty
            microprice = (ask * bid_qty + bid * ask_qty) / quantity_sum
            values.update(
                {
                    "mid_usdc_per_btc": mid,
                    "spread_bps": 10_000.0 * (ask - bid) / mid,
                    "best_bid_qty_btc": bid_qty,
                    "best_ask_qty_btc": ask_qty,
                    "bbo_imbalance": (bid_qty - ask_qty) / quantity_sum,
                    "microprice_deviation_bps": 10_000.0 * (microprice - mid) / mid,
                }
            )
            bbo_missing = False
        else:
            bbo_missing = True

    if l2 is not None and l2_index is not None:
        bid_px_row = np.asarray(l2.bid_px[l2_index], dtype=float)
        bid_qty_row = np.asarray(l2.bid_qty[l2_index], dtype=float)
        ask_px_row = np.asarray(l2.ask_px[l2_index], dtype=float)
        ask_qty_row = np.asarray(l2.ask_qty[l2_index], dtype=float)
        if (
            bid_px_row.ndim == 1
            and bid_qty_row.ndim == 1
            and ask_px_row.ndim == 1
            and ask_qty_row.ndim == 1
            and bid_px_row.size >= TOP_K_DEPTH_LEVELS
            and bid_qty_row.size >= TOP_K_DEPTH_LEVELS
            and ask_px_row.size >= TOP_K_DEPTH_LEVELS
            and ask_qty_row.size >= TOP_K_DEPTH_LEVELS
            and np.all(np.isfinite(bid_px_row[:TOP_K_DEPTH_LEVELS]))
            and np.all(np.isfinite(ask_px_row[:TOP_K_DEPTH_LEVELS]))
            and np.all(np.isfinite(bid_qty_row[:TOP_K_DEPTH_LEVELS]))
            and np.all(np.isfinite(ask_qty_row[:TOP_K_DEPTH_LEVELS]))
            and np.all(bid_px_row[:TOP_K_DEPTH_LEVELS] > 0.0)
            and np.all(ask_px_row[:TOP_K_DEPTH_LEVELS] > 0.0)
            and np.all(bid_qty_row[:TOP_K_DEPTH_LEVELS] >= 0.0)
            and np.all(ask_qty_row[:TOP_K_DEPTH_LEVELS] >= 0.0)
        ):
            bid_depth = float(np.sum(bid_qty_row[:TOP_K_DEPTH_LEVELS]))
            ask_depth = float(np.sum(ask_qty_row[:TOP_K_DEPTH_LEVELS]))
            total = bid_depth + ask_depth
            bid_shape = _depth_shape(bid_px_row, bid_qty_row, side="bid")
            ask_shape = _depth_shape(ask_px_row, ask_qty_row, side="ask")
            if total > 0.0 and bid_shape is not None and ask_shape is not None:
                values.update(
                    {
                        "topk_bid_depth_btc": bid_depth,
                        "topk_ask_depth_btc": ask_depth,
                        "depth_imbalance": (bid_depth - ask_depth) / total,
                        "bid_depth_slope_btc_per_tick": bid_shape[0],
                        "ask_depth_slope_btc_per_tick": ask_shape[0],
                        "bid_depth_convexity_btc_per_tick2": bid_shape[1],
                        "ask_depth_convexity_btc_per_tick2": ask_shape[1],
                    }
                )
                depth_missing = False
            else:
                depth_missing = True
        else:
            depth_missing = True
    return values, bbo_missing, depth_missing


def _normalize_trades(trades: pd.DataFrame | None) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if trades is None:
        return (
            np.empty(0, dtype=np.int64),
            np.empty(0, dtype=float),
            np.empty(0, dtype=np.int8),
        )
    if not isinstance(trades, pd.DataFrame):
        raise WindowExtractionError("trades must be a DataFrame")
    time_column = "transact_time" if "transact_time" in trades else "time"
    qty_column = "qty" if "qty" in trades else "quantity"
    required = {time_column, qty_column, "is_buyer_maker"}
    if not required.issubset(trades.columns):
        raise WindowExtractionError(f"trade schema is incomplete: {sorted(required)}")
    ts_ns = trades[time_column].to_numpy(dtype=np.int64, copy=True) * 1_000_000
    qty = trades[qty_column].to_numpy(dtype=float, copy=True)
    buyer_maker = trades["is_buyer_maker"].to_numpy(copy=True)
    if ts_ns.size and np.any(np.diff(ts_ns) < 0):
        raise WindowExtractionError("trade timestamps regressed")
    if np.any(~np.isfinite(qty)) or np.any(qty <= 0.0):
        raise WindowExtractionError("trade quantities are invalid")
    aggressor_buy = np.asarray([not bool(value) for value in buyer_maker], dtype=np.int8)
    return ts_ns, qty, aggressor_buy


def stream_causal_windows(
    *,
    contract: WindowExtractionContract,
    bbo: HistoricalBBOData,
    l2: HistoricalL2Data | None,
    trades: pd.DataFrame | None,
    audit: WindowExtractionAccumulator | None = None,
) -> Iterator[CausalWindowObservation]:
    """Yield completed windows without materializing a multi-day feature tape.

    Events exactly at a right boundary are excluded from that window and enter
    the next one.  Callers must consume the iterator before freezing ``audit``.
    Source arrays are copied only where the existing data ABI requires it.
    """

    bbo_ts_ms = _canonical_bucket_right_ms(np.asarray(bbo.ts_ms), role="BBO")
    l2_ts_ms = (
        _canonical_bucket_right_ms(np.asarray(l2.ts_ms), role="L2")
        if l2 is not None
        else np.empty(0, dtype=np.int64)
    )
    trade_ts_ns, trade_qty, trade_buy = _normalize_trades(trades)
    joint_trade_visible = contract.source_clock_profile == STRICT_EXCHANGE_TIME_PROFILE

    stats = audit if audit is not None else WindowExtractionAccumulator()
    previous_trade_side: int | None = None
    terminal_run = 0
    last_buy_ts_ns: int | None = None
    last_sell_ts_ns: int | None = None
    previous_depth: tuple[int, float, float] | None = None
    trade_start = int(
        np.searchsorted(trade_ts_ns, contract.left_ts_ns, side="left")
    )
    left_ms = contract.left_ts_ns // 1_000_000
    bbo_cursor = int(np.searchsorted(bbo_ts_ms, left_ms, side="right"))
    l2_cursor = int(np.searchsorted(l2_ts_ms, left_ms, side="right"))

    right_ns = contract.left_ts_ns + BASE_WINDOW_WIDTH_NS
    generation = 0
    while right_ns <= contract.right_ts_ns:
        left_ns = right_ns - BASE_WINDOW_WIDTH_NS
        right_ms = right_ns // 1_000_000
        while bbo_cursor < len(bbo_ts_ms) and int(bbo_ts_ms[bbo_cursor]) < right_ms:
            bbo_cursor += 1
        while l2_cursor < len(l2_ts_ms) and int(l2_ts_ms[l2_cursor]) < right_ms:
            l2_cursor += 1
        current_bbo_index = (
            bbo_cursor
            if bbo_cursor < len(bbo_ts_ms) and int(bbo_ts_ms[bbo_cursor]) == right_ms
            else None
        )
        current_l2_index = (
            l2_cursor
            if l2_cursor < len(l2_ts_ms) and int(l2_ts_ms[l2_cursor]) == right_ms
            else None
        )
        values = {spec.name: None for spec in CHANNELS_BY_BLOCK[contract.block]}
        book, bbo_bad, depth_bad = _book_values(
            bbo,
            current_bbo_index,
            l2,
            current_l2_index,
        )
        for name in values.keys() & book.keys():
            values[name] = book[name]
        stats.missing_bbo_windows += int(bbo_bad)
        if contract.block == "M2":
            stats.missing_depth_windows += int(depth_bad)
            if bbo_bad or depth_bad:
                previous_depth = None
            else:
                bid_depth = float(values["topk_bid_depth_btc"])
                ask_depth = float(values["topk_ask_depth_btc"])
                if previous_depth is not None:
                    previous_ts_ns, previous_bid_depth, previous_ask_depth = (
                        previous_depth
                    )
                    delta_s = (right_ns - previous_ts_ns) / 1_000_000_000.0
                    bid_delta = bid_depth - previous_bid_depth
                    ask_delta = ask_depth - previous_ask_depth
                    values.update(
                        {
                            "topk_bid_displayed_depth_increase_btc_per_s": (
                                max(bid_delta, 0.0) / delta_s
                            ),
                            "topk_bid_displayed_depth_decrease_btc_per_s": (
                                max(-bid_delta, 0.0) / delta_s
                            ),
                            "topk_ask_displayed_depth_increase_btc_per_s": (
                                max(ask_delta, 0.0) / delta_s
                            ),
                            "topk_ask_displayed_depth_decrease_btc_per_s": (
                                max(-ask_delta, 0.0) / delta_s
                            ),
                        }
                    )
                previous_depth = (right_ns, bid_depth, ask_depth)
            if joint_trade_visible:
                start = trade_start
                stop = int(np.searchsorted(trade_ts_ns, right_ns, side="left"))
                stats.boundary_trade_exclusion_count += int(
                    np.searchsorted(trade_ts_ns, right_ns, side="right") - stop
                )
                buy_qty = 0.0
                sell_qty = 0.0
                for index in range(start, stop):
                    side = int(trade_buy[index])
                    if side:
                        buy_qty += float(trade_qty[index])
                        last_buy_ts_ns = int(trade_ts_ns[index])
                    else:
                        sell_qty += float(trade_qty[index])
                        last_sell_ts_ns = int(trade_ts_ns[index])
                    terminal_run = terminal_run + 1 if previous_trade_side == side else 1
                    previous_trade_side = side
                total_qty = buy_qty + sell_qty
                values.update(
                    {
                        "aggressive_buy_qty_btc_per_s": buy_qty * 10.0,
                        "aggressive_sell_qty_btc_per_s": sell_qty * 10.0,
                        "signed_flow_imbalance": (
                            (buy_qty - sell_qty) / total_qty if total_qty > 0.0 else 0.0
                        ),
                        "trade_count_per_s": float(stop - start) * 10.0,
                        "buy_run_length": float(terminal_run if previous_trade_side == 1 else 0),
                        "sell_run_length": float(terminal_run if previous_trade_side == 0 else 0),
                        "last_aggressive_buy_age_s": (
                            None
                            if last_buy_ts_ns is None
                            else (right_ns - last_buy_ts_ns) / 1_000_000_000.0
                        ),
                        "last_aggressive_sell_age_s": (
                            None
                            if last_sell_ts_ns is None
                            else (right_ns - last_sell_ts_ns) / 1_000_000_000.0
                        ),
                    }
                )
                stats.trade_windows_observed += 1
                trade_start = stop
            else:
                stats.trade_windows_unobserved += 1
        generation += 1
        stats.window_count += 1
        yield CausalWindowObservation(
            left_ts_ns=left_ns,
            right_ts_ns=right_ns,
            feature_ready_ts_ns=right_ns,
            market_generation=generation,
            depth_generation=generation,
            values=values,
            source_gap=bool(
                bbo_bad or (contract.block == "M2" and depth_bad)
            ),
        )
        right_ns += BASE_WINDOW_WIDTH_NS


def iter_causal_windows(
    *,
    contract: WindowExtractionContract,
    bbo: HistoricalBBOData,
    l2: HistoricalL2Data | None,
    trades: pd.DataFrame | None,
) -> tuple[Iterator[CausalWindowObservation], WindowExtractionStats]:
    """Materialize a bounded window panel for tests and small audits.

    Full D-1/D/D+1 execution must use :func:`stream_causal_windows`; this
    compatibility API intentionally preserves the original immutable-stats
    return value for narrow callers.
    """

    audit = WindowExtractionAccumulator()
    rows = tuple(
        stream_causal_windows(
            contract=contract,
            bbo=bbo,
            l2=l2,
            trades=trades,
            audit=audit,
        )
    )
    return iter(rows), audit.freeze()


def window_formula_contract() -> dict[str, Any]:
    return {
        "identity": IDENTITY,
        "base_window": "[b-100ms,b)",
        "normalized_book_timestamp_semantics": (
            "last accepted source event in bucket mapped to canonical right edge"
        ),
        "event_at_right_boundary": "excluded",
        "top_k_depth_levels": TOP_K_DEPTH_LEVELS,
        "price_tick_size_usdc_per_btc": PRICE_TICK_SIZE_USDC_PER_BTC,
        "depth_shape_formula": (
            "x_i is side distance from best level in 0.1-USDC ticks; C_i is "
            "cumulative displayed BTC through level i; OLS fits "
            "C_i=beta0+beta1*x_i+0.5*gamma*x_i^2"
        ),
        "depth_slope_unit": "BTC_per_tick",
        "depth_convexity_unit": "BTC_per_tick2",
        "displayed_depth_change_formula": (
            "increase=max(D_t-D_prev,0)/dt; "
            "decrease=max(D_prev-D_t,0)/dt"
        ),
        "displayed_depth_change_unit": "BTC_per_s",
        "displayed_depth_change_requires_consecutive_complete_buckets": True,
        "displayed_depth_change_is_exact_depletion_refill": False,
        "strict_exchange_time_joint_trade_book": True,
        "provider_local_book_official_trade_joint_visibility": False,
        "active_channel_names": {
            block: [spec.name for spec in CHANNELS_BY_BLOCK[block]]
            for block in ("R0", "M1", "M2")
        },
        "economic_outcomes_read": False,
    }


__all__ = [
    "PROVIDER_BOOK_PROFILE",
    "SOURCE_CLOCK_PROFILES",
    "STRICT_EXCHANGE_TIME_PROFILE",
    "WindowExtractionContract",
    "WindowExtractionError",
    "WindowExtractionAccumulator",
    "WindowExtractionStats",
    "iter_causal_windows",
    "stream_causal_windows",
    "window_formula_contract",
]
