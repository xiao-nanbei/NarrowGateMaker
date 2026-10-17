"""Outcome-blind raw-native book features for cooldown-duration v2.

The stream reuses :class:`CryptoHFTExchangeBookTape` and
:class:`HistoricalExchangeBookScheduler`; it does not parse source Parquet or
reconstruct sequence state itself.  It emits only completed 100ms
``[left, right)`` windows.  Displayed quantity decreases/increases describe
public level updates and must not be interpreted as cancels, queue depletion,
or refill ownership.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol

import numpy as np
import pandas as pd

from models.exchange_book_replay import (
    CryptoHFTExchangeBookTape,
    ExchangeBookLevelChange,
    HistoricalExchangeBookScheduler,
)
from models.tick_data_types import HistoricalExchangeBookEvent
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

SCHEMA_VERSION = f"{IDENTITY}.raw_native_m2_book_window.v1"
WARMUP_HOURS = 24
DAY_NS = 86_400_000_000_000
EXCHANGE_WINDOW_READY_CLOCK = "exchange_window_right"
LOCAL_RECEIVE_READY_CLOCK = "max_right_edge_and_source_receive"
FEATURE_READY_CLOCKS = (EXCHANGE_WINDOW_READY_CLOCK, LOCAL_RECEIVE_READY_CLOCK)
DISPLAYED_CHANGE_SEMANTICS = (
    "exact public level quantity change; not cancel attribution, trade "
    "attribution, queue-ahead change, or liquidity-owner refill"
)
EXACT_DISPLAYED_RATE_CHANNELS = (
    "bid_exact_level_displayed_depletion_btc_per_s",
    "bid_exact_level_displayed_refill_btc_per_s",
    "ask_exact_level_displayed_depletion_btc_per_s",
    "ask_exact_level_displayed_refill_btc_per_s",
)

_BOOK_TO_M2_CHANNEL = {
    "mid_usdc_per_btc": "mid_usdc_per_btc",
    "spread_bps": "spread_bps",
    "best_bid_qty_btc": "best_bid_qty_btc",
    "best_ask_qty_btc": "best_ask_qty_btc",
    "bbo_imbalance": "bbo_imbalance",
    "microprice_deviation_bps": "microprice_deviation_bps",
    "topk_bid_depth_btc": "top20_bid_depth_btc",
    "topk_ask_depth_btc": "top20_ask_depth_btc",
    "depth_imbalance": "depth_imbalance",
    "bid_depth_slope_btc_per_tick": "bid_depth_slope_btc_per_tick",
    "ask_depth_slope_btc_per_tick": "ask_depth_slope_btc_per_tick",
    "bid_depth_convexity_btc_per_tick2": ("bid_depth_convexity_btc_per_tick2"),
    "ask_depth_convexity_btc_per_tick2": ("ask_depth_convexity_btc_per_tick2"),
    "topk_bid_displayed_depth_increase_btc_per_s": (
        "top20_bid_boundary_displayed_increase_btc_per_s"
    ),
    "topk_bid_displayed_depth_decrease_btc_per_s": (
        "top20_bid_boundary_displayed_decrease_btc_per_s"
    ),
    "topk_ask_displayed_depth_increase_btc_per_s": (
        "top20_ask_boundary_displayed_increase_btc_per_s"
    ),
    "topk_ask_displayed_depth_decrease_btc_per_s": (
        "top20_ask_boundary_displayed_decrease_btc_per_s"
    ),
    "bid_exact_level_displayed_depletion_btc_per_s": (
        "bid_exact_level_displayed_depletion_btc_per_s"
    ),
    "bid_exact_level_displayed_refill_btc_per_s": ("bid_exact_level_displayed_refill_btc_per_s"),
    "ask_exact_level_displayed_depletion_btc_per_s": (
        "ask_exact_level_displayed_depletion_btc_per_s"
    ),
    "ask_exact_level_displayed_refill_btc_per_s": ("ask_exact_level_displayed_refill_btc_per_s"),
}

_TRADE_M2_CHANNELS = (
    "aggressive_buy_qty_btc_per_s",
    "aggressive_sell_qty_btc_per_s",
    "signed_flow_imbalance",
    "trade_count_per_s",
    "buy_run_length",
    "sell_run_length",
    "last_aggressive_buy_age_s",
    "last_aggressive_sell_age_s",
)


class NativeFeatureError(FeatureContractError):
    """Raised when raw-native causal feature support is invalid."""


class NativeBookTape(Protocol):
    """Structural interface supplied by ``CryptoHFTExchangeBookTape``."""

    day_start_ns: int
    day_end_ns: int
    process_start_ns: int
    warmup_hours: int
    strict_complete: bool
    missing_paths: tuple[Path, ...]
    tick_size: float

    def __iter__(self) -> Iterator[HistoricalExchangeBookEvent]: ...


@dataclass(frozen=True, slots=True)
class NativeM2BookWindowContract:
    """Frozen raw-native window and source-clock semantics."""

    window_start_ns: int
    window_end_ns: int
    policy_start_ns: int
    window_width_ns: int = BASE_WINDOW_WIDTH_NS
    warmup_hours: int = WARMUP_HOURS
    top_depth_levels: int = TOP_K_DEPTH_LEVELS
    tick_size: float = PRICE_TICK_SIZE_USDC_PER_BTC
    require_receive_clock: bool = True
    max_source_silence_ns: int | None = None
    boundary: str = "left_closed_right_open"
    partial_window_policy: str = "exclude"
    feature_ready_clock: str = LOCAL_RECEIVE_READY_CLOCK

    def __post_init__(self) -> None:
        if self.window_start_ns <= 0 or self.window_end_ns <= self.window_start_ns:
            raise NativeFeatureError("native feature interval is invalid")
        if self.window_width_ns != BASE_WINDOW_WIDTH_NS:
            raise NativeFeatureError("raw-native v2 windows must remain 100ms")
        if self.window_start_ns % self.window_width_ns:
            raise NativeFeatureError("window_start_ns is not grid aligned")
        if self.policy_start_ns % self.window_width_ns:
            raise NativeFeatureError("policy_start_ns is not grid aligned")
        if self.window_start_ns > self.policy_start_ns:
            raise NativeFeatureError("window_start_ns cannot skip the D-1 side of policy_start_ns")
        if self.warmup_hours != WARMUP_HOURS:
            raise NativeFeatureError("raw-native v2 requires natural D-1 warmup")
        if self.top_depth_levels != TOP_K_DEPTH_LEVELS:
            raise NativeFeatureError("raw-native v2 requires top-20 depth")
        if not math.isfinite(self.tick_size) or self.tick_size <= 0.0:
            raise NativeFeatureError("tick_size must be positive and finite")
        if self.boundary != "left_closed_right_open":
            raise NativeFeatureError("window boundary semantics drifted")
        if self.partial_window_policy != "exclude":
            raise NativeFeatureError("partial windows must be excluded")
        if self.feature_ready_clock not in FEATURE_READY_CLOCKS:
            raise NativeFeatureError("feature-ready clock semantics drifted")
        if self.max_source_silence_ns is not None:
            if self.max_source_silence_ns <= 0:
                raise NativeFeatureError("max_source_silence_ns must be positive when supplied")


@dataclass(frozen=True, slots=True)
class DisplayedLevelChange:
    """One exact public price-level update inside a completed window."""

    exchange_ts_ns: int
    receive_ts_ns: int
    side: Literal["bid", "ask"]
    price_tick: int
    price_usdc_per_btc: float
    quantity_before_btc: float
    quantity_after_btc: float
    displayed_depletion_btc: float
    displayed_refill_btc: float
    segment_id: int
    update_id: int | None
    semantics: str = DISPLAYED_CHANGE_SEMANTICS


@dataclass(frozen=True, slots=True)
class TargetPriceDisplayedQuantity:
    """Displayed quantity lookup at the latest completed causal window."""

    side: Literal["bid", "ask"]
    order_price_tick: int
    order_price_usdc_per_btc: float
    status: Literal["exact", "known_zero", "unknown"]
    known: bool
    displayed_quantity_btc: float | None
    reason: str
    asof_window_right_ts_ns: int
    asof_exchange_ts_ns: int
    feature_ready_ts_ns: int
    segment_id: int
    displayed_quantity_is_queue_ahead: bool = False


@dataclass(frozen=True, slots=True)
class NativeM2BookWindow:
    """One causal completed-window raw-native book observation."""

    left_ts_ns: int
    right_ts_ns: int
    feature_ready_ts_ns: int
    market_generation: int
    depth_generation: int
    phase: Literal["D_MINUS_1_WARMUP", "POLICY"]
    policy_start_ns: int
    warmup_admitted: bool
    support_valid: bool
    support_state: Literal["OBSERVED", "UNOBSERVED"]
    unobserved_reasons: tuple[str, ...]
    values: Mapping[str, float | int | None]
    level_changes: tuple[DisplayedLevelChange, ...]
    source_event_count: int
    accepted_event_count: int
    rejected_event_count: int
    source_gap_count: int
    sequence_gap_count: int
    invalid_sequence_message_count: int
    message_time_reversal_count: int
    snapshot_reset: bool
    segment_id: int
    last_source_exchange_ts_ns: int
    last_source_receive_ts_ns: int
    source_exchange_age_ns: int | None
    source_receive_age_ns: int | None
    source_silence_limit_ns: int | None
    source_stale: bool
    receive_clock_valid: bool
    economic_outcomes_read: bool = False


@dataclass(frozen=True, slots=True)
class NativeM2BookFeatureAudit:
    schema_version: str
    window_count: int
    observed_window_count: int
    unobserved_window_count: int
    unobserved_reason_counts: Mapping[str, int]
    partial_trailing_window_excluded: bool
    partial_trailing_window_ns: int
    warmup_admitted: bool
    warmup_admission_finalized: bool
    warmup_expected_window_count: int
    warmup_window_count: int
    warmup_observed_window_count: int
    warmup_unobserved_window_count: int
    warmup_source_event_count: int
    warmup_source_gap_count: int
    warmup_sequence_gap_count: int
    warmup_invalid_sequence_message_count: int
    warmup_message_time_reversal_count: int
    warmup_missing_receive_timestamp_count: int
    warmup_receive_before_exchange_event_count: int
    source_event_count: int
    accepted_event_count: int
    rejected_event_count: int
    source_gap_count: int
    sequence_gap_count: int
    invalid_sequence_message_count: int
    message_time_reversal_count: int
    snapshot_reset_window_count: int
    missing_receive_clock_window_count: int
    receive_before_exchange_event_count: int
    exact_level_change_count: int
    bid_displayed_depletion_btc: float
    bid_displayed_refill_btc: float
    ask_displayed_depletion_btc: float
    ask_displayed_refill_btc: float
    max_source_exchange_age_ns: int
    max_source_receive_age_ns: int
    last_source_exchange_ts_ns: int
    last_source_receive_ts_ns: int
    economic_outcomes_read: bool = False


@dataclass(slots=True)
class NativeM2BookFeatureAccumulator:
    """Mutable online counters; no window rows or full-depth states are kept."""

    window_count: int = 0
    observed_window_count: int = 0
    unobserved_window_count: int = 0
    partial_trailing_window_excluded: bool = False
    partial_trailing_window_ns: int = 0
    warmup_admitted: bool = False
    warmup_admission_finalized: bool = False
    warmup_expected_window_count: int = 0
    warmup_window_count: int = 0
    warmup_observed_window_count: int = 0
    warmup_unobserved_window_count: int = 0
    warmup_source_event_count: int = 0
    warmup_source_gap_count: int = 0
    warmup_sequence_gap_count: int = 0
    warmup_invalid_sequence_message_count: int = 0
    warmup_message_time_reversal_count: int = 0
    warmup_missing_receive_timestamp_count: int = 0
    warmup_receive_before_exchange_event_count: int = 0
    source_event_count: int = 0
    accepted_event_count: int = 0
    rejected_event_count: int = 0
    source_gap_count: int = 0
    sequence_gap_count: int = 0
    invalid_sequence_message_count: int = 0
    message_time_reversal_count: int = 0
    snapshot_reset_window_count: int = 0
    missing_receive_clock_window_count: int = 0
    receive_before_exchange_event_count: int = 0
    exact_level_change_count: int = 0
    bid_displayed_depletion_btc: float = 0.0
    bid_displayed_refill_btc: float = 0.0
    ask_displayed_depletion_btc: float = 0.0
    ask_displayed_refill_btc: float = 0.0
    max_source_exchange_age_ns: int = 0
    max_source_receive_age_ns: int = 0
    last_source_exchange_ts_ns: int = 0
    last_source_receive_ts_ns: int = 0
    unobserved_reason_counts: Counter[str] | None = None

    def __post_init__(self) -> None:
        if self.unobserved_reason_counts is None:
            self.unobserved_reason_counts = Counter()

    def freeze(self) -> NativeM2BookFeatureAudit:
        return NativeM2BookFeatureAudit(
            schema_version=SCHEMA_VERSION,
            window_count=int(self.window_count),
            observed_window_count=int(self.observed_window_count),
            unobserved_window_count=int(self.unobserved_window_count),
            unobserved_reason_counts=MappingProxyType(
                dict(sorted((self.unobserved_reason_counts or {}).items()))
            ),
            partial_trailing_window_excluded=bool(self.partial_trailing_window_excluded),
            partial_trailing_window_ns=int(self.partial_trailing_window_ns),
            warmup_admitted=bool(self.warmup_admitted),
            warmup_admission_finalized=bool(self.warmup_admission_finalized),
            warmup_expected_window_count=int(self.warmup_expected_window_count),
            warmup_window_count=int(self.warmup_window_count),
            warmup_observed_window_count=int(self.warmup_observed_window_count),
            warmup_unobserved_window_count=int(self.warmup_unobserved_window_count),
            warmup_source_event_count=int(self.warmup_source_event_count),
            warmup_source_gap_count=int(self.warmup_source_gap_count),
            warmup_sequence_gap_count=int(self.warmup_sequence_gap_count),
            warmup_invalid_sequence_message_count=int(self.warmup_invalid_sequence_message_count),
            warmup_message_time_reversal_count=int(self.warmup_message_time_reversal_count),
            warmup_missing_receive_timestamp_count=int(self.warmup_missing_receive_timestamp_count),
            warmup_receive_before_exchange_event_count=int(
                self.warmup_receive_before_exchange_event_count
            ),
            source_event_count=int(self.source_event_count),
            accepted_event_count=int(self.accepted_event_count),
            rejected_event_count=int(self.rejected_event_count),
            source_gap_count=int(self.source_gap_count),
            sequence_gap_count=int(self.sequence_gap_count),
            invalid_sequence_message_count=int(self.invalid_sequence_message_count),
            message_time_reversal_count=int(self.message_time_reversal_count),
            snapshot_reset_window_count=int(self.snapshot_reset_window_count),
            missing_receive_clock_window_count=int(self.missing_receive_clock_window_count),
            receive_before_exchange_event_count=int(self.receive_before_exchange_event_count),
            exact_level_change_count=int(self.exact_level_change_count),
            bid_displayed_depletion_btc=float(self.bid_displayed_depletion_btc),
            bid_displayed_refill_btc=float(self.bid_displayed_refill_btc),
            ask_displayed_depletion_btc=float(self.ask_displayed_depletion_btc),
            ask_displayed_refill_btc=float(self.ask_displayed_refill_btc),
            max_source_exchange_age_ns=int(self.max_source_exchange_age_ns),
            max_source_receive_age_ns=int(self.max_source_receive_age_ns),
            last_source_exchange_ts_ns=int(self.last_source_exchange_ts_ns),
            last_source_receive_ts_ns=int(self.last_source_receive_ts_ns),
            economic_outcomes_read=False,
        )


@dataclass(frozen=True, slots=True)
class NativeM2TradeMergeAudit:
    schema_version: str
    window_count: int
    warmup_window_count: int
    policy_window_count: int
    source_unobserved_window_count: int
    official_trade_count: int
    aggressive_buy_trade_count: int
    aggressive_sell_trade_count: int
    right_boundary_exclusion_count: int
    first_window_left_ts_ns: int
    last_window_right_ts_ns: int
    economic_outcomes_read: bool = False


@dataclass(slots=True)
class NativeM2TradeMergeAccumulator:
    """Online audit for outcome-blind official-trade/window merging."""

    window_count: int = 0
    warmup_window_count: int = 0
    policy_window_count: int = 0
    source_unobserved_window_count: int = 0
    official_trade_count: int = 0
    aggressive_buy_trade_count: int = 0
    aggressive_sell_trade_count: int = 0
    right_boundary_exclusion_count: int = 0
    first_window_left_ts_ns: int = 0
    last_window_right_ts_ns: int = 0

    def freeze(self) -> NativeM2TradeMergeAudit:
        return NativeM2TradeMergeAudit(
            schema_version=f"{SCHEMA_VERSION}.official_trade_merge.v1",
            window_count=int(self.window_count),
            warmup_window_count=int(self.warmup_window_count),
            policy_window_count=int(self.policy_window_count),
            source_unobserved_window_count=int(self.source_unobserved_window_count),
            official_trade_count=int(self.official_trade_count),
            aggressive_buy_trade_count=int(self.aggressive_buy_trade_count),
            aggressive_sell_trade_count=int(self.aggressive_sell_trade_count),
            right_boundary_exclusion_count=int(self.right_boundary_exclusion_count),
            first_window_left_ts_ns=int(self.first_window_left_ts_ns),
            last_window_right_ts_ns=int(self.last_window_right_ts_ns),
            economic_outcomes_read=False,
        )


@dataclass(frozen=True, slots=True)
class _NormalizedOfficialTrades:
    exchange_ts_ns: np.ndarray
    quantity_btc: np.ndarray
    aggressive_buy: np.ndarray


def native_m2_observation_channel_names() -> tuple[str, ...]:
    """Return the exact outcome-blind M2 observation name universe."""

    names = [spec.name for spec in CHANNELS_BY_BLOCK["M2"]]
    for name in EXACT_DISPLAYED_RATE_CHANNELS:
        if name not in names:
            names.append(name)
    return tuple(names)


def _strict_buyer_maker(value: object) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        parsed = int(value)
        if parsed in {0, 1}:
            return bool(parsed)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    raise NativeFeatureError("is_buyer_maker must contain only explicit bool/0/1 values")


def _normalize_official_individual_trades(
    trades: pd.DataFrame,
) -> _NormalizedOfficialTrades:
    if not isinstance(trades, pd.DataFrame):
        raise NativeFeatureError("official_trades must be a pandas DataFrame")
    time_name = "transact_time" if "transact_time" in trades else "time"
    quantity_name = "qty" if "qty" in trades else "quantity"
    required = {time_name, quantity_name, "is_buyer_maker"}
    if not required.issubset(trades.columns):
        raise NativeFeatureError(
            f"official individual trade schema is incomplete: {sorted(required)}"
        )
    if trades.empty:
        return _NormalizedOfficialTrades(
            exchange_ts_ns=np.empty(0, dtype=np.int64),
            quantity_btc=np.empty(0, dtype=float),
            aggressive_buy=np.empty(0, dtype=np.bool_),
        )

    timestamp_ms = pd.to_numeric(trades[time_name], errors="coerce").to_numpy(
        dtype=float, copy=True
    )
    quantity = pd.to_numeric(trades[quantity_name], errors="coerce").to_numpy(
        dtype=float, copy=True
    )
    if (
        np.any(~np.isfinite(timestamp_ms))
        or np.any(timestamp_ms <= 0.0)
        or np.any(timestamp_ms != np.floor(timestamp_ms))
        or np.any(timestamp_ms > (np.iinfo(np.int64).max // 1_000_000))
    ):
        raise NativeFeatureError("official individual trade timestamps must be positive integer ms")
    if np.any(~np.isfinite(quantity)) or np.any(quantity <= 0.0):
        raise NativeFeatureError("official individual trade quantities must be positive and finite")
    exchange_ts_ns = timestamp_ms.astype(np.int64) * 1_000_000
    if np.any(np.diff(exchange_ts_ns) < 0):
        raise NativeFeatureError("official individual trade exchange timestamps regressed")
    buyer_maker = np.asarray(
        [_strict_buyer_maker(value) for value in trades["is_buyer_maker"]],
        dtype=np.bool_,
    )
    return _NormalizedOfficialTrades(
        exchange_ts_ns=exchange_ts_ns,
        quantity_btc=quantity,
        aggressive_buy=np.logical_not(buyer_maker),
    )


def stream_native_m2_causal_observations(
    *,
    book_windows: Iterable[NativeM2BookWindow],
    official_trades: pd.DataFrame,
    audit: NativeM2TradeMergeAccumulator | None = None,
) -> Iterator[CausalWindowObservation]:
    """Merge raw-native book windows with official individual trades.

    Trade timestamps use the official exchange ``transact_time`` in integer
    milliseconds.  Each trade is admitted to the 100ms window satisfying
    ``left <= transact_time < right``.  The merge has no fill, reward, PnL, or
    assignment-price input and does not claim receive-time transport authority
    for the official trade source.
    """

    normalized = _normalize_official_individual_trades(official_trades)
    stats = audit if audit is not None else NativeM2TradeMergeAccumulator()
    channel_names = native_m2_observation_channel_names()
    channel_name_set = frozenset(channel_names)
    book_name_set = frozenset(_BOOK_TO_M2_CHANNEL)
    trade_name_set = frozenset(_TRADE_M2_CHANNELS)
    if book_name_set | trade_name_set != channel_name_set:
        missing = sorted(channel_name_set - book_name_set - trade_name_set)
        extra = sorted((book_name_set | trade_name_set) - channel_name_set)
        raise NativeFeatureError(
            f"native M2 merge schema drifted: missing={missing}, extra={extra}"
        )

    previous_right_ns: int | None = None
    policy_start_ns: int | None = None
    trade_cursor = 0
    previous_trade_side: bool | None = None
    terminal_run = 0
    last_buy_ts_ns: int | None = None
    last_sell_ts_ns: int | None = None

    for book in book_windows:
        if book.right_ts_ns - book.left_ts_ns != BASE_WINDOW_WIDTH_NS:
            raise NativeFeatureError("book merge received a non-100ms window")
        if previous_right_ns is not None and book.left_ts_ns != previous_right_ns:
            raise NativeFeatureError("book windows must be contiguous and strictly ordered")
        if policy_start_ns is None:
            policy_start_ns = int(book.policy_start_ns)
            trade_cursor = int(
                np.searchsorted(
                    normalized.exchange_ts_ns,
                    book.left_ts_ns,
                    side="left",
                )
            )
        elif int(book.policy_start_ns) != policy_start_ns:
            raise NativeFeatureError("book policy_start_ns changed mid-stream")

        stop = int(
            np.searchsorted(
                normalized.exchange_ts_ns,
                book.right_ts_ns,
                side="left",
            )
        )
        boundary_stop = int(
            np.searchsorted(
                normalized.exchange_ts_ns,
                book.right_ts_ns,
                side="right",
            )
        )
        if stop < trade_cursor:
            raise NativeFeatureError("official trade cursor regressed")
        stats.right_boundary_exclusion_count += boundary_stop - stop

        buy_quantity = 0.0
        sell_quantity = 0.0
        for index in range(trade_cursor, stop):
            exchange_ts_ns = int(normalized.exchange_ts_ns[index])
            aggressive_buy = bool(normalized.aggressive_buy[index])
            quantity = float(normalized.quantity_btc[index])
            if aggressive_buy:
                buy_quantity += quantity
                last_buy_ts_ns = exchange_ts_ns
                stats.aggressive_buy_trade_count += 1
            else:
                sell_quantity += quantity
                last_sell_ts_ns = exchange_ts_ns
                stats.aggressive_sell_trade_count += 1
            terminal_run = terminal_run + 1 if previous_trade_side is aggressive_buy else 1
            previous_trade_side = aggressive_buy
        trade_count = stop - trade_cursor
        stats.official_trade_count += trade_count
        trade_cursor = stop

        values: dict[str, float | None] = {name: None for name in channel_names}
        if book.support_valid:
            for output_name, raw_name in _BOOK_TO_M2_CHANNEL.items():
                raw_value = book.values.get(raw_name)
                if raw_value is None:
                    values[output_name] = None
                    continue
                parsed = float(raw_value)
                if not math.isfinite(parsed):
                    raise NativeFeatureError(f"raw-native book value is non-finite: {raw_name}")
                values[output_name] = parsed

            total_quantity = buy_quantity + sell_quantity
            rate = 1_000_000_000.0 / BASE_WINDOW_WIDTH_NS
            values.update(
                {
                    "aggressive_buy_qty_btc_per_s": buy_quantity * rate,
                    "aggressive_sell_qty_btc_per_s": sell_quantity * rate,
                    "signed_flow_imbalance": (
                        (buy_quantity - sell_quantity) / total_quantity
                        if total_quantity > 0.0
                        else 0.0
                    ),
                    "trade_count_per_s": float(trade_count) * rate,
                    "buy_run_length": float(terminal_run if previous_trade_side is True else 0),
                    "sell_run_length": float(terminal_run if previous_trade_side is False else 0),
                    "last_aggressive_buy_age_s": (
                        None
                        if last_buy_ts_ns is None
                        else (book.right_ts_ns - last_buy_ts_ns) / 1_000_000_000.0
                    ),
                    "last_aggressive_sell_age_s": (
                        None
                        if last_sell_ts_ns is None
                        else (book.right_ts_ns - last_sell_ts_ns) / 1_000_000_000.0
                    ),
                }
            )
        else:
            previous_trade_side = None
            terminal_run = 0
            last_buy_ts_ns = None
            last_sell_ts_ns = None
            stats.source_unobserved_window_count += 1

        if frozenset(values) != channel_name_set:
            raise NativeFeatureError("merged M2 observation schema drifted")
        stats.window_count += 1
        stats.warmup_window_count += int(book.phase == "D_MINUS_1_WARMUP")
        stats.policy_window_count += int(book.phase == "POLICY")
        if stats.first_window_left_ts_ns == 0:
            stats.first_window_left_ts_ns = int(book.left_ts_ns)
        stats.last_window_right_ts_ns = int(book.right_ts_ns)
        previous_right_ns = int(book.right_ts_ns)

        yield CausalWindowObservation(
            left_ts_ns=int(book.left_ts_ns),
            right_ts_ns=int(book.right_ts_ns),
            feature_ready_ts_ns=max(int(book.feature_ready_ts_ns), int(book.right_ts_ns)),
            market_generation=int(book.market_generation),
            depth_generation=int(book.depth_generation),
            values=MappingProxyType(values),
            source_gap=not bool(book.support_valid),
            source_stale=bool(book.source_stale),
            warmup_admitted=bool(book.warmup_admitted),
        )


def _counter_delta(after: Any, before: Any, name: str) -> int:
    return int(getattr(after, name)) - int(getattr(before, name))


def _depth_shape(
    levels: list[tuple[float, float]],
    *,
    side: Literal["bid", "ask"],
) -> tuple[float, float] | None:
    if len(levels) != TOP_K_DEPTH_LEVELS:
        return None
    return fit_cumulative_depth_shape(
        [row[0] for row in levels],
        [row[1] for row in levels],
        side=side,
    )


def _empty_values() -> dict[str, float | int | None]:
    return {
        "best_bid_tick": None,
        "best_ask_tick": None,
        "best_bid_price_usdc_per_btc": None,
        "best_ask_price_usdc_per_btc": None,
        "best_bid_qty_btc": None,
        "best_ask_qty_btc": None,
        "mid_usdc_per_btc": None,
        "spread_ticks": None,
        "spread_usdc_per_btc": None,
        "spread_bps": None,
        "bbo_imbalance": None,
        "microprice_usdc_per_btc": None,
        "microprice_deviation_bps": None,
        "top20_bid_depth_btc": None,
        "top20_ask_depth_btc": None,
        "depth_imbalance": None,
        "bid_depth_slope_btc_per_tick": None,
        "ask_depth_slope_btc_per_tick": None,
        "bid_depth_convexity_btc_per_tick2": None,
        "ask_depth_convexity_btc_per_tick2": None,
        "top20_bid_boundary_displayed_increase_btc_per_s": None,
        "top20_bid_boundary_displayed_decrease_btc_per_s": None,
        "top20_ask_boundary_displayed_increase_btc_per_s": None,
        "top20_ask_boundary_displayed_decrease_btc_per_s": None,
        "bid_exact_level_displayed_depletion_btc": None,
        "bid_exact_level_displayed_refill_btc": None,
        "ask_exact_level_displayed_depletion_btc": None,
        "ask_exact_level_displayed_refill_btc": None,
        "bid_exact_level_displayed_depletion_btc_per_s": None,
        "bid_exact_level_displayed_refill_btc_per_s": None,
        "ask_exact_level_displayed_depletion_btc_per_s": None,
        "ask_exact_level_displayed_refill_btc_per_s": None,
    }


def _displayed_change(change: ExchangeBookLevelChange, tick_size: float) -> DisplayedLevelChange:
    delta = float(change.quantity_after - change.quantity_before)
    return DisplayedLevelChange(
        exchange_ts_ns=int(change.exchange_ts_ns),
        receive_ts_ns=int(change.receive_ts_ns),
        side=str(change.side),
        price_tick=int(change.price_tick),
        price_usdc_per_btc=float(change.price_tick) * float(tick_size),
        quantity_before_btc=float(change.quantity_before),
        quantity_after_btc=float(change.quantity_after),
        displayed_depletion_btc=max(-delta, 0.0),
        displayed_refill_btc=max(delta, 0.0),
        segment_id=int(change.segment_id),
        update_id=change.update_id,
    )


def _validate_tape(tape: NativeBookTape, contract: NativeM2BookWindowContract) -> None:
    required = (
        "day_start_ns",
        "day_end_ns",
        "process_start_ns",
        "warmup_hours",
        "strict_complete",
        "missing_paths",
        "tick_size",
    )
    missing = [name for name in required if not hasattr(tape, name)]
    if missing:
        raise NativeFeatureError(f"native tape interface is incomplete: {missing}")
    day_start = int(tape.day_start_ns)
    day_end = int(tape.day_end_ns)
    if day_end - day_start != DAY_NS:
        raise NativeFeatureError("native tape target is not one natural UTC day")
    if int(tape.warmup_hours) != WARMUP_HOURS:
        raise NativeFeatureError("native tape does not include full D-1 warmup")
    if int(tape.process_start_ns) != day_start - DAY_NS:
        raise NativeFeatureError("native tape warmup is not previous natural UTC day")
    if not bool(tape.strict_complete) or tuple(tape.missing_paths):
        raise NativeFeatureError("native tape is not strict-complete")
    if not math.isclose(float(tape.tick_size), contract.tick_size, rel_tol=0.0, abs_tol=1e-15):
        raise NativeFeatureError("native tape tick size drifted")
    if contract.policy_start_ns != day_start:
        raise NativeFeatureError("policy_start_ns must equal target UTC day start")
    if not (
        int(tape.process_start_ns) <= contract.window_start_ns < contract.window_end_ns <= day_end
    ):
        raise NativeFeatureError("feature interval is outside D-1 plus target day")


class RawNativeM2BookFeatureStream(Iterator[NativeM2BookWindow]):
    """Single-pass, bounded-memory native M2 book feature stream."""

    def __init__(
        self,
        *,
        tape: NativeBookTape,
        contract: NativeM2BookWindowContract,
        audit: NativeM2BookFeatureAccumulator | None = None,
    ) -> None:
        _validate_tape(tape, contract)
        self.tape = tape
        self.contract = contract
        self.audit = audit or NativeM2BookFeatureAccumulator()
        self._scheduler = HistoricalExchangeBookScheduler(
            tape,
            strict_sequence=False,
            strict_after_ns=0,
            allow_delta_bootstrap=False,
        )
        self._initialized = False
        self._exhausted = False
        self._next_right_ns = contract.window_start_ns + contract.window_width_ns
        self._last_window: NativeM2BookWindow | None = None
        self._previous_top20_depth: tuple[float, float] | None = None
        self._covers_complete_d1 = bool(
            contract.window_start_ns == int(tape.process_start_ns)
            and contract.window_end_ns >= contract.policy_start_ns
        )
        self._warmup_seen_observed = False
        remainder = (contract.window_end_ns - contract.window_start_ns) % contract.window_width_ns
        self.audit.partial_trailing_window_excluded = bool(remainder)
        self.audit.partial_trailing_window_ns = int(remainder)
        self.audit.warmup_expected_window_count = int(
            (contract.policy_start_ns - int(tape.process_start_ns)) // contract.window_width_ns
        )

    def __iter__(self) -> RawNativeM2BookFeatureStream:
        return self

    def _initialize(self) -> None:
        if self._initialized:
            return
        # A bounded sub-interval may start after process_start for mechanics
        # tests. Consume that prefix event-by-event without claiming D-1 EMA
        # admission. The formal factory starts exactly at process_start and
        # therefore enters this loop zero times.
        while (
            self._scheduler.next_exchange_ts_ns is not None
            and self._scheduler.next_exchange_ts_ns < self.contract.window_start_ns
        ):
            boundary = int(self._scheduler.next_exchange_ts_ns)
            self._scheduler.advance_to(boundary, inclusive=True)
        self._scheduler.advance_to(
            self.contract.window_start_ns,
            inclusive=False,
        )
        top_bid, top_ask = self._scheduler.top_levels(TOP_K_DEPTH_LEVELS)
        prefix_state_usable = bool(
            self._scheduler.sequence.initialized
            and len(top_bid) == TOP_K_DEPTH_LEVELS
            and len(top_ask) == TOP_K_DEPTH_LEVELS
        )
        if prefix_state_usable:
            self._previous_top20_depth = (
                float(sum(quantity for _, quantity in top_bid)),
                float(sum(quantity for _, quantity in top_ask)),
            )
        self._initialized = True

    def _finalize_warmup_admission(self) -> None:
        if self.audit.warmup_admission_finalized:
            return
        top_bid, top_ask = self._scheduler.top_levels(TOP_K_DEPTH_LEVELS)
        self.audit.warmup_admitted = bool(
            self._covers_complete_d1
            and self.audit.warmup_window_count == self.audit.warmup_expected_window_count
            and self._warmup_seen_observed
            and self._scheduler.sequence.initialized
            and len(top_bid) == TOP_K_DEPTH_LEVELS
            and len(top_ask) == TOP_K_DEPTH_LEVELS
            and self.audit.warmup_source_gap_count == 0
            and self.audit.warmup_sequence_gap_count == 0
            and self.audit.warmup_invalid_sequence_message_count == 0
            and self.audit.warmup_message_time_reversal_count == 0
            and (
                not self.contract.require_receive_clock
                or self.audit.warmup_missing_receive_timestamp_count == 0
            )
        )
        self.audit.warmup_admission_finalized = True

    def __next__(self) -> NativeM2BookWindow:
        self._initialize()
        if self._exhausted or self._next_right_ns > self.contract.window_end_ns:
            if (
                not self.audit.warmup_admission_finalized
                and self.contract.window_end_ns >= self.contract.policy_start_ns
                and self._next_right_ns - self.contract.window_width_ns
                >= self.contract.policy_start_ns
            ):
                self._finalize_warmup_admission()
            self._exhausted = True
            raise StopIteration

        right_ns = int(self._next_right_ns)
        left_ns = right_ns - self.contract.window_width_ns
        self._next_right_ns += self.contract.window_width_ns
        if left_ns < self.contract.policy_start_ns < right_ns:
            raise NativeFeatureError("a feature window crossed policy_start_ns")
        phase: Literal["D_MINUS_1_WARMUP", "POLICY"] = (
            "D_MINUS_1_WARMUP" if right_ns <= self.contract.policy_start_ns else "POLICY"
        )
        if phase == "POLICY":
            self._finalize_warmup_admission()
        before = self._scheduler.stats()
        advance = self._scheduler.advance_to(right_ns, inclusive=False)
        after = self._scheduler.stats()

        source_gap_count = _counter_delta(after, before, "source_gap_events")
        sequence_gap_count = _counter_delta(after, before, "sequence_gaps")
        invalid_sequence_count = _counter_delta(after, before, "invalid_sequence_messages")
        time_reversal_count = _counter_delta(after, before, "message_time_reversals")
        events = tuple(advance.source_events)
        receive_missing = any(
            event.event_type != "source_gap" and int(event.local_receive_ts_ns) <= 0
            for event in events
        )
        receive_before_exchange = sum(
            int(event.local_receive_ts_ns < event.exchange_ts_ns)
            for event in events
            if event.event_type != "source_gap" and int(event.local_receive_ts_ns) > 0
        )
        last_exchange_ts_ns = int(after.last_exchange_ts_ns)
        last_receive_ts_ns = int(self._scheduler.last_local_receive_ts_ns)
        feature_ready_ts_ns = (
            right_ns
            if self.contract.feature_ready_clock == EXCHANGE_WINDOW_READY_CLOCK
            else max(right_ns, last_receive_ts_ns)
        )
        exchange_age = right_ns - last_exchange_ts_ns if last_exchange_ts_ns > 0 else None
        receive_age = feature_ready_ts_ns - last_receive_ts_ns if last_receive_ts_ns > 0 else None
        source_stale = bool(
            self.contract.max_source_silence_ns is not None
            and (exchange_age is None or exchange_age > self.contract.max_source_silence_ns)
        )

        reasons: list[str] = []
        if phase == "POLICY" and not self.audit.warmup_admitted:
            reasons.append("D_minus_1_warmup_not_admitted")
        if source_gap_count:
            reasons.append("source_gap")
        if sequence_gap_count:
            reasons.append("sequence_gap")
        if invalid_sequence_count:
            reasons.append("invalid_sequence_message")
        if time_reversal_count:
            reasons.append("message_time_reversal")
        if bool(advance.invalidated):
            reasons.append("scheduler_invalidated")
        if bool(advance.snapshot_reset):
            reasons.append("snapshot_reset_in_window")
        if not self._scheduler.sequence.initialized:
            reasons.append("sequence_unavailable")
        if self.contract.require_receive_clock and receive_missing:
            reasons.append("missing_receive_timestamp")
        if source_stale:
            reasons.append("source_stale")

        top_bid, top_ask = self._scheduler.top_levels(TOP_K_DEPTH_LEVELS)
        top20_complete = bool(
            len(top_bid) == TOP_K_DEPTH_LEVELS and len(top_ask) == TOP_K_DEPTH_LEVELS
        )
        if self._scheduler.sequence.initialized and not top20_complete:
            reasons.append("top20_depth_incomplete")

        level_changes = tuple(
            _displayed_change(change, self.contract.tick_size) for change in advance.level_changes
        )
        values = _empty_values()
        source_valid = not any(
            reason
            in {
                "source_gap",
                "D_minus_1_warmup_not_admitted",
                "sequence_gap",
                "invalid_sequence_message",
                "message_time_reversal",
                "scheduler_invalidated",
                "sequence_unavailable",
                "missing_receive_timestamp",
                "source_stale",
                "top20_depth_incomplete",
            }
            for reason in reasons
        )
        state_usable = bool(source_valid and top20_complete)
        if state_usable:
            bid_tick, bid_qty = top_bid[0]
            ask_tick, ask_qty = top_ask[0]
            bid_tick_i = int(bid_tick)
            ask_tick_i = int(ask_tick)
            bid = bid_tick_i * self.contract.tick_size
            ask = ask_tick_i * self.contract.tick_size
            quantity_sum = float(bid_qty + ask_qty)
            if bid_tick_i <= 0 or ask_tick_i <= bid_tick_i or quantity_sum <= 0.0:
                reasons.append("invalid_native_bbo")
                state_usable = False
            else:
                mid = 0.5 * (bid + ask)
                microprice = (ask * bid_qty + bid * ask_qty) / quantity_sum
                bid_depth = float(sum(quantity for _, quantity in top_bid))
                ask_depth = float(sum(quantity for _, quantity in top_ask))
                depth_total = bid_depth + ask_depth
                bid_shape = _depth_shape(top_bid, side="bid")
                ask_shape = _depth_shape(top_ask, side="ask")
                if depth_total <= 0.0 or bid_shape is None or ask_shape is None:
                    reasons.append("invalid_top20_depth_shape")
                    state_usable = False
                else:
                    values.update(
                        {
                            "best_bid_tick": bid_tick_i,
                            "best_ask_tick": ask_tick_i,
                            "best_bid_price_usdc_per_btc": bid,
                            "best_ask_price_usdc_per_btc": ask,
                            "best_bid_qty_btc": float(bid_qty),
                            "best_ask_qty_btc": float(ask_qty),
                            "mid_usdc_per_btc": mid,
                            "spread_ticks": ask_tick_i - bid_tick_i,
                            "spread_usdc_per_btc": ask - bid,
                            "spread_bps": 10_000.0 * (ask - bid) / mid,
                            "bbo_imbalance": (bid_qty - ask_qty) / quantity_sum,
                            "microprice_usdc_per_btc": microprice,
                            "microprice_deviation_bps": (10_000.0 * (microprice - mid) / mid),
                            "top20_bid_depth_btc": bid_depth,
                            "top20_ask_depth_btc": ask_depth,
                            "depth_imbalance": ((bid_depth - ask_depth) / depth_total),
                            "bid_depth_slope_btc_per_tick": bid_shape[0],
                            "ask_depth_slope_btc_per_tick": ask_shape[0],
                            "bid_depth_convexity_btc_per_tick2": bid_shape[1],
                            "ask_depth_convexity_btc_per_tick2": ask_shape[1],
                        }
                    )
                    current_depth = (bid_depth, ask_depth)
                    if self._previous_top20_depth is not None:
                        prior_bid, prior_ask = self._previous_top20_depth
                        rate = 1_000_000_000.0 / self.contract.window_width_ns
                        bid_delta = bid_depth - prior_bid
                        ask_delta = ask_depth - prior_ask
                        values.update(
                            {
                                "top20_bid_boundary_displayed_increase_btc_per_s": (
                                    max(bid_delta, 0.0) * rate
                                ),
                                "top20_bid_boundary_displayed_decrease_btc_per_s": (
                                    max(-bid_delta, 0.0) * rate
                                ),
                                "top20_ask_boundary_displayed_increase_btc_per_s": (
                                    max(ask_delta, 0.0) * rate
                                ),
                                "top20_ask_boundary_displayed_decrease_btc_per_s": (
                                    max(-ask_delta, 0.0) * rate
                                ),
                            }
                        )
                    exact = {
                        (side, direction): sum(
                            (
                                change.displayed_depletion_btc
                                if direction == "depletion"
                                else change.displayed_refill_btc
                            )
                            for change in level_changes
                            if change.side == side
                        )
                        for side in ("bid", "ask")
                        for direction in ("depletion", "refill")
                    }
                    exact_rate = 1_000_000_000.0 / self.contract.window_width_ns
                    values.update(
                        {
                            "bid_exact_level_displayed_depletion_btc": exact[("bid", "depletion")],
                            "bid_exact_level_displayed_refill_btc": exact[("bid", "refill")],
                            "ask_exact_level_displayed_depletion_btc": exact[("ask", "depletion")],
                            "ask_exact_level_displayed_refill_btc": exact[("ask", "refill")],
                            "bid_exact_level_displayed_depletion_btc_per_s": (
                                exact[("bid", "depletion")] * exact_rate
                            ),
                            "bid_exact_level_displayed_refill_btc_per_s": (
                                exact[("bid", "refill")] * exact_rate
                            ),
                            "ask_exact_level_displayed_depletion_btc_per_s": (
                                exact[("ask", "depletion")] * exact_rate
                            ),
                            "ask_exact_level_displayed_refill_btc_per_s": (
                                exact[("ask", "refill")] * exact_rate
                            ),
                        }
                    )
                    if "snapshot_reset_in_window" in reasons:
                        self._previous_top20_depth = current_depth
                    elif not reasons:
                        self._previous_top20_depth = current_depth

        if not state_usable:
            values = _empty_values()
            if not self._scheduler.sequence.initialized:
                self._previous_top20_depth = None
        support_valid = bool(state_usable and not reasons)
        if not support_valid and not reasons:
            reasons.append("native_book_state_unavailable")
        if not support_valid:
            values = _empty_values()
        unique_reasons = tuple(dict.fromkeys(reasons))

        if phase == "D_MINUS_1_WARMUP":
            self.audit.warmup_window_count += 1
            self.audit.warmup_observed_window_count += int(support_valid)
            self.audit.warmup_unobserved_window_count += int(not support_valid)
            self.audit.warmup_source_event_count += len(events)
            self.audit.warmup_source_gap_count += source_gap_count
            self.audit.warmup_sequence_gap_count += sequence_gap_count
            self.audit.warmup_invalid_sequence_message_count += invalid_sequence_count
            self.audit.warmup_message_time_reversal_count += time_reversal_count
            self.audit.warmup_missing_receive_timestamp_count += int(receive_missing)
            self.audit.warmup_receive_before_exchange_event_count += int(receive_before_exchange)
            self._warmup_seen_observed = bool(self._warmup_seen_observed or support_valid)

        self.audit.window_count += 1
        self.audit.observed_window_count += int(support_valid)
        self.audit.unobserved_window_count += int(not support_valid)
        for reason in unique_reasons:
            assert self.audit.unobserved_reason_counts is not None
            self.audit.unobserved_reason_counts[reason] += 1
        self.audit.source_event_count += len(events)
        self.audit.accepted_event_count += int(advance.accepted_events)
        self.audit.rejected_event_count += int(advance.rejected_events)
        self.audit.source_gap_count += source_gap_count
        self.audit.sequence_gap_count += sequence_gap_count
        self.audit.invalid_sequence_message_count += invalid_sequence_count
        self.audit.message_time_reversal_count += time_reversal_count
        self.audit.snapshot_reset_window_count += int(advance.snapshot_reset)
        self.audit.missing_receive_clock_window_count += int(receive_missing)
        self.audit.receive_before_exchange_event_count += int(receive_before_exchange)
        self.audit.exact_level_change_count += len(level_changes)
        for change in level_changes:
            if change.side == "bid":
                self.audit.bid_displayed_depletion_btc += change.displayed_depletion_btc
                self.audit.bid_displayed_refill_btc += change.displayed_refill_btc
            else:
                self.audit.ask_displayed_depletion_btc += change.displayed_depletion_btc
                self.audit.ask_displayed_refill_btc += change.displayed_refill_btc
        self.audit.last_source_exchange_ts_ns = last_exchange_ts_ns
        self.audit.last_source_receive_ts_ns = last_receive_ts_ns
        if exchange_age is not None:
            self.audit.max_source_exchange_age_ns = max(
                self.audit.max_source_exchange_age_ns, exchange_age
            )
        if receive_age is not None:
            self.audit.max_source_receive_age_ns = max(
                self.audit.max_source_receive_age_ns, receive_age
            )

        window = NativeM2BookWindow(
            left_ts_ns=left_ns,
            right_ts_ns=right_ns,
            feature_ready_ts_ns=feature_ready_ts_ns,
            market_generation=self.audit.window_count,
            depth_generation=self.audit.window_count,
            phase=phase,
            policy_start_ns=self.contract.policy_start_ns,
            warmup_admitted=bool(self.audit.warmup_admitted),
            support_valid=support_valid,
            support_state="OBSERVED" if support_valid else "UNOBSERVED",
            unobserved_reasons=unique_reasons,
            values=MappingProxyType(values),
            level_changes=level_changes if support_valid else (),
            source_event_count=len(events),
            accepted_event_count=int(advance.accepted_events),
            rejected_event_count=int(advance.rejected_events),
            source_gap_count=source_gap_count,
            sequence_gap_count=sequence_gap_count,
            invalid_sequence_message_count=invalid_sequence_count,
            message_time_reversal_count=time_reversal_count,
            snapshot_reset=bool(advance.snapshot_reset),
            segment_id=int(self._scheduler.segment_id),
            last_source_exchange_ts_ns=last_exchange_ts_ns,
            last_source_receive_ts_ns=last_receive_ts_ns,
            source_exchange_age_ns=exchange_age,
            source_receive_age_ns=receive_age,
            source_silence_limit_ns=self.contract.max_source_silence_ns,
            source_stale=source_stale,
            receive_clock_valid=not receive_missing,
            economic_outcomes_read=False,
        )
        self._last_window = window
        return window

    def lookup_target_price(
        self,
        *,
        side: str,
        order_price_usdc_per_btc: float | None = None,
        order_price_tick: int | None = None,
    ) -> TargetPriceDisplayedQuantity:
        """Query displayed quantity without claiming queue-ahead authority."""

        if self._last_window is None:
            raise NativeFeatureError("target lookup requires a completed window")
        if (order_price_usdc_per_btc is None) == (order_price_tick is None):
            raise NativeFeatureError("provide exactly one order-price representation")
        normalized = str(side).strip().lower()
        if normalized in {"buy", "bid"}:
            native_side: Literal["bid", "ask"] = "bid"
        elif normalized in {"sell", "ask"}:
            native_side = "ask"
        else:
            raise NativeFeatureError(f"unsupported target-price side: {side!r}")
        if order_price_tick is None:
            price = float(order_price_usdc_per_btc)
            if not math.isfinite(price) or price <= 0.0:
                raise NativeFeatureError("order price must be positive and finite")
            tick = int(round(price / self.contract.tick_size))
            reconstructed = tick * self.contract.tick_size
            tolerance = max(abs(price) * 1e-12, self.contract.tick_size * 1e-9)
            if abs(price - reconstructed) > tolerance:
                raise NativeFeatureError("order price is not on the exchange tick")
        else:
            if isinstance(order_price_tick, bool):
                raise NativeFeatureError("order_price_tick must be an integer")
            tick = int(order_price_tick)
            if tick != order_price_tick or tick <= 0:
                raise NativeFeatureError("order_price_tick must be a positive integer")
            reconstructed = tick * self.contract.tick_size

        if not self._last_window.support_valid:
            return TargetPriceDisplayedQuantity(
                side=native_side,
                order_price_tick=tick,
                order_price_usdc_per_btc=reconstructed,
                status="unknown",
                known=False,
                displayed_quantity_btc=None,
                reason="window_unobserved:" + ",".join(self._last_window.unobserved_reasons),
                asof_window_right_ts_ns=self._last_window.right_ts_ns,
                asof_exchange_ts_ns=self._last_window.last_source_exchange_ts_ns,
                feature_ready_ts_ns=self._last_window.feature_ready_ts_ns,
                segment_id=self._last_window.segment_id,
            )
        lookup = self._scheduler.lookup(native_side, tick)
        status = str(lookup.status)
        if status not in {"exact", "known_zero", "unknown"}:
            raise NativeFeatureError(f"unexpected native lookup status: {status}")
        return TargetPriceDisplayedQuantity(
            side=native_side,
            order_price_tick=tick,
            order_price_usdc_per_btc=reconstructed,
            status=status,
            known=status in {"exact", "known_zero"},
            displayed_quantity_btc=lookup.quantity,
            reason=str(lookup.reason),
            asof_window_right_ts_ns=self._last_window.right_ts_ns,
            asof_exchange_ts_ns=int(lookup.asof_exchange_ts_ns),
            feature_ready_ts_ns=self._last_window.feature_ready_ts_ns,
            segment_id=int(lookup.segment_id),
        )


def open_cryptohft_native_m2_book_feature_stream(
    *,
    raw_root: Path,
    day: str,
    symbol: str = "BTCUSDC",
    tick_size: float = PRICE_TICK_SIZE_USDC_PER_BTC,
    window_start_ns: int | None = None,
    window_end_ns: int | None = None,
    cache_dir: Path | None = None,
    cache_read_only: bool = True,
    require_receive_clock: bool = True,
    feature_ready_clock: str = LOCAL_RECEIVE_READY_CLOCK,
    max_source_silence_ns: int | None = None,
    audit: NativeM2BookFeatureAccumulator | None = None,
) -> RawNativeM2BookFeatureStream:
    """Open the existing structured CryptoHFT tape/cache as a feature stream."""

    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(raw_root),
        day=day,
        symbol=symbol,
        tick_size=tick_size,
        warmup_hours=WARMUP_HOURS,
        continuation_hours=0,
        strict_complete=True,
        cache_dir=cache_dir,
        cache_enabled=True,
        cache_read_only=cache_read_only,
    )
    contract = NativeM2BookWindowContract(
        window_start_ns=int(window_start_ns or tape.process_start_ns),
        window_end_ns=int(window_end_ns or tape.day_end_ns),
        policy_start_ns=int(tape.day_start_ns),
        tick_size=float(tick_size),
        require_receive_clock=bool(require_receive_clock),
        feature_ready_clock=str(feature_ready_clock),
        max_source_silence_ns=max_source_silence_ns,
    )
    return RawNativeM2BookFeatureStream(
        tape=tape,
        contract=contract,
        audit=audit,
    )


def native_m2_book_feature_schema() -> dict[str, Any]:
    """Return the outcome-blind public contract for artifact binding."""

    return {
        "identity": IDENTITY,
        "schema_version": SCHEMA_VERSION,
        "base_window": "100ms_[left,right)",
        "partial_current_window": "excluded",
        "stream_interval": "D-1_00:00:00Z_through_target_day_end",
        "policy_start": "target_day_00:00:00Z",
        "warmup": "previous_natural_UTC_day_24h_no_D-2",
        "warmup_observation_semantics": (
            "pre-initial-snapshot windows may be UNOBSERVED; completed "
            "post-snapshot D-1 windows feed EMA state"
        ),
        "warmup_admission": (
            "finalized_at_policy_start; requires complete D-1 interval, "
            "initialized top-20 state, at least one observed warmup window, "
            "zero source/sequence/time-reversal errors, and complete receive "
            "timestamps when required"
        ),
        "policy_window_without_warmup_admission": "UNOBSERVED",
        "parser": "models.exchange_book_replay.CryptoHFTExchangeBookTape",
        "scheduler": "models.exchange_book_replay.HistoricalExchangeBookScheduler",
        "source_gap_or_invalid_sequence": "UNOBSERVED",
        "forward_fill_across_invalid_segment": False,
        "displayed_change_semantics": DISPLAYED_CHANGE_SEMANTICS,
        "target_price_lookup": "displayed_quantity_exact_known_zero_unknown",
        "target_price_lookup_is_queue_ahead": False,
        "target_price_lookup_in_ema_observation": False,
        "official_trade_merge": {
            "source": "official_individual_trades",
            "clock": "exchange_transact_time_ms",
            "boundary": "left_closed_right_open",
            "event_at_right_boundary": "excluded_then_enters_next_window",
            "feature_ready": "max(book_feature_ready,right_edge)",
            "receive_time_transport_authority": False,
        },
        "m2_observation_channel_names": list(native_m2_observation_channel_names()),
        "exact_displayed_rate_channel_names": list(EXACT_DISPLAYED_RATE_CHANNELS),
        "feature_ready_clock_profiles": {
            EXCHANGE_WINDOW_READY_CLOCK: (
                "historical exchange-event exploratory labels only; source "
                "receive clocks remain audited but are not joined to an unknown "
                "private-fill receive clock"
            ),
            LOCAL_RECEIVE_READY_CLOCK: (
                "prospective transport: max(right_edge,last_source_receive_ts_ns)"
            ),
        },
        "source_silence_threshold": ("caller_supplied_outcome_blind_or_report_only_when_null"),
        "streaming_state": ("scheduler_book_plus_previous_valid_top20_and_current_window_changes"),
        "economic_outcomes_read": False,
    }


__all__ = [
    "DISPLAYED_CHANGE_SEMANTICS",
    "EXCHANGE_WINDOW_READY_CLOCK",
    "EXACT_DISPLAYED_RATE_CHANNELS",
    "FEATURE_READY_CLOCKS",
    "LOCAL_RECEIVE_READY_CLOCK",
    "NativeFeatureError",
    "NativeM2BookFeatureAccumulator",
    "NativeM2BookFeatureAudit",
    "NativeM2BookWindow",
    "NativeM2BookWindowContract",
    "NativeM2TradeMergeAccumulator",
    "NativeM2TradeMergeAudit",
    "RawNativeM2BookFeatureStream",
    "SCHEMA_VERSION",
    "TargetPriceDisplayedQuantity",
    "DisplayedLevelChange",
    "native_m2_book_feature_schema",
    "native_m2_observation_channel_names",
    "open_cryptohft_native_m2_book_feature_stream",
    "stream_native_m2_causal_observations",
]
