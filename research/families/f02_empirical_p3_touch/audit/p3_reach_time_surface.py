#!/usr/bin/env python3
"""Reusable first-passage labels for the F02 aggressive-reach time surface.

The kernel records the farthest side-correct aggressive-trade reach observed
by each 100ms upper endpoint after a causal decision origin.  Distance and
horizon queries are then derived from the same compact cumulative path.  The
labels deliberately exclude activation, queue conversion, fills, cancel ACK,
inventory, and economic outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

SCHEMA_VERSION = "narrowgate_p3_aggressive_reach_time_labels.v1"

# Cumulative reach is non-negative when observed.  These sentinels therefore
# remain outside the valid tick-distance domain.
UNREACHED_TICKS = np.int16(-1)
INVALID_REACH_TICKS = np.int16(np.iinfo(np.int16).min)
RIGHT_CENSORED_TIME_MS = np.int32(-1)
INVALID_TIME_MS = np.int32(np.iinfo(np.int32).min)
INVALID_BINARY_LABEL = np.int8(-1)


@dataclass(frozen=True)
class ReachTimeGridSpec:
    """Discrete label-time contract; max horizon is administrative censoring."""

    time_step_ms: int = 100
    max_horizon_ms: int = 30_000
    max_distance_ticks: int = 1_200

    def __post_init__(self) -> None:
        if self.time_step_ms <= 0:
            raise ValueError("time_step_ms must be positive")
        if self.max_horizon_ms <= 0:
            raise ValueError("max_horizon_ms must be positive")
        if self.max_horizon_ms % self.time_step_ms:
            raise ValueError("max_horizon_ms must be divisible by time_step_ms")
        if not 0 < self.max_distance_ticks < int(np.iinfo(np.int16).max):
            raise ValueError("max_distance_ticks must fit in positive int16")

    @property
    def n_time_bins(self) -> int:
        return self.max_horizon_ms // self.time_step_ms

    def time_upper_ms(self) -> np.ndarray:
        return np.arange(
            self.time_step_ms,
            self.max_horizon_ms + self.time_step_ms,
            self.time_step_ms,
            dtype=np.int32,
        )


DEFAULT_GRID_SPEC = ReachTimeGridSpec()


@dataclass(frozen=True)
class ReachTimeLabelSurface:
    """Side-specific cumulative reach paths for a set of decision origins."""

    time_upper_ms: np.ndarray
    buy_cumulative_reach_ticks: np.ndarray
    sell_cumulative_reach_ticks: np.ndarray
    schema_version: str = SCHEMA_VERSION


def _integer_vector(name: str, values: np.ndarray) -> np.ndarray:
    raw = np.asarray(values)
    if raw.ndim != 1:
        raise ValueError(f"{name} must be one-dimensional")
    if not np.issubdtype(raw.dtype, np.integer):
        raise TypeError(f"{name} must use an integer dtype")
    return raw.astype(np.int64, copy=False)


def _boolean_vector(name: str, values: np.ndarray) -> np.ndarray:
    raw = np.asarray(values)
    if raw.ndim != 1:
        raise ValueError(f"{name} must be one-dimensional")
    if not np.issubdtype(raw.dtype, np.bool_):
        raise TypeError(f"{name} must use a boolean dtype")
    return raw.astype(bool, copy=False)


def _validate_inputs(
    *,
    decision_ts_ms: np.ndarray,
    best_bid_ticks: np.ndarray,
    best_ask_ticks: np.ndarray,
    trade_ts_ms: np.ndarray,
    trade_price_ticks: np.ndarray,
    is_buyer_maker: np.ndarray,
    valid_decisions: np.ndarray | None,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
]:
    decisions = _integer_vector("decision_ts_ms", decision_ts_ms)
    bids = _integer_vector("best_bid_ticks", best_bid_ticks)
    asks = _integer_vector("best_ask_ticks", best_ask_ticks)
    trade_ts = _integer_vector("trade_ts_ms", trade_ts_ms)
    trade_prices = _integer_vector("trade_price_ticks", trade_price_ticks)
    maker = _boolean_vector("is_buyer_maker", is_buyer_maker)

    if len(bids) != len(decisions) or len(asks) != len(decisions):
        raise ValueError("decision timestamps and BBO vectors must have equal length")
    if len(trade_prices) != len(trade_ts) or len(maker) != len(trade_ts):
        raise ValueError("trade vectors must have equal length")
    if len(decisions) > 1 and np.any(np.diff(decisions) <= 0):
        raise ValueError("decision_ts_ms must be strictly increasing")
    if len(trade_ts) > 1 and np.any(np.diff(trade_ts) < 0):
        raise ValueError("trade_ts_ms must be non-decreasing")
    if np.any(trade_prices <= 0):
        raise ValueError("trade_price_ticks must be positive")

    if valid_decisions is None:
        valid = np.ones(len(decisions), dtype=bool)
    else:
        valid = _boolean_vector("valid_decisions", valid_decisions)
        if len(valid) != len(decisions):
            raise ValueError("valid_decisions must match decision timestamps")
    valid &= (bids > 0) & (asks > 0) & (bids < asks)
    return decisions, bids, asks, trade_ts, trade_prices, maker, valid


def _accumulate_side(
    *,
    row: np.ndarray,
    event_bins: np.ndarray,
    reach_ticks: np.ndarray,
    max_distance_ticks: int,
) -> None:
    reached = reach_ticks >= 0
    if not np.any(reached):
        return
    clipped = np.minimum(reach_ticks[reached], max_distance_ticks).astype(
        np.int16,
        copy=False,
    )
    np.maximum.at(row, event_bins[reached], clipped)


def build_cumulative_reach_ticks(
    *,
    decision_ts_ms: np.ndarray,
    best_bid_ticks: np.ndarray,
    best_ask_ticks: np.ndarray,
    trade_ts_ms: np.ndarray,
    trade_price_ticks: np.ndarray,
    is_buyer_maker: np.ndarray,
    valid_decisions: np.ndarray | None = None,
    spec: ReachTimeGridSpec = DEFAULT_GRID_SPEC,
) -> ReachTimeLabelSurface:
    """Build interval-censored BUY/SELL aggressive-reach paths.

    A bin with upper endpoint ``h`` contains trades in ``(decision, decision+h]``.
    BUY-maker reach uses aggressive sells (``is_buyer_maker=True``), while
    SELL-maker reach uses aggressive buys.  A value of ``-1`` means no reach
    has occurred by that endpoint; invalid decision rows use the int16 minimum.
    """

    (
        decisions,
        bids,
        asks,
        trade_ts,
        trade_prices,
        maker,
        valid,
    ) = _validate_inputs(
        decision_ts_ms=decision_ts_ms,
        best_bid_ticks=best_bid_ticks,
        best_ask_ticks=best_ask_ticks,
        trade_ts_ms=trade_ts_ms,
        trade_price_ticks=trade_price_ticks,
        is_buyer_maker=is_buyer_maker,
        valid_decisions=valid_decisions,
    )

    shape = (len(decisions), spec.n_time_bins)
    buy = np.full(shape, UNREACHED_TICKS, dtype=np.int16)
    sell = np.full(shape, UNREACHED_TICKS, dtype=np.int16)

    for row_index, origin_ms in enumerate(decisions):
        if not valid[row_index]:
            buy[row_index, :] = INVALID_REACH_TICKS
            sell[row_index, :] = INVALID_REACH_TICKS
            continue

        first = int(np.searchsorted(trade_ts, origin_ms, side="right"))
        last = int(
            np.searchsorted(
                trade_ts,
                origin_ms + spec.max_horizon_ms,
                side="right",
            )
        )
        if first >= last:
            continue

        offsets_ms = trade_ts[first:last] - origin_ms
        event_bins = ((offsets_ms - 1) // spec.time_step_ms).astype(
            np.int64,
            copy=False,
        )
        prices = trade_prices[first:last]
        event_maker = maker[first:last]

        _accumulate_side(
            row=buy[row_index],
            event_bins=event_bins[event_maker],
            reach_ticks=bids[row_index] - prices[event_maker],
            max_distance_ticks=spec.max_distance_ticks,
        )
        _accumulate_side(
            row=sell[row_index],
            event_bins=event_bins[~event_maker],
            reach_ticks=prices[~event_maker] - asks[row_index],
            max_distance_ticks=spec.max_distance_ticks,
        )
        buy[row_index] = np.maximum.accumulate(buy[row_index])
        sell[row_index] = np.maximum.accumulate(sell[row_index])

    return ReachTimeLabelSurface(
        time_upper_ms=spec.time_upper_ms(),
        buy_cumulative_reach_ticks=buy,
        sell_cumulative_reach_ticks=sell,
    )


def first_reach_time_upper_ms(
    cumulative_reach_ticks: np.ndarray,
    *,
    distance_ticks: int,
    spec: ReachTimeGridSpec = DEFAULT_GRID_SPEC,
) -> np.ndarray:
    """Return the first 100ms-bin upper endpoint reaching ``distance_ticks``.

    ``RIGHT_CENSORED_TIME_MS`` means the distance was not reached by the
    administrative maximum horizon.  The returned value is an interval upper
    endpoint, not an exact event timestamp.
    """

    matrix = np.asarray(cumulative_reach_ticks)
    if matrix.ndim != 2 or matrix.shape[1] != spec.n_time_bins:
        raise ValueError("cumulative reach matrix has the wrong shape")
    if not np.issubdtype(matrix.dtype, np.integer):
        raise TypeError("cumulative reach matrix must use an integer dtype")
    if not 0 <= int(distance_ticks) <= spec.max_distance_ticks:
        raise ValueError("distance_ticks lies outside the frozen support")

    out = np.full(len(matrix), RIGHT_CENSORED_TIME_MS, dtype=np.int32)
    invalid = matrix[:, 0] == INVALID_REACH_TICKS
    out[invalid] = INVALID_TIME_MS
    reached = matrix >= int(distance_ticks)
    has_reach = np.any(reached, axis=1) & ~invalid
    first_bin = np.argmax(reached, axis=1)
    out[has_reach] = (first_bin[has_reach] + 1) * spec.time_step_ms
    return out


def reach_indicator_at_horizon(
    cumulative_reach_ticks: np.ndarray,
    *,
    distance_ticks: int,
    horizon_ms: int,
    spec: ReachTimeGridSpec = DEFAULT_GRID_SPEC,
) -> np.ndarray:
    """Derive a binary reach label at an exact supported grid endpoint."""

    if horizon_ms <= 0 or horizon_ms > spec.max_horizon_ms:
        raise ValueError("horizon_ms lies outside the frozen time support")
    if horizon_ms % spec.time_step_ms:
        raise ValueError("horizon_ms must align to the frozen time grid")
    if not 0 <= int(distance_ticks) <= spec.max_distance_ticks:
        raise ValueError("distance_ticks lies outside the frozen support")

    matrix = np.asarray(cumulative_reach_ticks)
    if matrix.ndim != 2 or matrix.shape[1] != spec.n_time_bins:
        raise ValueError("cumulative reach matrix has the wrong shape")
    if not np.issubdtype(matrix.dtype, np.integer):
        raise TypeError("cumulative reach matrix must use an integer dtype")

    values = matrix[:, horizon_ms // spec.time_step_ms - 1]
    out = (values >= int(distance_ticks)).astype(np.int8)
    out[values == INVALID_REACH_TICKS] = INVALID_BINARY_LABEL
    return out
