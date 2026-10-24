"""Causal replay event-clock assembly, independent of the execution loop.

One implementation serves prepared, direct Python and C++ replay adapters.
It constructs immutable input events; it owns no account, order or RNG state.
"""


import numpy as np
import pandas as pd

from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from strategy.replay_controls import SYNC_CENSOR_CODE, SYNC_EVENT_CODE


def build_replay_event_clock(
    trades_df: pd.DataFrame,
    *,
    mode: str,
    interval_ms: int,
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
    bbo_data: HistoricalBBOData | None = None,
    l2_data: HistoricalL2Data | None = None,
    empirical_ts_ms: np.ndarray | None = None,
    empirical_action: np.ndarray | None = None,
    system_event_ts_ms: np.ndarray | None = None,
    system_event_code: int = SYNC_EVENT_CODE,
    initial_clock_price: float | None = None,
    cold_flat_start: bool = False,
) -> tuple[pd.DataFrame, int]:
    """Return the causal event stream consumed by Python and C++ replay.

    ``trade`` preserves the legacy execution-trade clock. ``merged`` adds one
    zero-quantity clock event for each BBO/L2 state-change timestamp and fixed
    wall-clock interval. Synthetic events carry the latest causally visible
    execution price, or the latest causal BBO/L2 midpoint before the first
    trade when an explicit start is supplied. They advance state but cannot
    consume queue or create a fill. Native snapshot/delta queue state is
    consumed lazily by its independent scheduler at every one of these
    boundaries and before every execution trade.

    ``cold_flat_start`` may omit only the initially unpriced timer prefix;
    the caller must establish an empty account with no orders. It never uses
    a future price to seed the prefix or drops reconciliation/system events.
    """
    normalized_mode = str(mode or "trade").strip().lower()
    if normalized_mode not in {"trade", "merged", "empirical"}:
        raise ValueError(
            f"Unknown replay_event_clock={mode!r}; expected trade, merged, or empirical"
        )
    required = ("transact_time", "price", "quantity", "is_buyer_maker")
    missing = [name for name in required if name not in trades_df.columns]
    if missing:
        raise ValueError(f"Replay trades missing required columns: {missing}")
    n_execution_trades = int(len(trades_df))
    system_ts = np.asarray(
        [] if system_event_ts_ms is None else system_event_ts_ms,
        dtype=np.int64,
    ).reshape(-1)
    if system_ts.size and normalized_mode == "trade":
        raise ValueError("system replay events require replay_event_clock=merged or empirical")
    if normalized_mode == "trade" and (start_ts_ms is not None or end_ts_ms is not None):
        raise ValueError(
            "an explicit replay event start/end requires merged or empirical clock mode"
        )
    if start_ts_ms is not None:
        if (
            isinstance(start_ts_ms, (bool, np.bool_))
            or not isinstance(start_ts_ms, (int, np.integer))
            or start_ts_ms < 0
            or start_ts_ms > np.iinfo(np.int64).max
        ):
            raise ValueError("replay event start must be a nonnegative integer millisecond")
        if not n_execution_trades:
            raise ValueError("an explicit replay event start requires retained execution trades")
    if normalized_mode == "trade" or n_execution_trades == 0:
        return trades_df, n_execution_trades
    if normalized_mode == "merged" and int(interval_ms) <= 0:
        raise ValueError("replay_clock_interval_ms must be positive")

    execution_ts = trades_df["transact_time"].to_numpy(dtype=np.int64, copy=False)
    if np.any(execution_ts[1:] < execution_ts[:-1]):
        raise ValueError("Execution trades must be ordered by transact_time")
    start_ts = int(execution_ts[0]) if start_ts_ms is None else int(start_ts_ms)
    if start_ts > int(execution_ts[0]):
        raise ValueError("replay event start follows the first execution trade; slice inputs first")
    execution_end_ts = int(execution_ts[-1])
    end_ts = execution_end_ts if end_ts_ms is None else int(end_ts_ms)
    if end_ts < execution_end_ts:
        raise ValueError("replay event end precedes the final execution trade")
    clock_sources: list[np.ndarray] = []
    clock_action = np.empty(0, dtype=np.uint8)
    if normalized_mode == "empirical":
        clock_ts_raw = np.asarray(empirical_ts_ms, dtype=np.int64)
        clock_action_raw = np.asarray(empirical_action, dtype=np.uint8)
        if clock_ts_raw.size == 0 or clock_ts_raw.size != clock_action_raw.size:
            raise ValueError(
                "replay_event_clock=empirical requires matching non-empty "
                "_empirical_requote_ts_ms/_empirical_requote_action arrays"
            )
        visible = (clock_ts_raw >= start_ts) & (clock_ts_raw <= end_ts)
        clock_ts = clock_ts_raw[visible]
        clock_action = clock_action_raw[visible]
        if np.any(~np.isin(clock_action, (2, 3))):
            raise ValueError("empirical requote actions must be 2 (ok) or 3 (blocked)")
        if start_ts < execution_ts[0] and not np.any(clock_ts == start_ts):
            clock_ts = np.insert(clock_ts, 0, np.int64(start_ts))
            clock_action = np.insert(clock_action, 0, np.uint8(0))
        if end_ts_ms is not None and not np.any(clock_ts == end_ts):
            clock_ts = np.append(clock_ts, np.int64(end_ts))
            clock_action = np.append(clock_action, np.uint8(0))
    else:
        if start_ts_ms is not None:
            clock_sources.append(np.asarray([start_ts], dtype=np.int64))
        for data in (bbo_data, l2_data):
            if data is None:
                continue
            source_ts = np.asarray(data.ts_ms, dtype=np.int64)
            if source_ts.size:
                visible = source_ts[(source_ts >= start_ts) & (source_ts <= end_ts)]
                if visible.size:
                    clock_sources.append(visible)
        step = int(interval_ms)
        first_timer_ts = ((start_ts + step - 1) // step) * step
        if first_timer_ts <= end_ts:
            clock_sources.append(np.arange(first_timer_ts, end_ts + 1, step, dtype=np.int64))
        if end_ts_ms is not None:
            # A requested terminal boundary advances lifecycle timers even
            # when it falls between regular clock ticks. It is not a trade.
            clock_sources.append(np.asarray([end_ts], dtype=np.int64))
        if clock_sources:
            clock_ts = np.unique(np.concatenate(clock_sources))
            clock_action = np.zeros(clock_ts.size, dtype=np.uint8)
    if not clock_sources:
        if normalized_mode != "empirical":
            return trades_df, n_execution_trades
    if system_ts.size:
        visible_system = system_ts[(system_ts >= start_ts) & (system_ts <= end_ts)]
        if visible_system.size:
            if np.any(visible_system[1:] <= visible_system[:-1]):
                raise ValueError("system replay event timestamps must be sorted and unique")
            clock_ts = np.concatenate((clock_ts, visible_system))
            clock_action = np.concatenate(
                (
                    clock_action,
                    np.full(
                        visible_system.size,
                        int(system_event_code),
                        dtype=np.uint8,
                    ),
                )
            )
            clock_order = np.argsort(clock_ts, kind="stable")
            clock_ts = clock_ts[clock_order]
            clock_action = clock_action[clock_order]
    if normalized_mode == "merged":
        positions = np.searchsorted(execution_ts, clock_ts, side="left")
        collides = (positions < execution_ts.size) & (
            execution_ts[np.minimum(positions, execution_ts.size - 1)] == clock_ts
        )
        ordinary_clock = ~np.isin(
            clock_action,
            (SYNC_EVENT_CODE, SYNC_CENSOR_CODE),
        )
        keep = ~(collides & ordinary_clock)
        clock_ts = clock_ts[keep]
        clock_action = clock_action[keep]
    if clock_ts.size == 0:
        return trades_df, n_execution_trades

    asof_idx = np.searchsorted(execution_ts, clock_ts, side="right") - 1
    execution_price = trades_df["price"].to_numpy(dtype=np.float64, copy=False)
    clock_price = np.empty(clock_ts.size, dtype=np.float64)
    after_trade = asof_idx >= 0
    clock_price[after_trade] = execution_price[asof_idx[after_trade]]
    before_trade = ~after_trade
    if np.any(before_trade):
        boundaries = clock_ts[before_trade]
        latest_source = np.full(boundaries.size, -1, dtype=np.int64)
        bootstrap_mid = np.full(boundaries.size, np.nan, dtype=np.float64)
        # Use exchange-time book state only as the non-fill clock price seed.
        # Strategy visibility still follows its independently delayed feeds.
        # Latest source wins; BBO wins a same-timestamp tie with L2.
        for data in (l2_data, bbo_data):
            if data is None or not len(data.ts_ms):
                continue
            source_ts = np.asarray(data.ts_ms, dtype=np.int64)
            if np.any(source_ts[1:] < source_ts[:-1]):
                raise ValueError("explicit replay start requires ordered BBO/L2 timestamps")
            indices = np.searchsorted(source_ts, boundaries, side="right") - 1
            available = indices >= 0
            safe_indices = np.maximum(indices, 0)
            if data is bbo_data:
                bid = np.asarray(data.best_bid)[safe_indices]
                ask = np.asarray(data.best_ask)[safe_indices]
            else:
                bid = np.asarray(data.bid_px)[safe_indices, 0]
                ask = np.asarray(data.ask_px)[safe_indices, 0]
            valid = (
                available
                & np.isfinite(bid)
                & np.isfinite(ask)
                & (bid > 0.0)
                & (ask >= bid)
                & (source_ts[safe_indices] >= latest_source)
            )
            bootstrap_mid[valid] = bid[valid] / 2.0 + ask[valid] / 2.0
            latest_source[valid] = source_ts[safe_indices[valid]]
        if initial_clock_price is not None:
            if not np.isfinite(initial_clock_price) or initial_clock_price <= 0:
                raise ValueError("initial clock price must be a finite positive retained price")
            bootstrap_mid[:] = float(initial_clock_price)
        if np.any(~np.isfinite(bootstrap_mid)) and not cold_flat_start:
            raise ValueError(
                "explicit replay start has no causal BBO/L2 midpoint before the first trade"
            )
        clock_price[before_trade] = bootstrap_mid
        if cold_flat_start:
            # No price, exposure or orders exist yet. Do not seed midnight
            # from a later snapshot/trade merely to materialize empty timers.
            priced = np.isfinite(clock_price)
            if np.any(np.isin(clock_action[~priced], (SYNC_EVENT_CODE, SYNC_CENSOR_CODE))):
                raise ValueError("cannot omit system events from an unpriced cold-start prefix")
            clock_ts, clock_action, clock_price = (
                clock_ts[priced],
                clock_action[priced],
                clock_price[priced],
            )

    combined_ts = np.concatenate((execution_ts, clock_ts))
    combined_price = np.concatenate((execution_price, clock_price))
    combined_qty = np.concatenate(
        (
            trades_df["quantity"].to_numpy(dtype=np.float64, copy=False),
            np.zeros(clock_ts.size, dtype=np.float64),
        )
    )
    combined_maker = np.concatenate(
        (
            trades_df["is_buyer_maker"].to_numpy(dtype=np.uint8, copy=False),
            clock_action,
        )
    )
    combined_execution = np.concatenate(
        (
            np.ones(n_execution_trades, dtype=np.bool_),
            np.zeros(clock_ts.size, dtype=np.bool_),
        )
    )
    order = np.argsort(combined_ts, kind="stable")
    events = pd.DataFrame(
        {
            "transact_time": combined_ts[order],
            "price": combined_price[order],
            "quantity": combined_qty[order],
            "is_buyer_maker": combined_maker[order],
            "_is_execution_trade": combined_execution[order],
        }
    )
    return events, n_execution_trades
