#!/usr/bin/env python3
"""Build causal, side-specific taker-flow features for maker decisions.

The execution-side mapping is explicit:

* a BUY maker order can be executed by an aggressive SELL taker;
* a SELL maker order can be executed by an aggressive BUY taker.

The builder consumes Binance public *individual trades* for exchange-time
diagnostics.  It can expose those events through a conservative right-edge
bucket (100 ms by default), then attach the latest completed state to each
order decision.  A trade in ``[t, t + bucket)`` is therefore unavailable until
the bucket's right edge.

This module builds research features only.  It does not choose an action or
change live quotes.  Historical files contain exchange timestamps rather than
the EC2 process receive timestamp, so zero-delay output is marked diagnostic.
An injected delay must carry a frozen latency-profile id, but it remains a
latency-stressed diagnostic: individual child events are not policy-visible
until mapped parent ``aggTrade`` readiness.  This v1 builder must never mark
its raw-child output policy eligible.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "side_taker_flow_panel.v1"
SOURCE_ID = "binance_futures_individual_trades"
DEFAULT_WINDOWS_MS = (100, 250, 500, 1_000, 5_000)

RAW_COLUMN_ALIASES = {
    "trade_id": ("id", "trade_id", "agg_trade_id"),
    "price": ("price",),
    "quantity": ("qty", "quantity"),
    "quote_quantity": ("quote_qty", "quote_quantity"),
    "event_ts": ("time", "transact_time", "timestamp"),
    "is_buyer_maker": ("is_buyer_maker",),
}

SIDE_METRICS = (
    "count",
    "quantity",
    "quote",
    "arrival_rate_per_s",
    "avg_quote",
    "max_run",
    "sweep_bps",
    "interarrival_mean_ms",
    "burst_ratio",
)

MECHANISM_REGISTRY: tuple[dict[str, str], ...] = (
    {
        "mechanism": "toxicity_heads",
        "current_boundary": "already_split",
        "current_detail": "tox_bid and tox_ask are separate model heads",
        "research_next": "retain separate calibration by maker side and inventory role",
    },
    {
        "mechanism": "markout_feedback",
        "current_boundary": "side_state_shared_parameters",
        "current_detail": "bid/ask EMA states are separate; span, scale, and sign are shared",
        "research_next": "fit BUY/SELL response scale and decay independently",
    },
    {
        "mechanism": "adverse_guard",
        "current_boundary": "side_state_shared_parameters",
        "current_detail": "direction is side-aware; thresholds, pause decay, and widen are shared",
        "research_next": "estimate side-specific hazard-to-action uplift before wiring thresholds",
    },
    {
        "mechanism": "defense_guard",
        "current_boundary": "side_state_shared_parameters",
        "current_detail": "reducing direction is side-aware; release thresholds are shared",
        "research_next": "split LONG repair SELL from SHORT repair BUY",
    },
    {
        "mechanism": "fill_selection",
        "current_boundary": "buy_only",
        "current_detail": "BUY scorer exists; no equivalent SELL policy artifact",
        "research_next": "train SELL repair-vs-trend-through target rather than reuse BUY labels",
    },
    {
        "mechanism": "p3_fill_hazard_and_effective_kappa",
        "current_boundary": "shared",
        "current_detail": "one effective-kappa/P3 floor feeds both quote sides",
        "research_next": "calibrate BUY/SELL by distance, regime, and inventory role",
    },
    {
        "mechanism": "queue_reactive_hawkes",
        "current_boundary": "already_split_model",
        "current_detail": "queue-value bundles fit BUY and SELL separately",
        "research_next": "add both counterparty and away-side taker intensities, not only adverse count",
    },
    {
        "mechanism": "empirical_microprice",
        "current_boundary": "already_split_model",
        "current_detail": "queue-value calibration fits a separate first-hit artifact per maker side",
        "research_next": "condition on side-specific taker shock, refill, and recovery paths",
    },
    {
        "mechanism": "fill_cooldown_and_rearm",
        "current_boundary": "side_state_shared_parameters",
        "current_detail": "BUY/SELL clocks are separate but use the same base and weights",
        "research_next": "fit separate BUY-add and SELL-add rearm hazards",
    },
    {
        "mechanism": "replace_keep_cancel",
        "current_boundary": "inventory_role_only",
        "current_detail": "increasing/reducing thresholds differ; BUY/SELL thresholds do not",
        "research_next": "estimate side-specific queue reset cost and counterparty hazard",
    },
    {
        "mechanism": "post_fill_response",
        "current_boundary": "partly_split",
        "current_detail": "adverse amplitude and add-distance fraction have BUY/SELL fields",
        "research_next": "fit side-specific response kernels from individual-trade paths",
    },
    {
        "mechanism": "depth_kappa_and_book_exhaustion",
        "current_boundary": "shared",
        "current_detail": "one depth ratio and BER multiplier are applied symmetrically",
        "research_next": "separate bid depletion from ask depletion and test symmetry",
    },
    {
        "mechanism": "dynamic_cap",
        "current_boundary": "shared_risk_ceiling",
        "current_detail": "variance-driven cap is global and rarely active in current evidence",
        "research_next": "keep volatility estimate shared; evaluate side-specific cap action only if active",
    },
    {
        "mechanism": "inventory_campaign",
        "current_boundary": "signed_symmetric",
        "current_detail": "the same eta and inventory penalty govern long and short campaigns",
        "research_next": "model LONG and SHORT repair/tail competing risks independently",
    },
    {
        "mechanism": "accounting_safety_latency",
        "current_boundary": "must_remain_shared",
        "current_detail": "cashflow, fees, tick/lot, stale guards, and operation latency are contracts",
        "research_next": "do not create BUY/SELL variants without exchange or systems evidence",
    },
)


@dataclass(frozen=True)
class TakerFlowIdentity:
    schema_version: str
    source_id: str
    resolution_ms: int
    windows_ms: tuple[int, ...]
    visibility_mode: str
    visibility_delay_ms: float
    latency_profile_id: str
    policy_eligible: bool


def _first_present(frame: pd.DataFrame, aliases: Sequence[str]) -> str:
    for name in aliases:
        if name in frame:
            return name
    raise ValueError(f"individual-trade input is missing one of {tuple(aliases)}")


def _numeric_series(
    frame: pd.DataFrame,
    name: str,
    default: float = 0.0,
) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def _day_bootstrap_mean_interval(
    values: np.ndarray,
    *,
    seed: int,
    draws: int = 2_000,
) -> tuple[float, float]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size < 2:
        return math.nan, math.nan
    rng = np.random.default_rng(int(seed))
    sampled = rng.choice(finite, size=(int(draws), finite.size), replace=True)
    means = sampled.mean(axis=1)
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def _epoch_ms(values: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(numeric).all() or (numeric <= 0.0).any():
        raise ValueError("trade timestamps must be finite and positive")
    out = numeric.astype(np.int64)
    magnitude = np.abs(out)
    nanosecond = magnitude >= 100_000_000_000_000_000
    microsecond = (magnitude >= 100_000_000_000_000) & ~nanosecond
    out[nanosecond] //= 1_000_000
    out[microsecond] //= 1_000
    return out


def _bool_array(values: pd.Series) -> np.ndarray:
    if pd.api.types.is_bool_dtype(values.dtype):
        return values.to_numpy(dtype=bool)
    normalized = values.astype(str).str.strip().str.lower()
    valid = normalized.isin({"true", "false", "1", "0", "t", "f"})
    if not bool(valid.all()):
        raise ValueError("is_buyer_maker contains unrecognized values")
    return normalized.isin({"true", "1", "t"}).to_numpy(dtype=bool)


def normalize_individual_trades(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a stable event-ordered individual-trade table."""

    if frame.empty:
        raise ValueError("individual-trade input is empty")
    resolved = {
        key: _first_present(frame, aliases)
        for key, aliases in RAW_COLUMN_ALIASES.items()
        if key != "quote_quantity"
    }
    quote_column = next(
        (name for name in RAW_COLUMN_ALIASES["quote_quantity"] if name in frame),
        None,
    )
    price = pd.to_numeric(frame[resolved["price"]], errors="coerce").to_numpy(dtype=float)
    quantity = pd.to_numeric(frame[resolved["quantity"]], errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(price).all() or not np.isfinite(quantity).all():
        raise ValueError("trade price/quantity must be finite")
    if (price <= 0.0).any() or (quantity <= 0.0).any():
        raise ValueError("trade price/quantity must be positive")
    quote = (
        pd.to_numeric(frame[quote_column], errors="coerce").to_numpy(dtype=float)
        if quote_column is not None
        else price * quantity
    )
    if not np.isfinite(quote).all() or (quote <= 0.0).any():
        raise ValueError("trade quote quantity must be finite and positive")

    buyer_is_maker = _bool_array(frame[resolved["is_buyer_maker"]])
    event_ts_ms = _epoch_ms(frame[resolved["event_ts"]])
    trade_id = pd.to_numeric(
        frame[resolved["trade_id"]], errors="coerce"
    ).to_numpy(dtype=float)
    fallback_id = np.arange(len(frame), dtype=np.int64)
    trade_id = np.where(np.isfinite(trade_id), trade_id, fallback_id).astype(np.int64)

    out = pd.DataFrame(
        {
            "trade_id": trade_id,
            "event_ts_ms": event_ts_ms,
            "price": price,
            "quantity": quantity,
            "quote_quantity": quote,
            "is_buyer_maker": buyer_is_maker,
            "_input_order": fallback_id,
        }
    )
    out.sort_values(
        ["event_ts_ms", "trade_id", "_input_order"],
        kind="stable",
        inplace=True,
    )
    out.reset_index(drop=True, inplace=True)
    side_code = np.where(out["is_buyer_maker"].to_numpy(), -1, 1).astype(np.int8)
    starts = np.ones(len(out), dtype=bool)
    if len(out) > 1:
        starts[1:] = side_code[1:] != side_code[:-1]
    groups = np.cumsum(starts)
    run = pd.Series(groups).groupby(groups, sort=False).cumcount().to_numpy() + 1
    out["taker_side"] = np.where(side_code > 0, "BUY", "SELL")
    out["side_code"] = side_code
    out["same_side_run"] = run.astype(np.int32)
    out["interarrival_ms"] = (
        out["event_ts_ms"].diff().fillna(0.0).clip(lower=0.0).astype(float)
    )
    out["side_interarrival_ms"] = (
        out.groupby("taker_side", sort=False)["event_ts_ms"]
        .diff()
        .fillna(0.0)
        .clip(lower=0.0)
        .astype(float)
    )
    return out.drop(columns="_input_order")


def _validate_identity(
    *,
    resolution_ms: int,
    windows_ms: Sequence[int],
    visibility_mode: str,
    visibility_delay_ms: float,
    latency_profile_id: str,
) -> TakerFlowIdentity:
    if resolution_ms <= 0:
        raise ValueError("resolution_ms must be positive")
    normalized_windows = tuple(sorted({int(value) for value in windows_ms}))
    if not normalized_windows or normalized_windows[0] < resolution_ms:
        raise ValueError("windows_ms must be non-empty and at least resolution_ms")
    if visibility_delay_ms < 0.0 or not math.isfinite(float(visibility_delay_ms)):
        raise ValueError("visibility_delay_ms must be finite and non-negative")
    mode = str(visibility_mode).strip().lower()
    if mode not in {"exchange_time_diagnostic", "fixed_delay_replay"}:
        raise ValueError(
            "visibility_mode must be exchange_time_diagnostic or fixed_delay_replay"
        )
    profile = str(latency_profile_id).strip()
    if mode == "fixed_delay_replay" and not profile:
        raise ValueError("fixed_delay_replay requires latency_profile_id")
    return TakerFlowIdentity(
        schema_version=SCHEMA_VERSION,
        source_id=SOURCE_ID,
        resolution_ms=int(resolution_ms),
        windows_ms=normalized_windows,
        visibility_mode=mode,
        visibility_delay_ms=float(visibility_delay_ms),
        latency_profile_id=profile,
        # A fixed delay does not reproduce aggregate-message visibility.  The
        # mapped-aggTrade successor owns the policy-eligible contract.
        policy_eligible=False,
    )


def _aggregate_side_columns(events: pd.DataFrame, side: str) -> pd.DataFrame:
    side_events = events[events["taker_side"] == side]
    if side_events.empty:
        return pd.DataFrame(
            columns=(
                "count",
                "quantity",
                "quote",
                "max_run",
                "price_high",
                "price_low",
                "last_event_ts_ms",
                "interarrival_sum_ms",
                "interarrival_observations",
            )
        )
    grouped = side_events.groupby("feature_ready_ts_ms", sort=True)
    return grouped.agg(
        count=("trade_id", "size"),
        quantity=("quantity", "sum"),
        quote=("quote_quantity", "sum"),
        max_run=("same_side_run", "max"),
        price_high=("price", "max"),
        price_low=("price", "min"),
        last_event_ts_ms=("visible_event_ts_ms", "max"),
        interarrival_sum_ms=("side_interarrival_ms", "sum"),
        interarrival_observations=("side_interarrival_ms", lambda values: int((values > 0).sum())),
    )


def build_causal_taker_state(
    trades: pd.DataFrame,
    *,
    day: str,
    resolution_ms: int = 100,
    windows_ms: Sequence[int] = DEFAULT_WINDOWS_MS,
    visibility_mode: str = "exchange_time_diagnostic",
    visibility_delay_ms: float = 0.0,
    latency_profile_id: str = "",
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> tuple[pd.DataFrame, TakerFlowIdentity]:
    """Aggregate individual trades into a causal right-edge state stream."""

    identity = _validate_identity(
        resolution_ms=resolution_ms,
        windows_ms=windows_ms,
        visibility_mode=visibility_mode,
        visibility_delay_ms=visibility_delay_ms,
        latency_profile_id=latency_profile_id,
    )
    events = normalize_individual_trades(trades)
    delay_ms = int(math.ceil(identity.visibility_delay_ms))
    events["visible_event_ts_ms"] = events["event_ts_ms"] + delay_ms
    events["feature_ready_ts_ms"] = (
        (events["visible_event_ts_ms"] // identity.resolution_ms) + 1
    ) * identity.resolution_ms

    day_start_ms = int(pd.Timestamp(str(day), tz="UTC").timestamp() * 1_000)
    day_end_ms = day_start_ms + 86_400_000
    max_window = max(identity.windows_ms)
    requested_start = day_start_ms if start_ms is None else max(day_start_ms, int(start_ms))
    requested_end = day_end_ms - 1 if end_ms is None else min(day_end_ms - 1, int(end_ms))
    if requested_end < requested_start:
        raise ValueError("state end precedes state start")
    grid_start = ((max(day_start_ms, requested_start - max_window) // identity.resolution_ms) + 1) * identity.resolution_ms
    grid_end = (requested_end // identity.resolution_ms) * identity.resolution_ms
    if grid_end < grid_start:
        raise ValueError("requested state interval has no completed bucket")

    events = events[
        (events["feature_ready_ts_ms"] >= grid_start)
        & (events["feature_ready_ts_ms"] <= grid_end)
    ].copy()
    if events.empty:
        raise ValueError(f"{day} has no visible individual trades in the requested interval")

    grid = pd.Index(
        np.arange(
            grid_start,
            grid_end + identity.resolution_ms,
            identity.resolution_ms,
            dtype=np.int64,
        ),
        name="feature_ready_ts_ms",
    )
    side_frames: dict[str, pd.DataFrame] = {}
    for side in ("buy", "sell"):
        aggregate = _aggregate_side_columns(events, side.upper())
        aggregate = aggregate.reindex(grid)
        for column in (
            "count",
            "quantity",
            "quote",
            "max_run",
            "interarrival_sum_ms",
            "interarrival_observations",
        ):
            aggregate[column] = aggregate[column].fillna(0.0)
        aggregate["last_event_ts_ms"] = aggregate["last_event_ts_ms"].ffill()
        side_frames[side] = aggregate

    all_grouped = events.groupby("feature_ready_ts_ms", sort=True).agg(
        trade_price=("price", "last"),
        last_event_ts_ms=("visible_event_ts_ms", "max"),
        last_side=("taker_side", "last"),
        last_run=("same_side_run", "last"),
    ).reindex(grid)
    all_grouped["trade_price"] = all_grouped["trade_price"].ffill()
    all_grouped["last_event_ts_ms"] = all_grouped["last_event_ts_ms"].ffill()
    all_grouped["last_side"] = all_grouped["last_side"].ffill()
    all_grouped["last_run"] = all_grouped["last_run"].ffill().fillna(0.0)

    state_columns: dict[str, Any] = {
        "day": np.full(len(grid), str(day), dtype=object),
        "feature_ready_ts_ns": grid.to_numpy(dtype=np.int64) * 1_000_000,
        "taker_last_event_age_ms": (
            grid.to_numpy(dtype=float)
            - pd.to_numeric(
                all_grouped["last_event_ts_ms"], errors="coerce"
            ).to_numpy(dtype=float)
        ),
        "taker_last_side": all_grouped["last_side"].fillna("NONE").astype(str).to_numpy(),
        "taker_last_run": pd.to_numeric(
            all_grouped["last_run"], errors="coerce"
        ).fillna(0.0).to_numpy(dtype=float),
    }
    last_side_values = state_columns["taker_last_side"]
    last_run_values = state_columns["taker_last_run"]
    for side, aggregate in side_frames.items():
        last_event = pd.to_numeric(aggregate["last_event_ts_ms"], errors="coerce")
        state_columns[f"{side}_taker_last_event_age_ms"] = (
            grid.to_numpy(dtype=float) - last_event.to_numpy(dtype=float)
        )
        state_columns[f"{side}_taker_current_run"] = np.where(
            last_side_values == side.upper(),
            last_run_values,
            0.0,
        )

    for window_ms in identity.windows_ms:
        buckets = int(math.ceil(window_ms / identity.resolution_ms))
        for side, aggregate in side_frames.items():
            count = aggregate["count"].rolling(buckets, min_periods=1).sum()
            quantity = aggregate["quantity"].rolling(buckets, min_periods=1).sum()
            quote = aggregate["quote"].rolling(buckets, min_periods=1).sum()
            run = aggregate["max_run"].rolling(buckets, min_periods=1).max()
            high = aggregate["price_high"].rolling(buckets, min_periods=1).max()
            low = aggregate["price_low"].rolling(buckets, min_periods=1).min()
            interarrival_sum = aggregate["interarrival_sum_ms"].rolling(
                buckets, min_periods=1
            ).sum()
            interarrival_count = aggregate["interarrival_observations"].rolling(
                buckets, min_periods=1
            ).sum()
            sweep_mid = 0.5 * (high + low)
            state_columns[f"{side}_taker_count_{window_ms}ms"] = count.to_numpy(dtype=float)
            state_columns[f"{side}_taker_quantity_{window_ms}ms"] = quantity.to_numpy(dtype=float)
            state_columns[f"{side}_taker_quote_{window_ms}ms"] = quote.to_numpy(dtype=float)
            state_columns[f"{side}_taker_arrival_rate_per_s_{window_ms}ms"] = (
                count.to_numpy(dtype=float) / (window_ms / 1_000.0)
            )
            state_columns[f"{side}_taker_avg_quote_{window_ms}ms"] = np.divide(
                quote,
                count,
                out=np.zeros(len(grid), dtype=float),
                where=count.to_numpy(dtype=float) > 0.0,
            )
            state_columns[f"{side}_taker_max_run_{window_ms}ms"] = run.to_numpy(dtype=float)
            state_columns[f"{side}_taker_sweep_bps_{window_ms}ms"] = (
                ((high - low) / sweep_mid * 10_000.0)
                .replace([np.inf, -np.inf], np.nan)
                .fillna(0.0)
                .to_numpy(dtype=float)
            )
            state_columns[f"{side}_taker_interarrival_mean_ms_{window_ms}ms"] = np.divide(
                interarrival_sum,
                interarrival_count,
                out=np.zeros(len(grid), dtype=float),
                where=interarrival_count.to_numpy(dtype=float) > 0.0,
            )

        buy_quote = state_columns[f"buy_taker_quote_{window_ms}ms"]
        sell_quote = state_columns[f"sell_taker_quote_{window_ms}ms"]
        total_quote = buy_quote + sell_quote
        state_columns[f"taker_quote_imbalance_{window_ms}ms"] = np.divide(
            buy_quote - sell_quote,
            total_quote,
            out=np.zeros(len(grid), dtype=float),
            where=total_quote > 0.0,
        )
        shifted_price = all_grouped["trade_price"].shift(buckets)
        state_columns[f"trade_price_move_bps_{window_ms}ms"] = (
            (all_grouped["trade_price"] / shifted_price - 1.0) * 10_000.0
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0).to_numpy(dtype=float)

    long_window_ms = max(identity.windows_ms)
    for side in ("buy", "sell"):
        long_rate = state_columns[
            f"{side}_taker_arrival_rate_per_s_{long_window_ms}ms"
        ]
        for window_ms in identity.windows_ms:
            rate = state_columns[f"{side}_taker_arrival_rate_per_s_{window_ms}ms"]
            state_columns[f"{side}_taker_burst_ratio_{window_ms}ms"] = np.divide(
                rate,
                long_rate,
                out=np.zeros(len(grid), dtype=float),
                where=long_rate > 0.0,
            )

    state_columns.update(
        {
            "taker_feature_resolution_ms": np.full(
                len(grid), identity.resolution_ms, dtype=np.int64
            ),
            "taker_feature_source": np.full(
                len(grid), identity.source_id, dtype=object
            ),
            "taker_visibility_mode": np.full(
                len(grid), identity.visibility_mode, dtype=object
            ),
            "taker_latency_profile_id": np.full(
                len(grid), identity.latency_profile_id, dtype=object
            ),
            "taker_policy_eligible": np.full(
                len(grid), int(identity.policy_eligible), dtype=np.int8
            ),
        }
    )
    state = pd.DataFrame(state_columns)
    numeric = state.select_dtypes(include=[np.number]).columns
    state[numeric] = state[numeric].replace([np.inf, -np.inf], np.nan)
    return state, identity


def _mapped_column(
    frame: pd.DataFrame,
    *,
    maker_buy: np.ndarray,
    metric: str,
    window_ms: int,
    counterparty: bool,
) -> np.ndarray:
    buy_values = pd.to_numeric(
        frame[f"buy_taker_{metric}_{window_ms}ms"], errors="coerce"
    ).fillna(0.0).to_numpy(dtype=float)
    sell_values = pd.to_numeric(
        frame[f"sell_taker_{metric}_{window_ms}ms"], errors="coerce"
    ).fillna(0.0).to_numpy(dtype=float)
    if counterparty:
        return np.where(maker_buy, sell_values, buy_values)
    return np.where(maker_buy, buy_values, sell_values)


def attach_side_taker_features(
    decisions: pd.DataFrame,
    state: pd.DataFrame,
    *,
    windows_ms: Sequence[int] = DEFAULT_WINDOWS_MS,
) -> pd.DataFrame:
    """Attach latest completed taker state and maker-side semantic aliases."""

    required = {"day", "side", "decision_ts_ns"}
    missing = sorted(required - set(decisions.columns))
    if missing:
        raise ValueError(f"decision panel is missing columns: {missing}")
    state_required = {"day", "feature_ready_ts_ns"}
    missing_state = sorted(state_required - set(state.columns))
    if missing_state:
        raise ValueError(f"taker state is missing columns: {missing_state}")
    sides = decisions["side"].astype(str).str.upper()
    if set(sides) - {"BUY", "SELL"}:
        raise ValueError("decision side must be BUY or SELL")

    parts: list[pd.DataFrame] = []
    working = decisions.copy()
    working["_source_order"] = np.arange(len(working), dtype=np.int64)
    renamed_state = state.rename(
        columns={"feature_ready_ts_ns": "taker_feature_ready_ts_ns"}
    )
    state_columns = [column for column in renamed_state.columns if column != "day"]
    for day, day_decisions in working.groupby("day", sort=False):
        day_state = renamed_state[
            renamed_state["day"].astype(str) == str(day)
        ].copy()
        if day_state.empty:
            raise ValueError(f"no taker state rows for decision day {day}")
        left = day_decisions.sort_values("decision_ts_ns", kind="stable")
        right = day_state.sort_values(
            "taker_feature_ready_ts_ns", kind="stable"
        )
        merged = pd.merge_asof(
            left,
            right[state_columns],
            left_on="decision_ts_ns",
            right_on="taker_feature_ready_ts_ns",
            direction="backward",
            allow_exact_matches=True,
        )
        parts.append(merged)
    output = pd.concat(parts, ignore_index=True).sort_values(
        "_source_order", kind="stable"
    ).drop(columns="_source_order").reset_index(drop=True)
    ready = pd.to_numeric(output["taker_feature_ready_ts_ns"], errors="coerce")
    decision_ts = pd.to_numeric(output["decision_ts_ns"], errors="coerce")
    if (ready.notna() & ready.gt(decision_ts)).any():
        raise ValueError("taker feature state contains future observations")
    maker_buy = output["side"].astype(str).str.upper().eq("BUY").to_numpy()
    semantic: dict[str, Any] = {
        "taker_feature_available": ready.notna().astype(int).to_numpy(),
        "taker_feature_age_ms": ((decision_ts - ready) / 1_000_000.0).to_numpy(),
        "counterparty_taker_side": np.where(maker_buy, "SELL", "BUY"),
        "away_taker_side": np.where(maker_buy, "BUY", "SELL"),
    }
    for window_ms in sorted({int(value) for value in windows_ms}):
        for metric in SIDE_METRICS:
            semantic[f"counterparty_taker_{metric}_{window_ms}ms"] = _mapped_column(
                output,
                maker_buy=maker_buy,
                metric=metric,
                window_ms=window_ms,
                counterparty=True,
            )
            semantic[f"away_taker_{metric}_{window_ms}ms"] = _mapped_column(
                output,
                maker_buy=maker_buy,
                metric=metric,
                window_ms=window_ms,
                counterparty=False,
            )
        counterparty_quote = semantic[
            f"counterparty_taker_quote_{window_ms}ms"
        ]
        away_quote = semantic[f"away_taker_quote_{window_ms}ms"]
        total_quote = counterparty_quote + away_quote
        semantic[f"counterparty_taker_share_{window_ms}ms"] = np.divide(
            counterparty_quote,
            total_quote,
            out=np.zeros(len(output), dtype=float),
            where=total_quote > 0.0,
        )
        semantic[f"net_counterparty_pressure_{window_ms}ms"] = np.divide(
            counterparty_quote - away_quote,
            total_quote,
            out=np.zeros(len(output), dtype=float),
            where=total_quote > 0.0,
        )
        move = pd.to_numeric(
            output[f"trade_price_move_bps_{window_ms}ms"], errors="coerce"
        ).fillna(0.0).to_numpy(dtype=float)
        semantic[f"maker_adverse_trade_move_bps_{window_ms}ms"] = np.where(
            maker_buy,
            -move,
            move,
        )

    buy_age = pd.to_numeric(
        output["buy_taker_last_event_age_ms"], errors="coerce"
    ).to_numpy(dtype=float)
    sell_age = pd.to_numeric(
        output["sell_taker_last_event_age_ms"], errors="coerce"
    ).to_numpy(dtype=float)
    buy_run = pd.to_numeric(
        output["buy_taker_current_run"], errors="coerce"
    ).fillna(0.0).to_numpy(dtype=float)
    sell_run = pd.to_numeric(
        output["sell_taker_current_run"], errors="coerce"
    ).fillna(0.0).to_numpy(dtype=float)
    semantic["counterparty_taker_last_event_age_ms"] = np.where(
        maker_buy, sell_age, buy_age
    )
    semantic["counterparty_taker_current_run"] = np.where(
        maker_buy, sell_run, buy_run
    )
    collisions = sorted(set(semantic) & set(output.columns))
    if collisions:
        raise ValueError(f"decision panel already contains taker semantic columns: {collisions}")
    return pd.concat(
        [output, pd.DataFrame(semantic, index=output.index)],
        axis=1,
    )


def summarize_side_taker_panel(
    frame: pd.DataFrame,
    *,
    windows_ms: Sequence[int] = DEFAULT_WINDOWS_MS,
    outcome_columns: Sequence[str] = (
        "fill_value_markout_bps",
        "event_adverse_fill",
        "event_favorable_fill",
    ),
) -> dict[str, Any]:
    """Describe BUY/SELL behavior without treating association as uplift."""

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "rows": int(len(frame)),
        "days": sorted(frame["day"].astype(str).unique()) if "day" in frame else [],
        "mechanism_registry": list(MECHANISM_REGISTRY),
        "sides": {},
        "interpretation": (
            "descriptive side sorting only; it is not an action counterfactual or policy uplift"
        ),
    }
    paired_market_states = frame.copy()
    if "taker_feature_available" in paired_market_states:
        paired_market_states = paired_market_states[
            pd.to_numeric(
                paired_market_states["taker_feature_available"],
                errors="coerce",
            ).fillna(0.0) > 0.0
        ]
    if "taker_feature_ready_ts_ns" in paired_market_states:
        paired_market_states = paired_market_states.drop_duplicates(
            ["day", "taker_feature_ready_ts_ns"],
            keep="first",
        )
    paired_report: dict[str, Any] = {
        "rows": int(len(paired_market_states)),
        "denominator": "unique order-decision-visible market states",
        "windows": {},
    }
    for window_ms in sorted({int(value) for value in windows_ms}):
        window_report: dict[str, Any] = {}
        for metric in (
            "count",
            "quote",
            "max_run",
            "sweep_bps",
            "burst_ratio",
        ):
            buy_name = f"buy_taker_{metric}_{window_ms}ms"
            sell_name = f"sell_taker_{metric}_{window_ms}ms"
            if buy_name not in paired_market_states or sell_name not in paired_market_states:
                continue
            buy = pd.to_numeric(
                paired_market_states[buy_name], errors="coerce"
            )
            sell = pd.to_numeric(
                paired_market_states[sell_name], errors="coerce"
            )
            buy_mean = float(buy.mean()) if buy.notna().any() else math.nan
            sell_mean = float(sell.mean()) if sell.notna().any() else math.nan
            window_report[f"buy_mean_{metric}"] = buy_mean
            window_report[f"sell_mean_{metric}"] = sell_mean
            window_report[f"buy_minus_sell_{metric}"] = buy_mean - sell_mean
            window_report[f"buy_to_sell_{metric}_ratio"] = (
                buy_mean / sell_mean
                if math.isfinite(sell_mean) and abs(sell_mean) > 1e-12
                else math.nan
            )
            if "day" in paired_market_states:
                daily = pd.DataFrame(
                    {
                        "day": paired_market_states["day"].astype(str),
                        "buy": buy,
                        "sell": sell,
                    }
                ).groupby("day", sort=True)[["buy", "sell"]].mean()
                daily["difference"] = daily["buy"] - daily["sell"]
                daily["ratio"] = np.divide(
                    daily["buy"],
                    daily["sell"],
                    out=np.full(len(daily), np.nan, dtype=float),
                    where=np.abs(daily["sell"].to_numpy(dtype=float)) > 1e-12,
                )
                seed_material = f"{window_ms}:{metric}".encode()
                seed = int.from_bytes(
                    hashlib.sha256(seed_material).digest()[:8], "big"
                )
                lower, upper = _day_bootstrap_mean_interval(
                    daily["difference"].to_numpy(dtype=float),
                    seed=seed,
                )
                window_report[f"{metric}_paired_days"] = int(len(daily))
                window_report[f"{metric}_buy_gt_sell_day_rate"] = float(
                    daily["difference"].gt(0.0).mean()
                )
                window_report[f"{metric}_daily_ratio_median"] = float(
                    daily["ratio"].median()
                )
                window_report[f"{metric}_daily_difference_ci95"] = [
                    lower,
                    upper,
                ]
        paired_report["windows"][str(window_ms)] = window_report
    report["paired_market_states"] = paired_report

    for side in ("BUY", "SELL"):
        side_frame = frame[frame["side"].astype(str).str.upper() == side].copy()
        side_report: dict[str, Any] = {
            "rows": int(len(side_frame)),
            "days": int(side_frame["day"].astype(str).nunique()) if "day" in side_frame else 0,
            "filled_rows": int(
                _numeric_series(side_frame, "fill_ts_ns").fillna(0.0).gt(0.0).sum()
            ) if len(side_frame) else 0,
            "adverse_fill_events": int(
                _numeric_series(side_frame, "event_adverse_fill").fillna(0.0).sum()
            ) if len(side_frame) else 0,
            "favorable_fill_events": int(
                _numeric_series(side_frame, "event_favorable_fill").fillna(0.0).sum()
            ) if len(side_frame) else 0,
            "feature_available_rate": float(
                _numeric_series(side_frame, "taker_feature_available").fillna(0.0).mean()
            ) if len(side_frame) else 0.0,
            "windows": {},
        }
        for window_ms in sorted({int(value) for value in windows_ms}):
            pressure_column = f"net_counterparty_pressure_{window_ms}ms"
            window_report: dict[str, Any] = {}
            for name in (
                f"counterparty_taker_quote_{window_ms}ms",
                f"counterparty_taker_share_{window_ms}ms",
                f"counterparty_taker_max_run_{window_ms}ms",
                f"counterparty_taker_sweep_bps_{window_ms}ms",
                f"maker_adverse_trade_move_bps_{window_ms}ms",
            ):
                values = pd.to_numeric(side_frame.get(name), errors="coerce")
                window_report[f"mean_{name}"] = (
                    float(values.mean()) if values.notna().any() else math.nan
                )
            pressure = pd.to_numeric(
                side_frame.get(pressure_column), errors="coerce"
            )
            for outcome_column in outcome_columns:
                if outcome_column not in side_frame:
                    continue
                outcome = pd.to_numeric(
                    side_frame[outcome_column], errors="coerce"
                )
                valid = pressure.notna() & outcome.notna()
                if outcome_column == "fill_value_markout_bps" and "fill_ts_ns" in side_frame:
                    valid &= pd.to_numeric(
                        side_frame["fill_ts_ns"], errors="coerce"
                    ).fillna(0.0).gt(0.0)
                    if "fill_value_horizon_censored" in side_frame:
                        valid &= pd.to_numeric(
                            side_frame["fill_value_horizon_censored"],
                            errors="coerce",
                        ).fillna(1.0).eq(0.0)
                if int(valid.sum()) < 20:
                    continue
                ranks = pressure[valid].rank(method="average")
                outcome_ranks = outcome[valid].rank(method="average")
                correlation = (
                    ranks.corr(outcome_ranks)
                    if ranks.nunique() > 1 and outcome_ranks.nunique() > 1
                    else math.nan
                )
                low = pressure[valid].quantile(0.2)
                high = pressure[valid].quantile(0.8)
                low_mean = outcome[valid & pressure.le(low)].mean()
                high_mean = outcome[valid & pressure.ge(high)].mean()
                window_report[f"{outcome_column}_spearman"] = float(correlation)
                window_report[f"{outcome_column}_high_minus_low"] = float(
                    high_mean - low_mean
                )
                window_report[f"{outcome_column}_rows"] = int(valid.sum())
                if "day" in side_frame:
                    daily_effects: list[float] = []
                    valid_frame = pd.DataFrame(
                        {
                            "day": side_frame.loc[valid, "day"].astype(str),
                            "pressure": pressure[valid],
                            "outcome": outcome[valid],
                        }
                    )
                    for _, daily_frame in valid_frame.groupby("day", sort=True):
                        if len(daily_frame) < 5 or daily_frame["pressure"].nunique() < 2:
                            continue
                        daily_low = daily_frame["pressure"].quantile(0.2)
                        daily_high = daily_frame["pressure"].quantile(0.8)
                        daily_low_mean = daily_frame.loc[
                            daily_frame["pressure"].le(daily_low), "outcome"
                        ].mean()
                        daily_high_mean = daily_frame.loc[
                            daily_frame["pressure"].ge(daily_high), "outcome"
                        ].mean()
                        if math.isfinite(daily_low_mean) and math.isfinite(
                            daily_high_mean
                        ):
                            daily_effects.append(
                                float(daily_high_mean - daily_low_mean)
                            )
                    seed_material = (
                        f"{side}:{window_ms}:{outcome_column}:daily-high-low"
                    ).encode()
                    seed = int.from_bytes(
                        hashlib.sha256(seed_material).digest()[:8], "big"
                    )
                    daily_lower, daily_upper = _day_bootstrap_mean_interval(
                        np.asarray(daily_effects, dtype=float),
                        seed=seed,
                    )
                    window_report[
                        f"{outcome_column}_daily_effect_days"
                    ] = int(len(daily_effects))
                    window_report[
                        f"{outcome_column}_positive_day_rate"
                    ] = (
                        float(np.mean(np.asarray(daily_effects) > 0.0))
                        if daily_effects
                        else math.nan
                    )
                    window_report[
                        f"{outcome_column}_daily_high_minus_low_ci95"
                    ] = [daily_lower, daily_upper]
            side_report["windows"][str(window_ms)] = window_report
        report["sides"][side] = side_report
    return report


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_frame(path: Path) -> pd.DataFrame:
    path = Path(path).expanduser().resolve()
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    path = Path(path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)


def build_panel_from_files(
    panel: pd.DataFrame,
    *,
    raw_trades_dir: Path,
    symbol: str,
    resolution_ms: int,
    windows_ms: Sequence[int],
    visibility_mode: str,
    visibility_delay_ms: float,
    latency_profile_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    parts: list[pd.DataFrame] = []
    source_files: list[dict[str, Any]] = []
    identities: list[TakerFlowIdentity] = []
    raw_root = Path(raw_trades_dir).expanduser().resolve()
    for day, day_panel in panel.groupby("day", sort=True):
        candidates = [
            raw_root / f"{symbol}-trades-{day}.csv",
            raw_root / f"{symbol}-trades-{day}.csv.gz",
        ]
        raw_path = next((path for path in candidates if path.exists()), None)
        if raw_path is None:
            raise FileNotFoundError(f"missing individual trades for {day}: {candidates}")
        raw = pd.read_csv(raw_path)
        decision_ms = pd.to_numeric(
            day_panel["decision_ts_ns"], errors="coerce"
        ).to_numpy(dtype=np.int64) // 1_000_000
        state, identity = build_causal_taker_state(
            raw,
            day=str(day),
            resolution_ms=resolution_ms,
            windows_ms=windows_ms,
            visibility_mode=visibility_mode,
            visibility_delay_ms=visibility_delay_ms,
            latency_profile_id=latency_profile_id,
            start_ms=int(decision_ms.min()),
            end_ms=int(decision_ms.max()),
        )
        parts.append(
            attach_side_taker_features(
                day_panel,
                state,
                windows_ms=identity.windows_ms,
            )
        )
        identities.append(identity)
        source_files.append(
            {
                "day": str(day),
                "path": str(raw_path),
                "sha256": _sha256(raw_path),
                "rows": int(len(raw)),
            }
        )
    if not identities:
        raise ValueError("decision panel contains no days")
    if len({json.dumps(asdict(value), sort_keys=True) for value in identities}) != 1:
        raise ValueError("daily taker-flow identities do not match")
    output = pd.concat(parts, ignore_index=True)
    identity = identities[0]
    builder_path = Path(__file__).resolve()
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "identity": asdict(identity),
        "builder": {
            "path": str(builder_path),
            "sha256": _sha256(builder_path),
        },
        "source_files": source_files,
        "mechanism_registry": list(MECHANISM_REGISTRY),
    }
    return output, metadata


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-panel", type=Path, required=True)
    parser.add_argument("--raw-trades-dir", type=Path, required=True)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, default=None)
    parser.add_argument("--resolution-ms", type=int, default=100)
    parser.add_argument(
        "--windows-ms",
        nargs="+",
        type=int,
        default=list(DEFAULT_WINDOWS_MS),
    )
    parser.add_argument(
        "--visibility-mode",
        choices=("exchange_time_diagnostic", "fixed_delay_replay"),
        default="exchange_time_diagnostic",
    )
    parser.add_argument("--visibility-delay-ms", type=float, default=0.0)
    parser.add_argument("--latency-profile-id", default="")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    panel_path = args.input_panel.expanduser().resolve()
    panel = _read_frame(panel_path)
    output, metadata = build_panel_from_files(
        panel,
        raw_trades_dir=args.raw_trades_dir,
        symbol=str(args.symbol).upper(),
        resolution_ms=int(args.resolution_ms),
        windows_ms=tuple(args.windows_ms),
        visibility_mode=args.visibility_mode,
        visibility_delay_ms=float(args.visibility_delay_ms),
        latency_profile_id=args.latency_profile_id,
    )
    output_path = args.output.expanduser().resolve()
    _write_frame(output, output_path)
    summary = summarize_side_taker_panel(output, windows_ms=args.windows_ms)
    summary.update(metadata)
    summary["input_panel"] = {
        "path": str(panel_path),
        "sha256": _sha256(panel_path),
    }
    summary["output_path"] = str(output_path)
    summary_path = (
        args.summary_output.expanduser().resolve()
        if args.summary_output is not None
        else output_path.with_suffix(".summary.json")
    )
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(output_path), "summary": str(summary_path)}, indent=2))


if __name__ == "__main__":
    main()
