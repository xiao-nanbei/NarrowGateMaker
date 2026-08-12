#!/usr/bin/env python3
"""Build the causal maker lifecycle M0/M1 development panel.

The input is an action-level replay panel with exactly one preregistered
exposure-increasing add decision per campaign.  Labels are attached directly
to that replay ``decision_id``; this builder never performs a side/time-nearest
order match.

M0 contains decision-time local exact-L2/queue state, causal local path state,
campaign-so-far state, and the Binance BTCUSDT perpetual bridge.  M1 adds
Bitget, Bybit, and OKX spot/perpetual trade-state features.  Historical
one-second external states remain development evidence: their right-edge
timestamp is shifted by a labelled AWS Tokyo receive/feature latency before
as-of alignment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f04_external_market_alpha.external_venue_features import (
    EXTERNAL_FACTORS,
    EXTERNAL_VENUES,
    build_binance_bridge_feature_grid_1s,
    build_external_feature_grid_1s,
)

SCHEMA_VERSION = "maker_lifecycle_panel.v1"
LATENCY_PROFILE_SCHEMA = "aws_tokyo_market_data_latency.v1"
DEFAULT_LATENCY_MODES = (
    "right_edge_ideal",
    "aws_tokyo_p50",
    "aws_tokyo_p95",
    "aws_tokyo_p99",
)
EXTERNAL_HORIZONS_S = (1, 3, 5)
TAIL_THRESHOLD_USDC = -5.0
TICK_SIZE = 0.1

REQUIRED_SOURCE_COLUMNS = {
    "day",
    "decision_id",
    "decision_ts_ns",
    "decision_ts_ms",
    "campaign_id",
    "side",
    "inventory_role",
    "action",
    "behavior_propensity",
    "base_price",
    "decision_mtm",
    "terminal_mtm",
    "terminal_campaign_pnl",
    "campaign_closed",
    "campaign_censored",
    "campaign_duration_s",
    "campaign_mae",
    "reward",
    "campaign_cost",
    "queue_cost",
    "intervention_fill_count",
    "intervention_fill_qty",
    "fill_markout_30s_bps",
}

M0_SOURCE_FEATURES = (
    "action",
    "inventory",
    "inventory_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "fill_cooldown_elapsed_ms",
    "fill_cooldown_total_ms",
    "fill_cooldown_remaining_ms",
    "fill_cooldown_consecutive_units",
    "mid",
    "best_bid",
    "best_ask",
    "base_price",
    "path_elapsed_ms",
    "path_book_age_ms",
    "path_l2_snapshot_count",
    "path_trade_count",
    "shock_adverse_flow_imbalance_1s",
    "shock_adverse_flow_imbalance_5s",
    "shock_adverse_qty_to_depth_5s",
    "shock_adverse_qty_to_depth_since_fill",
    "shock_adverse_move_bps",
    "shock_time_to_extreme_ms",
    "refill_depletion_ratio",
    "refill_recovery_ratio",
    "refill_current_vs_start_ratio",
    "refill_half_life_ms",
    "refill_half_life_observed",
    "recovery_current_adverse_bps",
    "recovery_from_extreme_bps",
    "recovery_price_ratio",
    "recovery_microprice_current_adverse_bps",
    "recovery_microprice_ratio",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _numeric(frame: pd.DataFrame, name: str, default: float = math.nan) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def validate_source_panel(frame: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_SOURCE_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"maker lifecycle source missing columns: {missing}")
    if frame.empty:
        raise ValueError("maker lifecycle source is empty")
    if frame["decision_id"].astype("string").isna().any():
        raise ValueError("decision_id is required on every row")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be globally unique")
    campaign_rows = frame.groupby(["day", "campaign_id"], dropna=False).size()
    if int(campaign_rows.max()) != 1:
        raise ValueError("each UTC-day campaign may contain only one intervention")
    if set(frame["inventory_role"].astype(str)) != {"add"}:
        raise ValueError("maker lifecycle v1 accepts exposure-increasing add decisions only")
    if set(frame["side"].astype(str).str.upper()) - {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    decision_ns = _numeric(frame, "decision_ts_ns")
    decision_ms = _numeric(frame, "decision_ts_ms")
    if decision_ns.isna().any() or decision_ms.isna().any():
        raise ValueError("decision timestamps must be complete")
    if not np.allclose(decision_ns, decision_ms * 1_000_000.0, atol=0.0, rtol=0.0):
        raise ValueError("decision_ts_ns and decision_ts_ms disagree")
    reward = _numeric(frame, "reward")
    terminal_delta = _numeric(frame, "terminal_mtm") - _numeric(frame, "decision_mtm")
    if not np.allclose(reward, terminal_delta, atol=1e-9, rtol=0.0, equal_nan=False):
        raise ValueError("reward is not the direct decision-to-terminal MTM delta")
    closed = _numeric(frame, "campaign_closed", 0.0).fillna(0).astype(int)
    censored = _numeric(frame, "campaign_censored", 0.0).fillna(0).astype(int)
    if not ((closed + censored) == 1).all():
        raise ValueError("campaign_closed and campaign_censored must be exclusive/exhaustive")


def add_lifecycle_targets(
    frame: pd.DataFrame,
    *,
    tail_threshold_usdc: float = TAIL_THRESHOLD_USDC,
) -> pd.DataFrame:
    output = frame.copy()
    terminal = _numeric(output, "terminal_campaign_pnl")
    closed = _numeric(output, "campaign_closed", 0.0).fillna(0).astype(int)
    censored = _numeric(output, "campaign_censored", 0.0).fillna(0).astype(int)
    tail = terminal.le(float(tail_threshold_usdc)).astype(int)
    output["target_decision_to_terminal_mtm"] = _numeric(output, "reward")
    output["target_incremental_campaign_cost"] = _numeric(output, "campaign_cost")
    output["target_repair"] = closed
    output["target_tail"] = tail
    output["target_censored"] = censored
    output["target_fill"] = _numeric(output, "intervention_fill_count", 0.0).gt(0).astype(int)
    output["target_fill_markout_1s_bps"] = np.nan
    output["target_fill_markout_5s_bps"] = np.nan
    output["target_fill_markout_20s_bps"] = np.nan
    output["target_fill_markout_30s_bps"] = _numeric(output, "fill_markout_30s_bps")
    output["target_queue_cost"] = _numeric(output, "queue_cost")
    output["target_fill_value"] = _numeric(output, "fill_value")
    output["target_campaign_duration_s"] = _numeric(output, "campaign_duration_s")
    output["target_campaign_mae"] = _numeric(output, "campaign_mae")
    output["lifecycle_event"] = np.select(
        [
            (censored == 1) & (tail == 1),
            (censored == 1) & (tail == 0),
            (closed == 1) & (tail == 1),
        ],
        ["censored_tail_mtm", "censored_non_tail", "repair_tail"],
        default="repair_non_tail",
    )
    output["tail_threshold_usdc"] = float(tail_threshold_usdc)
    output["label_identity"] = "direct_replay_decision_id"
    output["client_order_id"] = (
        output["client_order_id"].astype("string")
        if "client_order_id" in output
        else pd.Series(pd.NA, index=output.index, dtype="string")
    )
    return output


def load_latency_profile(path: Path) -> tuple[str, dict[str, dict[str, float]]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    profile_id = str(payload.get("profile_id", "unknown"))
    delays: dict[str, dict[str, float]] = {}
    for row in payload.get("groups", []):
        if str(row.get("event_type", "")).lower() != "trade":
            continue
        lag = row.get("visibility_lag_ms") or {}
        if not lag:
            continue
        delays[str(row["market_id"])] = {
            "aws_tokyo_p50": float(lag.get("median", 0.0) or 0.0),
            "aws_tokyo_p95": float(lag.get("p95", 0.0) or 0.0),
            "aws_tokyo_p99": float(
                lag.get("p99_for_spike_sampling_only", lag.get("p99", 0.0)) or 0.0
            ),
            "right_edge_ideal": 0.0,
        }
    return profile_id, delays


def _align_state(
    decision_ts_ns: np.ndarray,
    state: pd.DataFrame,
    *,
    delay_ms: float,
    columns: Sequence[str],
) -> tuple[pd.DataFrame, np.ndarray]:
    """Backward as-of align a causal state after its simulated visibility delay."""

    if not isinstance(state.index, pd.DatetimeIndex):
        raise ValueError("causal state requires a DatetimeIndex")
    state = state.sort_index()
    # Parquet may preserve ``datetime64[ms]``.  ``view(int64)`` would then
    # expose milliseconds while the decision clock is nanoseconds, producing
    # a causally ordered but catastrophically stale match.  Normalize the unit
    # explicitly before integer alignment.
    base_ready_ns = pd.DatetimeIndex(state.index).as_unit("ns").asi8
    ready_ns = base_ready_ns + int(round(max(0.0, float(delay_ms)) * 1_000_000.0))
    decisions = np.asarray(decision_ts_ns, dtype=np.int64)
    indices = np.searchsorted(ready_ns, decisions, side="right") - 1
    valid = indices >= 0
    safe = np.maximum(indices, 0)
    selected = state.iloc[safe][list(columns)].reset_index(drop=True)
    selected.loc[~valid, :] = np.nan
    selected_ready = np.where(valid, ready_ns[safe], 0).astype(np.int64)
    if np.any(selected_ready[valid] > decisions[valid]):
        raise ValueError("causal state alignment exposed a future feature")
    return selected, selected_ready


def _l2_columns(path: Path) -> tuple[list[str], list[str], list[str], list[str]]:
    columns = pd.read_parquet(path).columns
    levels = []
    for level in range(1, 101):
        names = (f"bid_px_{level}", f"bid_qty_{level}", f"ask_px_{level}", f"ask_qty_{level}")
        if all(name in columns for name in names):
            levels.append(level)
    if not levels:
        raise ValueError(f"{path}: no exact L2 levels")
    return (
        [f"bid_px_{level}" for level in levels],
        [f"bid_qty_{level}" for level in levels],
        [f"ask_px_{level}" for level in levels],
        [f"ask_qty_{level}" for level in levels],
    )


def enrich_local_exact_l2(day_frame: pd.DataFrame, l2_root: Path, day: str) -> pd.DataFrame:
    path = Path(l2_root) / "l2" / f"BTCUSDC-l2-{day}.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    bid_px_cols, bid_qty_cols, ask_px_cols, ask_qty_cols = _l2_columns(path)
    columns = ["timestamp", *bid_px_cols, *bid_qty_cols, *ask_px_cols, *ask_qty_cols]
    l2 = pd.read_parquet(path, columns=columns).sort_values("timestamp", kind="stable")
    l2 = l2.drop_duplicates("timestamp", keep="last")
    ts_ms = pd.to_numeric(l2["timestamp"], errors="coerce").to_numpy(dtype=np.int64)
    decisions_ms = _numeric(day_frame, "decision_ts_ms").to_numpy(dtype=np.int64)
    indices = np.searchsorted(ts_ms, decisions_ms, side="right") - 1
    valid = indices >= 0
    safe = np.maximum(indices, 0)
    bid_px = l2[bid_px_cols].to_numpy(dtype=float)[safe]
    bid_qty = l2[bid_qty_cols].to_numpy(dtype=float)[safe]
    ask_px = l2[ask_px_cols].to_numpy(dtype=float)[safe]
    ask_qty = l2[ask_qty_cols].to_numpy(dtype=float)[safe]
    best_bid = bid_px[:, 0]
    best_ask = ask_px[:, 0]
    top_bid_qty = bid_qty[:, 0]
    top_ask_qty = ask_qty[:, 0]
    mid = 0.5 * (best_bid + best_ask)
    top_total = top_bid_qty + top_ask_qty
    microprice = np.divide(
        best_ask * top_bid_qty + best_bid * top_ask_qty,
        top_total,
        out=mid.copy(),
        where=top_total > 0.0,
    )
    side = day_frame["side"].astype(str).str.upper().to_numpy()
    base_price = _numeric(day_frame, "base_price").to_numpy(dtype=float)
    queue_visible = np.zeros(len(day_frame), dtype=float)
    quote_level = np.zeros(len(day_frame), dtype=int)
    for row_index in range(len(day_frame)):
        prices = bid_px[row_index] if side[row_index] == "BUY" else ask_px[row_index]
        quantities = bid_qty[row_index] if side[row_index] == "BUY" else ask_qty[row_index]
        hits = np.flatnonzero(np.isclose(prices, base_price[row_index], atol=TICK_SIZE * 0.25, rtol=0.0))
        if hits.size:
            level = int(hits[0])
            quote_level[row_index] = level + 1
            queue_visible[row_index] = max(0.0, float(quantities[level]))
    distance_ticks = np.where(
        side == "BUY",
        (best_bid - base_price) / TICK_SIZE,
        (base_price - best_ask) / TICK_SIZE,
    )
    output = day_frame.copy()
    output["m0_l2_feature_ready_ts_ns"] = np.where(valid, ts_ms[safe] * 1_000_000, 0)
    output["m0_l2_age_ms"] = np.where(valid, decisions_ms - ts_ms[safe], np.nan)
    output["m0_l2_available"] = valid.astype(int)
    output["m0_l2_best_bid"] = best_bid
    output["m0_l2_best_ask"] = best_ask
    output["m0_l2_spread_bps"] = np.divide(best_ask - best_bid, mid, out=np.zeros_like(mid), where=mid > 0) * 1e4
    output["m0_l2_top_imbalance"] = np.divide(
        top_bid_qty - top_ask_qty,
        top_total,
        out=np.zeros_like(top_total),
        where=top_total > 0,
    )
    output["m0_l2_microprice_shift_bps"] = np.divide(
        microprice - mid, mid, out=np.zeros_like(mid), where=mid > 0
    ) * 1e4
    depth_levels = min(5, bid_qty.shape[1])
    output["m0_l2_bid_depth_5"] = bid_qty[:, :depth_levels].sum(axis=1)
    output["m0_l2_ask_depth_5"] = ask_qty[:, :depth_levels].sum(axis=1)
    output["m0_l2_queue_visible_qty"] = queue_visible
    output["m0_l2_quote_level"] = quote_level
    output["m0_l2_quote_level_found"] = (quote_level > 0).astype(int)
    output["m0_l2_quote_distance_ticks"] = distance_ticks
    if (output.loc[valid, "m0_l2_feature_ready_ts_ns"] > output.loc[valid, "decision_ts_ns"]).any():
        raise ValueError("exact L2 feature timestamp exceeds decision timestamp")
    return output


def _external_market_id(venue: str, factor: str) -> str:
    return f"{venue}:{'perp' if factor == 'perp' else 'spot'}:BTCUSDT"


def _advance_source_age_ms(
    source_age_at_base_ms: pd.Series,
    decision_ns: np.ndarray,
    ready_ns: np.ndarray,
    *,
    injected_delay_ms: float,
) -> pd.Series:
    """Advance source age from base feature time to decision visibility time."""
    elapsed_after_ready_ms = (decision_ns - ready_ns) / 1_000_000.0
    total_elapsed_ms = elapsed_after_ready_ms + float(injected_delay_ms)
    return source_age_at_base_ms + np.where(ready_ns > 0, total_elapsed_ms, np.nan)


def enrich_bridge_and_external(
    day_frame: pd.DataFrame,
    data_dir: Path,
    day: str,
    *,
    latency_mode: str,
    latency_delays: dict[str, dict[str, float]],
) -> pd.DataFrame:
    output = day_frame.copy().reset_index(drop=True)
    decision_ns = _numeric(output, "decision_ts_ns").to_numpy(dtype=np.int64)

    bridge = build_binance_bridge_feature_grid_1s(data_dir, day, prefix="m0_bridge")
    bridge_columns = list(bridge.columns)
    bridge_delay = latency_delays.get("binance:perp:BTCUSDT", {}).get(latency_mode, 0.0)
    aligned_bridge, bridge_ready = _align_state(
        decision_ns,
        bridge,
        delay_ms=bridge_delay,
        columns=bridge_columns,
    )
    for name in bridge_columns:
        output[name] = aligned_bridge[name].to_numpy()
    output["m0_bridge_feature_ready_ts_ns"] = bridge_ready
    output["m0_bridge_visibility_age_ms"] = np.where(
        bridge_ready > 0, (decision_ns - bridge_ready) / 1_000_000.0, np.nan
    )
    output["m0_bridge_basis_bps"] = (
        (_numeric(output, "m0_bridge_close") - _numeric(output, "mid"))
        / _numeric(output, "mid").replace(0.0, np.nan)
        * 1e4
    )

    external = build_external_feature_grid_1s(
        data_dir, day, horizons_s=EXTERNAL_HORIZONS_S
    )
    for venue in EXTERNAL_VENUES:
        for factor in EXTERNAL_FACTORS:
            prefix = f"cv_external_{venue}_{factor}_"
            columns = [name for name in external if name.startswith(prefix)]
            delay = latency_delays.get(_external_market_id(venue, factor), {}).get(
                latency_mode, 0.0
            )
            selected, ready = _align_state(
                decision_ns,
                external,
                delay_ms=delay,
                columns=columns,
            )
            target_prefix = f"m1_{venue}_{factor}_"
            for source_name in columns:
                output[target_prefix + source_name[len(prefix) :]] = selected[
                    source_name
                ].to_numpy()
            ready_name = f"m1_{venue}_{factor}_feature_ready_ts_ns"
            output[ready_name] = ready
            source_age_name = f"m1_{venue}_{factor}_source_age_ms"
            if source_age_name in output:
                output[source_age_name] = _advance_source_age_ms(
                    _numeric(output, source_age_name),
                    decision_ns,
                    ready,
                    injected_delay_ms=float(delay),
                )
    output["market_data_latency_mode"] = latency_mode
    return add_external_consensus(output, included_venues=EXTERNAL_VENUES, prefix="m1_full")


def add_external_consensus(
    frame: pd.DataFrame,
    *,
    included_venues: Sequence[str],
    prefix: str,
) -> pd.DataFrame:
    """Recompute consensus from aligned venue rows for true LOO evaluation."""

    venues = tuple(str(value) for value in included_venues)
    if len(venues) < 2 or set(venues) - set(EXTERNAL_VENUES):
        raise ValueError("external consensus requires two or three registered venues")
    output = frame.copy()
    decision_ns = _numeric(output, "decision_ts_ns")
    factor_returns: dict[tuple[str, int], pd.Series] = {}
    factor_flows: dict[tuple[str, int], pd.Series] = {}
    factor_ready: dict[str, pd.Series] = {}
    for factor in EXTERNAL_FACTORS:
        available = pd.concat(
            [
                _numeric(output, f"m1_{venue}_{factor}_available", 0.0).gt(0.5).rename(venue)
                for venue in venues
            ],
            axis=1,
        )
        source_ages = pd.concat(
            [
                _numeric(output, f"m1_{venue}_{factor}_source_age_ms").where(available[venue]).rename(venue)
                for venue in venues
            ],
            axis=1,
        )
        ready = pd.concat(
            [
                _numeric(output, f"m1_{venue}_{factor}_feature_ready_ts_ns").where(available[venue]).rename(venue)
                for venue in venues
            ],
            axis=1,
        )
        factor_ready[factor] = ready.max(axis=1, skipna=True).fillna(0.0)
        output[f"{prefix}_{factor}_fresh_venues"] = available.sum(axis=1)
        output[f"{prefix}_{factor}_max_source_age_ms"] = source_ages.max(axis=1, skipna=True)
        for horizon in EXTERNAL_HORIZONS_S:
            returns = pd.concat(
                [
                    _numeric(output, f"m1_{venue}_{factor}_ret_{horizon}s")
                    .where(available[venue])
                    .rename(venue)
                    for venue in venues
                ],
                axis=1,
            )
            flows = pd.concat(
                [
                    _numeric(output, f"m1_{venue}_{factor}_flow_{horizon}s")
                    .where(available[venue])
                    .rename(venue)
                    for venue in venues
                ],
                axis=1,
            )
            ret_median = returns.median(axis=1, skipna=True)
            flow_median = flows.median(axis=1, skipna=True)
            signs = np.sign(returns).where(returns.abs() > 1e-12)
            positive = signs.gt(0).sum(axis=1)
            negative = signs.lt(0).sum(axis=1)
            denominator = signs.notna().sum(axis=1).replace(0, np.nan)
            agreement = pd.concat([positive, negative], axis=1).max(axis=1) / denominator
            dispersion = returns.sub(ret_median, axis=0).abs().median(axis=1, skipna=True) * 1e4
            enough = available.sum(axis=1).ge(2)
            output[f"{prefix}_{factor}_ret_{horizon}s"] = ret_median.where(enough)
            output[f"{prefix}_{factor}_flow_{horizon}s"] = flow_median.where(enough)
            output[f"{prefix}_{factor}_agreement_{horizon}s"] = agreement.where(enough)
            output[f"{prefix}_{factor}_dispersion_{horizon}s_bps"] = dispersion.where(enough)
            factor_returns[(factor, horizon)] = ret_median.where(enough)
            factor_flows[(factor, horizon)] = flow_median.where(enough)
        confidence = (
            output[f"{prefix}_{factor}_fresh_venues"].clip(upper=len(venues)) / len(venues)
        ) * output[f"{prefix}_{factor}_agreement_1s"].fillna(0.0) * np.exp(
            -output[f"{prefix}_{factor}_max_source_age_ms"].fillna(10_000.0) / 2_000.0
        )
        output[f"{prefix}_{factor}_confidence"] = confidence
        output[f"{prefix}_{factor}_feature_ready_ts_ns"] = factor_ready[factor]

    for horizon in EXTERNAL_HORIZONS_S:
        spot_ret = factor_returns[("spot", horizon)]
        perp_ret = factor_returns[("perp", horizon)]
        output[f"{prefix}_perp_minus_spot_ret_{horizon}s"] = perp_ret - spot_ret
        output[f"{prefix}_spot_perp_agreement_{horizon}s"] = (
            np.sign(spot_ret).eq(np.sign(perp_ret)) & spot_ret.notna() & perp_ret.notna()
        ).astype(float)
        global_ret = pd.concat([spot_ret, perp_ret], axis=1).median(axis=1, skipna=True)
        output[f"{prefix}_global_minus_bridge_ret_{horizon}s"] = global_ret - _numeric(
            output, f"m0_bridge_ret_{horizon}s"
        )
        global_flow = pd.concat(
            [factor_flows[("spot", horizon)], factor_flows[("perp", horizon)]], axis=1
        ).median(axis=1, skipna=True)
        output[f"{prefix}_global_flow_{horizon}s"] = global_flow
    output[f"{prefix}_venue_divergence_bps"] = pd.concat(
        [
            _numeric(output, f"{prefix}_spot_dispersion_1s_bps"),
            _numeric(output, f"{prefix}_perp_dispersion_1s_bps"),
        ],
        axis=1,
    ).max(axis=1, skipna=True)
    output[f"{prefix}_feature_ready_ts_ns"] = pd.concat(
        [factor_ready["spot"], factor_ready["perp"]], axis=1
    ).max(axis=1, skipna=True)
    future = _numeric(output, f"{prefix}_feature_ready_ts_ns") > decision_ns
    if future.any():
        raise ValueError(f"{prefix} contains {int(future.sum())} future-visible rows")
    return output


def build_panel(
    source: pd.DataFrame,
    *,
    data_dir: Path,
    l2_root: Path | None = None,
    latency_profile_id: str,
    latency_delays: dict[str, dict[str, float]],
    latency_modes: Iterable[str] = DEFAULT_LATENCY_MODES,
    tail_threshold_usdc: float = TAIL_THRESHOLD_USDC,
) -> pd.DataFrame:
    validate_source_panel(source)
    resolved_l2_root = (
        Path(data_dir) / "normalized_l2_100ms_v2"
        if l2_root is None
        else Path(l2_root)
    )
    labelled = add_lifecycle_targets(source, tail_threshold_usdc=tail_threshold_usdc)
    labelled["side"] = labelled["side"].astype(str).str.upper()
    parts: list[pd.DataFrame] = []
    modes = tuple(dict.fromkeys(str(mode) for mode in latency_modes))
    unknown = set(modes) - set(DEFAULT_LATENCY_MODES)
    if unknown:
        raise ValueError(f"unknown latency modes: {sorted(unknown)}")
    for day, raw_day in labelled.groupby("day", sort=True):
        local = enrich_local_exact_l2(
            raw_day.reset_index(drop=True),
            resolved_l2_root,
            str(day),
        )
        for mode in modes:
            part = enrich_bridge_and_external(
                local,
                data_dir,
                str(day),
                latency_mode=mode,
                latency_delays=latency_delays,
            )
            part["schema_version"] = SCHEMA_VERSION
            part["latency_profile_id"] = latency_profile_id
            part["research_status"] = "development_only_not_untouched"
            parts.append(part)
    panel = pd.concat(parts, ignore_index=True, sort=False)
    expected = len(source) * len(modes)
    if len(panel) != expected:
        raise ValueError(f"panel row count mismatch: expected {expected}, got {len(panel)}")
    if panel.groupby(["market_data_latency_mode", "decision_id"]).size().max() != 1:
        raise ValueError("decision_id must be unique within each latency mode")
    timing_columns = [name for name in panel if name.endswith("feature_ready_ts_ns")]
    decision = _numeric(panel, "decision_ts_ns")
    for name in timing_columns:
        ready = _numeric(panel, name, 0.0)
        future = ready.gt(0) & ready.gt(decision)
        if future.any():
            raise ValueError(f"{name} has {int(future.sum())} future-visible rows")
    panel = panel.copy()
    panel["feature_timing_valid"] = 1
    return panel


def feature_provenance(panel: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in panel.columns:
        if name in M0_SOURCE_FEATURES:
            rows.append(
                {
                    "name": name,
                    "family": "m0_existing_decision_state",
                    "available_at": "decision",
                    "source_timestamp_col": "decision_ts_ns",
                }
            )
        elif name.startswith("m0_l2_") and not name.endswith("feature_ready_ts_ns"):
            rows.append(
                {
                    "name": name,
                    "family": "m0_exact_l2",
                    "available_at": "decision",
                    "source_timestamp_col": "m0_l2_feature_ready_ts_ns",
                }
            )
        elif name.startswith("m0_bridge_") and not name.endswith("feature_ready_ts_ns"):
            rows.append(
                {
                    "name": name,
                    "family": "m0_binance_bridge",
                    "available_at": "decision",
                    "source_timestamp_col": "m0_bridge_feature_ready_ts_ns",
                }
            )
        elif name.startswith("m1_") and not name.endswith("feature_ready_ts_ns"):
            pieces = name.split("_")
            if len(pieces) >= 4 and pieces[1] in EXTERNAL_VENUES and pieces[2] in EXTERNAL_FACTORS:
                source_ts = f"m1_{pieces[1]}_{pieces[2]}_feature_ready_ts_ns"
            else:
                source_ts = "m1_full_feature_ready_ts_ns"
            rows.append(
                {
                    "name": name,
                    "family": "m1_external_trade_state",
                    "available_at": "decision",
                    "source_timestamp_col": source_ts,
                }
            )
    return rows


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        frame.to_parquet(path, index=False)
    elif path.suffix.lower() == ".csv":
        frame.to_csv(path, index=False)
    else:
        raise ValueError("output panel must end in .parquet or .csv")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-panel", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument(
        "--l2-root",
        type=Path,
        help=(
            "Normalized BTCUSDC book root containing bbo/ and l2/; defaults "
            "to <data-dir>/normalized_l2_100ms_v2."
        ),
    )
    parser.add_argument("--latency-profile", type=Path, required=True)
    parser.add_argument("--output-panel", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument(
        "--latency-modes", nargs="+", default=list(DEFAULT_LATENCY_MODES)
    )
    parser.add_argument("--tail-threshold-usdc", type=float, default=TAIL_THRESHOLD_USDC)
    args = parser.parse_args()

    source = pd.read_csv(args.input_panel)
    profile_id, delays = load_latency_profile(args.latency_profile)
    panel = build_panel(
        source,
        data_dir=args.data_dir,
        l2_root=args.l2_root,
        latency_profile_id=profile_id,
        latency_delays=delays,
        latency_modes=args.latency_modes,
        tail_threshold_usdc=args.tail_threshold_usdc,
    )
    _write_frame(panel, args.output_panel)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "status": "development_only_not_untouched",
        "input_panel": str(args.input_panel.resolve()),
        "input_sha256": _sha256(args.input_panel),
        "output_panel": str(args.output_panel.resolve()),
        "output_sha256": _sha256(args.output_panel),
        "data_dir": str(args.data_dir.resolve()),
        "l2_root": str(
            (
                args.l2_root
                if args.l2_root is not None
                else args.data_dir / "normalized_l2_100ms_v2"
            ).resolve()
        ),
        "latency_profile": str(args.latency_profile.resolve()),
        "latency_profile_sha256": _sha256(args.latency_profile),
        "latency_profile_id": profile_id,
        "latency_profile_schema": LATENCY_PROFILE_SCHEMA,
        "latency_modes": list(args.latency_modes),
        "latency_delays_ms": delays,
        "source_rows": len(source),
        "panel_rows": len(panel),
        "days": int(source["day"].astype(str).nunique()),
        "first_day": str(source["day"].min()),
        "last_day": str(source["day"].max()),
        "side_rows": source["side"].astype(str).str.upper().value_counts().to_dict(),
        "tail_threshold_usdc": float(args.tail_threshold_usdc),
        "primary_targets": [
            "target_decision_to_terminal_mtm",
            "target_incremental_campaign_cost",
            "target_repair",
            "target_tail",
            "target_censored",
            "lifecycle_event",
        ],
        "secondary_targets": [
            "target_fill",
            "target_fill_markout_1s_bps",
            "target_fill_markout_5s_bps",
            "target_fill_markout_20s_bps",
            "target_fill_markout_30s_bps",
            "target_queue_cost",
            "target_fill_value",
        ],
        "secondary_target_availability": {
            "fill_markout_1s_bps": "unavailable_in_current_action_panel",
            "fill_markout_5s_bps": "unavailable_in_current_action_panel",
            "fill_markout_20s_bps": "unavailable_in_current_action_panel",
            "fill_markout_30s_bps": "available_for_intervention_fills",
        },
        "identity": {
            "join": "direct_replay_decision_id",
            "nearest_side_time_matching": False,
            "one_intervention_per_day_campaign": True,
        },
        "split_governance": {
            "development_dates_previously_used": "2026-01-01..2026-07-03",
            "external_model_dates_previously_used": "2026-07-04..2026-07-06",
            "rearm_outcome_dates_previously_used": "2026-07-07..2026-07-11",
            "future_holdout": "must begin after maker_lifecycle_panel_v1 spec freeze",
        },
        "feature_provenance": feature_provenance(panel),
    }
    args.metadata.parent.mkdir(parents=True, exist_ok=True)
    args.metadata.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({key: metadata[key] for key in ("status", "panel_rows", "days", "output_sha256")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
