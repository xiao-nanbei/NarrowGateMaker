#!/usr/bin/env python3
"""Attribute dynamic quote mechanisms and reducing-side repair by campaign.

This is an observational mechanism audit.  It does not change live policy and
it does not estimate action uplift.  A reducing-side action family may be
created only when the predeclared Development support gates pass.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import re
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from models import backtest_tick as bt
from research.families.f01_fixed_parameter_racing import daily_smoke_sweep as smoke
from models.audit.evidence_split import load_evidence_panel
from models.audit.experiment_manifest import git_workspace_identity, sha256_file
from research.families.f09_campaign_action_uplift.audit.local_action_uplift import _live_like_params
from research.families.f10_live_replay_attribution.audit.metrics import build_campaigns, campaign_label_rows
from models.backtest_config import (
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit import _day_start_ts, _fills_to_trade_rows
from models.replay_contract import (
    configure_fixed_latency_distribution,
    freeze_replay_contract,
    validate_frozen_replay_contract,
    write_replay_contract,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "dynamic_mechanism_campaign_audit.v1"
SUPPORT_GATE_VERSION = "reducing_repair_support.v1"
SUPPORT_GATES = {
    "min_campaigns_per_side": 50,
    "min_active_days_per_side": 10,
    "min_trade_through_intervals_per_side": 20,
    "min_affected_add_negative_rate": 0.05,
}
IMMEDIATE_ADD_TOXIC_BPS = -0.5
REPAIR_FAILURE_S = 300.0

_LOG_TS = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
_KEY_VALUE = re.compile(r"(?P<key>[A-Za-z][A-Za-z0-9_]*)=(?P<value>[^\s]+)")


def _sha256(path: Path) -> str:
    return sha256_file(path.expanduser().resolve())


def _as_float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _numeric(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")


def _series_or_default(
    frame: pd.DataFrame,
    column: str,
    default: float | int | str,
) -> pd.Series:
    if column in frame:
        return frame[column]
    return pd.Series(default, index=frame.index)


def _campaign_keys(frame: pd.DataFrame) -> pd.Series:
    ids = pd.to_numeric(frame["campaign_id"], errors="coerce").fillna(0).astype(int)
    if "day" not in frame:
        return ids.astype(str)
    return frame["day"].astype(str) + ":" + ids.astype(str)


def _signed_quantity(side: str, quantity: float) -> float:
    return quantity if str(side).upper() == "BUY" else -quantity


def _campaign_side(position: float) -> str:
    if position > 1e-10:
        return "LONG"
    if position < -1e-10:
        return "SHORT"
    return "FLAT"


def _inventory_role(side: str, position: float) -> str:
    side = str(side).upper()
    if abs(position) <= 1e-10:
        return "opener"
    if (position > 0.0 and side == "BUY") or (position < 0.0 and side == "SELL"):
        return "add"
    return "reducing"


def reconstruct_live_campaigns(
    trades: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rebuild flat-to-flat campaigns from the live fill ledger."""

    fills = trades.copy()
    required = {"timestamp", "side", "qty", "price", "position"}
    missing = sorted(required - set(fills))
    if missing:
        raise ValueError(f"live trades are missing columns: {missing}")
    _numeric(
        fills,
        ("timestamp", "qty", "price", "commission", "position", "unrealized_pnl"),
    )
    fills = fills.sort_values("timestamp", kind="stable").reset_index(drop=True)
    fills["commission"] = fills.get("commission", 0.0).fillna(0.0)
    fills["signed_qty"] = [
        _signed_quantity(side, qty) for side, qty in zip(fills["side"], fills["qty"], strict=True)
    ]
    fills["position_before"] = fills["position"] - fills["signed_qty"]

    campaign_rows: list[dict[str, Any]] = []
    fill_campaign_id: list[int] = []
    fill_role: list[str] = []
    current: dict[str, Any] | None = None
    next_id = 0

    for row in fills.itertuples(index=False):
        before = float(row.position_before)
        after = float(row.position)
        signed_qty = float(row.signed_qty)
        ts = float(row.timestamp)
        price = float(row.price)
        commission = float(row.commission)
        if current is None:
            if abs(before) > 1e-8 or abs(after) <= 1e-8:
                raise ValueError("live audit window does not begin at a campaign boundary")
            next_id += 1
            current = {
                "campaign_id": next_id,
                "start_ts": ts,
                "side": _campaign_side(after),
                "start_position": after,
                "cash": 0.0,
                "fills": 0,
                "opener_fills": 0,
                "add_fills": 0,
                "reducing_fills": 0,
                "max_abs_inventory": 0.0,
                "mae": 0.0,
                "max_mtm": 0.0,
                "first_add_ts": math.nan,
                "first_add_side": "",
                "first_add_price": math.nan,
                "first_reducing_ts": math.nan,
                "first_reducing_after_add_ts": math.nan,
            }

        if abs(before) <= 1e-8:
            role = "opener"
        elif abs(after) > abs(before) + 1e-10:
            role = "add"
        elif abs(after) < abs(before) - 1e-10:
            role = "reducing"
        else:
            role = "flat_change"

        current["cash"] += -signed_qty * price - commission
        current["fills"] += 1
        current[f"{role}_fills"] = int(current.get(f"{role}_fills", 0)) + 1
        current["max_abs_inventory"] = max(float(current["max_abs_inventory"]), abs(after))
        mtm = float(current["cash"]) + after * price
        current["mae"] = min(float(current["mae"]), mtm)
        current["max_mtm"] = max(float(current["max_mtm"]), mtm)
        if role == "add" and not math.isfinite(float(current["first_add_ts"])):
            current["first_add_ts"] = ts
            current["first_add_side"] = str(row.side).upper()
            current["first_add_price"] = price
        if role == "reducing":
            if not math.isfinite(float(current["first_reducing_ts"])):
                current["first_reducing_ts"] = ts
            if (
                math.isfinite(float(current["first_add_ts"]))
                and not math.isfinite(float(current["first_reducing_after_add_ts"]))
                and ts >= float(current["first_add_ts"])
            ):
                current["first_reducing_after_add_ts"] = ts

        fill_campaign_id.append(int(current["campaign_id"]))
        fill_role.append(role)
        if abs(after) <= 1e-8:
            current["end_ts"] = ts
            current["duration_s"] = ts - float(current["start_ts"])
            current["terminal_pnl"] = float(current["cash"])
            current["closed"] = 1
            first_add_ts = float(current["first_add_ts"])
            first_reduce = float(current["first_reducing_after_add_ts"])
            current["add_to_first_reducing_s"] = (
                first_reduce - first_add_ts
                if math.isfinite(first_add_ts) and math.isfinite(first_reduce)
                else math.nan
            )
            campaign_rows.append(dict(current))
            current = None

    fills["campaign_id"] = fill_campaign_id
    fills["inventory_role"] = fill_role
    campaigns = pd.DataFrame(campaign_rows)
    if campaigns.empty:
        raise ValueError("live window produced no closed campaigns")
    campaigns["day"] = pd.to_datetime(campaigns["start_ts"], unit="s", utc=True).dt.strftime(
        "%Y-%m-%d"
    )
    return campaigns, fills


def _future_mid(
    quote_timestamps: np.ndarray,
    quote_mids: np.ndarray,
    target_s: float,
) -> float:
    index = int(np.searchsorted(quote_timestamps, target_s, side="left"))
    return float(quote_mids[index]) if index < quote_timestamps.size else math.nan


def attach_live_quote_state(
    campaigns: pd.DataFrame,
    fills: pd.DataFrame,
    quotes: pd.DataFrame,
    *,
    max_inventory: float,
    emergency_inventory_ratio: float,
    emergency_loss: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Attach causal live position/campaign state to every side decision."""

    decisions = quotes.copy()
    _numeric(
        decisions,
        (
            "timestamp",
            "allow_post",
            "allow_exposure_increase",
            "spread_mult",
            "size_mult",
            "mid",
            "base_price",
            "final_price",
        ),
    )
    decisions = decisions.sort_values("timestamp", kind="stable").reset_index(drop=True)
    state = fills[
        ["timestamp", "campaign_id", "position", "price", "signed_qty", "commission"]
    ].copy()
    state["campaign_cash_flow"] = -state["signed_qty"] * state["price"] - state["commission"]
    state["campaign_cash"] = state.groupby("campaign_id")["campaign_cash_flow"].cumsum()
    decisions = pd.merge_asof(
        decisions,
        state[["timestamp", "campaign_id", "position", "campaign_cash"]],
        on="timestamp",
        direction="backward",
        allow_exact_matches=True,
    )
    decisions[["campaign_id", "position", "campaign_cash"]] = decisions[
        ["campaign_id", "position", "campaign_cash"]
    ].fillna(0.0)
    decisions["campaign_id"] = decisions["campaign_id"].astype(int)
    decisions.loc[decisions["position"].abs() <= 1e-10, "campaign_id"] = 0
    decisions["inventory_role"] = [
        _inventory_role(side, position)
        for side, position in zip(decisions["side"], decisions["position"], strict=True)
    ]
    decisions["campaign_side"] = [_campaign_side(position) for position in decisions["position"]]
    decisions["campaign_mtm"] = (
        decisions["campaign_cash"] + decisions["position"] * decisions["mid"]
    )
    decisions["base_distance_bps"] = (
        (decisions["base_price"] - decisions["mid"]).abs() / decisions["mid"] * 10_000.0
    )
    decisions["final_distance_bps"] = (
        (decisions["final_price"] - decisions["mid"]).abs() / decisions["mid"] * 10_000.0
    )
    reasons = decisions["reason_text"].fillna("").astype(str)
    decisions["defense_pause"] = (
        decisions["inventory_role"].eq("reducing")
        & reasons.str.contains(r"(?:^|\|)defense(?:\||$)", regex=True)
        & decisions["allow_post"].eq(0)
    ).astype(int)
    decisions["fill_cooldown_pause"] = reasons.str.contains(
        r"(?:^|\|)fill_cd(?:\||$)", regex=True
    ).astype(int)
    decisions["widened"] = (decisions["spread_mult"] > 1.0 + 1e-12).astype(int)
    decisions["kept"] = decisions["action"].astype(str).eq("keep").astype(int)
    decisions["paused"] = decisions["allow_post"].eq(0).astype(int)
    decisions["inventory_emergency_eligible"] = (
        decisions["position"].abs() / max(max_inventory, 1e-12) >= emergency_inventory_ratio
    ).astype(int)
    decisions["loss_emergency_eligible"] = (
        decisions["campaign_mtm"] <= -abs(emergency_loss)
    ).astype(int)

    quote_ts = decisions["timestamp"].to_numpy(dtype=float)
    quote_mid = decisions["mid"].to_numpy(dtype=float)
    for index, campaign in campaigns.iterrows():
        start = float(campaign["start_ts"])
        end = float(campaign["end_ts"])
        mask = decisions["campaign_id"].eq(int(campaign["campaign_id"])) & decisions[
            "timestamp"
        ].between(start, end, inclusive="both")
        if mask.any():
            campaigns.loc[index, "mae"] = min(
                float(campaign["mae"]),
                float(decisions.loc[mask, "campaign_mtm"].min()),
            )
        add_ts = float(campaign.get("first_add_ts", math.nan))
        add_price = float(campaign.get("first_add_price", math.nan))
        if math.isfinite(add_ts) and math.isfinite(add_price) and add_price > 0.0:
            direction = 1.0 if campaign["side"] == "LONG" else -1.0
            for horizon_s in (30.0, 300.0):
                future = _future_mid(quote_ts, quote_mid, add_ts + horizon_s)
                campaigns.loc[index, f"first_add_markout_{int(horizon_s)}s_bps"] = (
                    direction * (future - add_price) / add_price * 10_000.0
                    if math.isfinite(future)
                    else math.nan
                )
    return campaigns, decisions


def _parse_log_timestamp(line: str) -> float | None:
    match = _LOG_TS.match(line)
    if match is None:
        return None
    parsed = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
    return parsed.replace(tzinfo=timezone.utc).timestamp()


def _parse_fields(line: str) -> dict[str, str]:
    return {match.group("key"): match.group("value") for match in _KEY_VALUE.finditer(line)}


def parse_live_mechanism_logs(
    paths: Iterable[Path],
    *,
    start_ts: float,
    end_ts: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    quote_rows: list[dict[str, Any]] = []
    depth_rows: list[dict[str, Any]] = []
    cooldown_rows: list[dict[str, Any]] = []
    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not any(token in line for token in ("QUOTE_DBG", "DEPTH_SHADOW", "FILL_CD:")):
                    continue
                ts = _parse_log_timestamp(line)
                if ts is None or ts < start_ts or ts > end_ts:
                    continue
                raw = _parse_fields(line)
                if "QUOTE_DBG" in line:
                    numeric = {
                        key: _as_float(raw.get(key))
                        for key in (
                            "mid",
                            "sigma_sq_raw",
                            "sigma_sq_blended",
                            "delta_raw",
                            "delta_after_regime",
                            "delta_pre_cap",
                            "delta_after_cap",
                            "max_spread",
                            "half_d",
                            "q",
                        )
                    }
                    quote_rows.append(
                        {
                            "timestamp": ts,
                            **numeric,
                            "cap_hit": int(str(raw.get("cap_hit", "False")) == "True"),
                            "cap_reason": str(raw.get("cap_reason", "none")),
                        }
                    )
                elif "DEPTH_SHADOW" in line:
                    depth_rows.append(
                        {
                            "timestamp": ts,
                            "kappa_depth_ratio_logged": _as_float(raw.get("kappa_ratio")),
                            "kappa_used": _as_float(raw.get("kappa_used")),
                            "near_depth_bid": _as_float(raw.get("bid_qty")),
                            "near_depth_ask": _as_float(raw.get("ask_qty")),
                        }
                    )
                else:
                    side_match = re.search(r"FILL_CD:\s+(BUY|SELL)", line)
                    cooldown_rows.append(
                        {
                            "timestamp": ts,
                            "side": side_match.group(1) if side_match else "",
                            "kind": str(raw.get("kind", "")),
                            "consecutive_units": _as_float(raw.get("consec")),
                            "base_s": _as_float(str(raw.get("base", "")).rstrip("s")),
                            "effective_base_s": _as_float(
                                str(raw.get("effective_base", "")).rstrip("s")
                            ),
                            "vol_mult": _as_float(raw.get("vol_mult")),
                            "add_mult": _as_float(raw.get("add_mult")),
                            "cooldown_s": _as_float(str(raw.get("cooldown", "")).rstrip("s")),
                        }
                    )
    return pd.DataFrame(quote_rows), pd.DataFrame(depth_rows), pd.DataFrame(cooldown_rows)


def _assign_campaign_by_interval(
    frame: pd.DataFrame,
    campaigns: pd.DataFrame,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.sort_values("timestamp", kind="stable").reset_index(drop=True).copy()
    starts = campaigns["start_ts"].to_numpy(dtype=float)
    ends = campaigns["end_ts"].to_numpy(dtype=float)
    ids = campaigns["campaign_id"].to_numpy(dtype=int)
    sides = campaigns["side"].astype(str).to_numpy()
    indices = np.searchsorted(starts, out["timestamp"].to_numpy(dtype=float), side="right") - 1
    valid = (indices >= 0) & (
        out["timestamp"].to_numpy(dtype=float) <= ends[np.maximum(indices, 0)]
    )
    out["campaign_id"] = 0
    out["campaign_side"] = "FLAT"
    if valid.any():
        out.loc[valid, "campaign_id"] = ids[indices[valid]]
        out.loc[valid, "campaign_side"] = sides[indices[valid]]
    out["campaign_id"] = out["campaign_id"].astype(int)
    return out


def enrich_live_mechanism_samples(
    quote_samples: pd.DataFrame,
    depth_samples: pd.DataFrame,
    cooldown_samples: pd.DataFrame,
    campaigns: pd.DataFrame,
    *,
    p3_delta_star: float,
    p3_kappa_eff: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    quote_samples = _assign_campaign_by_interval(quote_samples, campaigns)
    depth_samples = _assign_campaign_by_interval(depth_samples, campaigns)
    cooldown_samples = _assign_campaign_by_interval(cooldown_samples, campaigns)
    if not quote_samples.empty:
        quote_samples["regime_spread_scale"] = quote_samples["delta_after_regime"] / quote_samples[
            "delta_raw"
        ].clip(lower=1e-12)
        floor = 2.0 * p3_delta_star
        quote_samples["p3_floor_bound"] = (
            (quote_samples["delta_pre_cap"] - floor).abs() <= 0.03
        ).astype(int)
        quote_samples["cap_bps"] = (
            quote_samples["max_spread"] / quote_samples["mid"].clip(lower=1e-12) * 10_000.0
        )
    if not depth_samples.empty:
        depth_samples["kappa_vs_p3_ratio"] = depth_samples["kappa_used"] / max(p3_kappa_eff, 1e-12)
    return quote_samples, depth_samples, cooldown_samples


def _normalize_execution_trades(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "transact_time" in out:
        out["ts_ms"] = pd.to_numeric(out["transact_time"], errors="coerce")
    elif "time" in out:
        out["ts_ms"] = pd.to_numeric(out["time"], errors="coerce")
    elif "timestamp" in out:
        values = pd.to_numeric(out["timestamp"], errors="coerce")
        out["ts_ms"] = np.where(values < 1e11, values * 1000.0, values)
    else:
        raise ValueError("execution trades have no timestamp column")
    quantity_column = "quantity" if "quantity" in out else "qty"
    out["quantity"] = pd.to_numeric(out[quantity_column], errors="coerce").fillna(0.0)
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["is_buyer_maker"] = out["is_buyer_maker"].astype(str).str.lower().isin({"true", "1"})
    return out.dropna(subset=["ts_ms", "price"]).sort_values(["ts_ms"], kind="stable")


def defense_pause_intervals(
    decisions: pd.DataFrame,
    campaigns: pd.DataFrame,
    execution_trades: pd.DataFrame,
) -> pd.DataFrame:
    """Build support-only baseline-quote touch/through intervals."""

    if decisions.empty or campaigns.empty or "defense_pause" not in decisions:
        return pd.DataFrame()
    frame = decisions.copy()
    ts_column = "ts_ms" if "ts_ms" in frame else "timestamp"
    scale = 1.0 if ts_column == "ts_ms" else 1000.0
    frame["decision_ts_ms"] = pd.to_numeric(frame[ts_column], errors="coerce") * scale
    frame = frame.sort_values(["side", "decision_ts_ms"], kind="stable")
    frame["next_side_ts_ms"] = frame.groupby("side")["decision_ts_ms"].shift(-1)
    campaign_end = campaigns.set_index("campaign_id")["end_ts"].to_dict()
    rows = frame[
        pd.to_numeric(frame["defense_pause"], errors="coerce").fillna(0).eq(1)
        & pd.to_numeric(frame["campaign_id"], errors="coerce").fillna(0).gt(0)
    ].copy()
    rows = rows[rows["campaign_id"].astype(int).isin(campaign_end)]
    if rows.empty:
        return rows
    trades = _normalize_execution_trades(execution_trades)
    output: list[dict[str, Any]] = []
    for row in rows.itertuples(index=False):
        campaign_id = int(row.campaign_id)
        start_ms = float(row.decision_ts_ms)
        end_ms = _as_float(getattr(row, "next_side_ts_ms", math.nan))
        campaign_end_ms = float(campaign_end[campaign_id]) * 1000.0
        if not math.isfinite(end_ms):
            end_ms = campaign_end_ms
        end_ms = min(end_ms, campaign_end_ms)
        side = str(row.side).upper()
        price = float(row.base_price)
        interval = trades[trades["ts_ms"].ge(start_ms) & trades["ts_ms"].lt(end_ms)]
        if side == "BUY":
            aggressor = interval[interval["is_buyer_maker"]]
            touch = aggressor[aggressor["price"] <= price + 1e-12]
            through = aggressor[aggressor["price"] < price - 1e-12]
        else:
            aggressor = interval[~interval["is_buyer_maker"]]
            touch = aggressor[aggressor["price"] >= price - 1e-12]
            through = aggressor[aggressor["price"] > price + 1e-12]
        payload = row._asdict()
        payload.update(
            {
                "decision_ts_ms": int(start_ms),
                "interval_end_ts_ms": int(end_ms),
                "pause_duration_s": max(0.0, (end_ms - start_ms) / 1000.0),
                "baseline_price": price,
                "touch": int(not touch.empty),
                "strict_trade_through": int(not through.empty),
                "touch_qty": float(touch["quantity"].sum()),
                "strict_trade_through_qty": float(through["quantity"].sum()),
                "support_semantics": "individual_trade_touch_through_not_queue_fill",
            }
        )
        output.append(payload)
    return pd.DataFrame(output)


def _fill_timing_from_trace(fill_trace: pd.DataFrame) -> pd.DataFrame:
    if fill_trace.empty:
        return pd.DataFrame()
    frame = fill_trace.copy()
    _numeric(
        frame,
        (
            "fill_ts",
            "inventory_before_fill",
            "inventory_after_fill",
            "quote_px",
            "markout_30s",
        ),
    )
    frame = frame.sort_values("fill_ts", kind="stable").reset_index(drop=True)
    campaign_id = 0
    rows: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        before = float(row.inventory_before_fill)
        after = float(row.inventory_after_fill)
        if abs(before) <= 1e-10 and abs(after) > 1e-10:
            campaign_id += 1
            role = "opener"
        elif abs(after) > abs(before) + 1e-10:
            role = "add"
        elif abs(after) < abs(before) - 1e-10:
            role = "reducing"
        else:
            role = "flat_change"
        rows.append(
            {
                "campaign_id": campaign_id,
                "fill_ts_ms": int(row.fill_ts),
                "side": str(row.side),
                "inventory_role": role,
                "quote_px": float(row.quote_px),
                "markout_30s": float(row.markout_30s),
            }
        )
    annotated = pd.DataFrame(rows)
    output: list[dict[str, Any]] = []
    for cid, group in annotated.groupby("campaign_id", sort=True):
        add = group[group["inventory_role"].eq("add")]
        reducing = group[group["inventory_role"].eq("reducing")]
        first_add = float(add["fill_ts_ms"].min()) if not add.empty else math.nan
        first_add_row = add.iloc[0] if not add.empty else None
        reducing_after = (
            reducing[reducing["fill_ts_ms"] >= first_add]
            if math.isfinite(first_add)
            else reducing.iloc[0:0]
        )
        first_reduce = (
            float(reducing_after["fill_ts_ms"].min()) if not reducing_after.empty else math.nan
        )
        output.append(
            {
                "campaign_id": int(cid),
                "first_add_ts_ms": first_add,
                "first_reducing_after_add_ts_ms": first_reduce,
                "add_to_first_reducing_s": (
                    (first_reduce - first_add) / 1000.0
                    if math.isfinite(first_add) and math.isfinite(first_reduce)
                    else math.nan
                ),
                "first_add_markout_30s_bps": (
                    float(first_add_row["markout_30s"])
                    / max(float(first_add_row["quote_px"]), 1e-12)
                    * 10_000.0
                    if first_add_row is not None
                    else math.nan
                ),
            }
        )
    return pd.DataFrame(output)


def classify_add_failure(campaigns: pd.DataFrame) -> pd.DataFrame:
    out = campaigns.copy()
    add_count = pd.to_numeric(_series_or_default(out, "add_fills", 0), errors="coerce").fillna(0)
    markout = pd.to_numeric(
        _series_or_default(out, "first_add_markout_30s_bps", math.nan),
        errors="coerce",
    )
    repair_s = pd.to_numeric(
        _series_or_default(out, "add_to_first_reducing_s", math.nan),
        errors="coerce",
    )
    immediate = add_count.gt(0) & markout.le(IMMEDIATE_ADD_TOXIC_BPS)
    repair_failure = add_count.gt(0) & (repair_s.gt(REPAIR_FAILURE_S) | repair_s.isna())
    out["immediate_add_toxicity"] = immediate.astype(int)
    out["repair_failure"] = repair_failure.astype(int)
    out["add_failure_class"] = np.select(
        (
            add_count.le(0),
            immediate & repair_failure,
            immediate,
            repair_failure,
        ),
        ("no_add", "mixed", "immediate_add_toxicity", "repair_failure"),
        default="add_other",
    )
    return out


def summarize_campaign_decisions(
    campaigns: pd.DataFrame,
    decisions: pd.DataFrame,
    *,
    p3_delta_star: float,
    p3_kappa_eff: float,
) -> pd.DataFrame:
    result = campaigns.copy()
    if decisions.empty:
        return result
    frame = decisions.copy()
    continuous_defaults = (
        "sigma_sq_raw",
        "sigma_sq_blended",
        "regime_spread_scale",
        "delta_raw",
        "delta_after_regime",
        "delta_pre_cap",
        "delta_after_cap",
        "cap_bps",
        "kappa_used",
        "fill_cooldown_total_ms",
        "fill_cooldown_consecutive_units",
        "base_distance_bps",
        "final_distance_bps",
        "spread_mult",
    )
    binary_defaults = (
        "cap_hit",
        "delta_cap_hit",
        "final_compressed",
        "defense_pause",
        "inventory_emergency_eligible",
        "loss_emergency_eligible",
    )
    for column in continuous_defaults:
        if column not in frame:
            frame[column] = math.nan
    for column in binary_defaults:
        if column not in frame:
            frame[column] = 0 if column.startswith(("defense", "inventory", "loss")) else math.nan
    _numeric(
        frame,
        (
            "campaign_id",
            "sigma_sq_raw",
            "sigma_sq_blended",
            "regime_spread_scale",
            "delta_raw",
            "delta_after_regime",
            "delta_pre_cap",
            "delta_after_cap",
            "cap_bps",
            "cap_hit",
            "delta_cap_hit",
            "final_compressed",
            "kappa_used",
            "fill_cooldown_total_ms",
            "fill_cooldown_consecutive_units",
            "base_distance_bps",
            "final_distance_bps",
            "spread_mult",
            "defense_pause",
            "inventory_emergency_eligible",
            "loss_emergency_eligible",
        ),
    )
    if "regime_spread_scale" not in frame or frame["regime_spread_scale"].isna().all():
        frame["regime_spread_scale"] = frame["delta_after_regime"] / frame["delta_raw"].clip(
            lower=1e-12
        )
    floor = 2.0 * p3_delta_star
    frame["p3_floor_bound"] = np.where(
        frame["delta_pre_cap"].notna(),
        ((frame["delta_pre_cap"] - floor).abs() <= 0.03).astype(float),
        math.nan,
    )
    frame["kappa_vs_p3_ratio"] = frame["kappa_used"] / max(p3_kappa_eff, 1e-12)
    active = frame[pd.to_numeric(frame["campaign_id"], errors="coerce").fillna(0).gt(0)]

    rows: list[dict[str, Any]] = []
    group_keys: str | list[str] = "campaign_id"
    if "day" in active:
        group_keys = ["day", "campaign_id"]
    for identity, group in active.groupby(group_keys, sort=False):
        if isinstance(identity, tuple):
            day, cid = identity
        else:
            day, cid = None, identity
        add = group[group["inventory_role"].astype(str).eq("add")]
        reducing = group[group["inventory_role"].astype(str).eq("reducing")]
        payload = {
            "campaign_id": int(cid),
            "decision_count": len(group),
            "sigma_sq_raw_mean": float(group["sigma_sq_raw"].mean()),
            "sigma_sq_blended_mean": float(group["sigma_sq_blended"].mean()),
            "regime_spread_scale_mean": float(group["regime_spread_scale"].mean()),
            "p3_floor_bound_rate": float(group["p3_floor_bound"].mean()),
            "kappa_used_mean": float(group["kappa_used"].mean()),
            "kappa_used_std": float(group["kappa_used"].std(ddof=0)),
            "kappa_vs_p3_ratio_mean": float(group["kappa_vs_p3_ratio"].mean()),
            "dynamic_cap_bps_mean": float(group["cap_bps"].mean()),
            "dynamic_cap_bps_max": float(group["cap_bps"].max()),
            "cap_hit_rate": float(group["cap_hit"].mean()),
            "final_compress_rate": float(group["final_compressed"].mean()),
            "cooldown_total_s_max": float(add["fill_cooldown_total_ms"].max() / 1000.0)
            if not add.empty
            else 0.0,
            "cooldown_consecutive_max": float(add["fill_cooldown_consecutive_units"].max())
            if not add.empty
            else 0.0,
            "add_base_distance_bps_mean": float(add["base_distance_bps"].mean())
            if not add.empty
            else math.nan,
            "add_final_distance_bps_mean": float(add["final_distance_bps"].mean())
            if not add.empty
            else math.nan,
            "add_pause_rate": float(add["paused"].mean())
            if not add.empty and "paused" in add
            else math.nan,
            "add_widen_rate": float(add["widened"].mean())
            if not add.empty and "widened" in add
            else math.nan,
            "add_keep_rate": float(add["kept"].mean())
            if not add.empty and "kept" in add
            else math.nan,
            "reducing_base_distance_bps_mean": float(reducing["base_distance_bps"].mean())
            if not reducing.empty
            else math.nan,
            "reducing_final_distance_bps_mean": float(reducing["final_distance_bps"].mean())
            if not reducing.empty
            else math.nan,
            "reducing_pause_rate": float(reducing["paused"].mean())
            if not reducing.empty and "paused" in reducing
            else math.nan,
            "reducing_widen_rate": float(reducing["widened"].mean())
            if not reducing.empty and "widened" in reducing
            else math.nan,
            "reducing_keep_rate": float(reducing["kept"].mean())
            if not reducing.empty and "kept" in reducing
            else math.nan,
            "defense_pause_decisions": int(group["defense_pause"].sum()),
            "inventory_emergency_decisions": int(group["inventory_emergency_eligible"].sum()),
            "loss_emergency_decisions": int(group["loss_emergency_eligible"].sum()),
        }
        if day is not None:
            payload["day"] = str(day)
        rows.append(payload)
    summary = pd.DataFrame(rows)
    merge_keys = ["campaign_id"]
    if "day" in result and "day" in frame:
        merge_keys.insert(0, "day")
    return result.merge(summary, on=merge_keys, how="left")


def _group_summary(campaigns: pd.DataFrame, *, source: str) -> pd.DataFrame:
    frame = campaigns.copy()
    add_count = (
        pd.to_numeric(frame["add_fills"], errors="coerce")
        if "add_fills" in frame
        else pd.to_numeric(frame["exposure_increasing_fills"], errors="coerce") - 1
    ).clip(lower=0)
    frame["add_count"] = add_count
    pnl_column = "terminal_pnl" if "terminal_pnl" in frame else "final_total_pnl_delta"
    frame["audit_terminal_pnl"] = pd.to_numeric(frame[pnl_column], errors="coerce")
    masks = {
        "all": pd.Series(True, index=frame.index),
        "no_add_net_positive": frame["add_count"].eq(0),
        "add_net_negative": frame["add_count"].gt(0),
        "no_add_positive_individual": frame["add_count"].eq(0) & frame["audit_terminal_pnl"].gt(0),
        "add_negative_individual": frame["add_count"].gt(0) & frame["audit_terminal_pnl"].lt(0),
    }
    worst_index = frame.nsmallest(min(20, len(frame)), "audit_terminal_pnl").index
    masks["worst20"] = frame.index.isin(worst_index)
    output: list[dict[str, Any]] = []
    for group_name, mask in masks.items():
        subset = frame[mask]
        for side in ("ALL", "LONG", "SHORT"):
            side_rows = subset if side == "ALL" else subset[subset["side"].eq(side)]
            output.append(
                {
                    "source": source,
                    "group": group_name,
                    "side": side,
                    "campaigns": len(side_rows),
                    "terminal_pnl_sum": float(side_rows["audit_terminal_pnl"].sum()),
                    "terminal_pnl_mean": float(side_rows["audit_terminal_pnl"].mean())
                    if not side_rows.empty
                    else math.nan,
                    "mae_mean": float(
                        pd.to_numeric(
                            side_rows.get("mae", side_rows.get("min_total_pnl_delta")),
                            errors="coerce",
                        ).mean()
                    )
                    if not side_rows.empty
                    else math.nan,
                    "duration_s_median": float(
                        pd.to_numeric(side_rows["duration_s"], errors="coerce").median()
                    )
                    if not side_rows.empty
                    else math.nan,
                    "p3_floor_bound_rate": float(
                        pd.to_numeric(side_rows.get("p3_floor_bound_rate"), errors="coerce").mean()
                    )
                    if "p3_floor_bound_rate" in side_rows
                    else math.nan,
                    "cap_hit_rate": float(
                        pd.to_numeric(side_rows.get("cap_hit_rate"), errors="coerce").mean()
                    )
                    if "cap_hit_rate" in side_rows
                    else math.nan,
                    "defense_pause_campaigns": int(
                        pd.to_numeric(side_rows.get("defense_pause_decisions"), errors="coerce")
                        .fillna(0)
                        .gt(0)
                        .sum()
                    )
                    if "defense_pause_decisions" in side_rows
                    else 0,
                    "add_to_first_reducing_s_median": float(
                        pd.to_numeric(
                            side_rows.get("add_to_first_reducing_s"), errors="coerce"
                        ).median()
                    )
                    if "add_to_first_reducing_s" in side_rows
                    else math.nan,
                    "immediate_add_toxicity_rate": float(
                        pd.to_numeric(
                            side_rows.get("immediate_add_toxicity"), errors="coerce"
                        ).mean()
                    )
                    if "immediate_add_toxicity" in side_rows
                    else math.nan,
                    "repair_failure_rate": float(
                        pd.to_numeric(side_rows.get("repair_failure"), errors="coerce").mean()
                    )
                    if "repair_failure" in side_rows
                    else math.nan,
                }
            )
    return pd.DataFrame(output)


def _mechanism_rollup(campaigns: pd.DataFrame, *, source: str) -> pd.DataFrame:
    frame = campaigns.copy()
    add_count = pd.to_numeric(frame["add_fills"], errors="coerce").fillna(0)
    worst_index = frame.nsmallest(min(20, len(frame)), "terminal_pnl").index
    groups = {
        "all": pd.Series(True, index=frame.index),
        "no_add_net_positive": add_count.eq(0),
        "add_net_negative": add_count.gt(0),
        "worst20": frame.index.isin(worst_index),
    }
    mean_columns = (
        "sigma_sq_raw_mean",
        "sigma_sq_blended_mean",
        "regime_spread_scale_mean",
        "p3_floor_bound_rate",
        "kappa_used_mean",
        "kappa_vs_p3_ratio_mean",
        "dynamic_cap_bps_mean",
        "cap_hit_rate",
        "final_compress_rate",
        "add_base_distance_bps_mean",
        "add_final_distance_bps_mean",
        "add_pause_rate",
        "add_widen_rate",
        "add_keep_rate",
        "reducing_base_distance_bps_mean",
        "reducing_final_distance_bps_mean",
        "reducing_pause_rate",
        "reducing_widen_rate",
        "reducing_keep_rate",
    )
    rows: list[dict[str, Any]] = []
    for group_name, mask in groups.items():
        subset = frame[mask]
        for side in ("ALL", "LONG", "SHORT"):
            side_rows = subset if side == "ALL" else subset[subset["side"].eq(side)]
            payload: dict[str, Any] = {
                "source": source,
                "group": group_name,
                "side": side,
                "campaigns": len(side_rows),
            }
            for column in mean_columns:
                payload[column] = (
                    float(pd.to_numeric(side_rows[column], errors="coerce").mean())
                    if column in side_rows and not side_rows.empty
                    else math.nan
                )
            for column in (
                "cooldown_total_s_max",
                "cooldown_consecutive_max",
            ):
                payload[f"{column}_median"] = (
                    float(pd.to_numeric(side_rows[column], errors="coerce").median())
                    if column in side_rows and not side_rows.empty
                    else math.nan
                )
            for column in (
                "defense_pause_decisions",
                "inventory_emergency_decisions",
                "loss_emergency_decisions",
            ):
                payload[column.replace("_decisions", "_campaigns")] = (
                    int(pd.to_numeric(side_rows[column], errors="coerce").fillna(0).gt(0).sum())
                    if column in side_rows
                    else 0
                )
            rows.append(payload)
    return pd.DataFrame(rows)


def _add_failure_rollup(campaigns: pd.DataFrame, *, source: str) -> pd.DataFrame:
    frame = campaigns[pd.to_numeric(campaigns["add_fills"], errors="coerce").gt(0)]
    rows: list[dict[str, Any]] = []
    for side in ("ALL", "LONG", "SHORT"):
        side_rows = frame if side == "ALL" else frame[frame["side"].eq(side)]
        for failure_class, group in side_rows.groupby("add_failure_class", sort=True):
            pnl = pd.to_numeric(group["terminal_pnl"], errors="coerce")
            rows.append(
                {
                    "source": source,
                    "side": side,
                    "add_failure_class": str(failure_class),
                    "campaigns": len(group),
                    "terminal_pnl_sum": float(pnl.sum()),
                    "terminal_pnl_mean": float(pnl.mean()),
                    "duration_s_median": float(
                        pd.to_numeric(group["duration_s"], errors="coerce").median()
                    ),
                    "add_to_first_reducing_s_median": float(
                        pd.to_numeric(group["add_to_first_reducing_s"], errors="coerce").median()
                    ),
                    "first_add_markout_30s_bps_mean": float(
                        pd.to_numeric(group["first_add_markout_30s_bps"], errors="coerce").mean()
                    ),
                }
            )
    return pd.DataFrame(rows)


def _set_book_roots(bbo_dir: Path, l2_dir: Path) -> None:
    bt.BBO_DIR = bbo_dir.expanduser().resolve()
    bt.L2_DIR = l2_dir.expanduser().resolve()


def _run_development_day(task: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    day, raw_base = task
    base = dict(raw_base)
    _set_book_roots(Path(base["_historical_bbo_dir"]), Path(base["_historical_l2_dir"]))
    model_dir = base.get("resolved_model_dir") or base.get("model_dir")
    bt.configure_symbol("BTCUSDC", model_dir_override=model_dir)
    validate_frozen_replay_contract(base)
    window = smoke._load_window(day, base)
    started = time.perf_counter()
    result = bt._simulate_tick_with_engine(
        "python",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        base,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
    )
    fill_trace = pd.DataFrame(result.get("_fill_trace", ()))
    decisions = pd.DataFrame(result.get("_decision_trace", ()))
    trades = _fills_to_trade_rows(
        result.get("_fill_trace", ()),
        initial_inventory=0.0,
        initial_entry_price=0.0,
        day_start_ts=_day_start_ts(day),
    )
    campaigns = pd.DataFrame(campaign_label_rows(build_campaigns(trades)))
    if not campaigns.empty:
        campaigns = campaigns[pd.to_numeric(campaigns["closed"], errors="coerce").eq(1)].copy()
        campaigns["day"] = day
        campaigns["side"] = campaigns["start_side"]
        campaigns["add_fills"] = (
            pd.to_numeric(campaigns["exposure_increasing_fills"], errors="coerce") - 1
        ).clip(lower=0)
        campaigns["terminal_pnl"] = campaigns["final_total_pnl_delta"]
        campaigns["mae"] = campaigns["min_total_pnl_delta"]
        campaigns["start_ts"] = pd.to_datetime(campaigns["start_utc"], utc=True).map(
            pd.Timestamp.timestamp
        )
        campaigns["end_ts"] = pd.to_datetime(campaigns["end_utc"], utc=True).map(
            pd.Timestamp.timestamp
        )
    timing = _fill_timing_from_trace(fill_trace)
    if not timing.empty and not campaigns.empty:
        campaigns = campaigns.merge(timing, on="campaign_id", how="left")
    if not decisions.empty:
        decisions["day"] = day
        decisions["inventory_role"] = np.where(
            pd.to_numeric(decisions["inventory"], errors="coerce").abs().le(1e-10),
            "opener",
            np.where(
                pd.to_numeric(decisions["exposure_increasing"], errors="coerce").eq(1),
                "add",
                "reducing",
            ),
        )
        decisions["base_distance_bps"] = (
            (decisions["base_price"] - decisions["mid"]).abs()
            / decisions["mid"].clip(lower=1e-12)
            * 10_000.0
        )
        decisions["final_distance_bps"] = (
            (decisions["final_price"] - decisions["mid"]).abs()
            / decisions["mid"].clip(lower=1e-12)
            * 10_000.0
        )
        decisions["paused"] = (
            pd.to_numeric(decisions["allow_post"], errors="coerce").eq(0).astype(int)
        )
        decisions["widened"] = (
            pd.to_numeric(decisions["spread_mult"], errors="coerce") > 1.0 + 1e-12
        ).astype(int)
        decisions["kept"] = decisions["action"].astype(str).eq("keep").astype(int)
        decisions["inventory_emergency_eligible"] = (
            pd.to_numeric(decisions["inventory"], errors="coerce").abs()
            / max(float(base["max_inventory"]), 1e-12)
            >= float(base["defense_emergency_inventory_ratio"])
        ).astype(int)
        decisions["loss_emergency_eligible"] = (
            pd.to_numeric(decisions["campaign_pnl_so_far"], errors="coerce")
            <= -abs(float(base["defense_emergency_loss"]))
        ).astype(int)
    if not campaigns.empty:
        campaigns = summarize_campaign_decisions(
            campaigns,
            decisions,
            p3_delta_star=float(base["p3_delta_star"]),
            p3_kappa_eff=float(base["p3_kappa_eff"]),
        )
        campaigns = classify_add_failure(campaigns)
    intervals = defense_pause_intervals(decisions, campaigns, window["trades"])
    if not intervals.empty:
        intervals["day"] = day
    return {
        "day": day,
        "runtime_s": time.perf_counter() - started,
        "campaigns": campaigns.to_dict("records"),
        "defense": intervals.to_dict("records"),
        "decision_count": len(decisions),
    }


def run_development_replay(
    args: argparse.Namespace, output_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    days, split_identity = load_evidence_panel(
        args.evidence_split.expanduser().resolve(), "development"
    )
    config = args.config.expanduser().resolve()
    queue = args.queue_calibration_artifact.expanduser().resolve()
    telemetry = args.live_perf_telemetry.expanduser().resolve()
    bbo_dir = args.bbo_dir.expanduser().resolve()
    l2_dir = args.l2_dir.expanduser().resolve()
    _set_book_roots(bbo_dir, l2_dir)
    bt.configure_symbol("BTCUSDC")
    base = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=queue,
        strict_calibration=True,
    )
    _live_like_params(base)
    samples = bt._load_live_perf_latency_samples(telemetry, mode="avg")
    base["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
    base["_cancel_order_latency_samples_ms"] = samples["cancel_order_latency_samples_ms"]
    configure_fixed_latency_distribution(
        base,
        scenario="baseline",
        profile_id=args.latency_profile_id,
        environment=args.latency_environment,
    )
    base.update(
        {
            "_historical_bbo_dir": str(bbo_dir),
            "_historical_l2_dir": str(l2_dir),
            "trace_quotes_max": 0,
            "trace_decisions_max": int(args.trace_decisions_max),
            "trace_fills_max": int(args.trace_fills_max),
            "trace_queue_events_max": 0,
            "local_action_ope_enabled": False,
            "state_conditioned_rearm_enabled": False,
            "safe_add_rearm_randomized_enabled": False,
            "queue_value_keep_cancel_randomized_enabled": False,
            "queue_value_cancel_reenter_randomized_enabled": False,
        }
    )
    if args.window_cache_dir is not None:
        base["_window_cache_dir"] = str(args.window_cache_dir.expanduser().resolve())
    validate_formal_replay_calibration(base, require_latency=True)
    contract = freeze_replay_contract(
        base, purpose="formal", initial_state_mode="fresh_start", root=ROOT
    )
    write_replay_contract(contract, output_dir / "development.replay_contract.json")
    validate_frozen_replay_contract(base)

    tasks = [(day, base) for day in days]
    results: list[dict[str, Any]] = []
    workers = max(1, min(int(args.workers), len(tasks)))
    if workers == 1:
        iterator = map(_run_development_day, tasks)
        for item in iterator:
            results.append(item)
            print(f"{item['day']}: {item['runtime_s']:.1f}s", flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_run_development_day, task) for task in tasks]
            for future in concurrent.futures.as_completed(futures):
                item = future.result()
                results.append(item)
                print(f"{item['day']}: {item['runtime_s']:.1f}s", flush=True)
    results.sort(key=lambda item: item["day"])

    campaign_parts: list[pd.DataFrame] = []
    defense_parts: list[pd.DataFrame] = []
    for item in results:
        campaigns = pd.DataFrame(item["campaigns"])
        if not campaigns.empty:
            campaign_parts.append(campaigns)
        intervals = pd.DataFrame(item["defense"])
        if not intervals.empty:
            defense_parts.append(intervals)
    campaigns = pd.concat(campaign_parts, ignore_index=True) if campaign_parts else pd.DataFrame()
    defense = pd.concat(defense_parts, ignore_index=True) if defense_parts else pd.DataFrame()
    decisions = pd.DataFrame()
    p3_model_path = Path(str(base["fill_probability_model_path"])).expanduser().resolve()
    metadata = {
        "days": days,
        "split_identity": split_identity,
        "config_sha256": _sha256(config),
        "p3_sha256": _sha256(p3_model_path),
        "queue_sha256": _sha256(queue),
        "telemetry_sha256": _sha256(telemetry),
        "replay_contract_sha256": contract["contract_sha256"],
        "p3_delta_star": float(base["p3_delta_star"]),
        "p3_kappa_eff": float(base["p3_kappa_eff"]),
    }
    return campaigns, decisions, defense, metadata


def evaluate_reducing_support(
    campaigns: pd.DataFrame,
    defense: pd.DataFrame,
) -> dict[str, Any]:
    results: dict[str, Any] = {
        "schema_version": SUPPORT_GATE_VERSION,
        "gates": SUPPORT_GATES,
        "sides": {},
    }
    for campaign_side, quote_side in (("LONG", "SELL"), ("SHORT", "BUY")):
        side_campaigns = campaigns[campaigns["side"].eq(campaign_side)]
        side_defense = (
            defense[defense["side"].astype(str).str.upper().eq(quote_side)]
            if not defense.empty
            else defense
        )
        affected_keys = set(_campaign_keys(side_defense)) if not side_defense.empty else set()
        add_negative = side_campaigns[
            pd.to_numeric(side_campaigns["add_fills"], errors="coerce").gt(0)
            & pd.to_numeric(side_campaigns["terminal_pnl"], errors="coerce").lt(0)
        ]
        affected_add_negative = add_negative[_campaign_keys(add_negative).isin(affected_keys)]
        observed = {
            "campaigns": len(affected_keys),
            "active_days": int(side_defense["day"].nunique())
            if "day" in side_defense
            else 1
            if not side_defense.empty
            else 0,
            "strict_trade_through_intervals": int(
                pd.to_numeric(side_defense.get("strict_trade_through"), errors="coerce")
                .fillna(0)
                .sum()
            )
            if not side_defense.empty
            else 0,
            "touch_intervals": int(
                pd.to_numeric(side_defense.get("touch"), errors="coerce").fillna(0).sum()
            )
            if not side_defense.empty
            else 0,
            "pause_intervals": len(side_defense),
            "pause_duration_s": float(
                pd.to_numeric(side_defense.get("pause_duration_s"), errors="coerce").fillna(0).sum()
            )
            if not side_defense.empty
            else 0.0,
            "affected_add_negative_rate": len(affected_add_negative) / max(len(add_negative), 1),
            "add_negative_campaigns": len(add_negative),
            "affected_add_negative_campaigns": len(affected_add_negative),
        }
        passed = (
            observed["campaigns"] >= SUPPORT_GATES["min_campaigns_per_side"]
            and observed["active_days"] >= SUPPORT_GATES["min_active_days_per_side"]
            and observed["strict_trade_through_intervals"]
            >= SUPPORT_GATES["min_trade_through_intervals_per_side"]
            and observed["affected_add_negative_rate"]
            >= SUPPORT_GATES["min_affected_add_negative_rate"]
        )
        results["sides"][campaign_side] = {**observed, "passed": passed}
    results["passed_sides"] = [
        side for side, payload in results["sides"].items() if payload["passed"]
    ]
    results["open_action_family"] = bool(results["passed_sides"])
    return results


def _write_report(
    path: Path,
    *,
    live_groups: pd.DataFrame,
    development_groups: pd.DataFrame,
    live_mechanisms: pd.DataFrame,
    development_mechanisms: pd.DataFrame,
    add_failures: pd.DataFrame,
    live_support: dict[str, Any],
    development_support: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    lines = [
        "# Dynamic mechanism campaign audit v1",
        "",
        "Status: observational attribution only; no live strategy change.",
        "",
        "## Frozen identity",
        "",
        f"- Config SHA256: `{metadata['config_sha256']}`",
        f"- P3 SHA256: `{metadata['p3_sha256']}`",
        f"- Queue SHA256: `{metadata['queue_sha256']}`",
        f"- Development replay contract: `{metadata['replay_contract_sha256']}`",
        "",
        "## Live 48h groups",
        "",
        "```text",
        live_groups.to_string(index=False),
        "```",
        "",
        "## Development groups",
        "",
        "```text",
        development_groups.to_string(index=False),
        "```",
        "",
        "## Dynamic mechanism transmission",
        "",
        "Campaign-level means are observational. Live dynamic fields come from sampled "
        "logs; Development fields come from the authoritative replay decision trace.",
        "",
        "```text",
        pd.concat([live_mechanisms, development_mechanisms], ignore_index=True).to_string(
            index=False
        ),
        "```",
        "",
        "## Add toxicity versus repair failure",
        "",
        f"Diagnostic definitions: immediate toxicity is first-add 30s maker markout <= "
        f"{IMMEDIATE_ADD_TOXIC_BPS:.1f} bps; repair failure is more than "
        f"{REPAIR_FAILURE_S:.0f}s to the first reducing fill. These are attribution "
        "thresholds, not action-value estimates.",
        "",
        "```text",
        add_failures.to_string(index=False),
        "```",
        "",
        "## Reducing repair support",
        "",
        "Live is diagnostic and cannot open an action family by itself.",
        "",
        "```json",
        json.dumps(
            {"live": live_support, "development": development_support}, indent=2, sort_keys=True
        ),
        "```",
        "",
        "## Decision",
        "",
    ]
    if development_support["open_action_family"]:
        lines.extend(
            [
                "Development support passed for: "
                + ", ".join(development_support["passed_sides"])
                + ". Freeze `reducing_repair_release_v1` before reading D0/D1 outcomes.",
                "Validation and sealed holdout remain closed.",
            ]
        )
    else:
        lines.extend(
            [
                "No side passed the predeclared reducing-repair support gates.",
                "Close `reducing_repair_release_v1` at the support stage and move to first-add marginal order value.",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--live-start-ts", type=float, required=True)
    parser.add_argument("--live-end-ts", type=float, required=True)
    parser.add_argument("--raw-trades-dir", type=Path, required=True)
    parser.add_argument("--evidence-split", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--queue-calibration-artifact", type=Path, required=True)
    parser.add_argument("--bbo-dir", type=Path, required=True)
    parser.add_argument("--l2-dir", type=Path, required=True)
    parser.add_argument("--live-perf-telemetry", type=Path, required=True)
    parser.add_argument("--latency-profile-id", required=True)
    parser.add_argument(
        "--latency-environment",
        default="aws_tokyo_ec2_2vcpu_4g_amazon_linux",
    )
    parser.add_argument("--window-cache-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--trace-decisions-max", type=int, default=50_000)
    parser.add_argument("--trace-fills-max", type=int, default=20_000)
    parser.add_argument("--skip-development", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    live_dir = args.live_dir.expanduser().resolve()
    config_payload = yaml.safe_load((live_dir / "live_config.yaml").read_text(encoding="utf-8"))
    strategy = config_payload["strategy"]
    p3_payload = json.loads((live_dir / "fill_prob_params.json").read_text(encoding="utf-8"))
    p3_delta_star = float(p3_payload["delta_star"])
    p3_kappa_eff = float(p3_payload["kappa_eff"])

    trades = pd.read_csv(live_dir / "trades.csv.gz")
    quotes = pd.read_csv(live_dir / "quote_decisions.csv.gz")
    live_campaigns, live_fills = reconstruct_live_campaigns(trades)
    live_campaigns, live_decisions = attach_live_quote_state(
        live_campaigns,
        live_fills,
        quotes,
        max_inventory=float(strategy["max_inventory"]),
        emergency_inventory_ratio=float(strategy["defense_emergency_inventory_ratio"]),
        emergency_loss=float(strategy["defense_emergency_loss"]),
    )
    quote_samples, depth_samples, cooldown_samples = parse_live_mechanism_logs(
        (live_dir / "maker.log.1", live_dir / "maker.log"),
        start_ts=args.live_start_ts,
        end_ts=args.live_end_ts,
    )
    quote_samples, depth_samples, cooldown_samples = enrich_live_mechanism_samples(
        quote_samples,
        depth_samples,
        cooldown_samples,
        live_campaigns,
        p3_delta_star=p3_delta_star,
        p3_kappa_eff=p3_kappa_eff,
    )
    live_campaigns = summarize_campaign_decisions(
        live_campaigns,
        live_decisions,
        p3_delta_star=p3_delta_star,
        p3_kappa_eff=p3_kappa_eff,
    )
    for samples, prefix in (
        (quote_samples, "logged"),
        (depth_samples, "logged_depth"),
    ):
        if samples.empty:
            continue
        numeric = samples.select_dtypes(include=[np.number]).columns.difference(
            ["timestamp", "campaign_id"]
        )
        aggregate = (
            samples[samples["campaign_id"].gt(0)].groupby("campaign_id")[list(numeric)].mean()
        )
        aggregate.columns = [f"{prefix}_{column}_mean" for column in aggregate.columns]
        live_campaigns = live_campaigns.merge(aggregate, on="campaign_id", how="left")
    if not cooldown_samples.empty:
        cooldown_aggregate = (
            cooldown_samples[cooldown_samples["campaign_id"].gt(0)]
            .groupby("campaign_id")
            .agg(
                cooldown_total_s_max=("cooldown_s", "max"),
                cooldown_consecutive_max=("consecutive_units", "max"),
                cooldown_sample_count=("cooldown_s", "size"),
            )
        )
        live_campaigns = live_campaigns.drop(
            columns=["cooldown_total_s_max", "cooldown_consecutive_max"],
            errors="ignore",
        ).merge(cooldown_aggregate, on="campaign_id", how="left")

    logged_mapping = {
        "sigma_sq_raw_mean": "logged_sigma_sq_raw_mean",
        "sigma_sq_blended_mean": "logged_sigma_sq_blended_mean",
        "regime_spread_scale_mean": "logged_regime_spread_scale_mean",
        "p3_floor_bound_rate": "logged_p3_floor_bound_mean",
        "dynamic_cap_bps_mean": "logged_cap_bps_mean",
        "cap_hit_rate": "logged_cap_hit_mean",
        "kappa_used_mean": "logged_depth_kappa_used_mean",
        "kappa_vs_p3_ratio_mean": "logged_depth_kappa_vs_p3_ratio_mean",
    }
    for target, source in logged_mapping.items():
        if source in live_campaigns:
            live_campaigns[target] = live_campaigns[source]
    live_campaigns["final_compress_rate"] = math.nan
    live_campaigns = classify_add_failure(live_campaigns)

    execution_parts = []
    for day in ("2026-07-19", "2026-07-20"):
        path = args.raw_trades_dir.expanduser().resolve() / f"BTCUSDC-trades-{day}.csv"
        if path.is_file():
            execution_parts.append(pd.read_csv(path))
    live_execution = pd.concat(execution_parts, ignore_index=True)
    live_defense = defense_pause_intervals(live_decisions, live_campaigns, live_execution)
    if not live_defense.empty:
        live_defense["day"] = pd.to_datetime(
            live_defense["decision_ts_ms"], unit="ms", utc=True
        ).dt.strftime("%Y-%m-%d")

    if len(live_campaigns) != 508:
        raise RuntimeError(f"frozen live window expected 508 campaigns, got {len(live_campaigns)}")
    live_groups = _group_summary(live_campaigns, source="live_48h")
    live_mechanisms = _mechanism_rollup(live_campaigns, source="live_48h")
    live_add_failures = _add_failure_rollup(live_campaigns, source="live_48h")
    no_add_positive = live_groups.query("group == 'no_add_net_positive' and side == 'ALL'").iloc[0]
    add_negative = live_groups.query("group == 'add_net_negative' and side == 'ALL'").iloc[0]
    if int(no_add_positive["campaigns"]) != 408 or int(add_negative["campaigns"]) != 100:
        raise RuntimeError("frozen live campaign decomposition no longer matches 408/100")
    live_support = evaluate_reducing_support(live_campaigns, live_defense)

    development_campaigns = pd.DataFrame()
    development_decisions = pd.DataFrame()
    development_defense = pd.DataFrame()
    development_metadata: dict[str, Any] = {}
    if not args.skip_development:
        (
            development_campaigns,
            development_decisions,
            development_defense,
            development_metadata,
        ) = run_development_replay(args, output)
        development_groups = _group_summary(development_campaigns, source="development56")
        development_mechanisms = _mechanism_rollup(development_campaigns, source="development56")
        development_add_failures = _add_failure_rollup(
            development_campaigns, source="development56"
        )
        development_support = evaluate_reducing_support(development_campaigns, development_defense)
    else:
        development_groups = pd.DataFrame()
        development_mechanisms = pd.DataFrame()
        development_add_failures = pd.DataFrame()
        development_support = {
            "schema_version": SUPPORT_GATE_VERSION,
            "open_action_family": False,
            "passed_sides": [],
            "reason": "development skipped",
        }

    live_campaigns.to_csv(output / "live_48h.campaigns.csv", index=False)
    live_groups.to_csv(output / "live_48h.groups.csv", index=False)
    live_mechanisms.to_csv(output / "live_48h.mechanisms.csv", index=False)
    live_defense.to_csv(output / "live_48h.defense_intervals.csv", index=False)
    quote_samples.to_csv(output / "live_48h.quote_dbg.csv", index=False)
    depth_samples.to_csv(output / "live_48h.depth_shadow.csv", index=False)
    cooldown_samples.to_csv(output / "live_48h.fill_cooldown.csv", index=False)
    if not development_campaigns.empty:
        development_campaigns.to_csv(output / "development.campaigns.csv", index=False)
        development_groups.to_csv(output / "development.groups.csv", index=False)
        development_mechanisms.to_csv(output / "development.mechanisms.csv", index=False)
        development_defense.to_csv(output / "development.defense_intervals.csv", index=False)
    add_failures = pd.concat([live_add_failures, development_add_failures], ignore_index=True)
    add_failures.to_csv(output / "add_failure_rollup.csv", index=False)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "code": git_workspace_identity(ROOT),
        "live_window": {
            "start_ts": args.live_start_ts,
            "end_ts": args.live_end_ts,
            "campaigns": len(live_campaigns),
            "fills": len(live_fills) - 1,
        },
        "config_sha256": _sha256(args.config.expanduser().resolve()),
        "p3_sha256": _sha256(live_dir / "fill_prob_params.json"),
        "queue_sha256": _sha256(args.queue_calibration_artifact.expanduser().resolve()),
        "replay_contract_sha256": development_metadata.get("replay_contract_sha256", ""),
        "development": development_metadata,
        "support": {"live": live_support, "development": development_support},
        "field_provenance": {
            "live_quote_and_campaign": "direct logs plus causal reconstruction",
            "live_dynamic_mechanism": "maker.log QUOTE_DBG/DEPTH_SHADOW/FILL_CD sampled diagnostics",
            "development_dynamic_mechanism": "authoritative Python replay decision trace",
            "would_fill": "individual-trade touch/strict-through support only; not queue fill counterfactual",
        },
    }
    (output / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _write_report(
        output / "report.md",
        live_groups=live_groups,
        development_groups=development_groups,
        live_mechanisms=live_mechanisms,
        development_mechanisms=development_mechanisms,
        add_failures=add_failures,
        live_support=live_support,
        development_support=development_support,
        metadata=metadata,
    )
    print(output / "report.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
