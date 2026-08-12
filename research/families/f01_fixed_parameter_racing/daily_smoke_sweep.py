#!/usr/bin/env python3
"""Small daily mechanism smoke sweep before formal daily parameter selection.

This runner is deliberately *not* an optimizer.  It runs a tiny set of
mechanism arms and checks whether replay behavior still looks like the live
maker at the level that matters before a larger daily sweep:

- quoting activity: placed orders and fills per UTC-day equivalent, as warnings
- spread sanity: final pair spread median and spread<100 coverage
- block reason sanity: no guard/pause reason should dominate unexpectedly
- side balance and inventory bounds

Only arms that pass these smoke gates should be allowed into a formal daily
sweep.  PnL is written for context, but the smoke verdict never ranks by PnL.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from models.symbol_paths import DEFAULT_SYMBOL  # noqa: E402
from models.tick_ab import (  # noqa: E402
    base_params as _base_params,
    clean_result as _clean_result,
    load_window as _load_window,
    parse_bound as _parse_bound,
    slice_window as _slice_window,
)


@dataclass(frozen=True)
class SmokeArm:
    name: str
    group: str
    overrides: dict[str, Any] = field(default_factory=dict)
    note: str = ""


# 这些 arm 只用于机制 smoke：spread 是否落在 live 可解释区间、block reason 是否失真。
# 不要从这个列表里直接挑 “最优参数”；正式 sweep 只能在 smoke 过线后再做。
CORE_SMOKE_ARMS: tuple[SmokeArm, ...] = (
    SmokeArm("baseline", "baseline", {}, "Current live-style replay params."),
    SmokeArm(
        "kr1p25",
        "spread_probe",
        {"kappa_ratio": 1.25},
        "Lower effective kappa ratio widens AS/GLFT half-spread.",
    ),
    SmokeArm(
        "kr1p10",
        "spread_probe",
        {"kappa_ratio": 1.10},
        "More aggressive widening probe; not a promotion candidate by itself.",
    ),
    SmokeArm(
        "markout_scale_0p30",
        "guard_probe",
        {"markout_spread_scale": 0.30},
        "Slightly stronger markout-driven spread/asym adjustment.",
    ),
    SmokeArm(
        "guard_widen",
        "guard_probe",
        {"adverse_spread_mult": 1.50, "defense_spread_mult": 1.70},
        "Probe whether guard widening fixes spread without causing pause dominance.",
    ),
    SmokeArm(
        "campaign_soft_widen_1p01_inv006_age60",
        "campaign_risk_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.01,
        },
        "Very light campaign risk shadow: barely widen only the exposure-increasing side at abs inventory >=0.006 BTC or campaign age >=60m.",
    ),
    SmokeArm(
        "campaign_soft_widen_1p03_inv006_age60",
        "campaign_risk_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.03,
        },
        "Softer campaign risk shadow: lightly widen only the exposure-increasing side at abs inventory >=0.006 BTC or campaign age >=60m.",
    ),
    SmokeArm(
        "campaign_soft_widen_1p05_inv006_age60",
        "campaign_risk_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.05,
        },
        "Softer campaign risk shadow: lightly widen only the exposure-increasing side at abs inventory >=0.006 BTC or campaign age >=60m.",
    ),
    SmokeArm(
        "campaign_soft_gated_1p05_inv006_age60",
        "campaign_risk_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.05,
            "campaign_soft_gate_enabled": True,
            "campaign_soft_gate_campaign_inv_ref": 0.006,
            "campaign_soft_gate_campaign_age_ref_s": 60.0 * 60.0,
            # The current live ret head is usually inside +/-1.4e-5, so 1e-5
            # is already a high quote-time side-adverse trend score.
            "campaign_soft_gate_trend_ret_ref": 1e-5,
            "campaign_soft_gate_refill_ref": 0.10,
            "campaign_soft_gate_campaign_score": 1.0,
            "campaign_soft_gate_trend_score": 1.0,
            "campaign_soft_gate_refill_edge_max": 0.02,
            "campaign_soft_gate_reversion_max": 0.5,
        },
        "Conditional campaign soft widen: 1.05x only when campaign risk, side-adverse trend, and weak quote-time repair/refill all hit.",
    ),
    SmokeArm(
        "fill_cd_reduce_8",
        "cooldown_probe",
        {"fill_cooldown_reducing": 8.0},
        "Short same-side cooldown after inventory-reducing fills; default live behavior is unchanged.",
    ),
    SmokeArm(
        "fill_cd_reduce_12",
        "cooldown_probe",
        {"fill_cooldown_reducing": 12.0},
        "Medium short same-side cooldown after inventory-reducing fills; smoke only.",
    ),
    SmokeArm(
        "fill_cd_reduce_20",
        "cooldown_probe",
        {"fill_cooldown_reducing": 20.0},
        "Longer reducing-side pacing cooldown; still shorter than the 41s add-side fill cooldown.",
    ),
    SmokeArm(
        "add_cd_adaptive_risk",
        "cooldown_add_adaptive_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_min_mult": 0.75,
            "adaptive_add_cooldown_max_mult": 2.00,
            "adaptive_add_cooldown_w_flow": 0.20,
            "adaptive_add_cooldown_w_campaign": 0.25,
            "adaptive_add_cooldown_w_trend": 0.25,
            "adaptive_add_cooldown_w_refill_weak": 0.15,
        },
        "Adaptive add-side fill cooldown: lengthen only when flow persistence/campaign risk/adverse trend/refill weakness are present.",
    ),
    SmokeArm(
        "add_cd_adaptive_risk_reversion",
        "cooldown_add_adaptive_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_min_mult": 0.60,
            "adaptive_add_cooldown_max_mult": 2.00,
            "adaptive_add_cooldown_w_flow": 0.20,
            "adaptive_add_cooldown_w_campaign": 0.25,
            "adaptive_add_cooldown_w_trend": 0.25,
            "adaptive_add_cooldown_w_refill_weak": 0.15,
            "adaptive_add_cooldown_w_refill_good": 0.15,
            "adaptive_add_cooldown_w_reversion": 0.20,
        },
        "Adaptive add-side fill cooldown with quote-time refill/micro-reversion discount; still smoke-only.",
    ),
    SmokeArm(
        "add_cd_adaptive_mild",
        "cooldown_add_adaptive_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_min_mult": 0.80,
            "adaptive_add_cooldown_max_mult": 1.35,
            "adaptive_add_cooldown_w_flow": 0.08,
            "adaptive_add_cooldown_w_campaign": 0.10,
            "adaptive_add_cooldown_w_trend": 0.10,
            "adaptive_add_cooldown_w_refill_weak": 0.05,
            "adaptive_add_cooldown_w_refill_good": 0.08,
            "adaptive_add_cooldown_w_reversion": 0.10,
        },
        "Mild adaptive add-side cooldown: smaller envelope to avoid killing natural campaign repair.",
    ),
    SmokeArm(
        "add_cd_adaptive_reversion_tilt",
        "cooldown_add_adaptive_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_min_mult": 0.65,
            "adaptive_add_cooldown_max_mult": 1.25,
            "adaptive_add_cooldown_w_flow": 0.05,
            "adaptive_add_cooldown_w_campaign": 0.06,
            "adaptive_add_cooldown_w_trend": 0.08,
            "adaptive_add_cooldown_w_refill_weak": 0.04,
            "adaptive_add_cooldown_w_refill_good": 0.14,
            "adaptive_add_cooldown_w_reversion": 0.18,
        },
        "Mild adaptive add-side cooldown tilted toward quote-time refill/micro-reversion discounts.",
    ),
    SmokeArm(
        "add_cd_state_gate_70",
        "cooldown_add_state_gate_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_min_mult": 1.0,
            "adaptive_add_cooldown_max_mult": 1.75,
            "adaptive_add_cooldown_gate_mult": 70.0 / 41.0,
            "adaptive_add_cooldown_gate_campaign_score": 1.0,
            "adaptive_add_cooldown_gate_trend_score": 1.0,
            "adaptive_add_cooldown_gate_refill_edge_max": 0.0,
            "adaptive_add_cooldown_gate_reversion_max": 0.5,
            "adaptive_add_cooldown_campaign_inv_ref": 0.006,
            "adaptive_add_cooldown_campaign_age_ref_s": 60.0 * 60.0,
            "adaptive_add_cooldown_trend_ret_ref": 0.00002,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "State-gated add-side cooldown: keep 41s unless campaign risk + adverse trend + weak local repair; then approx 70s.",
    ),
    SmokeArm(
        "add_cd_state_gate_75",
        "cooldown_add_state_gate_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_min_mult": 1.0,
            "adaptive_add_cooldown_max_mult": 1.85,
            "adaptive_add_cooldown_gate_mult": 75.0 / 41.0,
            "adaptive_add_cooldown_gate_campaign_score": 1.0,
            "adaptive_add_cooldown_gate_trend_score": 1.0,
            "adaptive_add_cooldown_gate_refill_edge_max": 0.0,
            "adaptive_add_cooldown_gate_reversion_max": 0.5,
            "adaptive_add_cooldown_campaign_inv_ref": 0.006,
            "adaptive_add_cooldown_campaign_age_ref_s": 60.0 * 60.0,
            "adaptive_add_cooldown_trend_ret_ref": 0.00002,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "State-gated add-side cooldown: keep 41s unless campaign risk + adverse trend + weak local repair; then approx 75s.",
    ),
    SmokeArm(
        "add_cd_state_gate_75_repair_loose",
        "cooldown_add_state_gate_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_min_mult": 1.0,
            "adaptive_add_cooldown_max_mult": 1.85,
            "adaptive_add_cooldown_gate_mult": 75.0 / 41.0,
            "adaptive_add_cooldown_gate_campaign_score": 1.0,
            "adaptive_add_cooldown_gate_trend_score": 0.5,
            "adaptive_add_cooldown_gate_refill_edge_max": 0.05,
            "adaptive_add_cooldown_gate_reversion_max": 0.75,
            "adaptive_add_cooldown_campaign_inv_ref": 0.006,
            "adaptive_add_cooldown_campaign_age_ref_s": 60.0 * 60.0,
            "adaptive_add_cooldown_trend_ret_ref": 0.00002,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "State-gated add-side cooldown with looser trend/repair thresholds; campaign risk still must be high.",
    ),
    SmokeArm(
        "add_cd_state_gate_75_repair_loose_sell",
        "cooldown_add_state_gate_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_min_mult": 1.0,
            "adaptive_add_cooldown_max_mult": 1.85,
            "adaptive_add_cooldown_gate_mult": 75.0 / 41.0,
            "adaptive_add_cooldown_gate_campaign_score": 1.0,
            "adaptive_add_cooldown_gate_trend_score": 0.5,
            "adaptive_add_cooldown_gate_refill_edge_max": 0.05,
            "adaptive_add_cooldown_gate_reversion_max": 0.75,
            "adaptive_add_cooldown_gate_side": "SELL",
            "adaptive_add_cooldown_campaign_inv_ref": 0.006,
            "adaptive_add_cooldown_campaign_age_ref_s": 60.0 * 60.0,
            "adaptive_add_cooldown_trend_ret_ref": 0.00002,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "Same as repair_loose, but only lengthens SELL add-side cooldown; diagnostic for 2026-07-01 BUY open-risk distortion.",
    ),
    SmokeArm(
        "add_cd_state_gate_60_repair_loose",
        "cooldown_add_state_gate_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_min_mult": 1.0,
            "adaptive_add_cooldown_max_mult": 1.50,
            "adaptive_add_cooldown_gate_mult": 60.0 / 41.0,
            "adaptive_add_cooldown_gate_campaign_score": 1.0,
            "adaptive_add_cooldown_gate_trend_score": 0.5,
            "adaptive_add_cooldown_gate_refill_edge_max": 0.05,
            "adaptive_add_cooldown_gate_reversion_max": 0.75,
            "adaptive_add_cooldown_campaign_inv_ref": 0.006,
            "adaptive_add_cooldown_campaign_age_ref_s": 60.0 * 60.0,
            "adaptive_add_cooldown_trend_ret_ref": 0.00002,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "State-gated add-side cooldown with a smaller 60s target multiplier; checks whether 75s over-distorts campaign terminal PnL.",
    ),
    SmokeArm(
        "add_cd_state_gate_60_repair_loose_sell",
        "cooldown_add_state_gate_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_min_mult": 1.0,
            "adaptive_add_cooldown_max_mult": 1.50,
            "adaptive_add_cooldown_gate_mult": 60.0 / 41.0,
            "adaptive_add_cooldown_gate_campaign_score": 1.0,
            "adaptive_add_cooldown_gate_trend_score": 0.5,
            "adaptive_add_cooldown_gate_refill_edge_max": 0.05,
            "adaptive_add_cooldown_gate_reversion_max": 0.75,
            "adaptive_add_cooldown_gate_side": "SELL",
            "adaptive_add_cooldown_campaign_inv_ref": 0.006,
            "adaptive_add_cooldown_campaign_age_ref_s": 60.0 * 60.0,
            "adaptive_add_cooldown_trend_ret_ref": 0.00002,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "SELL-only version of the smaller 60s target multiplier.",
    ),
    SmokeArm(
        "add_cd_state_gate_75_risk_loose",
        "cooldown_add_state_gate_probe",
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_gate_enabled": True,
            "adaptive_add_cooldown_min_mult": 1.0,
            "adaptive_add_cooldown_max_mult": 1.85,
            "adaptive_add_cooldown_gate_mult": 75.0 / 41.0,
            "adaptive_add_cooldown_gate_campaign_score": 0.7,
            "adaptive_add_cooldown_gate_trend_score": 0.5,
            "adaptive_add_cooldown_gate_refill_edge_max": 0.05,
            "adaptive_add_cooldown_gate_reversion_max": 0.75,
            "adaptive_add_cooldown_campaign_inv_ref": 0.006,
            "adaptive_add_cooldown_campaign_age_ref_s": 60.0 * 60.0,
            "adaptive_add_cooldown_trend_ret_ref": 0.00002,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "State-gated add-side cooldown with looser campaign/trend/repair thresholds; diagnostic only.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_inv4_4s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 4.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 4.0,
        },
        "Conditional reducing cooldown: only pace reducers when abs inventory >= 4x order_size.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_inv6_4s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 4.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
        },
        "Conditional reducing cooldown: 4s at abs inventory >= 6x order_size.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_inv8_4s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 4.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 8.0,
        },
        "Conditional reducing cooldown: 4s at abs inventory >= 8x order_size.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_inv6_5s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 5.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
        },
        "Conditional reducing cooldown: 5s at abs inventory >= 6x order_size.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_inv6_8s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 8.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
        },
        "Conditional reducing cooldown: 8s at abs inventory >= 6x order_size.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_inv6_12s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 12.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
        },
        "Conditional reducing cooldown: 12s at abs inventory >= 6x order_size.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_age20m_4s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 4.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_age_s": 20.0 * 60.0,
        },
        "Conditional reducing cooldown: 4s once campaign age >=20m.",
    ),
    SmokeArm(
        "fill_cd_reduce_cond_inv6_age20m_4s",
        "cooldown_campaign_probe",
        {
            "fill_cooldown_reducing": 4.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
            "fill_cooldown_reducing_age_s": 20.0 * 60.0,
        },
        "Conditional reducing cooldown: 4s if abs inventory >=6x order_size or campaign age >=20m.",
    ),
)


SMOKE_ARMS: tuple[SmokeArm, ...] = CORE_SMOKE_ARMS


ACCEPTANCE = {
    # Hard gates: formal daily sweep candidates must pass these first.
    "pair_spread_p50": {"min": 45.0, "max": 75.0},
    "final_spread_lt_100_rate": {"min": 0.75},
    "allow_post_rate": {"min": 0.75},
    "pause_rate": {"max": 0.25},
    "bad_guard_block_rate": {"max": 0.12},
    "top_bad_reason_rate": {"max": 0.08},
    "fill_cd_block_rate": {"max": 0.18},
    "side_min_fill_share": {"min": 0.15, "min_fills": 20},
    "inventory_eps": 0.002,
    # Calibration warnings: useful for live/replay alignment, but not hard
    # spread/block-reason acceptance. Recalibrate these from fresh live windows.
    "placed_per_utc_day_warning": {"min": 12_000.0, "max": 55_000.0},
    "fills_per_utc_day_warning": {"min": 20.0, "max": 700.0},
    "fill_per_placed_warning": {"min": 0.0015, "max": 0.0250},
}

GOOD_BLOCK_REASONS = {"", "none", "fill_cd", "campaign_stop_add"}
BAD_REASON_TOKENS = (
    "adverse",
    "defense",
    "thin_depth",
    "local_extreme",
    "stale",
    "toxicity",
)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame.empty or column not in frame:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").replace([np.inf, -np.inf], np.nan)


def _safe_mean(series: pd.Series) -> float:
    vals = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(vals.mean()) if len(vals) else 0.0


def _safe_quantile(series: pd.Series, q: float) -> float:
    vals = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(vals.quantile(q)) if len(vals) else 0.0


def _reason_tokens(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw or raw == "nan":
        return ["none"]
    return [token.strip() for token in raw.split("|") if token.strip()] or ["none"]


def _block_reason_metrics(decisions: pd.DataFrame) -> dict[str, Any]:
    if decisions.empty:
        return {
            "decision_count": 0,
            "block_count": 0,
            "block_rate": 0.0,
            "allow_post_rate": 0.0,
            "pause_rate": 0.0,
            "fill_cd_block_rate": 0.0,
            "bad_guard_block_rate": 0.0,
            "top_block_reason": "none",
            "top_block_reason_rate": 0.0,
            "top_bad_reason": "none",
            "top_bad_reason_rate": 0.0,
        }

    total = int(len(decisions))
    allow_post = _numeric(decisions, "allow_post").fillna(0).astype(int)
    blocked = decisions[allow_post.eq(0)]
    reason_counts: dict[str, int] = {}
    blocked_bad_rows = 0
    fill_cd_rows = 0
    for reason_text in blocked.get("reason_text", pd.Series(dtype=str)):
        tokens = _reason_tokens(reason_text)
        token_set = set(tokens)
        if "fill_cd" in token_set:
            fill_cd_rows += 1
        row_bad = False
        for token in token_set:
            reason_counts[token] = reason_counts.get(token, 0) + 1
            if token not in GOOD_BLOCK_REASONS and any(bad in token for bad in BAD_REASON_TOKENS):
                row_bad = True
        if row_bad:
            blocked_bad_rows += 1

    top_reason, top_count = ("none", 0)
    if reason_counts:
        top_reason, top_count = max(reason_counts.items(), key=lambda kv: kv[1])

    bad_reason_counts = {
        reason: count
        for reason, count in reason_counts.items()
        if reason not in GOOD_BLOCK_REASONS and any(bad in reason for bad in BAD_REASON_TOKENS)
    }
    top_bad_reason, top_bad_count = ("none", 0)
    if bad_reason_counts:
        top_bad_reason, top_bad_count = max(bad_reason_counts.items(), key=lambda kv: kv[1])

    action = decisions.get("action", pd.Series(dtype=str)).astype(str)
    return {
        "decision_count": total,
        "block_count": int(len(blocked)),
        "block_rate": float(len(blocked) / max(total, 1)),
        "allow_post_rate": float(allow_post.mean()),
        "pause_rate": float(action.eq("pause").mean()),
        "fill_cd_block_rate": float(fill_cd_rows / max(total, 1)),
        "bad_guard_block_rate": float(blocked_bad_rows / max(total, 1)),
        "top_block_reason": top_reason,
        "top_block_reason_rate": float(top_count / max(total, 1)),
        "top_bad_reason": top_bad_reason,
        "top_bad_reason_rate": float(top_bad_count / max(total, 1)),
        "block_reason_counts_json": json.dumps(reason_counts, sort_keys=True, ensure_ascii=True),
    }


def _gate_status(row: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    max_inv_limit = float(base.get("max_inventory", 0.0) or 0.0) + float(ACCEPTANCE["inventory_eps"])
    side_min_share = min(float(row.get("buy_fill_share", 0.0)), float(row.get("sell_fill_share", 0.0)))
    side_gate = True
    if int(row.get("fills_total", 0) or 0) >= int(ACCEPTANCE["side_min_fill_share"]["min_fills"]):
        side_gate = side_min_share >= float(ACCEPTANCE["side_min_fill_share"]["min"])

    warnings = {
        "warn_activity_placed": not (
            float(ACCEPTANCE["placed_per_utc_day_warning"]["min"])
            <= float(row.get("placed_per_utc_day", 0.0))
            <= float(ACCEPTANCE["placed_per_utc_day_warning"]["max"])
        ),
        "warn_activity_fills": not (
            float(ACCEPTANCE["fills_per_utc_day_warning"]["min"])
            <= float(row.get("fills_per_utc_day", 0.0))
            <= float(ACCEPTANCE["fills_per_utc_day_warning"]["max"])
        ),
        "warn_fill_per_placed": not (
            float(ACCEPTANCE["fill_per_placed_warning"]["min"])
            <= float(row.get("fill_per_placed", 0.0))
            <= float(ACCEPTANCE["fill_per_placed_warning"]["max"])
        ),
    }

    gates = {
        "gate_spread_p50": (
            float(ACCEPTANCE["pair_spread_p50"]["min"])
            <= float(row.get("pair_spread_p50", 0.0))
            <= float(ACCEPTANCE["pair_spread_p50"]["max"])
        ),
        "gate_spread_lt100": (
            float(row.get("final_spread_lt_100_rate", 0.0))
            >= float(ACCEPTANCE["final_spread_lt_100_rate"]["min"])
        ),
        "gate_allow_post": (
            float(row.get("allow_post_rate", 0.0))
            >= float(ACCEPTANCE["allow_post_rate"]["min"])
        ),
        "gate_pause_rate": (
            float(row.get("pause_rate", 0.0))
            <= float(ACCEPTANCE["pause_rate"]["max"])
        ),
        "gate_bad_block_reason": (
            float(row.get("bad_guard_block_rate", 0.0))
            <= float(ACCEPTANCE["bad_guard_block_rate"]["max"])
            and float(row.get("top_bad_reason_rate", 0.0))
            <= float(ACCEPTANCE["top_bad_reason_rate"]["max"])
        ),
        "gate_fill_cd_reason": (
            float(row.get("fill_cd_block_rate", 0.0))
            <= float(ACCEPTANCE["fill_cd_block_rate"]["max"])
        ),
        "gate_side_balance": bool(side_gate),
        "gate_inventory": float(row.get("max_inventory", 0.0)) <= max_inv_limit,
        "gate_trace_complete": not (
            bool(row.get("trace_orders_truncated", False))
            or bool(row.get("trace_decisions_truncated", False))
        ),
    }
    failures = [key for key, ok in gates.items() if not ok]
    warning_names = [key for key, active in warnings.items() if active]
    gates["pass_mechanism_smoke"] = not failures
    gates["gate_failures"] = ",".join(failures)
    gates["smoke_warnings"] = ",".join(warning_names)
    return gates


def _summarize_case(
    *,
    day: str,
    arm: SmokeArm,
    result: dict[str, Any],
    orders: pd.DataFrame,
    decisions: pd.DataFrame,
    runtime_s: float,
    base: dict[str, Any],
) -> dict[str, Any]:
    clean = _clean_result(result)
    trades_days = max(float(clean.get("n_days", 1.0) or 1.0), 1e-9)
    unique_orders = int(orders["order_id"].nunique()) if not orders.empty and "order_id" in orders else 0
    fills_bid = int(clean.get("fills_bid", 0) or 0)
    fills_ask = int(clean.get("fills_ask", 0) or 0)
    fills_total = int(clean.get("fills_total", fills_bid + fills_ask) or 0)
    total_side_fills = max(fills_total, 1)

    spreads = _numeric(decisions, "final_pair_spread")
    quote_delta = _numeric(decisions, "final_quote_delta_to_bbo")
    decision_metrics = _block_reason_metrics(decisions)
    row: dict[str, Any] = {
        "day": day,
        "arm": arm.name,
        "group": arm.group,
        "override": ";".join(f"{k}={v}" for k, v in sorted(arm.overrides.items())) or "baseline",
        "note": arm.note,
        "runtime_s": round(runtime_s, 3),
        "n_days": trades_days,
        "pnl": float(clean.get("pnl", 0.0) or 0.0),
        "inventory_adjusted_pnl": float(clean.get("inventory_adjusted_pnl", 0.0) or 0.0),
        "max_inventory": float(clean.get("max_inventory", 0.0) or 0.0),
        "final_inventory": float(clean.get("final_inventory", 0.0) or 0.0),
        "abs_inventory_time_s": float(clean.get("abs_inventory_time_s", 0.0) or 0.0),
        "time_avg_abs_inventory": float(clean.get("time_avg_abs_inventory", 0.0) or 0.0),
        "avg_markout": float(clean.get("avg_markout", 0.0) or 0.0),
        "avg_markout_bid": float(clean.get("avg_markout_bid", 0.0) or 0.0),
        "avg_markout_ask": float(clean.get("avg_markout_ask", 0.0) or 0.0),
        "fills_bid": fills_bid,
        "fills_ask": fills_ask,
        "fills_total": fills_total,
        "buy_fill_share": fills_bid / total_side_fills,
        "sell_fill_share": fills_ask / total_side_fills,
        "placed_orders": unique_orders,
        "placed_per_utc_day": unique_orders / trades_days,
        "fills_per_utc_day": fills_total / trades_days,
        "fill_per_placed": fills_total / max(unique_orders, 1),
        "summary_avg_final_spread": float(clean.get("avg_final_spread", 0.0) or 0.0),
        "pair_spread_mean": _safe_mean(spreads),
        "pair_spread_p50": _safe_quantile(spreads, 0.50),
        "pair_spread_p90": _safe_quantile(spreads, 0.90),
        "final_spread_lt_100_rate": float(clean.get("final_spread_lt_100_rate", 0.0) or 0.0),
        "final_quote_delta_to_bbo_p50": _safe_quantile(quote_delta, 0.50),
        "cap_hit_rate": float(clean.get("cap_hit_rate", 0.0) or 0.0),
        "adverse_pause_rate": float(clean.get("adverse_pause_rate", 0.0) or 0.0),
        "defense_pause_rate": float(clean.get("defense_pause_rate", 0.0) or 0.0),
        "fill_cooldown_block_count": int(clean.get("fill_cooldown_block_count", 0) or 0),
        "fill_cooldown_reducing": float(clean.get("fill_cooldown_reducing", base.get("fill_cooldown_reducing", 0.0)) or 0.0),
        "fill_cooldown_reducing_campaign_only": bool(clean.get("fill_cooldown_reducing_campaign_only", base.get("fill_cooldown_reducing_campaign_only", False)) or False),
        "fill_cooldown_reducing_inv_threshold": float(clean.get("fill_cooldown_reducing_inv_threshold", base.get("fill_cooldown_reducing_inv_threshold", 0.0)) or 0.0),
        "fill_cooldown_reducing_inv_ratio": float(clean.get("fill_cooldown_reducing_inv_ratio", base.get("fill_cooldown_reducing_inv_ratio", 0.0)) or 0.0),
        "fill_cooldown_reducing_age_s": float(clean.get("fill_cooldown_reducing_age_s", base.get("fill_cooldown_reducing_age_s", 0.0)) or 0.0),
        "fill_cooldown_reducing_vol_ref": float(clean.get("fill_cooldown_reducing_vol_ref", base.get("fill_cooldown_reducing_vol_ref", 0.0)) or 0.0),
        "adaptive_add_cooldown_enabled": bool(clean.get("adaptive_add_cooldown_enabled", base.get("adaptive_add_cooldown_enabled", False)) or False),
        "adaptive_add_cooldown_min_mult": float(clean.get("adaptive_add_cooldown_min_mult", base.get("adaptive_add_cooldown_min_mult", 0.5)) or 0.5),
        "adaptive_add_cooldown_max_mult": float(clean.get("adaptive_add_cooldown_max_mult", base.get("adaptive_add_cooldown_max_mult", 2.5)) or 2.5),
        "adaptive_add_cooldown_w_flow": float(clean.get("adaptive_add_cooldown_w_flow", base.get("adaptive_add_cooldown_w_flow", 0.0)) or 0.0),
        "adaptive_add_cooldown_w_campaign": float(clean.get("adaptive_add_cooldown_w_campaign", base.get("adaptive_add_cooldown_w_campaign", 0.0)) or 0.0),
        "adaptive_add_cooldown_w_trend": float(clean.get("adaptive_add_cooldown_w_trend", base.get("adaptive_add_cooldown_w_trend", 0.0)) or 0.0),
        "adaptive_add_cooldown_w_refill_weak": float(clean.get("adaptive_add_cooldown_w_refill_weak", base.get("adaptive_add_cooldown_w_refill_weak", 0.0)) or 0.0),
        "adaptive_add_cooldown_w_refill_good": float(clean.get("adaptive_add_cooldown_w_refill_good", base.get("adaptive_add_cooldown_w_refill_good", 0.0)) or 0.0),
        "adaptive_add_cooldown_w_reversion": float(clean.get("adaptive_add_cooldown_w_reversion", base.get("adaptive_add_cooldown_w_reversion", 0.0)) or 0.0),
        "adaptive_add_cooldown_gate_enabled": bool(clean.get("adaptive_add_cooldown_gate_enabled", base.get("adaptive_add_cooldown_gate_enabled", False)) or False),
        "adaptive_add_cooldown_gate_mult": float(clean.get("adaptive_add_cooldown_gate_mult", base.get("adaptive_add_cooldown_gate_mult", 1.75)) or 1.75),
        "adaptive_add_cooldown_gate_campaign_score": float(clean.get("adaptive_add_cooldown_gate_campaign_score", base.get("adaptive_add_cooldown_gate_campaign_score", 1.0)) or 1.0),
        "adaptive_add_cooldown_gate_trend_score": float(clean.get("adaptive_add_cooldown_gate_trend_score", base.get("adaptive_add_cooldown_gate_trend_score", 1.0)) or 1.0),
        "adaptive_add_cooldown_gate_refill_edge_max": float(clean.get("adaptive_add_cooldown_gate_refill_edge_max", base.get("adaptive_add_cooldown_gate_refill_edge_max", 0.0)) or 0.0),
        "adaptive_add_cooldown_gate_reversion_max": float(clean.get("adaptive_add_cooldown_gate_reversion_max", base.get("adaptive_add_cooldown_gate_reversion_max", 0.5)) or 0.5),
        "adaptive_add_cooldown_gate_side": str(clean.get("adaptive_add_cooldown_gate_side", base.get("adaptive_add_cooldown_gate_side", "BOTH")) or "BOTH"),
        "adaptive_add_cooldown_hit_count": int(clean.get("adaptive_add_cooldown_hit_count", 0) or 0),
        "adaptive_add_cooldown_bid_hit_count": int(clean.get("adaptive_add_cooldown_bid_hit_count", 0) or 0),
        "adaptive_add_cooldown_ask_hit_count": int(clean.get("adaptive_add_cooldown_ask_hit_count", 0) or 0),
        "campaign_count": int(clean.get("campaign_count", 0) or 0),
        "campaign_closed_count": int(clean.get("campaign_closed_count", 0) or 0),
        "campaign_open_count": int(clean.get("campaign_open_count", 0) or 0),
        "campaign_max_abs_inventory": float(clean.get("campaign_max_abs_inventory", 0.0) or 0.0),
        "campaign_max_duration_s": float(clean.get("campaign_max_duration_s", 0.0) or 0.0),
        "campaign_max_adverse_excursion": float(clean.get("campaign_max_adverse_excursion", 0.0) or 0.0),
        "campaign_exposure_increasing_fills": int(clean.get("campaign_exposure_increasing_fills", 0) or 0),
        "campaign_reducing_fills": int(clean.get("campaign_reducing_fills", 0) or 0),
        "campaign_shadow_inv_006_blocks": int(clean.get("campaign_shadow_inv_006_blocks", 0) or 0),
        "campaign_shadow_age_60m_blocks": int(clean.get("campaign_shadow_age_60m_blocks", 0) or 0),
        "campaign_stop_add_count": int(clean.get("campaign_stop_add_count", 0) or 0),
        "bid_campaign_stop_add_count": int(clean.get("bid_campaign_stop_add_count", 0) or 0),
        "ask_campaign_stop_add_count": int(clean.get("ask_campaign_stop_add_count", 0) or 0),
        "campaign_soft_control_count": int(clean.get("campaign_soft_control_count", 0) or 0),
        "bid_campaign_soft_control_count": int(clean.get("bid_campaign_soft_control_count", 0) or 0),
        "ask_campaign_soft_control_count": int(clean.get("ask_campaign_soft_control_count", 0) or 0),
        "trace_orders": len(orders),
        "trace_decisions": len(decisions),
        "trace_orders_truncated": int(len(orders) >= int(base.get("trace_quotes_max", 0) or 0) > 0),
        "trace_decisions_truncated": int(len(decisions) >= int(base.get("trace_decisions_max", 0) or 0) > 0),
    }
    row.update(decision_metrics)
    row.update(_gate_status(row, base))
    return row


def _run_case(
    *,
    day: str,
    arm: SmokeArm,
    base: dict[str, Any],
    window: dict[str, Any],
    engine: str,
) -> dict[str, Any]:
    params = dict(base)
    params.update(arm.overrides)
    started = time.perf_counter()
    result = bt._simulate_tick_with_engine(
        engine,
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        params,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
        reference_event_tapes=window.get("reference_event_tapes"),
        campaign_repair_data=window.get("campaign_repair_data"),
        campaign_repair_model=window.get("campaign_repair_model"),
        historical_global_flow_data=window.get("historical_global_flow_data"),
    )
    runtime_s = time.perf_counter() - started
    orders = pd.DataFrame(result.get("_quote_trace", []))
    decisions = pd.DataFrame(result.get("_decision_trace", []))
    return _summarize_case(
        day=day,
        arm=arm,
        result=result,
        orders=orders,
        decisions=decisions,
        runtime_s=runtime_s,
        base=params,
    )


def _normalize_days(days: list[str]) -> list[str]:
    out: list[str] = []
    for item in days:
        token = str(item).strip()
        if len(token) != 10:
            raise ValueError(f"Use explicit UTC daily dates YYYY-MM-DD, not ranges/months: {token}")
        ts = pd.Timestamp(token)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        out.append(ts.strftime("%Y-%m-%d"))
    return sorted(set(out))


def _write_outputs(rows: list[dict[str, Any]], *, tag: str, symbol: str) -> None:
    out_dir = bt.RESULTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"daily_smoke_sweep_{tag}_{symbol.lower()}"
    csv_path = out_dir / f"{stem}.csv"
    summary_path = out_dir / f"{stem}_summary.csv"
    rollup_path = out_dir / f"{stem}_rollup.csv"
    json_path = out_dir / f"{stem}.acceptance.json"
    md_path = out_dir / f"{stem}.md"

    frame = pd.DataFrame(rows).sort_values(["day", "group", "arm"])
    frame.to_csv(csv_path, index=False)

    summary_cols = [
        "day",
        "arm",
        "group",
        "pass_mechanism_smoke",
        "gate_failures",
        "smoke_warnings",
        "placed_per_utc_day",
        "fills_per_utc_day",
        "fill_per_placed",
        "fills_bid",
        "fills_ask",
        "pair_spread_p50",
        "pair_spread_p90",
        "final_spread_lt_100_rate",
        "allow_post_rate",
        "pause_rate",
        "fill_cd_block_rate",
        "bad_guard_block_rate",
        "top_block_reason",
        "top_bad_reason",
        "max_inventory",
        "abs_inventory_time_s",
        "time_avg_abs_inventory",
        "avg_markout_bid",
        "avg_markout_ask",
        "pnl",
        "inventory_adjusted_pnl",
        "note",
    ]
    summary_cols = [col for col in summary_cols if col in frame.columns]
    summary = frame[summary_cols]
    summary.to_csv(summary_path, index=False)
    rollup = _build_rollup(frame)
    rollup.to_csv(rollup_path, index=False)

    payload = {
        "tag": tag,
        "symbol": symbol.upper(),
        "purpose": "mechanism smoke only; not optimal parameter selection",
        "acceptance": ACCEPTANCE,
        "eligible_arms": (
            rollup[rollup["eligible_for_formal_daily_sweep"].astype(bool)]["arm"].tolist()
            if not summary.empty
            else []
        ),
        "rows": json.loads(summary.to_json(orient="records")),
        "rollup": json.loads(rollup.to_json(orient="records")),
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    header = "| " + " | ".join(summary_cols) + " |"
    sep = "|" + "|".join("---" for _ in summary_cols) + "|"
    lines = [
        f"# Daily Mechanism Smoke Sweep {tag}",
        "",
        "This is a spread/block-reason smoke gate, not optimal parameter selection.",
        "",
        header,
        sep,
    ]
    for _, row in summary.iterrows():
        vals = []
        for col in summary_cols:
            val = row[col]
            if isinstance(val, (float, np.floating)):
                vals.append(f"{float(val):.4f}")
            else:
                vals.append(str(val))
        lines.append("| " + " | ".join(vals) + " |")
    if not rollup.empty:
        rollup_cols = [
            "arm",
            "eligible_for_formal_daily_sweep",
            "n_days",
            "pass_days",
            "pass_rate",
            "sum_pnl_passed",
            "sum_inv_adj_passed",
            "worst_pnl_day",
            "worst_pnl_passed",
            "tail_loss_days_passed",
            "worst_inv_adj_passed",
            "tail_inv_adj_loss_days_passed",
            "sum_abs_inventory_time_s_passed",
            "weighted_bid_markout_passed",
            "weighted_ask_markout_passed",
            "mean_pair_spread_p50_passed",
            "mean_bad_guard_block_rate_passed",
        ]
        rollup_cols = [col for col in rollup_cols if col in rollup.columns]
        lines.extend(["", "## Hard-gated performance rollup", ""])
        lines.append("| " + " | ".join(rollup_cols) + " |")
        lines.append("|" + "|".join("---" for _ in rollup_cols) + "|")
        for _, row in rollup[rollup_cols].iterrows():
            vals = []
            for col in rollup_cols:
                val = row[col]
                if isinstance(val, (float, np.floating)):
                    vals.append(f"{float(val):.4f}")
                else:
                    vals.append(str(val))
            lines.append("| " + " | ".join(vals) + " |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    for path in (csv_path, summary_path, rollup_path, json_path, md_path):
        print(f"Saved {path}")
    print(summary.to_string(index=False))
    if not rollup.empty:
        print("\nHard-gated rollup (performance read only after mechanism gates):")
        print(rollup.to_string(index=False))


def _weighted_mean(frame: pd.DataFrame, value_col: str, weight_col: str) -> float:
    if frame.empty or value_col not in frame or weight_col not in frame:
        return 0.0
    vals = pd.to_numeric(frame[value_col], errors="coerce")
    weights = pd.to_numeric(frame[weight_col], errors="coerce").fillna(0.0)
    mask = vals.notna() & weights.gt(0)
    if not bool(mask.any()):
        return 0.0
    return float((vals[mask] * weights[mask]).sum() / max(weights[mask].sum(), 1e-12))


def _build_rollup(frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate performance only after per-day mechanism hard gates pass."""
    if frame.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for arm, grp in frame.groupby("arm", sort=True):
        grp = grp.sort_values("day")
        passed = grp[grp["pass_mechanism_smoke"].astype(bool)].copy()
        n_days = int(len(grp))
        pass_days = int(len(passed))
        eligible = pass_days == n_days and n_days > 0
        worst_pnl_day = ""
        worst_inv_day = ""
        if not passed.empty:
            worst_pnl_idx = pd.to_numeric(passed["pnl"], errors="coerce").idxmin()
            worst_inv_idx = pd.to_numeric(passed["inventory_adjusted_pnl"], errors="coerce").idxmin()
            worst_pnl_day = str(passed.loc[worst_pnl_idx, "day"])
            worst_inv_day = str(passed.loc[worst_inv_idx, "day"])
        row: dict[str, Any] = {
            "arm": arm,
            "group": str(grp["group"].iloc[0]) if "group" in grp else "",
            "eligible_for_formal_daily_sweep": bool(eligible),
            "n_days": n_days,
            "pass_days": pass_days,
            "fail_days": int(n_days - pass_days),
            "pass_rate": float(pass_days / max(n_days, 1)),
            "failed_days": ",".join(grp.loc[~grp["pass_mechanism_smoke"].astype(bool), "day"].astype(str).tolist()),
        }
        if passed.empty:
            rows.append(row)
            continue

        pnl = pd.to_numeric(passed["pnl"], errors="coerce")
        inv_adj = pd.to_numeric(passed["inventory_adjusted_pnl"], errors="coerce")
        row.update({
            "sum_pnl_passed": float(pnl.sum()),
            "mean_pnl_passed": float(pnl.mean()),
            "median_pnl_passed": float(pnl.median()),
            "p10_pnl_passed": float(pnl.quantile(0.10)),
            "worst_pnl_day": worst_pnl_day,
            "worst_pnl_passed": float(pnl.min()),
            "tail_loss_days_passed": int((pnl < 0.0).sum()),
            "sum_inv_adj_passed": float(inv_adj.sum()),
            "mean_inv_adj_passed": float(inv_adj.mean()),
            "median_inv_adj_passed": float(inv_adj.median()),
            "p10_inv_adj_passed": float(inv_adj.quantile(0.10)),
            "worst_inv_adj_day": worst_inv_day,
            "worst_inv_adj_passed": float(inv_adj.min()),
            "tail_inv_adj_loss_days_passed": int((inv_adj < 0.0).sum()),
            "sum_abs_inventory_time_s_passed": float(pd.to_numeric(passed.get("abs_inventory_time_s", 0.0), errors="coerce").sum()),
            "mean_time_avg_abs_inventory_passed": float(pd.to_numeric(passed.get("time_avg_abs_inventory", 0.0), errors="coerce").mean()),
            "max_inventory_max_passed": float(pd.to_numeric(passed.get("max_inventory", 0.0), errors="coerce").max()),
            "weighted_bid_markout_passed": _weighted_mean(passed, "avg_markout_bid", "fills_bid"),
            "weighted_ask_markout_passed": _weighted_mean(passed, "avg_markout_ask", "fills_ask"),
            "mean_fills_per_utc_day_passed": float(pd.to_numeric(passed.get("fills_per_utc_day", 0.0), errors="coerce").mean()),
            "mean_placed_per_utc_day_passed": float(pd.to_numeric(passed.get("placed_per_utc_day", 0.0), errors="coerce").mean()),
            "mean_pair_spread_p50_passed": float(pd.to_numeric(passed.get("pair_spread_p50", 0.0), errors="coerce").mean()),
            "mean_final_spread_lt_100_rate_passed": float(pd.to_numeric(passed.get("final_spread_lt_100_rate", 0.0), errors="coerce").mean()),
            "mean_bad_guard_block_rate_passed": float(pd.to_numeric(passed.get("bad_guard_block_rate", 0.0), errors="coerce").mean()),
            "mean_pause_rate_passed": float(pd.to_numeric(passed.get("pause_rate", 0.0), errors="coerce").mean()),
        })
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    sort_cols = [col for col in ("eligible_for_formal_daily_sweep", "sum_inv_adj_passed", "sum_pnl_passed") if col in out]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=[False, False, False][: len(sort_cols)])
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--days", nargs="+", required=True, help="UTC days, e.g. 2026-06-26 2026-06-27")
    parser.add_argument("--tag", default="20260629_bookmid_smoke")
    parser.add_argument("--arms", nargs="+", default=[arm.name for arm in SMOKE_ARMS])
    parser.add_argument("--engine", choices=("python", "cpp"), default="python")
    parser.add_argument("--start-date", default=None, help="Optional UTC slice start for diagnostics")
    parser.add_argument("--end-date", default=None, help="Optional UTC slice end for diagnostics")
    parser.add_argument("--trace-quotes-max", type=int, default=120_000)
    parser.add_argument("--trace-decisions-max", type=int, default=120_000)
    parser.add_argument("--trace-fills-max", type=int, default=10_000)
    parser.add_argument(
        "--no-queue-regime-calibration",
        action="store_true",
        help="Disable live-fit queue regime calibration. Default is enabled for mechanism parity smoke.",
    )
    parser.add_argument("--window-cache-dir", default=None)
    parser.add_argument("--refresh-window-cache", action="store_true")
    args = parser.parse_args(argv)

    bt.configure_symbol(args.symbol)
    days = _normalize_days(args.days)
    arms_by_name = {arm.name: arm for arm in SMOKE_ARMS}
    unknown = [name for name in args.arms if name not in arms_by_name]
    if unknown:
        raise SystemExit(
            f"Unknown smoke arm(s): {', '.join(unknown)}. "
            f"Available: {', '.join(sorted(arms_by_name))}"
        )
    arms = [arms_by_name[name] for name in args.arms]

    base = _base_params(args.symbol)
    # Trace buffers are intentionally high enough for a full UTC day. If they
    # truncate, the row fails review even if the numeric gates look fine.
    base["trace_quotes_max"] = int(args.trace_quotes_max)
    base["trace_decisions_max"] = int(args.trace_decisions_max)
    base["trace_fills_max"] = int(args.trace_fills_max)
    base["trace_queue_events_max"] = 0
    base["queue_regime_calibration_enabled"] = not bool(args.no_queue_regime_calibration)
    if args.window_cache_dir:
        base["_window_cache_dir"] = args.window_cache_dir
    if args.refresh_window_cache:
        base["_refresh_window_cache"] = True
    if args.engine == "cpp":
        base["collect_curves"] = False

    start_ms = _parse_bound(args.start_date, is_end=False)
    end_ms = _parse_bound(args.end_date, is_end=True)
    print(
        f"Daily mechanism smoke: symbol={args.symbol.upper()} days={','.join(days)} "
        f"engine={args.engine} queue_regime_calibration={base['queue_regime_calibration_enabled']}"
    )
    print(f"Acceptance gates: {json.dumps(ACCEPTANCE, ensure_ascii=False)}")

    rows: list[dict[str, Any]] = []
    for day in days:
        print(f"\nLoading {day} ...")
        window = _load_window(day, base)
        window = _slice_window(window, start_ms, end_ms)
        for idx, arm in enumerate(arms, 1):
            print(f"  [{idx:02d}/{len(arms):02d}] {day} {arm.name} ...", flush=True)
            row = _run_case(day=day, arm=arm, base=base, window=window, engine=args.engine)
            rows.append(row)
            verdict = "PASS" if row["pass_mechanism_smoke"] else f"FAIL:{row['gate_failures']}"
            print(
                f"      {verdict} placed/day={row['placed_per_utc_day']:.0f} "
                f"fills/day={row['fills_per_utc_day']:.1f} spread50={row['pair_spread_p50']:.1f} "
                f"pause={row['pause_rate']:.3f} badBlock={row['bad_guard_block_rate']:.3f} "
                f"top={row['top_block_reason']}"
            )

    _write_outputs(rows, tag=args.tag, symbol=args.symbol)


if __name__ == "__main__":
    main()
