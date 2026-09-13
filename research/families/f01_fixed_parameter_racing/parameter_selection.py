"""Parameter registry and constrained selection helpers for NarrowGate sweeps.

This module is deliberately lightweight: it does not run replay by itself.
It answers three questions shared by the sweep runners:

1. Which live-config parameters are active, live-only, shadow, or archived?
2. Which active parameters can safely produce Python-replay arm specs?
3. How should a campaign/daily result be ranked after hard mechanism gates?

中文说明：不要把这个模块当成“自动找最优参数”的黑盒。它只负责把
参数搜索从人工散扫整理成可复现的候选生成与约束优先排序。
"""

from __future__ import annotations

import json
import math
import warnings
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class ParameterSpec:
    key: str
    flat_key: str
    category: str
    group: str
    python_replay: bool
    cpp_replay: bool
    search_values: tuple[Any, ...] = ()
    low: float | None = None
    high: float | None = None
    transform: str = "linear"
    note: str = ""

    @property
    def searchable(self) -> bool:
        return self.category == "active" and self.python_replay and (
            bool(self.search_values) or (self.low is not None and self.high is not None)
        )


@dataclass(frozen=True)
class ArmSpec:
    name: str
    group: str
    overrides: dict[str, Any]
    note: str = ""

    def to_json_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PairedSelectionLimits:
    """Risk budgets for rebasing daily evidence to the rolling live baseline."""

    min_days: int = 20
    min_coverage: float = 0.98
    max_pause_delta: float = 0.06
    max_keep_delta: float = 0.08
    max_place_replace_delta: float = 0.08
    max_spread_delta: float = 10.0
    min_side_share: float = 0.30
    min_campaign_ratio: float = 0.65
    max_campaign_ratio: float = 1.45
    strict_max_inventory_time_ratio: float = 1.10
    risk_max_inventory_time_ratio: float = 1.25
    strict_max_campaign_mae_ratio: float = 1.02
    risk_max_campaign_mae_ratio: float = 1.25
    strict_max_campaign_duration_ratio: float = 1.10
    risk_max_campaign_duration_ratio: float = 1.55
    risk_max_bad_campaign_delta: float = 0.015
    risk_min_inv_adj_delta: float = -5.0
    strict_min_win_rate: float = 0.50
    risk_min_win_rate: float = 0.45
    unit_quality_min_fills_ratio: float = 0.85
    unit_quality_max_inventory_time_ratio: float = 1.25
    unit_quality_max_campaign_mae_ratio: float = 1.25
    unit_quality_max_campaign_duration_ratio: float = 1.55


PARAMETER_SPECS: tuple[ParameterSpec, ...] = (
    # Quote/spread shape: active and replayable.
    ParameterSpec("strategy.gamma", "gamma", "active", "spread", True, True, search_values=(0.01, 0.025, 0.035, 0.05, 0.07), note="AS inventory risk aversion."),
    ParameterSpec("strategy.kappa", "kappa", "fallback", "spread", True, True, search_values=(), note="Fallback inverse-price distance-decay coefficient. Current live/tick quote path uses the local P3 log-touch slope when available; it is not an event arrival rate."),
    ParameterSpec("strategy.order_size", "order_size", "active", "sizing", True, True, search_values=(0.001, 0.002), note="Per-order size; compare with risk-normalized campaign metrics, not raw PnL alone."),
    ParameterSpec("strategy.max_inventory", "max_inventory", "active", "sizing", True, True, search_values=(0.01, 0.016, 0.02, 0.026, 0.03), note="Inventory hard budget."),
    ParameterSpec("strategy.kappa_ratio", "kappa_ratio", "active", "spread", True, True, search_values=(1.0, 1.1, 1.25, 1.5, 1.75), note="Primary half-spread shape parameter."),
    ParameterSpec("strategy.depth_kappa_ratio", "depth_kappa_ratio", "active", "spread", True, True, search_values=(0.3, 0.5, 0.75, 1.0), note="Depth-aware kappa adjustment."),
    ParameterSpec("strategy.vol_power", "vol_power", "active", "spread", True, True, search_values=(1.5, 2.0, 2.5), note="Volatility response convexity."),
    ParameterSpec("strategy.max_spread_bps", "max_spread_bps", "active", "spread", True, True, search_values=(12.0, 16.0, 20.0, 24.0, 28.0), note="Final spread cap; keep paired with dynamic cap base."),
    ParameterSpec("strategy.dynamic_cap_base_bps", "dynamic_cap_base_bps", "active", "spread", True, True, search_values=(12.0, 16.0, 20.0, 24.0, 28.0), note="Dynamic cap base. Usually tied to max_spread_bps."),
    ParameterSpec("strategy.requote_interval", "requote_interval", "active", "lifecycle", True, True, search_values=(5.0, 10.0, 15.0), note="Fallback/base requote interval; rq_min/rq_max are the primary adaptive bounds."),
    ParameterSpec("strategy.quote_horizon_s", "quote_horizon_s", "active", "spread", True, True, note="Explicit integration horizon for one-second absolute-price variance; deployment-calibrated, not a generic search knob."),
    ParameterSpec("strategy.rq_min", "rq_min", "active", "lifecycle", True, True, search_values=(3.0, 5.0, 7.0), note="Regime requote interval floor."),
    ParameterSpec("strategy.rq_max", "rq_max", "active", "lifecycle", True, True, search_values=(8.0, 10.0, 12.0), note="Regime requote interval ceiling."),
    ParameterSpec("strategy.position_timeout", "position_timeout", "active", "inventory_exit", True, False, search_values=(0.0, 300.0, 900.0), note="Taker/IOC timeout path; evaluate separately from pure maker alpha."),
    ParameterSpec("strategy.eta", "eta", "active", "sizing", True, True, search_values=(0.0, 0.5, 1.0), note="Inventory-dependent size decay; may be near no-op at lot-size order_size."),
    ParameterSpec("strategy.leverage", "leverage", "live-static", "exchange_contract", False, False, note="Margin/account setting. It changes account risk, not replay fill selection/PnL path."),
    # Guards and inventory lifecycle.
    ParameterSpec("strategy.adverse_markout_threshold", "adverse_markout_threshold", "active", "guard", True, True, search_values=(2.0, 2.5, 3.0, 4.0), note="Adverse widening threshold."),
    ParameterSpec("strategy.adverse_markout_pause_threshold", "adverse_markout_pause_threshold", "active", "guard", True, True, search_values=(5.0, 6.0, 7.5), note="Adverse pause threshold."),
    ParameterSpec("strategy.adverse_spread_mult", "adverse_spread_mult", "active", "guard", True, True, search_values=(1.15, 1.35, 1.5), note="Adverse widening multiplier."),
    ParameterSpec("strategy.defense_spread_mult", "defense_spread_mult", "active", "guard", True, True, search_values=(1.25, 1.5, 1.7), note="Inventory-reducing defense multiplier."),
    ParameterSpec("strategy.fill_cooldown", "fill_cooldown", "active", "cooldown", True, True, search_values=(20.0, 32.0, 41.0, 55.0, 70.0), note="Exposure-increasing same-side fill cooldown."),
    ParameterSpec("strategy.fill_cooldown_reducing", "fill_cooldown_reducing", "active", "cooldown", True, False, search_values=(0.0, 4.0, 5.0, 8.0, 12.0), note="Reducing-side cooldown; Python replay only for now."),
    ParameterSpec("strategy.fill_cooldown_reducing_inv_ratio", "fill_cooldown_reducing_inv_ratio", "active", "cooldown", True, False, search_values=(0.0, 4.0, 6.0, 8.0), note="Campaign-only reducing cooldown inventory ratio gate."),
    ParameterSpec("strategy.flat_unilateral_max_s", "flat_unilateral_max_s", "active", "guard", True, True, search_values=(60.0, 120.0, 180.0), note="Flat unilateral release guard."),
    # Execution lifecycle: system parameters, replayable only in Python today.
    ParameterSpec("strategy.replace_min_price_change_ticks", "replace_min_price_change_ticks", "active", "execution", True, False, search_values=(10.0, 20.0, 30.0), note="Exposure-increasing replace price threshold."),
    ParameterSpec("strategy.replace_min_price_change_ticks_reducing", "replace_min_price_change_ticks_reducing", "active", "execution", True, False, search_values=(5.0, 10.0, 15.0), note="Reducing-side replace price threshold."),
    ParameterSpec("strategy.replace_min_interval_ms", "replace_min_interval_ms", "active", "execution", True, False, search_values=(250.0, 500.0, 750.0), note="Exposure-increasing replace interval."),
    ParameterSpec("strategy.replace_min_interval_ms_reducing", "replace_min_interval_ms_reducing", "active", "execution", True, False, search_values=(125.0, 250.0, 500.0), note="Reducing-side replace interval."),
    ParameterSpec("strategy.replace_pending_coalesce", "replace_pending_coalesce", "active", "execution", True, False, search_values=(True, False), note="Live REST tail control; Python replay approximation."),
    # ML/cross-market active features, but most are not direct alpha switches.
    ParameterSpec("ml.enabled", "ml_enabled", "active", "ml", False, False, search_values=(), note="Main ML feature path; replay runners decide ML loading explicitly, so do not random-sweep this key."),
    ParameterSpec("ml.vol_blend", "vol_blend", "active", "ml", True, False, search_values=(0.25, 0.5, 0.75), note="Blend predicted and realized volatility."),
    ParameterSpec("ml.asym_strength", "asym_strength", "active", "ml", True, False, search_values=(0.0, 0.1, 0.2), note="ML side asymmetry strength."),
    ParameterSpec("ml.model_dir", "model_dir", "active", "ml", True, False, search_values=(), note="Model bundle is tested as named candidates, not random numeric sweep."),
    ParameterSpec("multi_market.enabled", "cross_market_enabled", "active", "ml", False, False, search_values=(), note="Reference/spot wiring switch; do not sweep as old true/false PnL arm. Use pending/shadow/re-center evidence instead."),
    # Live-only safety: never optimize by replay PnL.
    ParameterSpec("risk.sync_adjust_degrade_enabled", "sync_adjust_degrade_enabled", "live-only", "safety", False, False, note="User-stream/REST mismatch safety; validate in live logs."),
    ParameterSpec("risk.sync_adjust_degrade_count", "sync_adjust_degrade_count", "live-only", "safety", False, False, note="Live-only safety threshold."),
    ParameterSpec("risk.max_exec_book_age_s", "max_exec_book_age_s", "active", "safety", True, True, note="Shared live/replay stale-book hard guard; validate trigger rates rather than optimizing PnL."),
    ParameterSpec("risk.circuit_breaker_sigma", "circuit_breaker_sigma", "active", "safety", True, True, note="Shared live/replay absolute-price-variance safety stop; validate with fault injection."),
    ParameterSpec("risk.pnl_volatility_horizon_s", "pnl_volatility_horizon_s", "active", "safety", True, True, note="Explicit risk horizon used by circuit breaker and PnL urgency; deployment-calibrated."),
    ParameterSpec("websocket.exec_stream_silence_timeout_s", "exec_stream_silence_timeout_s", "live-only", "safety", False, False, note="Execution stream watchdog."),
    # Shadow/archived paths.
    ParameterSpec("strategy.local_extreme_guard_enabled", "local_extreme_guard_enabled", "shadow", "experimental", True, False, note="Parity experiment surface, default off."),
    ParameterSpec("strategy.fragile_order_ttl_s", "fragile_order_ttl_s", "shadow", "experimental", True, False, note="Parity experiment surface, default off."),
    ParameterSpec("strategy.adaptive_add_cooldown_enabled", "adaptive_add_cooldown_enabled", "shadow", "cooldown", True, False, note="Research/default-off adaptive lifecycle control."),
)


MODEL_DIR_VARIANTS: tuple[str, ...] = (
    "models/saved_btcusdc_causal_v2_formal_20260715",
)

LIVE_ACTIVE_SOBOL_AXES: tuple[tuple[str, tuple[Any, ...]], ...] = (
    # Quote/spread/fill-intensity axes.  p3_kappa_eff is not a live YAML leaf:
    # it normally comes from fill_prob_params.json.  We still sample it here
    # because the live quote path uses it ahead of the fallback `strategy.kappa`.
    ("p3_kappa_eff", (0.040, 0.045, 0.049923, 0.055, 0.060, 0.065)),
    ("gamma", (0.040, 0.046, 0.050, 0.056, 0.062, 0.068)),
    ("kappa_ratio", (1.10, 1.25, 1.50, 1.65, 1.75)),
    ("depth_kappa_ratio", (0.50, 0.75, 1.00)),
    ("vol_power", (1.5, 2.0, 2.5)),
    ("paired_cap_bps", (16.0, 20.0, 24.0, 28.0)),
    ("dynamic_cap_alpha", (0.25, 0.50, 0.75)),
    ("dynamic_cap_max_mult", (1.5, 2.0, 2.5)),
    # Guard stack axes.
    ("adverse_markout_threshold", (2.0, 2.5, 3.0)),
    ("adverse_markout_pause_threshold", (5.0, 6.0, 7.5)),
    ("adverse_spread_mult", (1.15, 1.35, 1.50)),
    ("defense_spread_mult", (1.25, 1.50, 1.70)),
    # Lifecycle/cooldown axes.  Non-zero reducing cooldown is intentionally
    # campaign-gated below, so Sobol does not accidentally test a broad global
    # reducing-side throttle as if it were the baseline-equivalent mechanism.
    ("fill_cooldown", (32.0, 41.0, 55.0, 70.0)),
    ("fill_cooldown_reducing", (0.0, 4.0, 8.0)),
    # Execution replace controls.  These are system/execution knobs, not alpha
    # knobs, but they change fill selection through queue lifetime and REST
    # churn.  Keep them in the broad active search and let mechanism gates
    # reject arms whose action mix drifts too far from live.
    ("replace_min_price_change_ticks", (10.0, 20.0, 30.0)),
    ("replace_min_price_change_ticks_reducing", (5.0, 10.0, 15.0)),
    ("replace_min_interval_ms", (250.0, 500.0, 750.0)),
    ("replace_min_interval_ms_reducing", (125.0, 250.0, 500.0)),
    # Active ML pricing axes.
    ("vol_blend", (0.25, 0.50, 0.75)),
    ("asym_strength", (0.0, 0.10, 0.20)),
    (
        "model_dir_variant",
        (
            "models/saved_btcusdc_causal_v2_formal_20260715",
        ),
    ),
)

# Mechanism-preserving local surface.  This intentionally removes the widest
# values from the broad Sobol search: the previous 512-arm smoke showed that
# large jumps often buy PnL by changing fills/action/spread/campaign behavior
# rather than by improving selection at the current live mechanism.
#
# 2026-07-08 tightening: the raw-positive failed region repeatedly used lower
# depth_kappa_ratio, reducing-side cooldown, model swaps, and wider guard moves
# to change the fill/campaign distribution.  Keep those axes fixed here and
# search only the closest quote/guard neighborhood before any retained run.
LOCAL_MECHANISM_AXES: tuple[tuple[str, tuple[Any, ...]], ...] = (
    ("p3_kappa_eff", (0.0475, 0.049923, 0.0525)),
    ("gamma", (0.047, 0.050, 0.053)),
    ("kappa_ratio", (1.40, 1.50, 1.60)),
    ("depth_kappa_ratio", (0.75,)),
    ("vol_power", (2.0,)),
    ("paired_cap_bps", (22.0, 24.0, 26.0)),
    ("dynamic_cap_alpha", (0.50,)),
    ("dynamic_cap_max_mult", (2.0,)),
    ("adverse_markout_threshold", (2.50, 2.75, 3.00)),
    ("adverse_markout_pause_threshold", (6.0, 6.75, 7.5)),
    ("adverse_spread_mult", (1.25, 1.35, 1.45)),
    ("defense_spread_mult", (1.40, 1.50)),
    ("fill_cooldown", (41.0,)),
    ("fill_cooldown_reducing", (0.0,)),
    ("vol_blend", (0.50,)),
    ("asym_strength", (0.10,)),
    (
        "model_dir_variant",
        (
            "models/saved_btcusdc_causal_v2_formal_20260715",
        ),
    ),
)

LOCAL_MECHANISM_PARENT_LIMITS: dict[str, float] = {
    # These limits are deliberately a little looser than the final hard gate.
    # They decide which failed arms may be used as local-search parents.  Arms
    # outside this envelope are treated as counterexamples, not as centers.
    "fills_retention_min": 0.85,
    "pause_delta_max": 0.04,
    "keep_delta_max": 0.06,
    "spread_delta_max": 5.0,
    "bad_campaign_delta_max": 0.02,
    "campaign_count_ratio_min": 0.75,
    "campaign_count_ratio_max": 1.35,
}

COMPOSITE_ARM_SPECS: tuple[ArmSpec, ...] = (
    ArmSpec(
        "campaign_soft_widen_1p01_inv006_age60",
        "lifecycle_campaign_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.01,
        },
        "Shadow-only campaign soft control: 1.01x exposure-increasing spread when abs inventory >=0.006 BTC or campaign age >=60m.",
    ),
    ArmSpec(
        "campaign_soft_widen_1p03_inv006_age60",
        "lifecycle_campaign_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.03,
        },
        "Shadow-only campaign soft control: 1.03x exposure-increasing spread when abs inventory >=0.006 BTC or campaign age >=60m.",
    ),
    ArmSpec(
        "campaign_soft_widen_1p05_inv006_age60",
        "lifecycle_campaign_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.05,
        },
        "Shadow-only campaign soft control: 1.05x exposure-increasing spread when abs inventory >=0.006 BTC or campaign age >=60m.",
    ),
    ArmSpec(
        "campaign_soft_gated_1p03_inv006_age60",
        "lifecycle_campaign_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.03,
            "campaign_soft_gate_enabled": True,
            "campaign_soft_gate_campaign_inv_ref": 0.006,
            "campaign_soft_gate_campaign_age_ref_s": 60.0 * 60.0,
            "campaign_soft_gate_trend_ret_ref": 1e-5,
            "campaign_soft_gate_refill_ref": 0.10,
            "campaign_soft_gate_campaign_score": 1.0,
            "campaign_soft_gate_trend_score": 1.0,
            "campaign_soft_gate_refill_edge_max": 0.02,
            "campaign_soft_gate_reversion_max": 0.5,
        },
        "Shadow-only gated campaign soft control: 1.03x only when campaign risk, adverse trend, and weak local repair hold.",
    ),
    ArmSpec(
        "campaign_soft_gated_sell_1p03_inv006_age60",
        "lifecycle_campaign_shadow",
        {
            "campaign_soft_control_enabled": True,
            "campaign_soft_inv_threshold": 0.006,
            "campaign_soft_age_s": 60.0 * 60.0,
            "campaign_soft_spread_mult": 1.03,
            "campaign_soft_gate_enabled": True,
            "campaign_soft_gate_campaign_inv_ref": 0.006,
            "campaign_soft_gate_campaign_age_ref_s": 60.0 * 60.0,
            "campaign_soft_gate_trend_ret_ref": 1e-5,
            "campaign_soft_gate_refill_ref": 0.10,
            "campaign_soft_gate_campaign_score": 1.0,
            "campaign_soft_gate_trend_score": 1.0,
            "campaign_soft_gate_refill_edge_max": 0.02,
            "campaign_soft_gate_reversion_max": 0.5,
            "campaign_soft_gate_side": "SELL",
        },
        "Shadow-only SELL add-short lifecycle probe; does not tighten or increase size.",
    ),
    ArmSpec(
        "fill_cd_reduce_cond_inv6_4s",
        "cooldown_campaign_shadow",
        {
            "fill_cooldown_reducing": 4.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
        },
        "Conditional reducing-side cooldown: 4s only when abs inventory >= 6x order_size.",
    ),
    ArmSpec(
        "fill_cd_reduce_cond_inv6_8s",
        "cooldown_campaign_shadow",
        {
            "fill_cooldown_reducing": 8.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
        },
        "Conditional reducing-side cooldown: 8s only when abs inventory >= 6x order_size.",
    ),
    ArmSpec(
        "fill_cd_reduce_cond_inv6_age20m_4s",
        "cooldown_campaign_shadow",
        {
            "fill_cooldown_reducing": 4.0,
            "fill_cooldown_reducing_campaign_only": True,
            "fill_cooldown_reducing_inv_ratio": 6.0,
            "fill_cooldown_reducing_age_s": 20.0 * 60.0,
        },
        "Conditional reducing-side cooldown: 4s if abs inventory >=6x order_size or campaign age >=20m.",
    ),
    ArmSpec(
        "ml_disabled_zero_asym_vol",
        "ml_model_dir",
        {
            "vol_blend": 0.0,
            "skew_strength": 0.0,
            "asym_strength": 0.0,
            "ret_skew": 0.0,
            "gamma_dir_bonus": 0.0,
        },
        "Approximate ml.enabled=false arm for replay: keep the loaded window but zero active ML pricing knobs.",
    ),
    ArmSpec(
        "add_cd_state_gate_60_repair_loose",
        "cooldown_add_shadow",
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
            "adaptive_add_cooldown_trend_ret_ref": 2e-5,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "State-gated add-side cooldown shadow: keep 41s unless campaign risk + adverse trend + weak repair; then about 60s.",
    ),
    ArmSpec(
        "add_cd_state_gate_60_repair_loose_sell",
        "cooldown_add_shadow",
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
            "adaptive_add_cooldown_trend_ret_ref": 2e-5,
            "adaptive_add_cooldown_refill_ref": 0.10,
        },
        "SELL-only state-gated add-side cooldown shadow.",
    ),
)


def specs_by_yaml_key() -> dict[str, ParameterSpec]:
    return {spec.key: spec for spec in PARAMETER_SPECS}


def _flatten(prefix: str, value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            out.update(_flatten(child_prefix, child))
        return out
    return {prefix: value}


def load_yaml_leaves(config_path: Path) -> dict[str, Any]:
    try:
        import yaml
    except Exception as exc:  # pragma: no cover - PyYAML is part of project deps.
        raise RuntimeError("PyYAML is required for parameter coverage reports") from exc
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return _flatten("", payload or {})


def _inferred_coverage(key: str) -> dict[str, Any]:
    """Conservative fallback classification for config leaves not in specs."""
    if key.startswith("api.") or key.startswith("logging.") or key.startswith("performance."):
        return {
            "category": "live-only",
            "group": "ops",
            "python_replay": False,
            "cpp_replay": False,
            "searchable": False,
            "note": "Operational/runtime setting; validate by live health/soak, not replay PnL.",
        }
    if key.startswith("websocket."):
        return {
            "category": "live-only",
            "group": "safety",
            "python_replay": False,
            "cpp_replay": False,
            "searchable": False,
            "note": "Stream subscription/freshness setting; validate by live watchdog and telemetry.",
        }
    if key in {"project_name", "symbol", "tick_size", "lot_size", "min_notional"} or key.startswith("fees."):
        return {
            "category": "live-static",
            "group": "exchange_contract",
            "python_replay": True,
            "cpp_replay": True,
            "searchable": False,
            "note": "Exchange/contract invariant for this symbol; not a tunable alpha parameter.",
        }
    if key.startswith("depth_execution."):
        return {
            "category": "shadow",
            "group": "depth_execution",
            "python_replay": True,
            "cpp_replay": False,
            "searchable": False,
            "note": "Depth execution feature/probe; promote only through explicit daily/campaign gates.",
        }
    if key.startswith("regime."):
        return {
            "category": "active",
            "group": "regime",
            "python_replay": True,
            "cpp_replay": False,
            "searchable": False,
            "note": "Active regime parameter but not in the default broad search surface yet.",
        }
    if key.startswith("external_venues."):
        return {
            "category": "shadow",
            "group": "external_venue",
            "python_replay": False,
            "cpp_replay": False,
            "searchable": False,
            "note": "Read-only external venue source/recording config; validate receive-time evidence before replay policy work.",
        }
    if key.startswith("multi_market."):
        return {
            "category": "active",
            "group": "market_data_wiring",
            "python_replay": True,
            "cpp_replay": False,
            "searchable": False,
            "note": "Reference/spot data wiring; not a direct expected-PnL switch.",
        }
    if key.startswith("ml."):
        return {
            "category": "active",
            "group": "ml",
            "python_replay": True,
            "cpp_replay": False,
            "searchable": False,
            "note": "Active ML parameter; add an explicit spec before using in random/Sobol search.",
        }
    if key.startswith("risk."):
        if "urgency" in key or key.endswith("exit_urgency_strength"):
            return {
                "category": "active",
                "group": "inventory_exit",
                "python_replay": True,
                "cpp_replay": False,
                "searchable": False,
                "note": "Inventory/exit urgency path; not in broad search surface until campaign labels pass.",
            }
        return {
            "category": "live-only",
            "group": "safety",
            "python_replay": False,
            "cpp_replay": False,
            "searchable": False,
            "note": "Live safety or account-risk control; validate by fault-injection/log audit, not PnL sweep.",
        }
    if key.startswith("rl."):
        return {
            "category": "archived",
            "group": "rl",
            "python_replay": False,
            "cpp_replay": False,
            "searchable": False,
            "note": "RL path disabled until replay/live parity and separate promotion evidence exist.",
        }
    if key.startswith("strategy.adaptive_add_cooldown_"):
        return {
            "category": "shadow",
            "group": "cooldown",
            "python_replay": True,
            "cpp_replay": False,
            "searchable": False,
            "note": "Default-off adaptive cooldown research parameter.",
        }
    if key.startswith("strategy.local_extreme_") or key.startswith("strategy.fragile_"):
        return {
            "category": "shadow",
            "group": "experimental_policy",
            "python_replay": True,
            "cpp_replay": False,
            "searchable": False,
            "note": "Experimental replay policy; not part of default live candidate surface.",
        }
    if key.startswith("strategy."):
        return {
            "category": "active",
            "group": "strategy",
            "python_replay": True,
            "cpp_replay": False,
            "searchable": False,
            "note": "Active strategy parameter; add an explicit spec before broad search.",
        }
    return {
        "category": "unclassified",
        "group": "unknown",
        "python_replay": False,
        "cpp_replay": False,
        "searchable": False,
        "note": "Not yet classified; do not optimize until mapped.",
    }


def coverage_rows(config_path: Path) -> list[dict[str, Any]]:
    leaves = load_yaml_leaves(config_path)
    by_key = specs_by_yaml_key()
    rows: list[dict[str, Any]] = []
    for key, value in sorted(leaves.items()):
        spec = by_key.get(key)
        inferred = _inferred_coverage(key) if spec is None else {}
        rows.append(
            {
                "yaml_key": key,
                "value": value,
                "category": spec.category if spec else inferred["category"],
                "group": spec.group if spec else inferred["group"],
                "flat_key": spec.flat_key if spec else "",
                "python_replay": bool(spec.python_replay) if spec else bool(inferred["python_replay"]),
                "cpp_replay": bool(spec.cpp_replay) if spec else bool(inferred["cpp_replay"]),
                "searchable": bool(spec.searchable) if spec else bool(inferred["searchable"]),
                "note": spec.note if spec else str(inferred["note"]),
            }
        )
    return rows


def write_coverage_report(config_path: Path, out_prefix: Path) -> dict[str, str]:
    rows = coverage_rows(config_path)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    csv_path = out_prefix.with_suffix(".csv")
    md_path = out_prefix.with_suffix(".md")
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    counts = pd.DataFrame(rows).groupby(["category", "group"], dropna=False).size().reset_index(name="n")
    lines = [
        "# NarrowGate Parameter Coverage Report",
        "",
        f"- config: `{config_path}`",
        f"- total leaf params: `{len(rows)}`",
        "",
        "## Category Counts",
        "",
        "| category | group | n |",
        "|---|---:|---:|",
    ]
    for _, row in counts.iterrows():
        lines.append(f"| {row['category']} | {row['group']} | {int(row['n'])} |")
    lines.extend(["", "## Parameters", "", "| yaml_key | category | group | Python replay | C++ replay | searchable | note |", "|---|---|---|---:|---:|---:|---|"])
    for row in rows:
        lines.append(
            "| {yaml_key} | {category} | {group} | {python_replay} | {cpp_replay} | {searchable} | {note} |".format(
                **{k: str(v).replace("|", "\\|") for k, v in row.items()}
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"csv": str(csv_path), "markdown": str(md_path)}


def _format_value_token(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return (f"{value:.6g}").replace("-", "m").replace(".", "p")
    return str(value).replace("/", "_").replace(".", "p")


def single_factor_arms(*, groups: Iterable[str] | None = None) -> list[ArmSpec]:
    group_set = set(groups or [])
    arms: list[ArmSpec] = [ArmSpec("baseline", "baseline", {}, "Current live config baseline.")]
    for spec in PARAMETER_SPECS:
        if not spec.searchable:
            continue
        if group_set and spec.group not in group_set:
            continue
        for value in spec.search_values:
            name = f"{spec.flat_key}_{_format_value_token(value)}"
            arms.append(
                ArmSpec(
                    name=name,
                    group=f"one_factor_{spec.group}",
                    overrides={spec.flat_key: value},
                    note=f"One-factor sensitivity: {spec.key}={value}. {spec.note}",
                )
            )
    return arms


def composite_arms(*, groups: Iterable[str] | None = None, include_model_dirs: bool = True) -> list[ArmSpec]:
    """Return named multi-parameter arms that should not be split into scalars.

    中文说明：campaign soft、conditional reducing cooldown 和 adaptive add
    cooldown 都是机制组合。把它们拆成单因子会产生无意义的 enabled-only
    或 threshold-only arm，所以这里用可读的 named arm 管理。
    """
    group_set = set(groups or [])
    arms: list[ArmSpec] = []
    group_aliases = {
        "lifecycle_campaign_shadow": {"lifecycle", "campaign", "campaign_soft"},
        "cooldown_campaign_shadow": {"cooldown", "reducing_cooldown"},
        "cooldown_add_shadow": {"cooldown", "adaptive_add_cooldown"},
        "ml_model_dir": {"ml", "model"},
    }
    for arm in COMPOSITE_ARM_SPECS:
        aliases = {arm.group, arm.group.replace("_shadow", "")}
        aliases.update(group_aliases.get(arm.group, set()))
        include = not group_set or bool(aliases & group_set)
        if include:
            arms.append(arm)
    if include_model_dirs and (not group_set or "ml" in group_set or "model" in group_set):
        for model_dir in MODEL_DIR_VARIANTS:
            path = ROOT / model_dir
            if not path.exists():
                continue
            arms.append(
                ArmSpec(
                    name=f"model_dir_{path.name}",
                    group="ml_model_dir",
                    overrides={"model_dir": model_dir, "resolved_model_dir": str(path.resolve())},
                    note=f"Model bundle variant; reloads ML predictions from {model_dir}.",
                )
            )
    return arms


def sampled_arms(
    *,
    n: int,
    groups: Iterable[str] | None = None,
    method: str = "sobol",
    seed: int = 7,
) -> list[ArmSpec]:
    """Generate compact multi-parameter candidates from searchable specs."""
    specs = [s for s in PARAMETER_SPECS if s.searchable and (not groups or s.group in set(groups))]
    if not specs or n <= 0:
        return []
    dim = len(specs)
    rng = np.random.default_rng(seed)
    if method == "sobol":
        try:
            from scipy.stats import qmc

            sampler = qmc.Sobol(d=dim, scramble=True, seed=seed)
            m = int(math.ceil(math.log2(max(1, n))))
            sample = sampler.random_base2(m=m)[:n]
        except Exception:
            sample = rng.random((n, dim))
    else:
        sample = rng.random((n, dim))
    arms: list[ArmSpec] = []
    for i in range(n):
        overrides: dict[str, Any] = {}
        tokens: list[str] = []
        for j, spec in enumerate(specs):
            u = float(sample[i, j])
            if spec.search_values:
                values = spec.search_values
                value = values[min(len(values) - 1, int(u * len(values)))]
            else:
                assert spec.low is not None and spec.high is not None
                if spec.transform == "log":
                    value = float(math.exp(math.log(spec.low) + u * (math.log(spec.high) - math.log(spec.low))))
                else:
                    value = float(spec.low + u * (spec.high - spec.low))
            overrides[spec.flat_key] = value
            tokens.append(f"{spec.flat_key}={_format_value_token(value)}")
        arms.append(
            ArmSpec(
                name=f"{method}_{i:03d}",
                group=f"{method}_sample",
                overrides=overrides,
                note="; ".join(tokens),
            )
        )
    return arms


def live_active_sobol_arms(
    *,
    n: int,
    method: str = "sobol",
    seed: int = 7,
) -> list[ArmSpec]:
    """Generate coupled live-active Sobol candidates.

    This is the broad-search surface used after the live/replay baseline repair.
    It differs from ``sampled_arms`` in three important ways:

    * it includes ``p3_kappa_eff`` because the quote path uses effective kappa
      from the fill-probability model rather than the legacy YAML fallback kappa;
    * it samples ``max_spread_bps`` and ``dynamic_cap_base_bps`` as one paired
      cap axis, avoiding artificial cap/base mismatches;
    * reducing-side cooldown candidates are campaign-gated to avoid turning the
      broad random search into an untested global reducing throttle sweep.
    """
    if n <= 0:
        return []
    axes = LIVE_ACTIVE_SOBOL_AXES
    dim = len(axes)
    rng = np.random.default_rng(seed)
    if method == "sobol":
        try:
            from scipy.stats import qmc

            sampler = qmc.Sobol(d=dim, scramble=True, seed=seed)
            m = int(math.ceil(math.log2(max(1, n))))
            sample = sampler.random_base2(m=m)[:n]
        except Exception:
            sample = rng.random((n, dim))
    else:
        sample = rng.random((n, dim))

    arms: list[ArmSpec] = [ArmSpec("baseline", "baseline", {}, "Current live config baseline.")]
    seen: set[tuple[tuple[str, Any], ...]] = set()
    for i in range(n):
        overrides: dict[str, Any] = {}
        tokens: list[str] = []
        for j, (key, values) in enumerate(axes):
            u = float(sample[i, j])
            value = values[min(len(values) - 1, int(u * len(values)))]
            if key == "paired_cap_bps":
                overrides["max_spread_bps"] = float(value)
                overrides["dynamic_cap_base_bps"] = float(value)
                tokens.append(f"cap={_format_value_token(value)}")
                continue
            if key == "model_dir_variant":
                model_dir = str(value)
                path = ROOT / model_dir
                if not path.exists():
                    # Keep candidate generation deterministic while ignoring
                    # model dirs that are absent in a public/sample checkout.
                    continue
                overrides["model_dir"] = model_dir
                overrides["resolved_model_dir"] = str(path.resolve())
                tokens.append(f"model={path.name}")
                continue
            overrides[key] = value
            tokens.append(f"{key}={_format_value_token(value)}")

        reducing_cd = float(overrides.get("fill_cooldown_reducing", 0.0) or 0.0)
        if reducing_cd > 0.0:
            overrides["fill_cooldown_reducing_campaign_only"] = True
            overrides["fill_cooldown_reducing_inv_ratio"] = 6.0
            tokens.append("reducing_cd_campaign_only=true")
            tokens.append("reducing_inv_ratio=6")
        else:
            overrides["fill_cooldown_reducing_campaign_only"] = False
            overrides["fill_cooldown_reducing_inv_ratio"] = 0.0

        key_tuple = tuple(sorted(overrides.items()))
        if key_tuple in seen:
            continue
        seen.add(key_tuple)
        arms.append(
            ArmSpec(
                name=f"live_active_{method}_{i:03d}",
                group=f"live_active_{method}",
                overrides=overrides,
                note="; ".join(tokens),
            )
        )
    return arms


def _selected_parent_names_for_local_search(
    scored: pd.DataFrame,
    *,
    n_raw: int,
    n_mechanism: int,
) -> list[str]:
    if scored.empty or "arm" not in scored:
        return []
    frame = scored.copy()
    frame = frame[frame["arm"].astype(str) != "baseline"].copy()
    if frame.empty:
        return []

    def num(col: str, default: float = 0.0) -> pd.Series:
        if col not in frame:
            return pd.Series(default, index=frame.index, dtype=float)
        return pd.to_numeric(frame[col], errors="coerce").fillna(default)

    notes = frame.get("constraint_notes", pd.Series("", index=frame.index)).fillna("").astype(str)
    frame["_note_count"] = notes.map(lambda s: len([x for x in s.split(",") if x and x != "pass"]))
    frame["_raw_rank"] = num("median_daily_raw_delta_vs_baseline") + 0.25 * num("raw_pnl_delta_vs_baseline")
    frame["_inv_rank"] = num("median_daily_inv_adj_delta_vs_baseline") + 0.10 * num("inv_adj_delta_vs_baseline")
    frame["_mech_rank"] = (
        -2.0 * frame["_note_count"].astype(float)
        - 20.0 * num("pause_rate_delta_abs")
        - 15.0 * num("keep_rate_delta_abs")
        - 0.5 * num("avg_final_spread_delta_abs")
        + 0.10 * frame["_raw_rank"]
    )

    limits = LOCAL_MECHANISM_PARENT_LIMITS
    mechanism_mask = (
        (num("fills_retention_vs_baseline", 1.0) >= limits["fills_retention_min"])
        & (num("pause_rate_delta_abs") <= limits["pause_delta_max"])
        & (num("keep_rate_delta_abs") <= limits["keep_delta_max"])
        & (num("avg_final_spread_delta_abs") <= limits["spread_delta_max"])
        & (num("bad_campaign_delta_vs_baseline") <= limits["bad_campaign_delta_max"])
        & (num("campaign_count_ratio_vs_baseline", 1.0) >= limits["campaign_count_ratio_min"])
        & (num("campaign_count_ratio_vs_baseline", 1.0) <= limits["campaign_count_ratio_max"])
    )
    parent_frame = frame[mechanism_mask].copy()
    if parent_frame.empty:
        # 前一轮如果没有任何机制近邻 parent，就不要继续围绕“raw 好但机制歪”
        # 的向量采样；后续轴会退回 baseline 附近的小网格。
        return []

    def pnum(col: str, default: float = 0.0) -> pd.Series:
        if col not in parent_frame:
            return pd.Series(default, index=parent_frame.index, dtype=float)
        return pd.to_numeric(parent_frame[col], errors="coerce").fillna(default)

    raw_pool = parent_frame[
        (pnum("raw_pnl_delta_vs_baseline") > 0.0) | (pnum("median_daily_raw_delta_vs_baseline") > 0.0)
    ]
    raw_names = (
        raw_pool.sort_values(["_raw_rank", "_inv_rank"], ascending=[False, False])
        .head(max(0, n_raw))["arm"]
        .astype(str)
        .tolist()
    )
    mech_names = (
        parent_frame.sort_values(["_note_count", "_mech_rank", "_raw_rank"], ascending=[True, False, False])
        .head(max(0, n_mechanism))["arm"]
        .astype(str)
        .tolist()
    )
    return list(dict.fromkeys(raw_names + mech_names))


def _nearest_axis_values(axis_values: tuple[Any, ...], center: Any, *, max_neighbors: int = 3) -> list[Any]:
    if isinstance(center, str):
        values = [v for v in axis_values if isinstance(v, str)]
        if center in values:
            return [center]
        return values[:1]
    if isinstance(center, bool):
        return [center] if center in axis_values else [axis_values[0]]
    try:
        c = float(center)
    except Exception:
        return [axis_values[0]]
    numeric = sorted(axis_values, key=lambda v: abs(float(v) - c))
    out: list[Any] = []
    for value in numeric:
        if value not in out:
            out.append(value)
        if len(out) >= max_neighbors:
            break
    return out


def _local_numeric_triplet(center: float, *, rel: float = 0.10, abs_step: float = 0.0, floor: float = 0.0) -> tuple[float, ...]:
    step = max(abs(center) * rel, abs_step)
    lo = max(floor, center - step)
    hi = center + step
    values = (round(lo, 6), round(center, 6), round(hi, 6))
    return tuple(dict.fromkeys(values))


def _local_mechanism_axis_values(
    key: str,
    fallback_values: tuple[Any, ...],
    baseline_hints: dict[str, Any],
) -> tuple[Any, ...]:
    """Return a tiny baseline-relative axis for mechanism-preserving search."""
    if key == "model_dir_variant":
        return (str(baseline_hints.get("model_dir", fallback_values[0])),)
    if key == "paired_cap_bps":
        center = float(
            baseline_hints.get("dynamic_cap_base_bps")
            or baseline_hints.get("max_spread_bps")
            or fallback_values[0]
            or 0.0
        )
        return _local_numeric_triplet(center, rel=0.0, abs_step=2.0, floor=1.0)
    if key == "p3_kappa_eff":
        center = float(baseline_hints.get(key, 0.0) or 0.0)
        if center <= 0.0:
            return (0.0, 0.025, 0.05)
        return _local_numeric_triplet(center, rel=0.10, abs_step=0.0025, floor=0.0)
    if key == "gamma":
        center = float(baseline_hints.get(key, fallback_values[0]) or fallback_values[0])
        return _local_numeric_triplet(center, rel=0.15, abs_step=0.003, floor=0.0)
    if key == "kappa_ratio":
        center = float(baseline_hints.get(key, fallback_values[0]) or fallback_values[0])
        return _local_numeric_triplet(center, rel=0.0, abs_step=0.10, floor=0.05)
    if key in {"adverse_markout_threshold", "adverse_markout_pause_threshold"}:
        center = float(baseline_hints.get(key, fallback_values[0]) or fallback_values[0])
        if center <= 0.0:
            return (center,)
        return _local_numeric_triplet(center, rel=0.0, abs_step=0.5, floor=0.0)
    if key in {"adverse_spread_mult", "defense_spread_mult"}:
        center = float(baseline_hints.get(key, fallback_values[0]) or fallback_values[0])
        return _local_numeric_triplet(center, rel=0.0, abs_step=0.10, floor=0.1)
    if key in {
        "depth_kappa_ratio",
        "vol_power",
        "dynamic_cap_alpha",
        "dynamic_cap_max_mult",
        "fill_cooldown",
        "fill_cooldown_reducing",
        "vol_blend",
        "asym_strength",
    }:
        return (baseline_hints.get(key, fallback_values[0]),)
    return fallback_values


def mechanism_local_sobol_arms(
    *,
    scored: pd.DataFrame,
    source_arms: list[ArmSpec],
    n: int,
    method: str = "sobol",
    seed: int = 7,
    n_raw_parents: int = 12,
    n_mechanism_parents: int = 8,
    baseline_hints: dict[str, Any] | None = None,
) -> list[ArmSpec]:
    """Generate a local Sobol surface around failed-but-informative parents.

    The broad live-active Sobol search is useful for finding directions, but a
    failed arm can improve raw PnL simply by changing the mechanism: fewer fills,
    different keep/pause rates, or much wider spreads.  This generator uses the
    previous scored rollup only to choose parent hints, then samples a narrower
    neighborhood anchored to the current live baseline.

    中文说明：这不是放宽 hard gate。它是把上一轮 raw 改善但机制失真的 arm
    当作方向提示，然后回到 baseline 附近重新采样，目标是先保持 fills /
    pause / keep / spread 口径，再比较 raw、InvAdj 和 campaign tail。
    """
    if n <= 0:
        return []
    # Formal sweeps should pass baseline_hints loaded from the active --config.
    # If omitted, the static axis values below are only a dry-run fallback; they
    # must not be interpreted as the live baseline.
    hints = {k: v for k, v in (baseline_hints or {}).items() if v is not None}
    by_name = {arm.name: arm for arm in source_arms}
    parent_names = _selected_parent_names_for_local_search(
        scored,
        n_raw=n_raw_parents,
        n_mechanism=n_mechanism_parents,
    )
    parents = [by_name[name] for name in parent_names if name in by_name]

    axes: list[tuple[str, tuple[Any, ...]]] = []
    for key, configured_values in LOCAL_MECHANISM_AXES:
        base_values = _local_mechanism_axis_values(key, configured_values, hints)
        values: list[Any] = []
        if not parents:
            # No previous failed arm was close enough to the current mechanism,
            # so sample the deliberately small baseline-neighborhood grid.
            values = list(base_values)
        elif key == "paired_cap_bps":
            baseline_cap = float(hints.get("dynamic_cap_base_bps") or hints.get("max_spread_bps") or base_values[0])
            values.extend(_nearest_axis_values(base_values, baseline_cap, max_neighbors=2))
            for parent in parents:
                center = parent.overrides.get("max_spread_bps", parent.overrides.get("dynamic_cap_base_bps", baseline_cap))
                values.extend(_nearest_axis_values(base_values, center, max_neighbors=2))
        elif key == "model_dir_variant":
            baseline_model = str(hints.get("model_dir", base_values[0]))
            values.append(baseline_model)
            for parent in parents:
                model_dir = str(parent.overrides.get("model_dir", baseline_model))
                if model_dir in base_values:
                    values.append(model_dir)
            # Keep at most one non-baseline model in the local surface; model
            # changes are high leverage and should not dominate mechanism repair.
            values = [v for v in dict.fromkeys(values) if (ROOT / str(v)).exists()][:2]
        else:
            baseline_value = hints.get(key, base_values[0])
            values.extend(_nearest_axis_values(base_values, baseline_value, max_neighbors=2))
            for parent in parents:
                if key in parent.overrides:
                    values.extend(_nearest_axis_values(base_values, parent.overrides[key], max_neighbors=2))

        if not values:
            values = list(base_values[:1])
        # Stable order by the original axis order, not by parent selection order.
        value_set = set(values)
        ordered = tuple(v for v in base_values if v in value_set)
        axes.append((key, ordered or tuple(values)))

    dim = len(axes)
    rng = np.random.default_rng(seed)
    if method == "sobol":
        try:
            from scipy.stats import qmc

            sampler = qmc.Sobol(d=dim, scramble=True, seed=seed)
            m = int(math.ceil(math.log2(max(1, n))))
            sample = sampler.random_base2(m=m)[:n]
        except Exception:
            sample = rng.random((n, dim))
    else:
        sample = rng.random((n, dim))

    arms: list[ArmSpec] = [ArmSpec("baseline", "baseline", {}, "Current live config baseline.")]
    seen: set[tuple[tuple[str, Any], ...]] = {tuple()}
    noop_overrides: dict[str, Any] = {}
    noop_tokens: list[str] = []
    for key, configured_values in LOCAL_MECHANISM_AXES:
        base_values = _local_mechanism_axis_values(key, configured_values, hints)
        if key == "paired_cap_bps":
            cap = float(hints.get("dynamic_cap_base_bps") or hints.get("max_spread_bps") or base_values[0])
            noop_overrides["max_spread_bps"] = cap
            noop_overrides["dynamic_cap_base_bps"] = cap
            noop_tokens.append(f"cap={_format_value_token(cap)}")
            continue
        if key == "model_dir_variant":
            model_dir = str(hints.get("model_dir", base_values[0]))
            path = ROOT / model_dir
            if path.exists():
                noop_overrides["model_dir"] = model_dir
                noop_overrides["resolved_model_dir"] = str(path.resolve())
                noop_tokens.append(f"model={path.name}")
            continue
        value = hints.get(key, base_values[0])
        noop_overrides[key] = value
        noop_tokens.append(f"{key}={_format_value_token(value)}")
    noop_overrides["fill_cooldown_reducing_campaign_only"] = False
    noop_overrides["fill_cooldown_reducing_inv_ratio"] = 0.0
    noop_key = tuple(sorted(noop_overrides.items()))
    seen.add(noop_key)
    arms.append(
        ArmSpec(
            name="local_mech_noop_hints",
            group="local_mechanism_control",
            overrides=noop_overrides,
            note=(
                "Baseline-hint no-op control. If this does not match baseline "
                "mechanism, the local-search hints are stale. "
                + "; ".join(noop_tokens)
            ),
        )
    )
    parent_note = ",".join(parent.name for parent in parents[: min(8, len(parents))])
    for i in range(n):
        overrides: dict[str, Any] = {}
        tokens: list[str] = []
        for j, (key, values) in enumerate(axes):
            u = float(sample[i, j])
            value = values[min(len(values) - 1, int(u * len(values)))]
            if key == "paired_cap_bps":
                overrides["max_spread_bps"] = float(value)
                overrides["dynamic_cap_base_bps"] = float(value)
                tokens.append(f"cap={_format_value_token(value)}")
                continue
            if key == "model_dir_variant":
                model_dir = str(value)
                path = ROOT / model_dir
                if not path.exists():
                    continue
                overrides["model_dir"] = model_dir
                overrides["resolved_model_dir"] = str(path.resolve())
                tokens.append(f"model={path.name}")
                continue
            overrides[key] = value
            tokens.append(f"{key}={_format_value_token(value)}")

        reducing_cd = float(overrides.get("fill_cooldown_reducing", 0.0) or 0.0)
        if reducing_cd > 0.0:
            overrides["fill_cooldown_reducing_campaign_only"] = True
            overrides["fill_cooldown_reducing_inv_ratio"] = 6.0
            tokens.append("reducing_cd_campaign_only=true")
            tokens.append("reducing_inv_ratio=6")
        else:
            overrides["fill_cooldown_reducing_campaign_only"] = False
            overrides["fill_cooldown_reducing_inv_ratio"] = 0.0

        key_tuple = tuple(sorted(overrides.items()))
        if key_tuple in seen:
            continue
        seen.add(key_tuple)
        arms.append(
            ArmSpec(
                name=f"local_mech_{method}_{i:03d}",
                group=f"local_mechanism_{method}",
                overrides=overrides,
                note=(
                    "Mechanism-preserving local Sobol around raw-improving failed parents. "
                    f"parents={parent_note}; "
                    + "; ".join(tokens)
                ),
            )
        )
    return arms


def write_arm_specs(arms: list[ArmSpec], path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"arms": [arm.to_json_row() for arm in arms]}
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _winsorized_paired_stats(values: pd.Series) -> dict[str, float]:
    clean = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if clean.empty:
        return {
            "sum": 0.0,
            "mean": math.nan,
            "median": math.nan,
            "p10": math.nan,
            "min": math.nan,
            "win_rate": math.nan,
            "tie_rate": math.nan,
            "t_stat": math.nan,
        }
    low, high = clean.quantile([0.05, 0.95])
    winsorized = clean.clip(lower=float(low), upper=float(high))
    mean = float(winsorized.mean())
    std = float(winsorized.std(ddof=1)) if len(winsorized) > 1 else 0.0
    if std <= 1e-15:
        t_stat = 0.0 if abs(mean) <= 1e-15 else math.copysign(99.0, mean)
    else:
        t_stat = mean / (std / math.sqrt(len(winsorized)))
    return {
        "sum": float(clean.sum()),
        "mean": mean,
        "median": float(clean.median()),
        "p10": float(clean.quantile(0.10)),
        "min": float(clean.min()),
        "win_rate": float((clean > 0.0).mean()),
        "tie_rate": float((clean.abs() <= 1e-12).mean()),
        "t_stat": float(t_stat),
    }


def _daily_numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


def _daily_delta(candidate: pd.DataFrame, baseline: pd.DataFrame, column: str) -> pd.Series:
    return _daily_numeric(candidate, column) - _daily_numeric(baseline, column)


def _daily_rate(frame: pd.DataFrame, numerator: str, denominator: str) -> float:
    den = float(_daily_numeric(frame, denominator).sum())
    return float(_daily_numeric(frame, numerator).sum()) / den if den > 0.0 else 0.0


def _daily_weighted_mean(frame: pd.DataFrame, value: str, weight: str) -> float:
    values = _daily_numeric(frame, value)
    weights = _daily_numeric(frame, weight, 1.0)
    valid = weights > 0.0
    if valid.any():
        return float((values[valid] * weights[valid]).sum() / weights[valid].sum())
    return float(values.mean()) if len(values) else 0.0


def _campaign_risk_mean(frame: pd.DataFrame, primary: str, fallback: str | None = None) -> float:
    """Return a campaign-weighted positive risk magnitude.

    C++ replay did not historically populate every summary-level campaign MAE
    field.  The campaign-label audit does populate ``early_20m_drawdown_*``;
    prefer that observable label and only fall back to the replay summary.
    """
    if primary in frame:
        values = _daily_numeric(frame, primary).abs()
        if bool((values > 0.0).any()):
            weights = _daily_numeric(frame, "campaigns", 1.0).clip(lower=0.0)
            if float(weights.sum()) > 0.0:
                return float((values * weights).sum() / weights.sum())
            return float(values.mean())
    if fallback and fallback in frame:
        values = _daily_numeric(frame, fallback).abs()
        weights = _daily_numeric(frame, "campaigns", 1.0).clip(lower=0.0)
        if float(weights.sum()) > 0.0:
            return float((values * weights).sum() / weights.sum())
        return float(values.mean())
    return 0.0


def _risk_ratio(candidate: float, baseline: float) -> float:
    if baseline > 1e-15:
        return candidate / baseline
    return 1.0 if candidate <= 1e-15 else math.inf


def build_paired_daily_evidence(
    daily: pd.DataFrame,
    *,
    baseline_arm: str = "baseline",
) -> pd.DataFrame:
    """Build paired UTC-day evidence without ranking or promotion semantics."""
    if daily.empty or not {"day", "arm"}.issubset(daily.columns):
        raise ValueError("daily evidence requires day and arm columns")
    frame = daily.copy()
    frame["day"] = frame["day"].astype(str)
    frame["arm"] = frame["arm"].astype(str)
    duplicate = frame.duplicated(["day", "arm"], keep=False)
    if duplicate.any():
        sample = frame.loc[duplicate, ["day", "arm"]].head(5).to_dict("records")
        raise ValueError(f"daily evidence has duplicate day/arm rows: {sample}")
    baseline = frame.loc[frame["arm"] == baseline_arm].set_index("day").sort_index()
    if baseline.empty:
        raise ValueError(f"baseline arm not found in daily evidence: {baseline_arm}")
    baseline_days = baseline.index
    n_search_arms = max(2, int(frame["arm"].nunique()) - 1)
    search_t_threshold = math.sqrt(2.0 * math.log(n_search_arms))

    rows: list[dict[str, Any]] = []
    for arm, raw_group in frame.groupby("arm", sort=False):
        group = raw_group.set_index("day").sort_index()
        common_days = baseline_days.intersection(group.index)
        candidate = group.reindex(common_days)
        base = baseline.reindex(common_days)
        n_days = len(common_days)
        coverage = n_days / max(len(baseline_days), 1)

        raw_stats = _winsorized_paired_stats(_daily_delta(candidate, base, "replay_pnl"))
        terminal_stats = _winsorized_paired_stats(
            _daily_delta(candidate, base, "terminal_pnl_sum")
        )
        inv_adj_stats = _winsorized_paired_stats(
            _daily_delta(candidate, base, "replay_inv_adj")
        )
        tail_delta_daily = _daily_delta(candidate, base, "loss_tail")
        tail_delta = int(round(float(tail_delta_daily.sum())))
        tail_worse_days = int((tail_delta_daily > 0.0).sum())
        tail_better_days = int((tail_delta_daily < 0.0).sum())

        candidate_fills = float(_daily_numeric(candidate, "fills_total").sum())
        baseline_fills = float(_daily_numeric(base, "fills_total").sum())
        fills_ratio = candidate_fills / baseline_fills if baseline_fills > 0.0 else 1.0
        candidate_campaigns = float(_daily_numeric(candidate, "campaigns").sum())
        baseline_campaigns = float(_daily_numeric(base, "campaigns").sum())
        campaign_ratio = candidate_campaigns / baseline_campaigns if baseline_campaigns > 0.0 else 1.0
        candidate_raw_sum = float(_daily_numeric(candidate, "replay_pnl").sum())
        baseline_raw_sum = float(_daily_numeric(base, "replay_pnl").sum())
        candidate_terminal_sum = float(_daily_numeric(candidate, "terminal_pnl_sum").sum())
        baseline_terminal_sum = float(_daily_numeric(base, "terminal_pnl_sum").sum())
        # Separate selection quality from the mechanical benefit of doing less.
        # If an arm retains r of fills/campaigns with unchanged unit economics,
        # its expected PnL is approximately r * baseline PnL.
        activity_adjusted_raw_delta = candidate_raw_sum - baseline_raw_sum * fills_ratio
        campaign_adjusted_terminal_delta = (
            candidate_terminal_sum - baseline_terminal_sum * campaign_ratio
        )
        raw_per_fill_delta = (
            candidate_raw_sum / candidate_fills - baseline_raw_sum / baseline_fills
            if candidate_fills > 0.0 and baseline_fills > 0.0
            else 0.0
        )
        terminal_per_campaign_delta = (
            candidate_terminal_sum / candidate_campaigns
            - baseline_terminal_sum / baseline_campaigns
            if candidate_campaigns > 0.0 and baseline_campaigns > 0.0
            else 0.0
        )
        inventory_time_base = float(_daily_numeric(base, "replay_abs_inventory_time_s").sum())
        inventory_time_ratio = (
            float(_daily_numeric(candidate, "replay_abs_inventory_time_s").sum()) / inventory_time_base
            if inventory_time_base > 0.0
            else 1.0
        )
        pause_delta = _daily_rate(candidate, "decision_pause_count", "decision_total") - _daily_rate(
            base, "decision_pause_count", "decision_total"
        )
        keep_delta = _daily_rate(candidate, "decision_keep_count", "decision_total") - _daily_rate(
            base, "decision_keep_count", "decision_total"
        )
        candidate_place_replace = _daily_rate(candidate, "decision_place_count", "decision_total") + _daily_rate(
            candidate, "decision_replace_count", "decision_total"
        )
        baseline_place_replace = _daily_rate(base, "decision_place_count", "decision_total") + _daily_rate(
            base, "decision_replace_count", "decision_total"
        )
        place_replace_delta = candidate_place_replace - baseline_place_replace
        spread_delta = _daily_weighted_mean(
            candidate, "replay_avg_final_spread", "replay_n_final_spread"
        ) - _daily_weighted_mean(base, "replay_avg_final_spread", "replay_n_final_spread")
        buy_fills = float(_daily_numeric(candidate, "fills_bid_buy").sum())
        sell_fills = float(_daily_numeric(candidate, "fills_ask_sell").sum())
        side_min_share = min(buy_fills, sell_fills) / max(buy_fills + sell_fills, 1.0)
        bad_rate = float(_daily_numeric(candidate, "bad_campaigns").sum()) / max(candidate_campaigns, 1.0)
        baseline_bad_rate = float(_daily_numeric(base, "bad_campaigns").sum()) / max(baseline_campaigns, 1.0)
        bad_delta = bad_rate - baseline_bad_rate
        repair_rate = float(_daily_numeric(candidate, "repaired_campaigns").sum()) / max(candidate_campaigns, 1.0)
        baseline_repair_rate = float(_daily_numeric(base, "repaired_campaigns").sum()) / max(
            baseline_campaigns, 1.0
        )
        repair_delta = repair_rate - baseline_repair_rate
        candidate_mae = _campaign_risk_mean(
            candidate,
            "early_20m_drawdown_mean",
            "replay_campaign_max_adverse_excursion",
        )
        baseline_mae = _campaign_risk_mean(
            base,
            "early_20m_drawdown_mean",
            "replay_campaign_max_adverse_excursion",
        )
        campaign_mae_delta = float(candidate_mae - baseline_mae)
        campaign_mae_ratio = _risk_ratio(candidate_mae, baseline_mae)
        candidate_duration = _campaign_risk_mean(candidate, "duration_mean_s")
        baseline_duration = _campaign_risk_mean(base, "duration_mean_s")
        campaign_duration_ratio = _risk_ratio(candidate_duration, baseline_duration)

        offensive_votes = int(fills_ratio > 1.03) + int(spread_delta < -1.0)
        defensive_votes = int(fills_ratio < 0.97) + int(spread_delta > 1.0)
        if offensive_votes and not defensive_votes:
            behavior_class = "offensive"
        elif defensive_votes and not offensive_votes:
            behavior_class = "defensive"
        else:
            behavior_class = "mixed"

        risk_tail_budget = max(2, int(math.ceil(n_days * 0.15)))
        rows.append(
            {
                "arm": arm,
                "group": str(raw_group["group"].iloc[0]) if "group" in raw_group else "",
                "baseline_arm": baseline_arm,
                "n_days": n_days,
                "coverage": coverage,
                "behavior_class": behavior_class,
                "raw_delta_sum": raw_stats["sum"],
                "raw_delta_mean_winsor": raw_stats["mean"],
                "raw_delta_median": raw_stats["median"],
                "raw_delta_p10": raw_stats["p10"],
                "raw_delta_min": raw_stats["min"],
                "raw_win_rate": raw_stats["win_rate"],
                "raw_t_stat": raw_stats["t_stat"],
                "terminal_delta_sum": terminal_stats["sum"],
                "terminal_delta_median": terminal_stats["median"],
                "terminal_win_rate": terminal_stats["win_rate"],
                "terminal_t_stat": terminal_stats["t_stat"],
                "inv_adj_delta_sum": inv_adj_stats["sum"],
                "inv_adj_delta_median": inv_adj_stats["median"],
                "inv_adj_t_stat": inv_adj_stats["t_stat"],
                "tail_campaign_delta": tail_delta,
                "tail_worse_days": tail_worse_days,
                "tail_better_days": tail_better_days,
                "risk_tail_budget": risk_tail_budget,
                "bad_campaign_rate_delta": bad_delta,
                "repair_rate_delta": repair_delta,
                "campaign_mae_delta": campaign_mae_delta,
                "campaign_mae_ratio": campaign_mae_ratio,
                "campaign_duration_ratio": campaign_duration_ratio,
                "fills_ratio": fills_ratio,
                "campaign_ratio": campaign_ratio,
                "activity_adjusted_raw_delta": activity_adjusted_raw_delta,
                "campaign_adjusted_terminal_delta": campaign_adjusted_terminal_delta,
                "raw_per_fill_delta": raw_per_fill_delta,
                "terminal_per_campaign_delta": terminal_per_campaign_delta,
                "inventory_time_ratio": inventory_time_ratio,
                "pause_rate_delta": pause_delta,
                "keep_rate_delta": keep_delta,
                "place_replace_rate_delta": place_replace_delta,
                "final_spread_delta": spread_delta,
                "side_min_fill_share": side_min_share,
                "search_t_threshold": search_t_threshold,
                "multiple_test_signal": bool(
                    min(raw_stats["t_stat"], terminal_stats["t_stat"]) >= search_t_threshold
                ),
                "note": str(raw_group["note"].iloc[0]) if "note" in raw_group else "",
            }
        )

    result = pd.DataFrame(rows)
    result["joint_paired_t"] = result[["raw_t_stat", "terminal_t_stat"]].min(axis=1)
    return result.reset_index(drop=True)


def _attach_legacy_paired_selection_fields(
    evidence: pd.DataFrame,
    *,
    baseline_arm: str,
    limits: PairedSelectionLimits,
) -> pd.DataFrame:
    """Preserve historical selector columns without granting them authority."""

    result = evidence.copy()

    def classify(row: pd.Series) -> pd.Series:
        mechanism_notes: list[str] = []
        behavior = str(row["behavior_class"])
        fill_min, fill_max = {
            "offensive": (0.95, 1.30),
            "defensive": (0.70, 1.05),
        }.get(behavior, (0.85, 1.20))
        if int(row["n_days"]) < limits.min_days:
            mechanism_notes.append("insufficient_days")
        if float(row["coverage"]) < limits.min_coverage:
            mechanism_notes.append("incomplete_day_coverage")
        if not fill_min <= float(row["fills_ratio"]) <= fill_max:
            mechanism_notes.append("fills_outside_direction_budget")
        if not (
            limits.min_campaign_ratio
            <= float(row["campaign_ratio"])
            <= limits.max_campaign_ratio
        ):
            mechanism_notes.append("campaign_count_drift")
        for field, budget, failure in (
            ("pause_rate_delta", limits.max_pause_delta, "pause_drift"),
            ("keep_rate_delta", limits.max_keep_delta, "keep_drift"),
            (
                "place_replace_rate_delta",
                limits.max_place_replace_delta,
                "place_replace_drift",
            ),
            ("final_spread_delta", limits.max_spread_delta, "spread_drift"),
        ):
            if abs(float(row[field])) > budget:
                mechanism_notes.append(failure)
        if float(row["side_min_fill_share"]) < limits.min_side_share:
            mechanism_notes.append("side_split_drift")
        mechanism_pass = not mechanism_notes
        is_baseline = str(row["arm"]) == baseline_arm
        strict_candidate = bool(
            not is_baseline
            and mechanism_pass
            and float(row["raw_delta_sum"]) > 0.0
            and float(row["terminal_delta_sum"]) > 0.0
            and float(row["inv_adj_delta_sum"]) >= 0.0
            and float(row["activity_adjusted_raw_delta"]) >= 0.0
            and float(row["campaign_adjusted_terminal_delta"]) >= 0.0
            and float(row["raw_delta_median"]) >= 0.0
            and float(row["terminal_delta_median"]) >= 0.0
            and float(row["raw_win_rate"]) >= limits.strict_min_win_rate
            and float(row["terminal_win_rate"]) >= limits.strict_min_win_rate
            and float(row["tail_campaign_delta"]) <= 0.0
            and float(row["bad_campaign_rate_delta"]) <= 0.0
            and float(row["repair_rate_delta"]) >= 0.0
            and float(row["inventory_time_ratio"])
            <= limits.strict_max_inventory_time_ratio
            and float(row["campaign_mae_ratio"])
            <= limits.strict_max_campaign_mae_ratio
            and float(row["campaign_duration_ratio"])
            <= limits.strict_max_campaign_duration_ratio
        )
        risk_budget_candidate = bool(
            not is_baseline
            and mechanism_pass
            and not strict_candidate
            and float(row["raw_delta_sum"]) > 0.0
            and float(row["terminal_delta_sum"]) > 0.0
            and float(row["inv_adj_delta_sum"]) >= limits.risk_min_inv_adj_delta
            and (
                float(row["activity_adjusted_raw_delta"]) >= 0.0
                or float(row["campaign_adjusted_terminal_delta"]) >= 0.0
            )
            and float(row["raw_delta_median"]) >= 0.0
            and float(row["raw_win_rate"]) >= limits.risk_min_win_rate
            and float(row["terminal_win_rate"]) >= limits.risk_min_win_rate
            and float(row["tail_campaign_delta"]) <= float(row["risk_tail_budget"])
            and float(row["bad_campaign_rate_delta"])
            <= limits.risk_max_bad_campaign_delta
            and float(row["inventory_time_ratio"])
            <= limits.risk_max_inventory_time_ratio
            and float(row["campaign_mae_ratio"]) <= limits.risk_max_campaign_mae_ratio
            and float(row["campaign_duration_ratio"])
            <= limits.risk_max_campaign_duration_ratio
        )
        unit_notes: list[str] = []
        if int(row["n_days"]) < limits.min_days:
            unit_notes.append("insufficient_days")
        if float(row["coverage"]) < limits.min_coverage:
            unit_notes.append("incomplete_day_coverage")
        if float(row["activity_adjusted_raw_delta"]) <= 0.0:
            unit_notes.append("activity_adjusted_raw_nonpositive")
        if float(row["campaign_adjusted_terminal_delta"]) <= 0.0:
            unit_notes.append("campaign_adjusted_terminal_nonpositive")
        if float(row["fills_ratio"]) < limits.unit_quality_min_fills_ratio:
            unit_notes.append("fills_retention_lt_85pct")
        if float(row["tail_campaign_delta"]) > 0.0:
            unit_notes.append("tail_increase")
        if float(row["campaign_mae_ratio"]) > limits.unit_quality_max_campaign_mae_ratio:
            unit_notes.append("campaign_mae_over_budget")
        if (
            float(row["campaign_duration_ratio"])
            > limits.unit_quality_max_campaign_duration_ratio
        ):
            unit_notes.append("campaign_duration_over_budget")
        if float(row["inventory_time_ratio"]) > limits.unit_quality_max_inventory_time_ratio:
            unit_notes.append("inventory_time_over_budget")
        return pd.Series(
            {
                "mechanism_pass": mechanism_pass,
                "mechanism_notes": ",".join(mechanism_notes) if mechanism_notes else "pass",
                "strict_candidate": strict_candidate,
                "risk_budget_candidate": risk_budget_candidate,
                "unit_quality_candidate": bool(not is_baseline and not unit_notes),
                "unit_quality_notes": ",".join(unit_notes) if unit_notes else "pass",
            }
        )

    result = pd.concat([result, result.apply(classify, axis=1)], axis=1)

    def tier(row: pd.Series) -> str:
        if str(row["arm"]) == baseline_arm:
            return "baseline"
        if bool(row["strict_candidate"]):
            return "strict_candidate"
        if bool(row["unit_quality_candidate"]):
            return "unit_quality_candidate"
        if bool(row["risk_budget_candidate"]):
            return "risk_budget_candidate"
        if bool(row.get("pareto_front", False)) and bool(row["mechanism_pass"]):
            if float(row["raw_delta_sum"]) > 0.0 and float(row["terminal_delta_sum"]) > 0.0:
                return "exploratory_pareto"
        return "mechanism_only" if bool(row["mechanism_pass"]) else "reject"

    result["selection_tier"] = result.apply(tier, axis=1)
    result["candidate_for_blocked_oos"] = result["selection_tier"].isin(
        {"strict_candidate", "unit_quality_candidate", "risk_budget_candidate"}
    )
    result["selection_tier_compatibility_only"] = True
    result["candidate_for_blocked_oos_promotion_authority"] = False
    result["scorecard_promotion_status"] = result["scorecard_screening_status"]
    result["scorecard_promotion_status_compatibility_only"] = True
    return result


def paired_daily_selection(
    daily: pd.DataFrame,
    *,
    baseline_arm: str = "baseline",
    limits: PairedSelectionLimits | None = None,
) -> pd.DataFrame:
    """Deprecated compatibility adapter for historical paired-selection users."""

    warnings.warn(
        "paired_daily_selection() is deprecated; use "
        "research.families.f01_fixed_parameter_racing.audit.paired_screening.screen_paired_daily_arms(). Legacy tier "
        "fields are compatibility-only and have no promotion authority.",
        DeprecationWarning,
        stacklevel=2,
    )
    from research.families.f01_fixed_parameter_racing.audit.paired_screening import screen_paired_daily_arms

    screened = screen_paired_daily_arms(
        daily,
        baseline_arm=baseline_arm,
    )
    return _attach_legacy_paired_selection_fields(
        screened,
        baseline_arm=baseline_arm,
        limits=limits or PairedSelectionLimits(),
    )


def constraint_score_rollup(rollup: pd.DataFrame) -> pd.DataFrame:
    """Add constraint-first scores to campaign rollup rows.

    The score is deliberately relative to the baseline row. It first enforces
    mechanism gates (fills, action mix, spread, side split, tail campaigns).
    Passing arms are ranked by robust daily raw PnL, then penalized for tail
    campaigns, campaign MAE proxy, inventory time, and live-mechanism distance.
    """
    if rollup.empty or "arm" not in rollup:
        return rollup.copy()
    frame = rollup.copy()
    base_rows = frame[frame["arm"].astype(str) == "baseline"]
    if base_rows.empty:
        frame["hard_gate_pass"] = False
        frame["constraint_first_score"] = -1e9
        frame["constraint_notes"] = "missing baseline"
        return frame
    base = base_rows.iloc[0]

    def f(row: pd.Series, key: str, default: float = 0.0) -> float:
        return float(row.get(key, default) or 0.0)

    rows: list[dict[str, Any]] = []
    base_fills = max(f(base, "fills_total"), 1.0)
    base_inv_time = max(f(base, "replay_abs_inventory_time_s_sum"), 1.0)
    base_pause = f(base, "decision_pause_rate")
    base_keep = f(base, "decision_keep_rate")
    base_place_replace = f(base, "decision_place_rate") + f(base, "decision_replace_rate")
    base_spread = f(base, "replay_avg_final_spread")
    base_tail = f(base, "loss_tail")
    base_bad = f(base, "bad_campaign_rate")
    base_campaigns = max(f(base, "campaigns"), 1.0)
    base_raw_median = f(base, "replay_pnl_median_by_day", f(base, "replay_pnl_sum"))
    base_inv_adj_median = f(base, "replay_inv_adj_median_by_day", f(base, "replay_inv_adj_sum"))
    base_campaign_mae = f(base, "replay_campaign_mae_proxy")
    for _, row in frame.iterrows():
        notes: list[str] = []
        fills_retention = f(row, "fills_total") / base_fills
        inv_time_ratio = f(row, "replay_abs_inventory_time_s_sum") / base_inv_time
        campaign_count_ratio = f(row, "campaigns") / base_campaigns
        pause_delta = abs(f(row, "decision_pause_rate") - base_pause)
        keep_delta = abs(f(row, "decision_keep_rate") - base_keep)
        place_replace_delta = abs((f(row, "decision_place_rate") + f(row, "decision_replace_rate")) - base_place_replace)
        spread_delta = abs(f(row, "replay_avg_final_spread") - base_spread) if base_spread > 0 else 0.0
        side_min_share = min(f(row, "buy_fill_share"), f(row, "sell_fill_share"))
        tail_delta = f(row, "loss_tail") - base_tail
        bad_delta = f(row, "bad_campaign_rate") - base_bad
        if fills_retention < 0.85:
            notes.append("fills_retention_lt_85pct")
        if pause_delta > 0.04:
            notes.append("pause_drift_gt4pct")
        if keep_delta > 0.06:
            notes.append("keep_drift_gt6pct")
        if place_replace_delta > 0.06:
            notes.append("place_replace_drift_gt6pct")
        if spread_delta > 5.0:
            notes.append("spread_drift_gt5usd")
        if side_min_share < 0.35:
            notes.append("side_split_drift")
        if tail_delta > 2:
            notes.append("tail_campaign_worse")
        if bad_delta > 0.02:
            notes.append("bad_campaign_rate_worse")
        if campaign_count_ratio < 0.75 or campaign_count_ratio > 1.35:
            notes.append("campaign_count_drift_gt35pct")
        hard_gate = not notes
        median_raw_delta = f(row, "replay_pnl_median_by_day", f(row, "replay_pnl_sum")) - base_raw_median
        median_inv_adj_delta = f(row, "replay_inv_adj_median_by_day", f(row, "replay_inv_adj_sum")) - base_inv_adj_median
        campaign_mae_delta = f(row, "replay_campaign_mae_proxy") - base_campaign_mae
        mechanism_distance_penalty = (
            120.0 * pause_delta
            + 80.0 * keep_delta
            + 80.0 * place_replace_delta
            + 0.25 * spread_delta
            + 10.0 * abs(inv_time_ratio - 1.0)
            + 6.0 * abs(campaign_count_ratio - 1.0)
        )
        # Constraint-first ranking:
        # median daily raw - tail loss - campaign MAE - inventory time - mechanism distance.
        score = (
            median_raw_delta
            + 0.20 * median_inv_adj_delta
            - 8.0 * max(tail_delta, 0.0)
            - 30.0 * max(bad_delta, 0.0)
            - 1.0 * max(campaign_mae_delta, 0.0)
            - mechanism_distance_penalty
        )
        out = row.to_dict()
        out.update(
            {
                "fills_retention_vs_baseline": fills_retention,
                "inventory_time_ratio_vs_baseline": inv_time_ratio,
                "campaign_count_ratio_vs_baseline": campaign_count_ratio,
                "pause_rate_delta_abs": pause_delta,
                "keep_rate_delta_abs": keep_delta,
                "place_replace_rate_delta_abs": place_replace_delta,
                "avg_final_spread_delta_abs": spread_delta,
                "tail_delta_vs_baseline": tail_delta,
                "bad_campaign_delta_vs_baseline": bad_delta,
                "campaign_mae_delta_vs_baseline": campaign_mae_delta,
                "median_daily_raw_delta_vs_baseline": median_raw_delta,
                "median_daily_inv_adj_delta_vs_baseline": median_inv_adj_delta,
                # Backward-compatible aliases for older notebooks/reports.
                "raw_pnl_delta_vs_baseline": f(row, "replay_pnl_sum") - f(base, "replay_pnl_sum"),
                "inv_adj_delta_vs_baseline": f(row, "replay_inv_adj_sum") - f(base, "replay_inv_adj_sum"),
                "hard_gate_pass": hard_gate,
                "constraint_first_score": score if hard_gate else -1e9,
                "constraint_notes": ",".join(notes) if notes else "pass",
            }
        )
        rows.append(out)
    return pd.DataFrame(rows).sort_values(["hard_gate_pass", "constraint_first_score"], ascending=[False, False])
