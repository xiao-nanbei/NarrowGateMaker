#!/usr/bin/env python3
"""Replay campaign-level outcome audit for small policy arms.

This runner answers a narrower question than daily smoke:

    Does an arm improve flat -> nonzero -> flat campaign outcomes, or does it
    merely shorten inventory time?

It deliberately reuses ``research.families.f10_live_replay_attribution.audit.metrics`` campaign labels so live,
replay, and order-level evidence use the same terminal campaign vocabulary.
Python remains the authority for mechanisms that have not reached C++ parity.
Parity-passing baseline, integrity-diagnostic, and executable-random arms may
use the C++ engine for retained-day screening.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import math
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from models import data_windows  # noqa: E402
from models.audit.support import norm_side, safe_float  # noqa: E402
from models.backtest_config import (  # noqa: E402
    REPLAY_LOCATOR_FIELDS,
    add_queue_calibration_params,
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from models.exchange_book_replay import (  # noqa: E402
    CryptoHFTExchangeBookTape,
    build_configured_cooldown_policy_adapter,
)
from models.replay.continuous_accounting import funding_cashflow_usdc  # noqa: E402
from models.replay_contract import (  # noqa: E402
    DEFAULT_LATENCY_ENVIRONMENT,
    configure_fixed_latency_distribution,
    freeze_replay_contract,
    load_standard_initial_state,
    validate_frozen_replay_contract,
    write_replay_contract,
)
from models.symbol_paths import DEFAULT_SYMBOL  # noqa: E402
from research.families.f01_fixed_parameter_racing import daily_smoke_sweep as smoke  # noqa: E402
from research.families.f04_external_market_alpha.reference_replay import (  # noqa: E402
    apply_global_flow_visibility_delay,
    load_causal_1s_global_flow,
)
from research.families.f10_live_replay_attribution.audit.metrics import (  # noqa: E402
    TradeRow,
    build_campaigns,
    campaign_label_rows,
)
from research.system_engineering.audit.market_data_latency import (  # noqa: E402
    SIMULATION_MODES,
    MarketDataLatencySimulator,
)
from research.system_engineering.audit.rest_latency_calibration import (  # noqa: E402
    load_runtime_timing_samples,
    runtime_compute_overrides,
)
from strategy.campaign_repair import CampaignRepairModel  # noqa: E402


def _normalize_days(days: list[str]) -> list[str]:
    return smoke._normalize_days(days)


def _load_funding_history(path: Path, *, symbol: str) -> list[dict[str, Any]]:
    """Read a frozen public funding-rate response; never fetch during replay."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("funding history must be a list of settlement records")
    last_ms = -1
    for row in payload:
        if row["symbol"] != symbol:
            raise ValueError("funding history symbol differs from replay symbol")
        ms = int(row["fundingTime"])
        if ms <= last_ms:
            raise ValueError("funding timestamps must be unique and strictly increasing")
        funding_cashflow_usdc(0, row["markPrice"], row["fundingRate"])
        last_ms = ms
    return payload


def _apply_runtime_timing_samples(
    base: dict[str, Any], path: Path, *, effective_time_assumption: str,
    arms: list[smoke.SmokeArm] | None = None,
    bulk_cancel_model: str = "unmodeled",
    private_fill_model: str = "unmodeled",
) -> dict[str, Any]:
    """Bind measured gateway rows without changing the configured transport."""
    if (
        base.get("order_transport") != "rest"
        or base.get("async_order_lanes_enabled") is not True
        or base.get("cross_side_order_lanes_enabled") is not False
    ):
        raise ValueError("runtime timing samples require the configured REST async GLOBAL FIFO")
    capacity = base.get("async_order_lane_capacity")
    if type(capacity) is not int or capacity <= 0:
        raise ValueError("runtime timing samples require the configured positive lane capacity")
    calibrated = load_runtime_timing_samples(
        path, effective_time_assumption=effective_time_assumption,
        bulk_cancel_model=bulk_cancel_model,
        private_fill_model=private_fill_model,
    )
    bound_fields = set(calibrated["params"]) | {
        "async_order_lane_capacity", "rng_seed", "strict_calibration",
    }
    # Action arms may change policy, not silently replace the measured
    # environment while the report still describes the original samples.
    for arm in arms or ():
        forbidden = []
        for name in arm.overrides:
            normalized = name.lstrip("_")
            if name in bound_fields or "latency" in name or normalized.startswith((
                "replay_", "rest_gateway_", "serial_rest_", "bulk_cancel_", "empirical_requote_",
                "exec_book_visibility_", "exec_depth_visibility_", "exec_trade_visibility_",
                "decision_to_gateway_", "pre_snapshot_compute_", "requote_tail_work_",
                "main_loop_work_",
                "runtime_compute_", "exec_message_delivery", "exec_source_stratified_",
                "private_fill_", "market_data_latency_",
            )):
                forbidden.append(name)
        if forbidden:
            raise ValueError(
                f"runtime timing arm {arm.name!r} changes bound environment fields: "
                + ", ".join(sorted(forbidden))
            )
    base.update(calibrated["params"])
    base["replay_evidence_scope"] = "runtime_gateway_diagnostic"
    return calibrated["calibration"]


def _runtime_compute_for_window(
    window: dict[str, Any], params: dict[str, Any], calibration: dict[str, Any],
    *, clock: str, start_ms: int,
) -> dict[str, Any]:
    """Restore the completed signal bucket from this window's causal pre-roll."""
    ml_data = window.get("ml_data")
    if ml_data is None:
        raise ValueError(
            "runtime compute requires a prediction pre-roll, not a synthetic watermark"
        )
    prediction_ms = np.asarray(ml_data[0])
    if (
        prediction_ms.ndim != 1 or prediction_ms.dtype.kind not in "iu"
        or not prediction_ms.size or np.any(prediction_ms[1:] <= prediction_ms[:-1])
    ):
        raise ValueError("runtime compute requires ordered integer prediction timestamps")
    if clock == "prediction_delivery":
        delivery = (params.get("_exec_message_delivery") or {}).get("prediction")
        if params.get("exec_book_visibility_mode") != "message_schedule" or delivery is None:
            raise ValueError("prediction_delivery compute requires a prediction message schedule")
        exchange_ns = np.asarray(delivery["exchange_ts_ns"])
        ready_ns = np.asarray(delivery["feature_ready_ts_ns"])
        if (
            ready_ns.dtype.kind not in "iu" or ready_ns.shape != prediction_ms.shape
            or not np.array_equal(exchange_ns, prediction_ms * 1_000_000)
            or np.any(ready_ns < exchange_ns)
        ):
            raise ValueError("runtime compute prediction delivery must align with prediction rows")
    elif clock == "source_time_assumption":
        ready_ns = prediction_ms * 1_000_000
    else:
        raise ValueError("runtime compute clock must explicitly identify its source")
    completed = prediction_ms[ready_ns < start_ms * 1_000_000]
    if not completed.size:
        raise ValueError("runtime compute requires a completed prediction before replay start")
    overrides = runtime_compute_overrides(
        calibration, initial_bucket_end_ms=int(completed[-1]), clock=clock,
    )
    for rows in overrides["_runtime_compute_samples_by_path"].values():
        rows.flags.writeable = False
    return overrides


def _runtime_timing_report(
    calibration: dict[str, Any], daily_rows: list[dict[str, Any]], *,
    compute_clock: str | None,
) -> dict[str, Any]:
    """Describe actual compute consumption without rewriting source metadata."""
    if compute_clock is None:
        return calibration
    phase_semantics = sorted({
        str(row["runtime_compute_phase_placement"])
        for row in daily_rows if row.get("runtime_compute_phase_placement")
    })
    return {
        **calibration,
        "compute": {
            **calibration["compute"],
            "clock": compute_clock,
            "consumed_by_replay": any(
                any(row.get("runtime_compute_path_counts", {}).values()) for row in daily_rows
            ),
            "phase_placement": phase_semantics,
        },
        "limitations": [
            item for item in calibration["limitations"] if item != (
                "Compute paths remain metadata; no measured compute samples are injected."
            )
        ] + [
            f"Compute clock: {compute_clock}; initial completed bucket comes from causal pre-roll."
        ] + [f"Compute phase placement: {item}" for item in phase_semantics],
    }


def _resolve_cpp_parity_days(
    replay_days: list[str],
    requested_days: list[str],
) -> list[str]:
    if not replay_days:
        raise ValueError("C++ parity gate requires at least one replay day")
    if not requested_days:
        return [replay_days[0]]
    if len(requested_days) == 1 and requested_days[0].strip().lower() == "all":
        return list(replay_days)
    normalized = _normalize_days(requested_days)
    missing = [day for day in normalized if day not in replay_days]
    if missing:
        raise ValueError(
            "C++ parity days must be included in --days: " + ", ".join(missing)
        )
    return normalized


HISTORICAL_MARKET_DATA_LATENCY_MODES = tuple(
    mode for mode in SIMULATION_MODES if mode != "captured"
)


def _sha256(path: Path | None) -> str:
    if path is None:
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _delay_historical_global_flow(
    data,
    *,
    profile_payload: dict[str, Any] | None,
    mode: str,
    seed: int,
    day: str,
    market_id: str,
):
    normalized_mode = str(mode).strip().lower()
    if normalized_mode == "exchange_zero":
        return data
    if profile_payload is None:
        raise ValueError("profile market-data latency mode requires a profile payload")
    simulator = MarketDataLatencySimulator(profile_payload)
    row = {
        "market_id": str(market_id),
        "event_type": "book",
        "transport": "websocket",
    }
    day_seed = int("".join(ch for ch in str(day) if ch.isdigit()) or 0)
    rng = random.Random(int(seed) ^ day_seed)
    if normalized_mode.startswith("profile_") and normalized_mode not in {
        "profile_empirical",
        "profile_stable_spike",
    }:
        delays_ms: float | np.ndarray = simulator.delay_ms(
            row,
            mode=normalized_mode,
            rng=rng,
        )
    else:
        delays_ms = np.fromiter(
            (
                simulator.delay_ms(row, mode=normalized_mode, rng=rng)
                for _ in range(data.ts_ns.size)
            ),
            dtype=np.float64,
            count=data.ts_ns.size,
        )
    return apply_global_flow_visibility_delay(
        data,
        delays_ms,
        profile_id=simulator.profile_id,
        mode=normalized_mode,
    )


def _arm_map() -> dict[str, smoke.SmokeArm]:
    return {arm.name: arm for arm in smoke.SMOKE_ARMS}


def _load_arm_spec_json(path: Path | None) -> list[smoke.SmokeArm]:
    """Load generated arm specs from parameter-selection tooling.

    The JSON format is intentionally tiny and stable:

    ``[{"name": "...", "group": "...", "overrides": {...}, "note": "..."}]``

    中文说明：这个入口只扩展 replay 可测的 arm 列表，不改变 baseline
    参数和 gate 逻辑。这样参数搜索框架可以生成候选，但 campaign audit
    仍然复用原来的 Python 权威 replay。
    """
    if path is None:
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("arms", [])
    if not isinstance(payload, list):
        raise SystemExit(f"arm spec JSON must contain a list or {{'arms': list}}: {path}")
    arms: list[smoke.SmokeArm] = []
    seen: set[str] = set()
    for idx, raw in enumerate(payload):
        if not isinstance(raw, dict):
            raise SystemExit(f"arm spec row {idx} is not an object: {raw!r}")
        name = str(raw.get("name", "")).strip()
        group = str(raw.get("group", "generated")).strip() or "generated"
        overrides = raw.get("overrides", {})
        note = str(raw.get("note", "")).strip()
        if not name:
            raise SystemExit(f"arm spec row {idx} missing name")
        if name in seen:
            raise SystemExit(f"duplicate arm spec name: {name}")
        if not isinstance(overrides, dict):
            raise SystemExit(f"arm spec {name} overrides must be an object")
        seen.add(name)
        arms.append(smoke.SmokeArm(name=name, group=group, overrides=overrides, note=note))
    return arms


def _load_campaign_repair_models(
    path: Path | None,
    *,
    panel: str,
    days: list[str],
) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("bundle_schema_version") != "campaign_repair_model_bundle.v1":
        raise SystemExit(f"unsupported campaign repair model bundle: {path}")
    panels = dict(payload.get("panels", {}))
    if panel not in panels:
        raise SystemExit(
            f"campaign repair model panel {panel!r} not found in {path}; available={sorted(panels)}"
        )
    panel_payload = dict(panels[panel])
    models = dict(panel_payload.get("models", {}))
    day_map = dict(panel_payload.get("day_to_model_id", {}))
    missing = [day for day in days if day not in day_map]
    if missing:
        raise SystemExit(f"campaign repair panel {panel!r} has no causal model for days: {missing}")
    return {day: dict(models[day_map[day]]) for day in days}


def _fill_cooldown_grid_arms(values: list[float]) -> list[smoke.SmokeArm]:
    """Build temporary fixed add-side cooldown arms for campaign outcome scans.

    中文说明：这个入口专门回答“固定 41s 是否只是旧参数”。
    它只改 exposure-increasing add-side `fill_cooldown`，不碰 reducing
    cooldown、campaign soft control 或 quote EV，避免把多个机制揉在一起。
    """
    arms: list[smoke.SmokeArm] = []
    seen: set[str] = set()
    for raw in values:
        value = max(0.0, float(raw))
        token = (f"{value:.3f}").rstrip("0").rstrip(".").replace(".", "p")
        name = f"fill_cd_add_{token}s"
        if name in seen:
            continue
        seen.add(name)
        arms.append(
            smoke.SmokeArm(
                name=name,
                group="fixed_add_cooldown_grid",
                overrides={"fill_cooldown": value},
                note=f"Fixed exposure-increasing add-side fill cooldown = {value:g}s.",
            )
        )
    return arms


def _causal_post_fill_stop_add_arm() -> smoke.SmokeArm:
    """Predeclared M1 campaign moderator; thresholds are not test-panel tuned."""

    return smoke.SmokeArm(
        name="post_fill_stop_add_causal_v1",
        group="multi_market_campaign_moderator",
        overrides={
            "multi_market_policy_enabled": True,
            "multi_market_policy_mode": "post_fill_stop_add",
            # Historical external inputs are causal one-second right-edge states.
            # This is a campaign moderator, not a claimed sub-second cancel arm.
            "multi_market_policy_horizon_ms": 1_000,
            "multi_market_policy_min_abs_inventory": 0.002,
            "multi_market_policy_min_campaign_age_s": 0.0,
            "multi_market_policy_min_campaign_fills": 1,
            "multi_market_policy_require_spot_and_perp": True,
            "multi_market_policy_min_venue_agreement": 2.0 / 3.0,
            "multi_market_policy_min_external_move_bps": 0.25,
            "multi_market_policy_min_global_flow_pressure": 0.10,
            "multi_market_policy_min_bridge_move_bps": 0.10,
            "multi_market_policy_max_repair_probability": 0.60,
            "multi_market_policy_min_repair_probability_drop": 0.05,
            "multi_market_policy_repair_lookback_ms": 60_000,
            "multi_market_policy_repair_max_age_ms": 1_500,
        },
        note=(
            "M1 causal post-fill stop-add: block only the inventory-increasing side "
            "when three-venue spot/perp and Binance bridge are adverse and the "
            "candidate-path repair probability has deteriorated."
        ),
    )


def _post_fill_quote_response_arms() -> list[smoke.SmokeArm]:
    """Predeclared QN/Q1/Q2/Q3 decomposition of hard cooldown replacement."""

    common = {
        # Remove the hard add-side silence in every decomposition arm. QN is
        # required to identify the effect of removing cooldown by itself.
        "fill_cooldown": 0.0,
        "fill_cooldown_reducing": 0.0,
        "adaptive_add_cooldown_enabled": False,
        "post_fill_inventory_ticks_per_order_unit": 0.25,
        "post_fill_inventory_max_ticks": 4.0,
        "post_fill_flow_ticks_per_excitation": 2.0,
        "post_fill_flow_max_ticks": 8.0,
        "post_fill_flow_excitation_per_order_unit": 1.0,
        "post_fill_flow_max_excitation": 4.0,
        "post_fill_response_half_life_s": 20.0,
        "post_fill_response_half_life_min_s": 4.0,
        "post_fill_response_half_life_max_s": 120.0,
        "post_fill_response_volatility_ref_bps": 3.0,
        "post_fill_response_volatility_weight": 0.35,
        "post_fill_response_refill_edge_ref": 0.10,
        "post_fill_response_refill_weight": 0.75,
        "post_fill_response_repair_probability_anchor": 0.60,
        "post_fill_response_repair_probability_weight": 1.0,
    }
    return [
        smoke.SmokeArm(
            name="post_fill_qn_no_hard_cd",
            group="post_fill_quote_response",
            overrides=dict(common),
            note="Identification control: hard add cooldown off, I=0, A=0.",
        ),
        smoke.SmokeArm(
            name="post_fill_q1_inventory_shift",
            group="post_fill_quote_response",
            overrides={
                **common,
                "post_fill_quote_response_enabled": True,
                "post_fill_quote_response_mode": "inventory_shift",
            },
            note="Q1: pair-center inventory shift I only; pair spread unchanged.",
        ),
        smoke.SmokeArm(
            name="post_fill_q2_flow_add_widen",
            group="post_fill_quote_response",
            overrides={
                **common,
                "post_fill_quote_response_enabled": True,
                "post_fill_quote_response_mode": "flow_add_widen",
            },
            note="Q2: response-kernel A only; reducing quote invariant.",
        ),
        smoke.SmokeArm(
            name="post_fill_q3_hybrid",
            group="post_fill_quote_response",
            overrides={
                **common,
                "post_fill_quote_response_enabled": True,
                "post_fill_quote_response_mode": "hybrid",
            },
            note="Q3: inventory center shift I plus one-sided flow defense A.",
        ),
    ]


def _post_fill_quote_response_q4_arm() -> smoke.SmokeArm:
    """Hybrid response with the rolling baseline cooldown as a safety backstop."""

    q3 = _post_fill_quote_response_arms()[-1]
    overrides = {
        key: value
        for key, value in q3.overrides.items()
        if key
        not in {
            "fill_cooldown",
            "fill_cooldown_reducing",
            "adaptive_add_cooldown_enabled",
        }
    }
    return smoke.SmokeArm(
        name="post_fill_q4_baseline_backstop_hybrid",
        group="post_fill_quote_response",
        overrides=overrides,
        note=(
            "Q4 transition arm: I+A with the rolling baseline hard cooldown "
            "retained only as a maximum safety backstop."
        ),
    )


def _post_fill_quote_response_q1_arm() -> smoke.SmokeArm:
    """Return only the predeclared Q1 inventory-shift arm."""

    return _post_fill_quote_response_arms()[1]


def _post_fill_fitted_a_arms() -> list[smoke.SmokeArm]:
    """Identification control plus the chronological fitted transient-A arm."""

    qn = _post_fill_quote_response_arms()[0]
    fitted = smoke.SmokeArm(
        name="post_fill_a_fitted_transient_48t_hl9",
        group="post_fill_quote_response",
        overrides={
            "fill_cooldown": 0.0,
            "fill_cooldown_reducing": 0.0,
            "adaptive_add_cooldown_enabled": False,
            "post_fill_quote_response_enabled": True,
            "post_fill_quote_response_mode": "flow_add_widen",
            "post_fill_flow_amplitude_mode": "expected_adverse",
            "post_fill_flow_expected_adverse_buy_ticks": 48.36418413816037,
            "post_fill_flow_expected_adverse_sell_ticks": 46.86530738249553,
            "post_fill_flow_add_distance_fraction_buy": 0.1454561928967229,
            "post_fill_flow_add_distance_fraction_sell": 0.14052565931782768,
            "post_fill_flow_max_ticks": 100.0,
            "post_fill_flow_excitation_per_order_unit": 1.0,
            "post_fill_flow_max_excitation": 2.0,
            "post_fill_response_half_life_s": 9.053650957633629,
            "post_fill_response_half_life_min_s": 0.25,
            "post_fill_response_half_life_max_s": 60.0,
            # Chronological validation did not support incremental sorting from
            # these modifiers. Keep the first policy arm equal to the fitted
            # unconditional transient response rather than forcing coefficients.
            "post_fill_response_volatility_weight": 0.0,
            "post_fill_response_refill_weight": 0.0,
            "post_fill_response_repair_probability_weight": 0.0,
        },
        note=(
            "Fitted transient A only: side-specific 5s-minus-30s adverse ticks, "
            "bounded by baseline add distance; existing quote-core inventory "
            "shift remains the only I term."
        ),
    )
    return [qn, fitted]


def _random_passive_arms(
    trials: int,
    *,
    seed: int,
    side_mirror_prob: float,
    timing_jitter_fraction: float,
) -> list[smoke.SmokeArm]:
    """Build executable passive-null arms with independent deterministic seeds."""
    return [
        smoke.SmokeArm(
            name=f"random_passive_{seed + idx}",
            group="executable_random_passive",
            overrides={
                "random_passive_enabled": True,
                "random_passive_seed": seed + idx,
                "random_passive_side_mirror_prob": side_mirror_prob,
                "random_passive_timing_jitter_fraction": timing_jitter_fraction,
                "random_passive_preserve_inventory_skew": True,
            },
            note=(
                "Executable passive null: randomized quote cadence and flat-state side geometry; "
                "full queue, latency, lifecycle, cooldown, inventory and terminal accounting retained."
            ),
        )
        for idx in range(max(0, int(trials)))
    ]


def _weighted_mean(frame: pd.DataFrame, value_col: str, weight_col: str) -> float:
    """Return a finite weighted mean for replay summary fields."""
    if value_col not in frame.columns:
        return math.nan
    values = pd.to_numeric(frame[value_col], errors="coerce")
    if weight_col not in frame.columns:
        clean = values.dropna()
        return float(clean.mean()) if len(clean) else math.nan
    weights = pd.to_numeric(frame[weight_col], errors="coerce")
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        clean = values.dropna()
        return float(clean.mean()) if len(clean) else math.nan
    return float((values[mask] * weights[mask]).sum() / weights[mask].sum())


def _random_passive_null_table(daily: pd.DataFrame) -> pd.DataFrame:
    """Build daily and pooled baseline-vs-executable-null comparisons.

    中文说明：random passive 会改变后续库存、cooldown 和 queue path，因此
    成交数不应被假定与 baseline 完全相等。正式报告必须同时给出 activity、
    spread、action mix、inventory time 和 tail，避免把少成交或多成交误读成
    fill-selection alpha。
    """
    required = {"day", "arm", "group"}
    if daily.empty or not required.issubset(daily.columns):
        return pd.DataFrame()
    baseline = daily.loc[daily["arm"] == "baseline"].copy()
    random_daily = daily.loc[daily["group"] == "executable_random_passive"].copy()
    if baseline.empty or random_daily.empty:
        return pd.DataFrame()

    metric_modes = {
        "replay_pnl": "sum",
        "replay_inv_adj": "sum",
        "mtm_before_terminal_fee": "sum",
        "terminal_fee_drag": "sum",
        "terminal_liquidation_fee_estimate": "sum",
        "fills_total": "sum",
        "replay_abs_inventory_time_s": "sum",
        "terminal_pnl_sum": "sum",
        "loss_tail": "sum",
        "avg_markout": ("weighted", "fills_total"),
        "replay_avg_final_spread": ("weighted", "replay_n_final_spread"),
        "decision_place_rate": ("weighted", "decision_total"),
        "decision_replace_rate": ("weighted", "decision_total"),
        "decision_pause_rate": ("weighted", "decision_total"),
        "decision_keep_rate": ("weighted", "decision_total"),
        "decision_pending_coalesce_rate": ("weighted", "decision_total"),
        "decision_cancel_first_rate": ("weighted", "decision_total"),
        "buy_fill_share": ("weighted", "fills_total"),
    }

    def summarize(
        row: dict[str, Any], metric: str, baseline_value: float, values: pd.Series
    ) -> None:
        clean = pd.to_numeric(values, errors="coerce").dropna()
        row[f"baseline_{metric}"] = baseline_value
        row[f"random_{metric}_mean"] = float(clean.mean()) if len(clean) else math.nan
        row[f"random_{metric}_p10"] = float(clean.quantile(0.10)) if len(clean) else math.nan
        row[f"random_{metric}_median"] = float(clean.median()) if len(clean) else math.nan
        row[f"random_{metric}_p90"] = float(clean.quantile(0.90)) if len(clean) else math.nan
        row[f"baseline_minus_random_{metric}_mean"] = (
            baseline_value - float(clean.mean())
            if len(clean) and math.isfinite(baseline_value)
            else math.nan
        )

    rows: list[dict[str, Any]] = []
    baseline_by_day = baseline.drop_duplicates("day", keep="last").set_index("day")
    for day, grp in random_daily.groupby("day", sort=True):
        if day not in baseline_by_day.index:
            continue
        base_row = baseline_by_day.loc[day]
        row: dict[str, Any] = {"scope": "daily", "day": day, "trials": int(len(grp))}
        for metric in metric_modes:
            base_value = float(
                pd.to_numeric(pd.Series([base_row.get(metric)]), errors="coerce").iloc[0]
            )
            summarize(row, metric, base_value, grp[metric])
        base_fills = float(row["baseline_fills_total"])
        null_fills = float(row["random_fills_total_mean"])
        base_inv_time = float(row["baseline_replay_abs_inventory_time_s"])
        null_inv_time = float(row["random_replay_abs_inventory_time_s_mean"])
        row["fills_retention_baseline_over_random_mean"] = (
            base_fills / null_fills if null_fills > 0 else math.nan
        )
        row["inventory_time_ratio_baseline_over_random_mean"] = (
            base_inv_time / null_inv_time if null_inv_time > 0 else math.nan
        )
        row["baseline_replay_pnl_per_fill"] = (
            float(row["baseline_replay_pnl"]) / base_fills if base_fills > 0 else math.nan
        )
        random_pnl_per_fill = pd.to_numeric(grp["replay_pnl"], errors="coerce") / pd.to_numeric(
            grp["fills_total"], errors="coerce"
        ).replace(0, math.nan)
        clean_per_fill = random_pnl_per_fill.dropna()
        row["random_replay_pnl_per_fill_mean"] = (
            float(clean_per_fill.mean()) if len(clean_per_fill) else math.nan
        )
        rows.append(row)

    seed_rows: list[dict[str, Any]] = []
    for arm, grp in random_daily.groupby("arm", sort=True):
        seed_row: dict[str, Any] = {"arm": arm}
        for metric, mode in metric_modes.items():
            if mode == "sum":
                seed_row[metric] = float(pd.to_numeric(grp[metric], errors="coerce").sum())
            else:
                _, weight_col = mode
                seed_row[metric] = _weighted_mean(grp, metric, weight_col)
        fills = float(seed_row["fills_total"])
        seed_row["replay_pnl_per_fill"] = (
            float(seed_row["replay_pnl"]) / fills if fills > 0 else math.nan
        )
        seed_rows.append(seed_row)

    if seed_rows:
        seed_df = pd.DataFrame(seed_rows)
        pooled: dict[str, Any] = {
            "scope": "pooled",
            "day": "__pooled__",
            "trials": int(len(seed_df)),
        }
        for metric, mode in metric_modes.items():
            if mode == "sum":
                base_value = float(pd.to_numeric(baseline[metric], errors="coerce").sum())
            else:
                _, weight_col = mode
                base_value = _weighted_mean(baseline, metric, weight_col)
            summarize(pooled, metric, base_value, seed_df[metric])
        base_fills = float(pooled["baseline_fills_total"])
        null_fills = float(pooled["random_fills_total_mean"])
        base_inv_time = float(pooled["baseline_replay_abs_inventory_time_s"])
        null_inv_time = float(pooled["random_replay_abs_inventory_time_s_mean"])
        pooled["fills_retention_baseline_over_random_mean"] = (
            base_fills / null_fills if null_fills > 0 else math.nan
        )
        pooled["inventory_time_ratio_baseline_over_random_mean"] = (
            base_inv_time / null_inv_time if null_inv_time > 0 else math.nan
        )
        pooled["baseline_replay_pnl_per_fill"] = (
            float(pooled["baseline_replay_pnl"]) / base_fills if base_fills > 0 else math.nan
        )
        pooled["random_replay_pnl_per_fill_mean"] = float(
            pd.to_numeric(seed_df["replay_pnl_per_fill"], errors="coerce").mean()
        )
        rows.append(pooled)

    return pd.DataFrame(rows)


def _integrity_diagnostic_arms() -> list[smoke.SmokeArm]:
    """Canonical markout-sign and spread-cap action A/B arms."""
    return [
        smoke.SmokeArm(
            "markout_asym_off",
            "markout_sign_ab",
            {"markout_spread_scale": 0.0, "markout_side_asymmetry_sign": -1.0},
            "Markout side feedback disabled; isolates whether the historical feedback helps at all.",
        ),
        smoke.SmokeArm(
            "markout_asym_sign_corrected",
            "markout_sign_ab",
            {"markout_side_asymmetry_sign": 1.0},
            "Sign-corrected side feedback: better maker-signed markout makes that side closer, not farther.",
        ),
        smoke.SmokeArm(
            "cap_pause_exposure",
            "spread_cap_action_ab",
            {"spread_cap_mode": "pause_exposure"},
            "When required spread exceeds cap, block only the exposure-increasing side.",
        ),
        smoke.SmokeArm(
            "cap_observe_only",
            "spread_cap_action_ab",
            {"spread_cap_mode": "observe"},
            "Record cap hits without inward compression or a cap-specific block.",
        ),
    ]


def _day_start_ts(day: str) -> float:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()


def _signed_qty(side: str, qty: float) -> float:
    side_norm = norm_side(side)
    if side_norm == "BUY":
        return qty
    if side_norm == "SELL":
        return -qty
    return 0.0


def _load_initial_states_from_trades_csv(
    path: Path, days: list[str]
) -> dict[str, dict[str, float]]:
    """Recover live day-start inventory state from ``logs/trades.csv``.

    中文说明：正式 retained replay 默认 daily fresh-start；但做 live/baseline
    单日校准时，live 可能从前一日 carry 一个 campaign 进入 UTC 00:00。
    这里只为这种校准入口恢复 day-start position，避免把 carry 库存导致的
    减仓成交误判成 replay 机制错误。
    """
    if not path.exists():
        raise FileNotFoundError(path)
    raw_rows: list[dict[str, float | str]] = []
    handle = (
        gzip.open(path, mode="rt", newline="")
        if path.suffix.lower() == ".gz"
        else path.open(mode="r", newline="")
    )
    with handle as f:
        for row in csv.DictReader(f):
            try:
                ts = float(row.get("timestamp") or 0.0)
                qty = float(row.get("qty") or 0.0)
                price = float(row.get("price") or 0.0)
                position = float(row.get("position") or 0.0)
                avg_entry = float(row.get("avg_entry") or 0.0)
            except (TypeError, ValueError):
                continue
            if ts <= 0.0:
                continue
            raw_rows.append(
                {
                    "ts": ts,
                    "side": row.get("side", ""),
                    "trade_type": row.get("trade_type", ""),
                    "qty": qty,
                    "price": price,
                    "position": position,
                    "avg_entry": avg_entry,
                }
            )
    raw_rows.sort(key=lambda r: float(r["ts"]))

    states: dict[str, dict[str, float]] = {}
    for day in days:
        start_ts = _day_start_ts(day)
        prev: dict[str, float | str] | None = None
        first_after: dict[str, float | str] | None = None
        for row in raw_rows:
            ts = float(row["ts"])
            if ts < start_ts:
                prev = row
                continue
            first_after = row
            break
        if prev is not None:
            position = float(prev["position"])
            entry = float(prev["avg_entry"]) or float(prev["price"])
        elif first_after is not None:
            # Fallback for truncated ledgers: infer pre-fill position from the
            # first observed row after the window start. This is less precise
            # than a prior row but preserves the carry direction for calibration.
            delta = _signed_qty(str(first_after["side"]), float(first_after["qty"]))
            position = float(first_after["position"]) - delta
            entry = float(first_after["avg_entry"]) or float(first_after["price"])
        else:
            position = 0.0
            entry = 0.0
        if abs(position) <= 1e-12:
            entry = 0.0
        states[day] = {"initial_inventory": position, "initial_entry_price": entry}
    return states


def _load_initial_live_states_json(
    path: Path,
    days: list[str],
) -> dict[str, dict[str, Any]]:
    """Load full live warm-start state for explicit calibration runs.

    The artifact is deliberately day-keyed so a multi-day command cannot
    silently reuse one boundary snapshot for another UTC day. This state is
    calibration-only and must not be used for fresh-start promotion evidence.
    """
    if not path.exists():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("initial live state artifact must be a JSON object")
    raw_days = payload.get("days", payload)
    if not isinstance(raw_days, dict):
        raise ValueError("initial live state artifact requires a day-keyed 'days' object")
    states: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for day in days:
        state = raw_days.get(day)
        if not isinstance(state, dict):
            missing.append(day)
            continue
        states[day] = state
    if missing:
        raise ValueError(
            "initial live state artifact is missing requested UTC day(s): " + ", ".join(missing)
        )
    return states


def _ordered_fill_trace(fill_trace: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Preserve physical execution order, including same-millisecond fills."""
    if any("fill_sequence" in row for row in fill_trace):
        if not all("fill_sequence" in row for row in fill_trace):
            raise ValueError("fill trace mixes present and missing fill_sequence")
        sequences = [int(row["fill_sequence"]) for row in fill_trace]
        if len(set(sequences)) != len(sequences):
            raise ValueError("fill trace contains duplicate fill_sequence")
        return sorted(fill_trace, key=lambda row: int(row["fill_sequence"]))
    # Legacy traces have no physical sequence. Stable input order is a better
    # tie-breaker than order ID, which does not identify execution priority.
    return sorted(fill_trace, key=lambda row: safe_float(row, "fill_ts"))


def _fills_to_trade_rows(
    fill_trace: list[dict[str, Any]],
    *,
    initial_inventory: float = 0.0,
    initial_entry_price: float = 0.0,
    day_start_ts: float = 0.0,
    terminal_ts: float | None = None,
    terminal_mark_price: float | None = None,
    funding_events: list[dict[str, Any]] | None = None,
    funding_trace: list[dict[str, Any]] | None = None,
) -> list[TradeRow]:
    """Reconstruct a minimal replay trade ledger from fill trace rows.

    Use execution price and signed commission, not the triggering public trade
    price. Cross-zero executions produce closing/opening economic legs. This
    Funding uses physical fill timestamps, not callback visibility. Settlement
    precedes equal-ms fills (an explicit model convention, not exchange truth).
    This fill/settlement-marked ledger is not a continuous market-MTM path.
    An explicit completed-window mark values residual inventory without a fill.
    """
    rows: list[TradeRow] = []
    q = float(initial_inventory or 0.0)
    entry = float(initial_entry_price or 0.0)
    cash = -q * entry if abs(q) > 1e-12 and entry > 0.0 else 0.0
    settlements = list(funding_events or [])
    settlement_index = 0
    previous_funding_ms = -1
    for event in settlements:
        funding_ms = int(event["fundingTime"])
        if funding_ms <= previous_funding_ms:
            raise ValueError("funding timestamps must be unique and strictly increasing")
        previous_funding_ms = funding_ms
        funding_cashflow_usdc(0, event["markPrice"], event["fundingRate"])
        if (funding_ms < day_start_ts * 1000 or terminal_ts is None
                or funding_ms > terminal_ts * 1000):
            raise ValueError("funding settlement is outside the explicit replay window")
    ordered_fills = _ordered_fill_trace(fill_trace)
    fill_counts_by_ms = Counter(
        round(ts if ts > 10_000_000_000 else ts * 1000)
        for row in ordered_fills for ts in [safe_float(row, "fill_ts")]
    )

    def apply_funding_through(ts_ms: int) -> None:
        nonlocal cash, settlement_index
        while (settlement_index < len(settlements)
               and int(settlements[settlement_index]["fundingTime"]) <= ts_ms):
            event = settlements[settlement_index]
            settlement_index += 1
            ms = int(event["fundingTime"])
            mark, rate = float(event["markPrice"]), float(event["fundingRate"])
            payment = funding_cashflow_usdc(q, mark, rate)
            cash += payment
            rows.append(TradeRow(
                ts=ms / 1000, side="FUNDING", trade_type="SYNC_ADJUST", qty=0.0,
                price=mark, position=q, realized_pnl=cash, unrealized_pnl=q * mark,
            ))
            if funding_trace is not None:
                funding_trace.append({
                    "funding_ts_ms": ms, "position_btc": q, "mark_price": mark,
                    "funding_rate": rate, "funding_cashflow_usdc": payment,
                    "same_ms_fill_count": fill_counts_by_ms[ms],
                    "same_ms_ordering": "settlement_before_equal_ms_fills",
                })
    if abs(q) > 1e-12 and day_start_ts > 0.0:
        rows.append(
            TradeRow(
                ts=day_start_ts,
                side="INIT",
                trade_type="SYNC_ADJUST",
                qty=0.0,
                price=entry,
                position=q,
                realized_pnl=cash,
                unrealized_pnl=q * entry,
            )
        )
    for raw in ordered_fills:
        side = norm_side(raw.get("side", ""))
        qty = safe_float(raw, "fill_qty", safe_float(raw, "quantity", 0.0))
        px = safe_float(
            raw, "quote_px", safe_float(raw, "price", safe_float(raw, "fill_trade_px", 0.0))
        )
        ts_raw = safe_float(raw, "fill_ts", 0.0)
        ts = ts_raw / 1000.0 if ts_raw > 10_000_000_000 else ts_raw
        if side not in {"BUY", "SELL"} or qty <= 0.0 or px <= 0.0 or ts <= 0.0:
            continue
        apply_funding_through(round(ts * 1000))
        fee = float(raw.get("fill_fee_usdc", 0.0))
        if not math.isfinite(fee):
            raise ValueError("fill_fee_usdc must be finite")
        signed = 1.0 if side == "BUY" else -1.0
        closing = min(abs(q), qty) if q * signed < 0.0 else 0.0
        quantities = (closing, qty - closing) if 0.0 < closing < qty else (qty,)
        remaining_fee = fee
        for index, leg_qty in enumerate(quantities):
            leg_fee = remaining_fee if index == len(quantities) - 1 else fee * leg_qty / qty
            remaining_fee -= leg_fee
            cash -= signed * leg_qty * px + leg_fee
            q += signed * leg_qty
            if abs(q) <= 1e-12:
                q = 0.0
            rows.append(
                TradeRow(
                    ts=ts, side=side, trade_type="FILL", qty=leg_qty, price=px,
                    position=q, realized_pnl=cash, unrealized_pnl=q * px,
                    fee_usdc=leg_fee,
                )
            )
    if terminal_ts is not None or terminal_mark_price is not None:
        if terminal_ts is None or terminal_mark_price is None:
            raise ValueError("terminal mark requires both timestamp and price")
        if (
            not math.isfinite(terminal_ts) or not math.isfinite(terminal_mark_price)
            or terminal_mark_price <= 0.0
            or terminal_ts < (rows[-1].ts if rows else day_start_ts)
        ):
            raise ValueError("terminal mark has invalid time or price")
        apply_funding_through(round(terminal_ts * 1000))
        rows.append(TradeRow(
            ts=terminal_ts, side="MARK", trade_type="SYNC_ADJUST", qty=0.0,
            price=terminal_mark_price, position=q, realized_pnl=cash,
            unrealized_pnl=q * terminal_mark_price,
        ))
    return rows


def _fill_split(
    fill_trace: list[dict[str, Any]], *, initial_inventory: float = 0.0
) -> dict[str, Any]:
    """Count replay fills by side and whether they increase inventory exposure.

    中文说明：campaign 风险不只看 BUY/SELL 数量，还要看这笔成交是在加仓
    还是减仓。这里按日度 fresh-start replay 从 0 仓位顺序重放成交，和
    campaign label 使用同一个简化账本口径。
    """
    q = float(initial_inventory or 0.0)
    out = {
        "buy_exposure_fills": 0,
        "buy_reducing_fills": 0,
        "sell_exposure_fills": 0,
        "sell_reducing_fills": 0,
        "buy_fill_qty": 0.0,
        "sell_fill_qty": 0.0,
        "buy_fill_notional": 0.0,
        "sell_fill_notional": 0.0,
    }
    for raw in _ordered_fill_trace(fill_trace):
        side = norm_side(raw.get("side", ""))
        qty = safe_float(raw, "fill_qty", safe_float(raw, "quantity", 0.0))
        px = safe_float(
            raw, "quote_px", safe_float(raw, "price", safe_float(raw, "fill_trade_px", 0.0))
        )
        if side not in {"BUY", "SELL"} or qty <= 0.0:
            continue
        q_before = q
        q = q + qty if side == "BUY" else q - qty
        exposure_increasing = abs(q) > abs(q_before) + 1e-12
        if side == "BUY":
            out["buy_exposure_fills" if exposure_increasing else "buy_reducing_fills"] += 1
            out["buy_fill_qty"] += qty
            out["buy_fill_notional"] += qty * px
        else:
            out["sell_exposure_fills" if exposure_increasing else "sell_reducing_fills"] += 1
            out["sell_fill_qty"] += qty
            out["sell_fill_notional"] += qty * px
    out["buy_avg_fill_price"] = (
        out["buy_fill_notional"] / out["buy_fill_qty"] if out["buy_fill_qty"] > 1e-12 else 0.0
    )
    out["sell_avg_fill_price"] = (
        out["sell_fill_notional"] / out["sell_fill_qty"] if out["sell_fill_qty"] > 1e-12 else 0.0
    )
    return out


def _campaign_daily_row(
    *,
    day: str,
    arm: smoke.SmokeArm,
    result: dict[str, Any],
    label_rows: list[dict[str, Any]],
    runtime_s: float,
    fill_split: dict[str, Any],
    initial_inventory: float = 0.0,
    initial_entry_price: float = 0.0,
) -> dict[str, Any]:
    labels = Counter(str(r.get("campaign_label", "")) for r in label_rows)
    closed = [r for r in label_rows if int(float(r.get("closed", 0) or 0)) == 1]
    final_pnls = pd.to_numeric(
        pd.Series([r.get("final_total_pnl_delta", "") for r in label_rows]), errors="coerce"
    )
    early20 = pd.to_numeric(
        pd.Series([r.get("early_drawdown_20m", "") for r in label_rows]), errors="coerce"
    )
    max_inv = pd.to_numeric(
        pd.Series([r.get("max_abs_inventory", "") for r in label_rows]), errors="coerce"
    )
    duration = pd.to_numeric(
        pd.Series([r.get("duration_s", "") for r in label_rows]), errors="coerce"
    )
    fills = pd.to_numeric(pd.Series([r.get("fills", "") for r in label_rows]), errors="coerce")
    decision_counts = {
        "place": int(result.get("decision_place_count", 0) or 0),
        "replace": int(result.get("decision_replace_count", 0) or 0),
        "keep": int(result.get("decision_keep_count", 0) or 0),
        "pause": int(result.get("decision_pause_count", 0) or 0),
        "pending_coalesce": int(result.get("decision_pending_coalesce_count", 0) or 0),
        "cancel_first": int(result.get("decision_cancel_first_count", 0) or 0),
        "skip_filter": int(result.get("decision_skip_filter_count", 0) or 0),
        "none": int(result.get("decision_none_count", 0) or 0),
    }
    decision_total = max(sum(decision_counts.values()), 1)
    fills_bid = int(result.get("fills_bid", 0) or 0)
    fills_ask = int(result.get("fills_ask", 0) or 0)
    fills_total = int(result.get("fills_total", fills_bid + fills_ask) or (fills_bid + fills_ask))
    return {
        "day": day,
        "arm": arm.name,
        "group": arm.group,
        "runtime_s": runtime_s,
        "initial_inventory": float(initial_inventory or 0.0),
        "initial_entry_price": float(initial_entry_price or 0.0),
        "campaigns": len(label_rows),
        "closed_campaigns": len(closed),
        "open_campaigns": len(label_rows) - len(closed),
        "positive_flat": labels.get("positive_flat", 0),
        "repaired_after_drawdown": labels.get("repaired_after_drawdown", 0),
        "negative_flat": labels.get("negative_flat", 0),
        "loss_tail": labels.get("loss_tail", 0),
        "open_risk": labels.get("open_risk", 0),
        "bad_campaigns": labels.get("negative_flat", 0)
        + labels.get("loss_tail", 0)
        + labels.get("open_risk", 0),
        "repaired_campaigns": labels.get("positive_flat", 0)
        + labels.get("repaired_after_drawdown", 0),
        "tail_loss_rate": labels.get("loss_tail", 0) / max(len(label_rows), 1),
        "bad_rate": (
            labels.get("negative_flat", 0) + labels.get("loss_tail", 0) + labels.get("open_risk", 0)
        )
        / max(len(label_rows), 1),
        "repair_rate": (labels.get("positive_flat", 0) + labels.get("repaired_after_drawdown", 0))
        / max(len(label_rows), 1),
        "terminal_pnl_sum": float(final_pnls.sum(skipna=True)) if len(final_pnls) else 0.0,
        "terminal_pnl_mean": float(final_pnls.mean(skipna=True)) if len(final_pnls) else math.nan,
        "terminal_pnl_median": float(final_pnls.median(skipna=True))
        if len(final_pnls)
        else math.nan,
        "terminal_pnl_p10": float(final_pnls.quantile(0.10))
        if len(final_pnls.dropna())
        else math.nan,
        "terminal_pnl_min": float(final_pnls.min(skipna=True))
        if len(final_pnls.dropna())
        else math.nan,
        "early_20m_drawdown_mean": float(early20.mean(skipna=True))
        if len(early20.dropna())
        else math.nan,
        "early_20m_drawdown_p90": float(early20.quantile(0.90))
        if len(early20.dropna())
        else math.nan,
        "max_abs_inventory_mean": float(max_inv.mean(skipna=True))
        if len(max_inv.dropna())
        else math.nan,
        "max_abs_inventory_max": float(max_inv.max(skipna=True))
        if len(max_inv.dropna())
        else math.nan,
        "duration_mean_s": float(duration.mean(skipna=True))
        if len(duration.dropna())
        else math.nan,
        "duration_p90_s": float(duration.quantile(0.90)) if len(duration.dropna()) else math.nan,
        "fills_per_campaign_mean": float(fills.mean(skipna=True))
        if len(fills.dropna())
        else math.nan,
        "replay_pnl": float(result.get("pnl", 0.0) or 0.0),
        "cash_before_terminal": float(result.get("cash_before_terminal", 0.0) or 0.0),
        "terminal_mtm_pnl": float(
            result.get("terminal_mtm_pnl", result.get("mtm_before_terminal_fee", 0.0)) or 0.0
        ),
        "mtm_before_terminal_fee": float(result.get("mtm_before_terminal_fee", 0.0) or 0.0),
        "terminal_fee_drag": float(result.get("terminal_fee_drag", 0.0) or 0.0),
        "terminal_liquidation_fee_estimate": float(
            result.get("terminal_liquidation_fee_estimate", 0.0) or 0.0
        ),
        "terminal_liquidation_applied": bool(result.get("terminal_liquidation_applied", False)),
        "maker_fee_rate": float(result.get("maker_fee_rate", 0.0) or 0.0),
        "final_inventory": float(result.get("final_inventory", 0.0) or 0.0),
        "circuit_breaker_exit_mode": str(result.get("circuit_breaker_exit_mode", "")),
        "circuit_breaker_count": int(result.get("circuit_breaker_count", 0) or 0),
        "circuit_breaker_closing": bool(result.get("circuit_breaker_closing", False)),
        "circuit_breaker_close_place_count": int(
            result.get("circuit_breaker_close_place_count", 0) or 0
        ),
        "circuit_breaker_close_keep_count": int(
            result.get("circuit_breaker_close_keep_count", 0) or 0
        ),
        "circuit_breaker_close_fill_count": int(
            result.get("circuit_breaker_close_fill_count", 0) or 0
        ),
        "circuit_breaker_close_gtx_reject_count": int(
            result.get("circuit_breaker_close_gtx_reject_count", 0) or 0
        ),
        "circuit_breaker_close_ioc_place_count": int(
            result.get("circuit_breaker_close_ioc_place_count", 0) or 0
        ),
        "circuit_breaker_close_ioc_fill_count": int(
            result.get("circuit_breaker_close_ioc_fill_count", 0) or 0
        ),
        "circuit_breaker_close_ioc_expire_count": int(
            result.get("circuit_breaker_close_ioc_expire_count", 0) or 0
        ),
        "replay_inv_adj": float(result.get("inventory_adjusted_pnl", 0.0) or 0.0),
        "replay_abs_inventory_time_s": float(result.get("abs_inventory_time_s", 0.0) or 0.0),
        "replay_campaign_max_adverse_excursion": float(
            result.get("campaign_max_adverse_excursion", 0.0) or 0.0
        ),
        "replay_max_inventory": float(result.get("max_inventory", 0.0) or 0.0),
        "replay_avg_spread": float(result.get("avg_spread", 0.0) or 0.0),
        "replay_avg_final_spread": float(result.get("avg_final_spread", 0.0) or 0.0),
        "replay_n_final_spread": float(result.get("n_final_spread", 0.0) or 0.0),
        "exec_book_visibility_delay_applied_count": int(
            result.get("exec_book_visibility_delay_applied_count", 0) or 0
        ),
        "exec_book_visibility_delay_applied_avg_ms": float(
            result.get("exec_book_visibility_delay_applied_avg_ms", 0.0) or 0.0
        ),
        "exec_book_visibility_delay_applied_max_ms": float(
            result.get("exec_book_visibility_delay_applied_max_ms", 0.0) or 0.0
        ),
        "exec_depth_visibility_delay_applied_avg_ms": float(
            result.get("exec_depth_visibility_delay_applied_avg_ms", 0.0) or 0.0
        ),
        "exec_depth_visibility_delay_applied_max_ms": float(
            result.get("exec_depth_visibility_delay_applied_max_ms", 0.0) or 0.0
        ),
        "exec_trade_visibility_delay_applied_avg_ms": float(
            result.get("exec_trade_visibility_delay_applied_avg_ms", 0.0) or 0.0
        ),
        "exec_trade_visibility_delay_applied_max_ms": float(
            result.get("exec_trade_visibility_delay_applied_max_ms", 0.0) or 0.0
        ),
        "exec_book_visibility_paired_hit_count": int(
            result.get("exec_book_visibility_paired_hit_count", 0) or 0
        ),
        "exec_book_visibility_paired_miss_count": int(
            result.get("exec_book_visibility_paired_miss_count", 0) or 0
        ),
        "exec_book_visibility_mid_override_count": int(
            result.get("exec_book_visibility_mid_override_count", 0) or 0
        ),
        "exec_depth_visibility_source_offset_ms": int(
            result.get("exec_depth_visibility_source_offset_ms", 0) or 0
        ),
        "queue_l2_cancel_ahead_enabled": bool(result.get("queue_l2_cancel_ahead_enabled", False)),
        "queue_l2_cancel_ahead_event_count": int(
            result.get("queue_l2_cancel_ahead_event_count", 0) or 0
        ),
        "queue_l2_cancel_ahead_bid_event_count": int(
            result.get("queue_l2_cancel_ahead_bid_event_count", 0) or 0
        ),
        "queue_l2_cancel_ahead_ask_event_count": int(
            result.get("queue_l2_cancel_ahead_ask_event_count", 0) or 0
        ),
        "queue_l2_cancel_ahead_qty": float(result.get("queue_l2_cancel_ahead_qty", 0.0) or 0.0),
        "exchange_book_queue_mode": str(
            result.get("exchange_book_queue_mode", "disabled") or "disabled"
        ),
        "exchange_book_queue_scope": str(
            result.get("exchange_book_queue_scope", "disabled") or "disabled"
        ),
        "exchange_book_queue_lookup_count": int(
            result.get("exchange_book_queue_lookup_count", 0) or 0
        ),
        "exchange_book_queue_exact_count": int(
            result.get("exchange_book_queue_exact_count", 0) or 0
        ),
        "exchange_book_queue_known_zero_count": int(
            result.get("exchange_book_queue_known_zero_count", 0) or 0
        ),
        "exchange_book_queue_missing_count": int(
            result.get("exchange_book_queue_missing_count", 0) or 0
        ),
        "exchange_book_queue_invalidated_order_count": int(
            result.get("exchange_book_queue_invalidated_order_count", 0) or 0
        ),
        "exchange_book_queue_cancel_ahead_event_count": int(
            result.get("exchange_book_queue_cancel_ahead_event_count", 0) or 0
        ),
        "exchange_book_queue_cancel_ahead_qty": float(
            result.get("exchange_book_queue_cancel_ahead_qty", 0.0) or 0.0
        ),
        "exchange_book_queue_ambiguous_event_count": int(
            result.get("exchange_book_queue_ambiguous_event_count", 0) or 0
        ),
        "exchange_book_events_consumed": int(result.get("exchange_book_events_consumed", 0) or 0),
        "exchange_book_events_accepted": int(result.get("exchange_book_events_accepted", 0) or 0),
        "exchange_book_events_rejected": int(result.get("exchange_book_events_rejected", 0) or 0),
        "exchange_book_source_gap_events": int(
            result.get("exchange_book_source_gap_events", 0) or 0
        ),
        "exchange_book_invalid_sequence_messages": int(
            result.get("exchange_book_invalid_sequence_messages", 0) or 0
        ),
        "exchange_book_sequence_gaps": int(result.get("exchange_book_sequence_gaps", 0) or 0),
        "exchange_book_message_time_reversals": int(
            result.get("exchange_book_message_time_reversals", 0) or 0
        ),
        "strict_calibration_validated": bool(result.get("strict_calibration_validated", False)),
        "strict_calibration_identity_validated": bool(
            result.get("strict_calibration_identity_validated", False)
        ),
        "replay_evidence_scope": str(result.get("replay_evidence_scope", "") or ""),
        "replay_contract_sha256": str(result.get("replay_contract_sha256", "") or ""),
        "replay_purpose": str(result.get("replay_purpose", "") or ""),
        "replay_initial_state_mode": str(result.get("replay_initial_state_mode", "") or ""),
        "replay_promotion_eligible": bool(result.get("replay_promotion_eligible", False)),
        "latency_sampler_version": str(result.get("latency_sampler_version", "") or ""),
        "latency_profile_id": str(result.get("latency_profile_id", "") or ""),
        "latency_scenario": str(result.get("latency_scenario", "") or ""),
        "latency_seed": int(result.get("latency_seed", 0) or 0),
        "replay_quote_spread_lt_100_rate": float(
            result.get("quote_spread_lt_100_rate", 0.0) or 0.0
        ),
        "replay_final_spread_lt_100_rate": float(
            result.get("final_spread_lt_100_rate", 0.0) or 0.0
        ),
        "avg_markout": float(result.get("avg_markout", 0.0) or 0.0),
        "avg_markout_bid": float(result.get("avg_markout_bid", 0.0) or 0.0),
        "avg_markout_ask": float(result.get("avg_markout_ask", 0.0) or 0.0),
        "cap_hit_rate": float(result.get("cap_hit_rate", 0.0) or 0.0),
        "final_cap_compress_rate": float(result.get("final_cap_compress_rate", 0.0) or 0.0),
        "post_policy_cap_rate": float(result.get("post_policy_cap_rate", 0.0) or 0.0),
        "cap_exposure_block_rate": float(result.get("cap_exposure_block_rate", 0.0) or 0.0),
        "fills_final_compressed": int(result.get("fills_final_compressed", 0) or 0),
        "fills_not_final_compressed": int(result.get("fills_not_final_compressed", 0) or 0),
        "avg_markout_final_compressed": float(
            result.get("avg_markout_final_compressed", 0.0) or 0.0
        ),
        "avg_markout_not_final_compressed": float(
            result.get("avg_markout_not_final_compressed", 0.0) or 0.0
        ),
        "fills_total": fills_total,
        "fills_bid_buy": fills_bid,
        "fills_ask_sell": fills_ask,
        "buy_fill_share": fills_bid / max(fills_bid + fills_ask, 1),
        "sell_fill_share": fills_ask / max(fills_bid + fills_ask, 1),
        **fill_split,
        "decision_total": decision_total,
        "decision_place_count": decision_counts["place"],
        "decision_replace_count": decision_counts["replace"],
        "decision_keep_count": decision_counts["keep"],
        "decision_pause_count": decision_counts["pause"],
        "decision_pending_coalesce_count": decision_counts["pending_coalesce"],
        "decision_cancel_first_count": decision_counts["cancel_first"],
        "decision_skip_filter_count": decision_counts["skip_filter"],
        "decision_none_count": decision_counts["none"],
        **{
            f"decision_{side}_{role}_{action}_count": int(
                result.get(
                    f"decision_{side}_{role}_{action}_count",
                    0,
                )
                or 0
            )
            for side in ("buy", "sell")
            for role in ("exposure", "reducing")
            for action in (
                "place",
                "replace",
                "keep",
                "pause",
                "none",
                "skip_filter",
                "pending_coalesce",
                "cancel_first",
            )
        },
        "decision_place_rate": decision_counts["place"] / decision_total,
        "decision_replace_rate": decision_counts["replace"] / decision_total,
        "decision_keep_rate": decision_counts["keep"] / decision_total,
        "decision_pause_rate": decision_counts["pause"] / decision_total,
        "decision_pending_coalesce_rate": decision_counts["pending_coalesce"] / decision_total,
        "decision_cancel_first_rate": decision_counts["cancel_first"] / decision_total,
        "buy_fill_selection_live_eval_count": int(
            result.get("buy_fill_selection_live_eval_count", 0) or 0
        ),
        "buy_fill_selection_live_hit_count": int(
            result.get("buy_fill_selection_live_hit_count", 0) or 0
        ),
        "buy_fill_selection_live_hit_rate": float(
            result.get("buy_fill_selection_live_hit_rate", 0.0) or 0.0
        ),
        "buy_fill_selection_live_score_mean": float(
            result.get("buy_fill_selection_live_score_mean", 0.0) or 0.0
        ),
        "buy_fill_selection_live_score_max": float(
            result.get("buy_fill_selection_live_score_max", 0.0) or 0.0
        ),
        "buy_fill_selection_live_score_p10": float(
            result.get("buy_fill_selection_live_score_p10", 0.0) or 0.0
        ),
        "buy_fill_selection_live_score_median": float(
            result.get("buy_fill_selection_live_score_median", 0.0) or 0.0
        ),
        "buy_fill_selection_live_score_p90": float(
            result.get("buy_fill_selection_live_score_p90", 0.0) or 0.0
        ),
        "buy_fill_selection_live_score_ge_044": int(
            result.get("buy_fill_selection_live_score_ge_044", 0) or 0
        ),
        "adaptive_add_cooldown_hit_count": int(
            result.get("adaptive_add_cooldown_hit_count", 0) or 0
        ),
        "adaptive_add_cooldown_bid_hit_count": int(
            result.get("adaptive_add_cooldown_bid_hit_count", 0) or 0
        ),
        "adaptive_add_cooldown_ask_hit_count": int(
            result.get("adaptive_add_cooldown_ask_hit_count", 0) or 0
        ),
        "post_fill_quote_response_eval_count": int(
            result.get("post_fill_quote_response_eval_count", 0) or 0
        ),
        "post_fill_quote_response_active_count": int(
            result.get("post_fill_quote_response_active_count", 0) or 0
        ),
        "post_fill_quote_response_inventory_count": int(
            result.get("post_fill_quote_response_inventory_count", 0) or 0
        ),
        "post_fill_quote_response_flow_count": int(
            result.get("post_fill_quote_response_flow_count", 0) or 0
        ),
        "post_fill_quote_response_cap_limited_count": int(
            result.get("post_fill_quote_response_cap_limited_count", 0) or 0
        ),
        "post_fill_quote_response_add_fill_count": int(
            result.get("post_fill_quote_response_add_fill_count", 0) or 0
        ),
        "post_fill_quote_response_avg_inventory_ticks": float(
            result.get("post_fill_quote_response_avg_inventory_ticks", 0.0) or 0.0
        ),
        "post_fill_quote_response_avg_add_ticks": float(
            result.get("post_fill_quote_response_avg_add_ticks", 0.0) or 0.0
        ),
        "post_fill_quote_response_max_add_ticks": float(
            result.get("post_fill_quote_response_max_add_ticks", 0.0) or 0.0
        ),
        "post_fill_quote_response_avg_half_life_s": float(
            result.get("post_fill_quote_response_avg_half_life_s", 0.0) or 0.0
        ),
        "campaign_soft_control_count": int(result.get("campaign_soft_control_count", 0) or 0),
        "bid_campaign_soft_control_count": int(
            result.get("bid_campaign_soft_control_count", 0) or 0
        ),
        "ask_campaign_soft_control_count": int(
            result.get("ask_campaign_soft_control_count", 0) or 0
        ),
        "multi_market_policy_eval_count": int(result.get("multi_market_policy_eval_count", 0) or 0),
        "multi_market_policy_hit_count": int(result.get("multi_market_policy_hit_count", 0) or 0),
        "multi_market_policy_effective_block_count": int(
            result.get("multi_market_policy_effective_block_count", 0) or 0
        ),
        "bid_multi_market_policy_effective_block_count": int(
            result.get("bid_multi_market_policy_effective_block_count", 0) or 0
        ),
        "ask_multi_market_policy_effective_block_count": int(
            result.get("ask_multi_market_policy_effective_block_count", 0) or 0
        ),
        "campaign_repair_trace_count": int(result.get("campaign_repair_trace_count", 0) or 0),
        "campaign_repair_model_id": str(result.get("campaign_repair_model_id", "")),
        "campaign_repair_model_training_end_day": str(
            result.get("campaign_repair_model_training_end_day", "")
        ),
        "multi_market_reference_replay_source": str(
            result.get("multi_market_reference_replay_source", "")
        ),
        "note": arm.note,
        # Preserve emitted clock/model counters, not inferred zero-valued
        # activation claims. Nested per-source/path diagnostics remain intact.
        **{
            key: value for key, value in result.items()
            if key.startswith((
                "runtime_compute_", "exec_message_", "pre_snapshot_compute_",
                "requote_tail_work_", "decision_to_gateway_", "rest_gateway_",
                "private_fill_visibility_",
            ))
            or key in {
                "risk_emergency_ownership_conflict_count", "risk_emergency_stop_reason",
                "economic_pnl_complete", "economic_pnl_status",
            }
        },
    }


def _rollup(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if daily.empty:
        return pd.DataFrame()
    for arm, grp in daily.groupby("arm", sort=False):

        def sum_col(name: str, frame: pd.DataFrame = grp) -> float:
            if name not in frame.columns:
                return 0.0
            return float(pd.to_numeric(frame[name], errors="coerce").sum())

        def median_col(name: str, frame: pd.DataFrame = grp) -> float:
            if name not in frame.columns:
                return 0.0
            vals = pd.to_numeric(frame[name], errors="coerce").dropna()
            return float(vals.median()) if len(vals) else 0.0

        def weighted_mean(
            value_col: str,
            weight_col: str,
            frame: pd.DataFrame = grp,
        ) -> float:
            if value_col not in frame.columns:
                return 0.0
            vals = pd.to_numeric(frame[value_col], errors="coerce")
            weights = (
                pd.to_numeric(frame[weight_col], errors="coerce")
                if weight_col in frame.columns
                else pd.Series(1.0, index=frame.index)
            )
            mask = vals.notna() & weights.notna() & (weights > 0)
            if not mask.any():
                return float(vals.dropna().mean()) if len(vals.dropna()) else 0.0
            return float((vals[mask] * weights[mask]).sum() / weights[mask].sum())

        campaign_mae_series = pd.to_numeric(
            grp["replay_campaign_max_adverse_excursion"], errors="coerce"
        ).dropna()
        campaign_mae_proxy = (
            abs(float(campaign_mae_series.min())) if len(campaign_mae_series) else 0.0
        )

        row = {
            "arm": arm,
            "group": str(grp["group"].iloc[0]),
            "n_days": int(len(grp)),
            "observation_unit": (
                "continuous_segment"
                if "accounting_window" in grp
                and grp["accounting_window"].eq("continuous_segment").any()
                else "utc_day"
            ),
            "covered_utc_days": int(sum_col("window_day_count"))
            if "window_day_count" in grp else int(len(grp)),
            "funding_cashflow_usdc": sum_col("funding_cashflow_usdc"),
            "replay_net_pnl_sum": sum_col("replay_net_pnl")
            if "replay_net_pnl" in grp else sum_col("replay_pnl"),
            "campaigns": int(pd.to_numeric(grp["campaigns"], errors="coerce").sum()),
            "closed_campaigns": int(pd.to_numeric(grp["closed_campaigns"], errors="coerce").sum()),
            "loss_tail": int(pd.to_numeric(grp["loss_tail"], errors="coerce").sum()),
            "negative_flat": int(pd.to_numeric(grp["negative_flat"], errors="coerce").sum()),
            "positive_flat": int(pd.to_numeric(grp["positive_flat"], errors="coerce").sum()),
            "repaired_after_drawdown": int(
                pd.to_numeric(grp["repaired_after_drawdown"], errors="coerce").sum()
            ),
            "open_risk": int(pd.to_numeric(grp["open_risk"], errors="coerce").sum()),
            "terminal_pnl_sum": float(
                pd.to_numeric(grp["terminal_pnl_sum"], errors="coerce").sum()
            ),
            "mtm_before_terminal_fee_sum": sum_col("mtm_before_terminal_fee"),
            "terminal_fee_drag_sum": sum_col("terminal_fee_drag"),
            "terminal_liquidation_fee_estimate_sum": sum_col("terminal_liquidation_fee_estimate"),
            "terminal_liquidation_applied_days": int(
                pd.to_numeric(grp["terminal_liquidation_applied"], errors="coerce").fillna(0).sum()
            ),
            "maker_fee_rate_max": float(
                pd.to_numeric(grp["maker_fee_rate"], errors="coerce").fillna(0.0).max()
            ),
            "final_inventory_sum": sum_col("final_inventory"),
            "final_inventory_abs_max": float(
                pd.to_numeric(grp["final_inventory"], errors="coerce").abs().max()
            ),
            "circuit_breaker_exit_mode": "|".join(
                sorted({str(value) for value in grp["circuit_breaker_exit_mode"] if str(value)})
            ),
            "circuit_breaker_count": int(sum_col("circuit_breaker_count")),
            "circuit_breaker_closing_days": int(
                pd.to_numeric(grp["circuit_breaker_closing"], errors="coerce").fillna(0).sum()
            ),
            "circuit_breaker_close_place_count": int(sum_col("circuit_breaker_close_place_count")),
            "circuit_breaker_close_keep_count": int(sum_col("circuit_breaker_close_keep_count")),
            "circuit_breaker_close_fill_count": int(sum_col("circuit_breaker_close_fill_count")),
            "circuit_breaker_close_gtx_reject_count": int(
                sum_col("circuit_breaker_close_gtx_reject_count")
            ),
            "circuit_breaker_close_ioc_place_count": int(
                sum_col("circuit_breaker_close_ioc_place_count")
            ),
            "circuit_breaker_close_ioc_fill_count": int(
                sum_col("circuit_breaker_close_ioc_fill_count")
            ),
            "circuit_breaker_close_ioc_expire_count": int(
                sum_col("circuit_breaker_close_ioc_expire_count")
            ),
            "terminal_pnl_mean_by_day": float(
                pd.to_numeric(grp["terminal_pnl_mean"], errors="coerce").mean()
            ),
            "terminal_pnl_min_day_min": float(
                pd.to_numeric(grp["terminal_pnl_min"], errors="coerce").min()
            ),
            "tail_loss_days": int((pd.to_numeric(grp["loss_tail"], errors="coerce") > 0).sum()),
            "bad_rate_mean": float(pd.to_numeric(grp["bad_rate"], errors="coerce").mean()),
            "repair_rate_mean": float(pd.to_numeric(grp["repair_rate"], errors="coerce").mean()),
            "early_20m_drawdown_mean": float(
                pd.to_numeric(grp["early_20m_drawdown_mean"], errors="coerce").mean()
            ),
            "early_20m_drawdown_p90_mean": float(
                pd.to_numeric(grp["early_20m_drawdown_p90"], errors="coerce").mean()
            ),
            "max_abs_inventory_max": float(
                pd.to_numeric(grp["max_abs_inventory_max"], errors="coerce").max()
            ),
            "duration_p90_s_mean": float(
                pd.to_numeric(grp["duration_p90_s"], errors="coerce").mean()
            ),
            "replay_pnl_sum": float(pd.to_numeric(grp["replay_pnl"], errors="coerce").sum()),
            "replay_pnl_median_by_day": median_col("replay_pnl"),
            "replay_inv_adj_sum": float(
                pd.to_numeric(grp["replay_inv_adj"], errors="coerce").sum()
            ),
            "avg_markout_mean_by_day": float(
                pd.to_numeric(grp["avg_markout"], errors="coerce").mean()
            ),
            "avg_markout_bid_mean_by_day": float(
                pd.to_numeric(grp["avg_markout_bid"], errors="coerce").mean()
            ),
            "avg_markout_ask_mean_by_day": float(
                pd.to_numeric(grp["avg_markout_ask"], errors="coerce").mean()
            ),
            "cap_hit_rate_mean": float(pd.to_numeric(grp["cap_hit_rate"], errors="coerce").mean()),
            "final_cap_compress_rate_mean": float(
                pd.to_numeric(grp["final_cap_compress_rate"], errors="coerce").mean()
            ),
            "post_policy_cap_rate_mean": float(
                pd.to_numeric(grp["post_policy_cap_rate"], errors="coerce").mean()
            ),
            "cap_exposure_block_rate_mean": float(
                pd.to_numeric(grp["cap_exposure_block_rate"], errors="coerce").mean()
            ),
            "fills_final_compressed": sum_col("fills_final_compressed"),
            "fills_not_final_compressed": sum_col("fills_not_final_compressed"),
            "avg_markout_final_compressed": weighted_mean(
                "avg_markout_final_compressed", "fills_final_compressed"
            ),
            "avg_markout_not_final_compressed": weighted_mean(
                "avg_markout_not_final_compressed", "fills_not_final_compressed"
            ),
            "replay_inv_adj_median_by_day": median_col("replay_inv_adj"),
            "replay_abs_inventory_time_s_sum": float(
                pd.to_numeric(grp["replay_abs_inventory_time_s"], errors="coerce").sum()
            ),
            "replay_abs_inventory_time_s_median_by_day": median_col("replay_abs_inventory_time_s"),
            "exec_book_visibility_delay_applied_count": int(
                sum_col("exec_book_visibility_delay_applied_count")
            ),
            "exec_book_visibility_delay_applied_avg_ms_mean_by_day": float(
                pd.to_numeric(
                    grp["exec_book_visibility_delay_applied_avg_ms"],
                    errors="coerce",
                ).mean()
            ),
            "exec_book_visibility_delay_applied_max_ms_max": float(
                pd.to_numeric(
                    grp["exec_book_visibility_delay_applied_max_ms"],
                    errors="coerce",
                ).max()
            ),
            "exec_trade_visibility_delay_applied_avg_ms_mean_by_day": float(
                pd.to_numeric(
                    grp["exec_trade_visibility_delay_applied_avg_ms"],
                    errors="coerce",
                ).mean()
            ),
            "exec_trade_visibility_delay_applied_max_ms_max": float(
                pd.to_numeric(
                    grp["exec_trade_visibility_delay_applied_max_ms"],
                    errors="coerce",
                ).max()
            ),
            "exchange_book_queue_mode": "|".join(
                sorted(
                    {
                        str(value)
                        for value in grp.get("exchange_book_queue_mode", pd.Series(dtype=str))
                        if str(value)
                    }
                )
            ),
            "exchange_book_queue_lookup_count": int(sum_col("exchange_book_queue_lookup_count")),
            "exchange_book_queue_exact_count": int(sum_col("exchange_book_queue_exact_count")),
            "exchange_book_queue_known_zero_count": int(
                sum_col("exchange_book_queue_known_zero_count")
            ),
            "exchange_book_queue_missing_count": int(sum_col("exchange_book_queue_missing_count")),
            "exchange_book_queue_invalidated_order_count": int(
                sum_col("exchange_book_queue_invalidated_order_count")
            ),
            "exchange_book_queue_cancel_ahead_event_count": int(
                sum_col("exchange_book_queue_cancel_ahead_event_count")
            ),
            "exchange_book_queue_cancel_ahead_qty": float(
                sum_col("exchange_book_queue_cancel_ahead_qty")
            ),
            "exchange_book_queue_ambiguous_event_count": int(
                sum_col("exchange_book_queue_ambiguous_event_count")
            ),
            "exchange_book_events_consumed": int(sum_col("exchange_book_events_consumed")),
            "exchange_book_sequence_gaps": int(sum_col("exchange_book_sequence_gaps")),
            "exchange_book_message_time_reversals": int(
                sum_col("exchange_book_message_time_reversals")
            ),
            "replay_campaign_max_adverse_excursion_min": float(campaign_mae_series.min())
            if len(campaign_mae_series)
            else 0.0,
            "replay_campaign_mae_proxy": campaign_mae_proxy,
            "replay_avg_spread": weighted_mean("replay_avg_spread", "decision_total"),
            "replay_avg_final_spread": weighted_mean(
                "replay_avg_final_spread", "replay_n_final_spread"
            ),
            "replay_quote_spread_lt_100_rate": weighted_mean(
                "replay_quote_spread_lt_100_rate", "decision_total"
            ),
            "replay_final_spread_lt_100_rate": weighted_mean(
                "replay_final_spread_lt_100_rate", "replay_n_final_spread"
            ),
            "fills_total": int(pd.to_numeric(grp["fills_total"], errors="coerce").sum()),
            "fills_bid_buy": int(pd.to_numeric(grp["fills_bid_buy"], errors="coerce").sum()),
            "fills_ask_sell": int(pd.to_numeric(grp["fills_ask_sell"], errors="coerce").sum()),
            "buy_fill_qty": float(sum_col("buy_fill_qty")),
            "sell_fill_qty": float(sum_col("sell_fill_qty")),
            "buy_fill_notional": float(sum_col("buy_fill_notional")),
            "sell_fill_notional": float(sum_col("sell_fill_notional")),
            "buy_exposure_fills": int(
                pd.to_numeric(grp["buy_exposure_fills"], errors="coerce").sum()
            ),
            "buy_reducing_fills": int(
                pd.to_numeric(grp["buy_reducing_fills"], errors="coerce").sum()
            ),
            "sell_exposure_fills": int(
                pd.to_numeric(grp["sell_exposure_fills"], errors="coerce").sum()
            ),
            "sell_reducing_fills": int(
                pd.to_numeric(grp["sell_reducing_fills"], errors="coerce").sum()
            ),
            "decision_total": int(pd.to_numeric(grp["decision_total"], errors="coerce").sum()),
            "decision_place_count": int(
                pd.to_numeric(grp["decision_place_count"], errors="coerce").sum()
            ),
            "decision_replace_count": int(
                pd.to_numeric(grp["decision_replace_count"], errors="coerce").sum()
            ),
            "decision_keep_count": int(
                pd.to_numeric(grp["decision_keep_count"], errors="coerce").sum()
            ),
            "decision_pause_count": int(
                pd.to_numeric(grp["decision_pause_count"], errors="coerce").sum()
            ),
            "decision_pending_coalesce_count": int(
                pd.to_numeric(grp["decision_pending_coalesce_count"], errors="coerce").sum()
            ),
            "decision_cancel_first_count": int(
                pd.to_numeric(grp["decision_cancel_first_count"], errors="coerce").sum()
            ),
            "buy_fill_selection_live_eval_count": int(
                sum_col("buy_fill_selection_live_eval_count")
            ),
            "buy_fill_selection_live_hit_count": int(sum_col("buy_fill_selection_live_hit_count")),
            "buy_fill_selection_live_score_mean": weighted_mean(
                "buy_fill_selection_live_score_mean",
                "buy_fill_selection_live_eval_count",
            ),
            "buy_fill_selection_live_score_max": float(
                pd.to_numeric(grp["buy_fill_selection_live_score_max"], errors="coerce").max()
            ),
            "buy_fill_selection_live_score_p10_mean_by_day": float(
                pd.to_numeric(grp["buy_fill_selection_live_score_p10"], errors="coerce").mean()
            ),
            "buy_fill_selection_live_score_median_by_day": float(
                pd.to_numeric(grp["buy_fill_selection_live_score_median"], errors="coerce").mean()
            ),
            "buy_fill_selection_live_score_p90_mean_by_day": float(
                pd.to_numeric(grp["buy_fill_selection_live_score_p90"], errors="coerce").mean()
            ),
            "buy_fill_selection_live_score_ge_044": int(
                sum_col("buy_fill_selection_live_score_ge_044")
            ),
            "adaptive_add_cooldown_hit_count": int(sum_col("adaptive_add_cooldown_hit_count")),
            "adaptive_add_cooldown_bid_hit_count": int(
                sum_col("adaptive_add_cooldown_bid_hit_count")
            ),
            "adaptive_add_cooldown_ask_hit_count": int(
                sum_col("adaptive_add_cooldown_ask_hit_count")
            ),
            "post_fill_quote_response_eval_count": int(
                sum_col("post_fill_quote_response_eval_count")
            ),
            "post_fill_quote_response_active_count": int(
                sum_col("post_fill_quote_response_active_count")
            ),
            "post_fill_quote_response_inventory_count": int(
                sum_col("post_fill_quote_response_inventory_count")
            ),
            "post_fill_quote_response_flow_count": int(
                sum_col("post_fill_quote_response_flow_count")
            ),
            "post_fill_quote_response_cap_limited_count": int(
                sum_col("post_fill_quote_response_cap_limited_count")
            ),
            "post_fill_quote_response_add_fill_count": int(
                sum_col("post_fill_quote_response_add_fill_count")
            ),
            "post_fill_quote_response_avg_inventory_ticks": weighted_mean(
                "post_fill_quote_response_avg_inventory_ticks",
                "post_fill_quote_response_eval_count",
            ),
            "post_fill_quote_response_avg_add_ticks": weighted_mean(
                "post_fill_quote_response_avg_add_ticks",
                "post_fill_quote_response_eval_count",
            ),
            "post_fill_quote_response_max_add_ticks": float(
                pd.to_numeric(
                    grp["post_fill_quote_response_max_add_ticks"],
                    errors="coerce",
                ).max()
            ),
            "post_fill_quote_response_avg_half_life_s": weighted_mean(
                "post_fill_quote_response_avg_half_life_s",
                "post_fill_quote_response_eval_count",
            ),
            "campaign_soft_control_count": int(sum_col("campaign_soft_control_count")),
            "bid_campaign_soft_control_count": int(sum_col("bid_campaign_soft_control_count")),
            "ask_campaign_soft_control_count": int(sum_col("ask_campaign_soft_control_count")),
            "multi_market_policy_eval_count": int(sum_col("multi_market_policy_eval_count")),
            "multi_market_policy_hit_count": int(sum_col("multi_market_policy_hit_count")),
            "multi_market_policy_effective_block_count": int(
                sum_col("multi_market_policy_effective_block_count")
            ),
            "bid_multi_market_policy_effective_block_count": int(
                sum_col("bid_multi_market_policy_effective_block_count")
            ),
            "ask_multi_market_policy_effective_block_count": int(
                sum_col("ask_multi_market_policy_effective_block_count")
            ),
            "campaign_repair_trace_count": int(sum_col("campaign_repair_trace_count")),
        }
        row["tail_loss_rate"] = row["loss_tail"] / max(row["campaigns"], 1)
        row["bad_campaign_rate"] = (
            row["loss_tail"] + row["negative_flat"] + row["open_risk"]
        ) / max(row["campaigns"], 1)
        row["repair_campaign_rate"] = (row["positive_flat"] + row["repaired_after_drawdown"]) / max(
            row["campaigns"], 1
        )
        row["buy_fill_share"] = row["fills_bid_buy"] / max(
            row["fills_bid_buy"] + row["fills_ask_sell"], 1
        )
        row["sell_fill_share"] = row["fills_ask_sell"] / max(
            row["fills_bid_buy"] + row["fills_ask_sell"], 1
        )
        row["buy_avg_fill_price"] = (
            row["buy_fill_notional"] / row["buy_fill_qty"] if row["buy_fill_qty"] > 1e-12 else 0.0
        )
        row["sell_avg_fill_price"] = (
            row["sell_fill_notional"] / row["sell_fill_qty"]
            if row["sell_fill_qty"] > 1e-12
            else 0.0
        )
        row["decision_place_rate"] = row["decision_place_count"] / max(row["decision_total"], 1)
        row["decision_replace_rate"] = row["decision_replace_count"] / max(row["decision_total"], 1)
        row["decision_keep_rate"] = row["decision_keep_count"] / max(row["decision_total"], 1)
        row["decision_pause_rate"] = row["decision_pause_count"] / max(row["decision_total"], 1)
        row["decision_pending_coalesce_rate"] = row["decision_pending_coalesce_count"] / max(
            row["decision_total"], 1
        )
        row["decision_cancel_first_rate"] = row["decision_cancel_first_count"] / max(
            row["decision_total"], 1
        )
        row["buy_fill_selection_live_hit_rate"] = row["buy_fill_selection_live_hit_count"] / max(
            row["buy_fill_selection_live_eval_count"], 1
        )
        if "risk_emergency_ownership_conflict_count" in grp:
            row["risk_emergency_ownership_conflict_count"] = int(
                sum_col("risk_emergency_ownership_conflict_count")
            )
        if "risk_emergency_stop_reason" in grp:
            row["risk_emergency_stop_reason"] = "|".join(sorted({
                str(value) for value in grp["risk_emergency_stop_reason"].dropna()
            }))
        if "economic_pnl_complete" in grp:
            incomplete = grp["economic_pnl_complete"].eq(False)
            complete = grp["economic_pnl_complete"].eq(True)
            if incomplete.any() or complete.all():
                row["economic_pnl_complete"] = bool(complete.all())
            row["economic_pnl_incomplete_days"] = int(incomplete.sum())
            if "economic_pnl_status" in grp:
                status_rows = grp.loc[incomplete] if incomplete.any() else grp
                row["economic_pnl_status"] = "|".join(sorted({
                    str(value) for value in status_rows["economic_pnl_status"].dropna()
                }))
            if incomplete.any():
                # Keep the observed partial day/arm diagnostics, but do not
                # represent their sum as the full requested-window reward.
                row["replay_promotion_eligible"] = False
                for name in (
                    "terminal_pnl_sum", "terminal_pnl_mean_by_day", "terminal_pnl_min_day_min",
                    "replay_pnl_sum", "replay_pnl_median_by_day", "replay_inv_adj_sum",
                    "replay_inv_adj_median_by_day", "mtm_before_terminal_fee_sum",
                ):
                    row[name] = None
        rows.append(row)
    return pd.DataFrame(rows)


def _write_markdown(
    path: Path, daily: pd.DataFrame, rollup: pd.DataFrame, meta: dict[str, Any]
) -> None:
    lines = [
        f"# Campaign Outcome Replay Audit - {meta['tag']}",
        "",
        "This is a campaign-level shadow audit, not a live policy decision.",
        "",
        f"- symbol: `{meta['symbol']}`",
        f"- days: `{', '.join(meta['days'])}`",
        f"- arms: `{', '.join(meta['arms'])}`",
        "",
        "## Rollup",
        "",
        f"```text\n{rollup.to_string(index=False) if not rollup.empty else '(empty)'}\n```",
        "",
        "## Daily",
        "",
        f"```text\n{daily.to_string(index=False) if not daily.empty else '(empty)'}\n```",
        "",
    ]
    calibration = meta.get("runtime_timing_calibration")
    if calibration:
        clock = meta.get("runtime_compute_clock")
        components = ["Gateway"]
        if clock:
            components.append(f"phase-conditioned compute (`{clock}`)")
        if calibration.get("private_fill_model", {}).get("mode") == "observed_callback":
            components.append("observed private-fill callback visibility")
        lines[9:9] = [
            "## Runtime timing scope",
            "",
            " + ".join(components) + " diagnostic; not a complete current-live baseline.",
            "",
            *[f"- {item}" for item in calibration["limitations"]],
            "",
        ]
    if (
        "economic_pnl_complete" in daily
        and daily["economic_pnl_complete"].eq(False).any()
    ):
        lines[9:9] = [
            "## Incomplete economic path",
            "",
            "At least one day/arm stopped before its economic ledger was complete. "
            "Full-window PnL aggregates are unset and promotion is false. "
            "Daily values preserve the partial path for diagnosis, not a completed baseline.",
            "",
        ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_partial_day_outputs(out_dir: Path, stem: str, day_result: dict[str, Any]) -> None:
    """Persist one completed day immediately.

    中文说明：大规模参数面可能要跑很久。以前所有 worker 都结束后才落盘，
    一旦中断就没有可复用结果。这里按 day 写 partial daily/rollup/campaign，
    每行仍然是一组 day/arm，所以可以看到每个 arm 的早期结果。
    """
    day = str(day_result.get("day", "unknown"))
    label = str(day_result.get("task_label") or day)
    safe_day = label.replace("/", "-").replace(":", "-")
    daily_df = pd.DataFrame(day_result.get("daily_rows", []))
    campaign_df = pd.DataFrame(day_result.get("campaign_rows", []))
    adaptive_df = pd.DataFrame(day_result.get("adaptive_hit_rows", []))
    repair_df = pd.DataFrame(day_result.get("campaign_repair_rows", []))
    response_df = pd.DataFrame(day_result.get("post_fill_quote_response_rows", []))
    quote_trace_df = pd.DataFrame(day_result.get("quote_trace_rows", []))
    fill_trace_df = pd.DataFrame(day_result.get("fill_trace_rows", []))
    decision_trace_df = pd.DataFrame(day_result.get("decision_trace_rows", []))
    rollup_df = _rollup(daily_df)
    out_dir.mkdir(parents=True, exist_ok=True)
    daily_df.to_csv(out_dir / f"{stem}.partial.{safe_day}.daily.csv", index=False)
    rollup_df.to_csv(out_dir / f"{stem}.partial.{safe_day}.rollup.csv", index=False)
    campaign_df.to_csv(out_dir / f"{stem}.partial.{safe_day}.campaign_labels.csv", index=False)
    adaptive_df.to_csv(out_dir / f"{stem}.partial.{safe_day}.adaptive_add_hits.csv", index=False)
    repair_df.to_csv(
        out_dir / f"{stem}.partial.{safe_day}.campaign_repair_sequence.csv", index=False
    )
    response_df.to_csv(
        out_dir / f"{stem}.partial.{safe_day}.post_fill_quote_response.csv",
        index=False,
    )
    if not quote_trace_df.empty:
        quote_trace_df.to_csv(
            out_dir / f"{stem}.partial.{safe_day}.quote_trace.csv",
            index=False,
        )
    if not fill_trace_df.empty:
        fill_trace_df.to_csv(
            out_dir / f"{stem}.partial.{safe_day}.fill_trace.csv",
            index=False,
        )
    if not decision_trace_df.empty:
        decision_trace_df.to_csv(
            out_dir / f"{stem}.partial.{safe_day}.decision_trace.csv",
            index=False,
        )


def _run_day_campaign_audit(
    *,
    day: str,
    symbol: str,
    base: dict[str, Any],
    arms: list[smoke.SmokeArm],
    engine: str,
    day_initial: dict[str, float],
    day_live_state: dict[str, Any] | None,
    use_initial_state: bool,
    campaign_repair_model_payload: dict[str, Any] | None = None,
    historical_global_flow_root: str = "",
    market_data_latency_profile_payload: dict[str, Any] | None = None,
    market_data_latency_mode: str = "exchange_zero",
    market_data_latency_seed: int = 7,
    market_data_latency_market_id: str = "binance:perp:BTCUSDT",
    task_label: str = "",
    task_order: int = 0,
    save_quote_trace: bool = False,
    save_fill_trace: bool = False,
    save_decision_trace: bool = False,
    native_exchange_book_root: str = "",
    native_exchange_book_mode: str = "strict",
    native_exchange_book_warmup_hours: int = 24,
    runtime_compute_calibration: dict[str, Any] | None = None,
    runtime_compute_clock: str | None = None,
    funding_events: list[dict[str, Any]] | None = None,
    continuous_days: list[str] | None = None,
) -> dict[str, Any]:
    """Run all requested arms for one UTC day.

    中文说明：campaign outcome audit 的慢点在 Python tick replay。按 day
    并行能让每个 worker 只加载一次窗口，然后顺序跑该日所有 arms；这比按
    arm 并行更少重复读 cache/parquet，也更容易保持 daily fresh-start 语义。
    """
    source_days = continuous_days or [day]
    if continuous_days:
        if day != continuous_days[0] or historical_global_flow_root:
            raise ValueError("continuous segment needs its first day and no unmerged flow input")
        base = dict(base)
        base["replay_event_clock_start_ts_ms"] = int(_day_start_ts(day) * 1000)
        base["replay_event_clock_end_ts_ms"] = int(
            (_day_start_ts(source_days[-1]) + 86400) * 1000 - 1
        )
    if runtime_compute_clock is not None:
        if runtime_compute_calibration is None:
            raise ValueError("runtime compute requires the already-loaded timing calibration")
        base = dict(base)
        base["runtime_compute_clock"] = runtime_compute_clock
        start_ms = int(_day_start_ts(day) * 1000)
        base.setdefault("replay_event_clock_start_ts_ms", start_ms)
        base.setdefault("replay_event_clock_end_ts_ms", start_ms + 86_400_000 - 1)
    # 中文说明：worker 进程是全新 Python 解释器时，裸 configure_symbol(symbol)
    # 会把 MODEL_DIR 退回 symbol 默认目录（例如 models/saved_btcusdc）。
    # parent 里 load_tick_base_params() 可能已经按 live/config.yaml 选择了
    # resolved_model_dir（当前 live baseline 是 taker_tempo bundle）。这里必须
    # 显式恢复同一个 model_dir，否则并行 replay 会悄悄换模型。
    model_dir_override = base.get("resolved_model_dir") or base.get("model_dir")
    bt.configure_symbol(symbol, model_dir_override=model_dir_override)
    display_label = f"{day} {task_label}" if task_label else day
    logs: list[str] = [f"\nLoading {display_label} ..."]
    window_cache: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    compute_cache: dict[int, dict[str, Any]] = {}
    message_cache: dict[int, dict[str, Any]] = {}

    def _window_for_params(params: dict[str, Any]) -> dict[str, Any]:
        """Load/reuse the day window for the arm's model bundle.

        中文说明：大部分参数 arm 不影响数据窗口，可以共享同一个 trades/BBO/L2
        window；但 `model_dir` arm 会改变 ML predictions，必须用自己的 cache key
        重新加载。否则 model_dir sweep 会实际复用 baseline predictions。
        """
        raw_model = str(params.get("resolved_model_dir") or params.get("model_dir") or "")
        execution_trade_source = str(
            params.get("execution_trade_source", "aggTrades") or "aggTrades"
        ).strip()
        market_context_warmup_days = max(
            0,
            int(params.get("market_context_warmup_days", 1) or 0),
        )
        key = (
            raw_model,
            str(params.get("cross_market_enabled", True)),
            execution_trade_source,
            market_context_warmup_days,
        )
        if key not in window_cache:
            windows = [smoke._load_window(source_day, params) for source_day in source_days]
            window_cache[key] = (
                data_windows.concatenate_tick_windows(source_days, windows)
                if continuous_days else windows[0]
            )
        return window_cache[key]

    window = _window_for_params(base)
    execution_message_profile = bool(base.get("exec_message_delivery_profile_path"))
    parent_trades, parent_source_identity = None, []
    if execution_message_profile:
        if market_data_latency_profile_payload is None or market_data_latency_mode != "profile_empirical":
            raise ValueError("execution message delivery requires the paired empirical profile")
        parent_parts, parent_source_identity = [], []
        for source_day in source_days:
            frame, identities = data_windows.load_replay_aggregate_parents(source_day, base)
            parent_parts.append(frame)
            parent_source_identity.extend(identities)
        parent_trades = parent_parts[0]
        if len(parent_parts) > 1:
            parent_trades = pd.concat(parent_parts, ignore_index=True).drop_duplicates(
                "agg_trade_id"
            )
            parent_source_identity = list(
                {row["path"]: row for row in parent_source_identity}.values()
            )
    campaign_rows_all: list[dict[str, Any]] = []
    label_rows_all: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    adaptive_hit_rows: list[dict[str, Any]] = []
    campaign_repair_rows: list[dict[str, Any]] = []
    post_fill_quote_response_rows: list[dict[str, Any]] = []
    quote_trace_rows: list[dict[str, Any]] = []
    fill_trace_rows: list[dict[str, Any]] = []
    funding_trace_rows: list[dict[str, Any]] = []
    decision_trace_rows: list[dict[str, Any]] = []
    campaign_repair_model = (
        CampaignRepairModel.from_dict(campaign_repair_model_payload)
        if campaign_repair_model_payload
        else None
    )
    historical_global_flow_data = (
        load_causal_1s_global_flow(day, historical_global_flow_root)
        if historical_global_flow_root
        else None
    )
    if historical_global_flow_data is not None:
        historical_global_flow_data = _delay_historical_global_flow(
            historical_global_flow_data,
            profile_payload=market_data_latency_profile_payload,
            mode=market_data_latency_mode,
            seed=market_data_latency_seed,
            day=day,
            market_id=market_data_latency_market_id,
        )

    native_exchange_book_tape = None
    native_exchange_book_identity: dict[str, Any] = {}
    if native_exchange_book_root:
        native_exchange_book_tape = CryptoHFTExchangeBookTape(
            raw_root=Path(native_exchange_book_root),
            day=day,
            symbol=symbol,
            tick_size=float(base.get("tick_size", 0.1) or 0.1),
            warmup_hours=max(0, int(native_exchange_book_warmup_hours)),
            continuation_hours=24 * (len(source_days) - 1),
            strict_complete=native_exchange_book_mode == "strict",
        )
        native_exchange_book_identity = native_exchange_book_tape.identity(
            include_sha256=bool(
                str(base.get("replay_purpose", "")).lower() == "formal"
                and native_exchange_book_mode == "strict"
            )
        )

    for idx, arm in enumerate(arms, 1):
        params = dict(base)
        params.update(arm.overrides)
        arm_queue_path = arm.overrides.get("queue_calibration_path")
        if arm_queue_path:
            add_queue_calibration_params(
                params,
                symbol=symbol,
                strict=bool(params.get("strict_calibration", False)),
                path=arm_queue_path,
            )
            if bool(params.get("strict_calibration", False)):
                inherited_contract = base.get("replay_contract") or {}
                inherited_initial = inherited_contract.get("initial_state") or {}
                initial_artifact = str(inherited_initial.get("artifact_path", "") or "")
                freeze_replay_contract(
                    params,
                    purpose=str(params.get("replay_purpose", "formal")),
                    initial_state_mode=str(params.get("replay_initial_state_mode", "fresh_start")),
                    initial_state_artifact=initial_artifact or None,
                    root=ROOT,
                )
        if bool(params.get("strict_calibration", False)):
            calibration_params = dict(params)
            evidence_clock = str(params.get("replay_event_clock", "trade") or "trade").lower()
            if (
                str(params.get("replay_purpose", "formal")) == "live_alignment"
                and evidence_clock == "empirical"
            ):
                calibration_params["replay_event_clock"] = "merged"
            validate_formal_replay_calibration(
                calibration_params,
                require_latency=True,
            )
            validate_frozen_replay_contract(params)
            params["strict_calibration_identity_validated"] = True
            params["strict_calibration_validated"] = True
        if native_exchange_book_tape is not None:
            if engine != "python":
                raise ValueError("native exchange-book replay is currently Python-only")
            if bool(params.get("queue_l2_cancel_ahead_enabled", False)):
                raise ValueError(
                    "native exchange-book replay cannot be combined with "
                    "sampled top-N queue_l2_cancel_ahead"
                )
            params["exchange_book_queue_mode"] = native_exchange_book_mode
        if use_initial_state:
            params["initial_inventory"] = float(day_initial.get("initial_inventory", 0.0) or 0.0)
            params["initial_entry_price"] = float(
                day_initial.get("initial_entry_price", 0.0) or 0.0
            )
        if day_live_state:
            params["initial_live_state"] = dict(day_live_state)
        if (
            bool(params.get("strict_calibration", False))
            and str(params.get("replay_purpose", "formal")) == "formal"
        ):
            validate_frozen_replay_contract(params)
        started = time.perf_counter()
        logs.append(f"  [{idx:02d}/{len(arms):02d}] {day} {arm.name} ...")
        window = _window_for_params(params)
        if execution_message_profile:
            if id(window) not in message_cache:
                message_cache[id(window)] = data_windows.execution_message_delivery_params(
                    window, symbol=symbol, profile=market_data_latency_profile_payload,
                    seed=market_data_latency_seed, parent_trades=parent_trades,
                    parent_source_identity=parent_source_identity,
                    unmatched_child_mode="matching_only",
                )
            params.update(message_cache[id(window)])
        cooldown_policy = build_configured_cooldown_policy_adapter(window=window, params=params)
        if cooldown_policy is not None:
            params["cooldown_duration_policy_evaluator"] = cooldown_policy
            params["cooldown_v2_snapshot_emitter"] = cooldown_policy
        if runtime_compute_clock is not None:
            if id(window) not in compute_cache:
                compute_cache[id(window)] = _runtime_compute_for_window(
                    window, params, runtime_compute_calibration,
                    clock=runtime_compute_clock,
                    start_ms=int(params["replay_event_clock_start_ts_ms"]),
                )
            params.update(compute_cache[id(window)])
            params["replay_evidence_scope"] = "runtime_gateway_diagnostic"
        if execution_message_profile and bool(params.get("strict_calibration", False)):
            initial = (base.get("replay_contract") or {}).get("initial_state") or {}
            freeze_replay_contract(
                params, purpose="diagnostic",
                initial_state_mode=str(params.get("replay_initial_state_mode", "fresh_start")),
                initial_state_artifact=initial.get("artifact_path") or None, root=ROOT,
            )
            validate_frozen_replay_contract(params)
        arm_multi_market = bool(params.get("multi_market_policy_enabled", False))
        arm_response_mode = str(
            params.get("post_fill_quote_response_mode", "noop") or "noop"
        ).lower()
        arm_needs_repair = arm_multi_market or (
            bool(params.get("post_fill_quote_response_enabled", False))
            and arm_response_mode in {"flow_add_widen", "hybrid"}
        )
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
            reference_event_tapes=(
                window.get("reference_event_tapes") if arm_multi_market else None
            ),
            campaign_repair_data=(window.get("campaign_repair_data") if arm_needs_repair else None),
            campaign_repair_model=(campaign_repair_model if arm_needs_repair else None),
            historical_global_flow_data=(historical_global_flow_data if arm_multi_market else None),
            exchange_book_event_tape=native_exchange_book_tape,
        )
        result["strict_calibration_validated"] = bool(
            params.get("strict_calibration_validated", False)
        )
        result["strict_calibration_identity_validated"] = bool(
            params.get("strict_calibration_identity_validated", False)
        )
        result["replay_evidence_scope"] = str(params.get("replay_evidence_scope", "") or "")
        if runtime_compute_clock is not None:
            result["runtime_compute_initial_bucket_end_ms"] = params[
                "runtime_compute_initial_bucket_end_ms"
            ]
        runtime_s = time.perf_counter() - started
        for hit in result.get("_adaptive_add_cooldown_trace", []) or []:
            adaptive_hit_rows.append({"day": day, "arm": arm.name, "group": arm.group, **hit})
        for score_row in result.get("_campaign_repair_trace", []) or []:
            campaign_repair_rows.append(
                {"day": day, "arm": arm.name, "group": arm.group, **score_row}
            )
        for response_row in result.get("_post_fill_quote_response_trace", []) or []:
            post_fill_quote_response_rows.append(
                {
                    "day": day,
                    "arm": arm.name,
                    "group": arm.group,
                    **response_row,
                }
            )
        fill_trace = result.get("_fill_trace", [])
        if save_quote_trace:
            quote_trace_rows.extend(
                {"day": day, "arm": arm.name, "group": arm.group, **row}
                for row in result.get("_quote_trace", [])
            )
        if save_fill_trace:
            fill_trace_rows.extend(
                {"day": day, "arm": arm.name, "group": arm.group, **row} for row in fill_trace
            )
        if save_decision_trace:
            decision_trace_rows.extend(
                {"day": day, "arm": arm.name, "group": arm.group, **row}
                for row in result.get("_decision_trace", [])
            )
        initial_inventory = float(params.get("initial_inventory", 0.0) or 0.0)
        initial_entry_price = float(params.get("initial_entry_price", 0.0) or 0.0)
        arm_funding_trace: list[dict[str, Any]] = []
        trades = _fills_to_trade_rows(
            fill_trace,
            initial_inventory=initial_inventory,
            initial_entry_price=initial_entry_price,
            day_start_ts=_day_start_ts(day),
            funding_events=funding_events,
            funding_trace=arm_funding_trace,
            **({
                "terminal_ts": float(params.get(
                    "replay_event_clock_end_ts_ms", (_day_start_ts(day) + 86400) * 1000,
                )) / 1000.0,
                "terminal_mark_price": float(result["terminal_mark_price"]),
            } if result.get("economic_pnl_complete") is not False
                and float(result.get("terminal_mark_price", 0.0) or 0.0) > 0.0 else {}),
        )
        campaigns = build_campaigns(trades)
        c_rows = []
        for row in campaign_label_rows(campaigns):
            out = {"day": day, "arm": arm.name, "group": arm.group, **row}
            c_rows.append(out)
            label_rows_all.append(out)
        campaign_rows_all.extend(c_rows)
        daily = _campaign_daily_row(
            day=day,
            arm=arm,
            result=result,
            label_rows=c_rows,
            runtime_s=runtime_s,
            fill_split=_fill_split(fill_trace, initial_inventory=initial_inventory),
            initial_inventory=initial_inventory,
            initial_entry_price=initial_entry_price,
        )
        funding_value = sum(row["funding_cashflow_usdc"] for row in arm_funding_trace)
        daily.update({
            "accounting_window": (
                "continuous_segment" if continuous_days else "daily_fresh_start"
            ),
            "window_end_day": source_days[-1],
            "window_day_count": len(source_days),
            "funding_mode": "frozen_settlement_tape" if funding_events is not None else "unmodeled",
            "funding_cashflow_usdc": funding_value,
            "replay_net_pnl": daily["replay_pnl"] + funding_value,
            "funding_risk_feedback": "not_applied_current_live_uses_trading_pnl",
            "funding_same_ms_fill_count": sum(row["same_ms_fill_count"] for row in arm_funding_trace),
        })
        funding_trace_rows.extend(
            {"day": day, "arm": arm.name, **row} for row in arm_funding_trace
        )
        daily_rows.append(daily)
        logs.append(
            f"      campaigns={daily['campaigns']} tail={daily['loss_tail']} "
            f"bad_rate={daily['bad_rate']:.3f} repair={daily['repair_rate']:.3f} "
            f"terminal_sum={daily['terminal_pnl_sum']:.3f} "
            f"replay_inv_time={daily['replay_abs_inventory_time_s']:.1f}"
        )

    return {
        "day": day,
        "task_label": task_label,
        "task_order": task_order,
        "campaign_rows": campaign_rows_all,
        "label_rows": label_rows_all,
        "daily_rows": daily_rows,
        "adaptive_hit_rows": adaptive_hit_rows,
        "campaign_repair_rows": campaign_repair_rows,
        "post_fill_quote_response_rows": post_fill_quote_response_rows,
        "quote_trace_rows": quote_trace_rows,
        "fill_trace_rows": fill_trace_rows,
        "funding_trace_rows": funding_trace_rows,
        "decision_trace_rows": decision_trace_rows,
        "native_exchange_book_identity": native_exchange_book_identity,
        "logs": logs,
    }


def _arm_chunks(arms: list[smoke.SmokeArm], chunk_size: int) -> list[list[smoke.SmokeArm]]:
    """Split arms into small contiguous batches for faster smoke feedback.

    中文说明：旧路径是一个 worker 跑完某一天的全部 arms 才落盘。广泛参数
    搜索时，一个 day/49 arms 可能二三十分钟没有任何 partial。这里把同一天
    的 arms 拆成小批次，最终 rollup 等价，但 smoke 能更早看到结果，也能在
    day 数少于 CPU worker 数时利用更多并行度。
    """
    if chunk_size <= 0 or chunk_size >= len(arms):
        return [arms]
    return [arms[i : i + chunk_size] for i in range(0, len(arms), chunk_size)]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=(
            "Optional live config path. Use this for private/current live baselines; "
            "otherwise the public template live/config.yaml is used."
        ),
    )
    parser.add_argument("--days", nargs="+", required=True)
    parser.add_argument(
        "--continuous", action="store_true",
        help="Run contiguous --days in one state machine, without midnight strategy resets",
    )
    parser.add_argument(
        "--funding-history", type=Path,
        help="Frozen Binance fundingRate JSON; book settlement on each arm's physical inventory",
    )
    parser.add_argument("--arms", nargs="+", default=[])
    parser.add_argument(
        "--arm-spec-json",
        type=Path,
        default=None,
        help="Optional generated arm spec JSON. Used by parameter_racing_sweep.py.",
    )
    parser.add_argument(
        "--fill-cooldown-grid",
        nargs="+",
        type=float,
        default=[],
        help="Generate temporary fixed add-side fill_cooldown arms in seconds for campaign-outcome scans.",
    )
    parser.add_argument(
        "--causal-post-fill-stop-add-arm",
        action="store_true",
        help="Add the predeclared M1 causal post-fill stop-add arm and baseline.",
    )
    parser.add_argument(
        "--post-fill-quote-response-arms",
        action="store_true",
        help=(
            "Add QN/Q1/Q2/Q3: no hard cooldown control, inventory I, "
            "flow-persistence A, and hybrid I+A. Python replay only."
        ),
    )
    parser.add_argument(
        "--post-fill-q1-inventory-arm",
        action="store_true",
        help="Add only baseline plus Q1 (inventory center shift, hard add cooldown off).",
    )
    parser.add_argument(
        "--post-fill-q4-backstop-arm",
        action="store_true",
        help="Add only baseline plus Q4 (I+A with current cooldown backstop).",
    )
    parser.add_argument(
        "--post-fill-fitted-a-arm",
        action="store_true",
        help="Add baseline, no-cooldown control, and fitted transient-A arm.",
    )
    parser.add_argument("--random-passive-trials", type=int, default=0)
    parser.add_argument("--random-passive-seed", type=int, default=20260710)
    parser.add_argument("--random-passive-side-mirror-prob", type=float, default=0.5)
    parser.add_argument("--random-passive-timing-jitter-fraction", type=float, default=0.35)
    parser.add_argument(
        "--integrity-diagnostic-arms",
        action="store_true",
        help="Add canonical markout-sign and spread-cap-action A/B arms plus baseline.",
    )
    parser.add_argument("--tag", default="campaign_outcome_replay_audit")
    parser.add_argument("--trace-quotes-max", type=int, default=100_000)
    parser.add_argument(
        "--save-quote-trace",
        action="store_true",
        help=(
            "Persist replay placed-order lifecycle and quote-core diagnostics for "
            "live/replay calibration. Disabled by default because the trace is large."
        ),
    )
    parser.add_argument("--trace-fills-max", type=int, default=20_000)
    parser.add_argument(
        "--save-fill-trace",
        action="store_true",
        help=(
            "Persist replay fill-level traces for live/replay calibration. "
            "Disabled by default because broad sweeps can produce large files."
        ),
    )
    parser.add_argument("--trace-decisions-max", type=int, default=100_000)
    parser.add_argument(
        "--save-decision-trace",
        action="store_true",
        help=(
            "Persist every replay side decision with inventory role and guard "
            "diagnostics for live/replay path calibration."
        ),
    )
    parser.add_argument("--trace-campaign-repair-max", type=int, default=100_000)
    parser.add_argument(
        "--trace-post-fill-quote-response-max",
        type=int,
        default=100_000,
    )
    parser.add_argument(
        "--campaign-repair-model-bundle",
        type=Path,
        default=None,
        help="Causal campaign_repair_model_bundle.v1 produced by campaign_repair_probability.py.",
    )
    parser.add_argument(
        "--campaign-repair-model-panel",
        choices=("chronological", "blocked71", "late4"),
        default="chronological",
    )
    parser.add_argument(
        "--historical-global-flow-root",
        type=Path,
        default=None,
        help=(
            "external_venues root containing causal three-venue spot/perp 1s states. "
            "Missing requested days fail closed."
        ),
    )
    parser.add_argument(
        "--market-data-latency-profile",
        type=Path,
        default=None,
        help=(
            "Environment-labeled market-data profile. With --runtime-timing-samples, "
            "profile_empirical assigns observed paired clocks to execution book/depth/"
            "aggTrade inputs and delays frozen feature publication. Otherwise this "
            "only delays historical external/global-flow visibility."
        ),
    )
    parser.add_argument(
        "--market-data-latency-mode",
        choices=HISTORICAL_MARKET_DATA_LATENCY_MODES,
        default="exchange_zero",
        help=(
            "Visibility model; measured-runtime execution inputs require profile_empirical "
            "with complete same-message clock pairs, not marginal CDFs or snapshot ages."
        ),
    )
    parser.add_argument("--market-data-latency-seed", type=int, default=7)
    parser.add_argument(
        "--market-data-latency-market-id",
        default="binance:perp:BTCUSDT",
        help=(
            "Profile group representing full-state readiness. The current "
            "hierarchical reference is bridge-limited, so BTCUSDT perp book is "
            "the default rather than the faster external 2-of-3 lower bound."
        ),
    )
    parser.add_argument("--window-cache-dir", default=None)
    parser.add_argument("--refresh-window-cache", action="store_true")
    parser.add_argument(
        "--execution-trade-source",
        choices=("aggTrades", "trades"),
        default="trades",
        help=(
            "Execution event tape. Formal evidence defaults to Binance USD-M "
            "individual trades; aggTrades remains available for diagnostics."
        ),
    )
    parser.add_argument(
        "--individual-trades-manifest-path",
        type=Path,
        default=None,
        help=(
            "Frozen manifest binding formal individual-trades replay to raw "
            "file hashes, including the repaired 2026-07-04..11 side flags."
        ),
    )
    parser.add_argument(
        "--individual-trades-integrity-report-path",
        type=Path,
        default=None,
        help="Frozen per-day individual-trades integrity report.",
    )
    parser.add_argument(
        "--individual-trades-manifest-sha256",
        default="",
        help="Optional preregistered SHA256 for the individual-trades manifest.",
    )
    parser.add_argument(
        "--individual-trades-integrity-report-sha256",
        default="",
        help="Optional preregistered SHA256 for the integrity report.",
    )
    parser.add_argument(
        "--market-context-warmup-days",
        type=int,
        default=1,
        help="Causal BBO/L2/bar context loaded before each target UTC day.",
    )
    parser.add_argument(
        "--verify-formal-l2-hashes",
        action="store_true",
        help="Rehash every normalized formal BBO/L2 input before replay.",
    )
    parser.add_argument(
        "--queue-calibration-path",
        type=Path,
        default=None,
        help="Explicit queue-v3 artifact used by strict replay.",
    )
    parser.add_argument("--no-queue-regime-calibration", action="store_true")
    parser.add_argument(
        "--strict-calibration",
        action="store_true",
        help=(
            "Fail fast unless the explicit private config, P3/effective-kappa, "
            "daily queue calibration, historical book requirement, and order "
            "latency calibration are all present."
        ),
    )
    parser.add_argument(
        "--replay-purpose",
        choices=("formal", "live_alignment", "diagnostic"),
        default="formal",
        help=(
            "formal produces strategy evidence under a frozen replay contract; "
            "live_alignment is diagnostic-only for units, clocks, state machines, "
            "and gate ordering; diagnostic is model/sensitivity analysis without "
            "promotion authority."
        ),
    )
    parser.add_argument(
        "--initial-state-mode",
        choices=("fresh_start", "frozen_standard"),
        default="fresh_start",
        help="Formal replay starts flat unless a frozen standard state artifact is named.",
    )
    parser.add_argument(
        "--standard-initial-state-json",
        type=Path,
        default=None,
        help=(
            "Versioned narrowgate_standard_initial_state.v1 artifact. It may contain "
            "inventory and entry price only, never live active orders."
        ),
    )
    parser.add_argument("--rng-seed", type=int, default=42)
    parser.add_argument("--latency-seed", type=int, default=59)
    parser.add_argument(
        "--replay-locator-projection",
        type=Path,
        default=None,
        help=(
            "Diagnostic-only local model/policy path projection bound to the original "
            "--config bytes. Does not change policy values or grant live authority."
        ),
    )
    parser.add_argument(
        "--runtime-timing-samples",
        type=Path,
        default=None,
        help=(
            "Anonymous per-request HTTP/private-ACK timing JSON for a Python diagnostic "
            "with an explicit REST async GLOBAL FIFO config. Compute is opt-in; "
            "snapshot ages are never treated as message delays."
        ),
    )
    parser.add_argument(
        "--runtime-compute-clock",
        choices=("prediction_delivery", "source_time_assumption"),
        default=None,
        help=(
            "Inject paired local compute phases from --runtime-timing-samples. "
            "The first option requires per-message prediction delivery; the second "
            "explicitly assumes source-time availability. Initial signal state comes "
            "from the loaded window's completed prediction pre-roll."
        ),
    )
    parser.add_argument(
        "--runtime-private-fill-model",
        choices=("unmodeled", "observed_callback"),
        default="unmodeled",
        help="Optionally simulate measured exchange-fill to local private callback visibility.",
    )
    parser.add_argument(
        "--runtime-effective-time-assumption",
        choices=("dispatch", "exchange_event_proxy", "observable_upper_bound"),
        default=None,
        help="Required with --runtime-timing-samples; exchange-effective time is not observed.",
    )
    parser.add_argument(
        "--runtime-bulk-cancel-model",
        choices=("unmodeled", "matched_risk_case"),
        default="unmodeled",
        help=(
            "Optional single matched non-shutdown risk case (n=1), not a stable distribution. "
            "matched_risk_case reuses shared effective/private/HTTP phases for all batch "
            "targets as an explicit modeling assumption. Default leaves bulk cancel unmodeled."
        ),
    )
    parser.add_argument(
        "--latency-profile-id",
        default="provider_neutral_latency_profile_v1",
        help="Environment/version identity for the frozen REST/book latency distribution.",
    )
    parser.add_argument(
        "--latency-environment",
        default=DEFAULT_LATENCY_ENVIRONMENT,
        help="Host/region/instance/OS label carried into every replay artifact.",
    )
    parser.add_argument(
        "--latency-scenario",
        choices=("baseline", "stress"),
        default="baseline",
        help="Rare synthetic stalls are enabled only by the stress scenario.",
    )
    parser.add_argument(
        "--latency-baseline-clip-quantile",
        type=float,
        default=1.0,
        help=(
            "1.0 preserves all observed latency samples; smaller values explicitly "
            "select trimmed sensitivity or historical reproduction."
        ),
    )
    parser.add_argument("--latency-stress-spike-probability", type=float, default=0.001)
    parser.add_argument("--latency-stress-spike-multiplier", type=float, default=5.0)
    parser.add_argument(
        "--live-like-replay-baseline",
        action="store_true",
        help="Use the current live-aligned replay diagnostics: persistent same-side fill_cd count and calibrated side/regime queue multipliers.",
    )
    parser.add_argument(
        "--engine",
        choices=["python", "cpp"],
        default="python",
        help=(
            "Replay engine. Python remains authoritative; cpp is for parity-passing "
            "fast screening and is guarded by a baseline parity check by default."
        ),
    )
    parser.add_argument(
        "--no-cpp-baseline-parity-gate",
        action="store_true",
        help="Disable the Python-vs-C++ baseline gate before engine=cpp runs.",
    )
    parser.add_argument(
        "--cpp-parity-days",
        nargs="+",
        default=[],
        help=(
            "Requested replay days to include in the Python/C++ baseline parity "
            "matrix. Every value must also appear in --days; use 'all' for the "
            "full panel. The legacy default remains the first requested day."
        ),
    )
    parser.add_argument("--cpp-parity-max-fill-diff-rate", type=float, default=0.05)
    parser.add_argument(
        "--cpp-parity-max-pnl-diff",
        type=float,
        default=5.0,
        help=(
            "Max absolute daily PnL difference allowed by the C++ baseline gate. "
            "Fill count is the primary smoke gate; this catches large path drift "
            "without rejecting normal few-dollar daily replay noise."
        ),
    )
    parser.add_argument(
        "--live-perf-telemetry",
        type=Path,
        default=None,
        help="Optional logs/live_perf_telemetry.csv used to replay empirical REST new/cancel latency samples.",
    )
    parser.add_argument(
        "--live-perf-latency-mode",
        choices=["avg", "max", "sum"],
        default="avg",
        help="How to map live REST telemetry rows into replay latency samples.",
    )
    parser.add_argument(
        "--exec-book-visibility-profile",
        type=Path,
        default=None,
        help=(
            "Optional quote-decision/live-telemetry CSV containing receive-time "
            "depth_age_s or exec_book_age_s."
        ),
    )
    parser.add_argument(
        "--exec-book-visibility-profile-id",
        default="provider_neutral_visibility_profile_v1",
        help="Environment/version label stored with the replay artifacts.",
    )
    parser.add_argument(
        "--exec-book-visibility-mode",
        choices=("sampled", "paired"),
        default="sampled",
        help=(
            "sampled draws deterministic ages from the environment profile; "
            "paired reuses each logged requote's book/depth age and observed mid "
            "for same-day live alignment only."
        ),
    )
    parser.add_argument(
        "--exec-book-visibility-delay-seed",
        type=int,
        default=20260718,
        help="Deterministic receive-time visibility sampling seed.",
    )
    parser.add_argument(
        "--exec-depth-visibility-source-offset-ms",
        type=int,
        default=0,
        help=(
            "Explicit exchange-time boundary correction for reconstructed L2. "
            "Keep zero for formal retained replay; nonzero values require a "
            "named environment/live-parity calibration identity."
        ),
    )
    parser.add_argument(
        "--initial-state-trades-csv",
        type=Path,
        default=None,
        help=(
            "Optional live logs/trades.csv used to seed each UTC day with live day-start "
            "inventory for live/baseline calibration. Do not use for retained fresh-start evidence."
        ),
    )
    parser.add_argument(
        "--initial-live-state-json",
        type=Path,
        default=None,
        help=(
            "Optional day-keyed calibration artifact restoring active orders, "
            "markout EMA, fill cooldown/consecutive counts, and campaign-so-far state. "
            "Python replay only; never use for fresh-start promotion evidence."
        ),
    )
    parser.add_argument(
        "--native-exchange-book-root",
        type=Path,
        default=None,
        help=(
            "Optional CryptoHFT raw root. When set, replay seeds and updates "
            "queue state from native snapshot/delta events instead of sampled "
            "top-N cancel-ahead inference. Python replay only."
        ),
    )
    parser.add_argument(
        "--native-exchange-book-mode",
        choices=("diagnostic", "strict"),
        default="strict",
    )
    parser.add_argument(
        "--native-exchange-book-warmup-hours",
        type=int,
        default=24,
    )
    parser.add_argument(
        "--sync-adjust-replay-mode",
        choices=("disabled", "frozen_tape", "censor", "stress"),
        default="disabled",
        help=(
            "Replay the exogenous sync-degrade control from a frozen tape, "
            "censor at its first event, or run a deterministic stress path."
        ),
    )
    parser.add_argument(
        "--sync-adjust-event-tape",
        type=Path,
        default=None,
        help="Environment-labeled narrowgate_sync_degrade_event_tape.v1 JSON.",
    )
    parser.add_argument("--sync-adjust-event-tape-sha256", default="")
    parser.add_argument(
        "--sync-adjust-event-environment",
        default=DEFAULT_LATENCY_ENVIRONMENT,
    )
    parser.add_argument("--sync-adjust-stress-seed", type=int, default=20260729)
    parser.add_argument(
        "--sync-adjust-stress-interval-s",
        type=float,
        default=21_600.0,
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallelize by UTC day. Default keeps serial behavior for easier debugging.",
    )
    parser.add_argument(
        "--arm-chunk-size",
        type=int,
        default=0,
        help=(
            "Optional smoke-speed mode: split each day into arm chunks of this size. "
            "Final rollup is unchanged, but partial chunk outputs arrive earlier and "
            "more workers can be used when the day count is small."
        ),
    )
    args = parser.parse_args(argv)
    if args.continuous:
        if args.workers != 1 or args.engine != "python" or args.arm_chunk_size:
            raise SystemExit(
                "--continuous currently requires --workers 1 --engine python, no chunks"
            )
        if args.initial_state_trades_csv or args.initial_live_state_json:
            raise SystemExit("--continuous does not accept per-day initial state artifacts")
    if args.replay_locator_projection is not None and (
        args.replay_purpose != "diagnostic" or args.config is None
    ):
        raise SystemExit("--replay-locator-projection requires diagnostic and explicit --config")
    if args.runtime_timing_samples is not None:
        if args.replay_purpose != "diagnostic" or args.engine != "python":
            raise SystemExit(
                "--runtime-timing-samples requires --replay-purpose diagnostic --engine python"
            )
        if args.config is None or args.runtime_effective_time_assumption is None:
            raise SystemExit(
                "--runtime-timing-samples requires --config and "
                "--runtime-effective-time-assumption"
            )
        if args.live_perf_telemetry is not None or args.exec_book_visibility_profile is not None:
            raise SystemExit(
                "--runtime-timing-samples cannot be combined with --live-perf-telemetry "
                "or --exec-book-visibility-profile; row means and snapshot ages are not "
                "per-request/per-message latency samples"
            )
        if args.latency_scenario != "baseline" or args.latency_baseline_clip_quantile != 1.0:
            raise SystemExit(
                "--runtime-timing-samples uses unchanged empirical rows without clipping"
            )
        if args.market_data_latency_profile is not None:
            if args.market_data_latency_mode != "profile_empirical":
                raise SystemExit(
                    "runtime execution message clocks require "
                    "--market-data-latency-mode profile_empirical"
                )
            if args.runtime_compute_clock == "source_time_assumption":
                raise SystemExit("execution message clocks require --runtime-compute-clock prediction_delivery")
            if args.execution_trade_source != "trades":
                raise SystemExit("execution message clocks require individual --execution-trade-source trades")
        elif args.market_data_latency_mode != "exchange_zero":
            raise SystemExit("runtime market-data latency mode requires --market-data-latency-profile")
    elif args.runtime_effective_time_assumption is not None:
        raise SystemExit("--runtime-effective-time-assumption requires --runtime-timing-samples")
    elif args.runtime_bulk_cancel_model != "unmodeled":
        raise SystemExit("--runtime-bulk-cancel-model requires --runtime-timing-samples")
    elif args.runtime_private_fill_model != "unmodeled":
        raise SystemExit("--runtime-private-fill-model requires --runtime-timing-samples")
    elif args.runtime_compute_clock is not None:
        raise SystemExit("--runtime-compute-clock requires --runtime-timing-samples")
    if args.trace_fills_max <= 0:
        raise SystemExit(
            "campaign outcome audit requires --trace-fills-max > 0; "
            "fill traces are needed to reconstruct flat -> nonzero -> flat campaigns. "
            "Use a summary-only replay runner when campaign labels are not required."
        )
    if args.replay_purpose == "formal" and not args.strict_calibration:
        raise SystemExit(
            "--replay-purpose formal requires --strict-calibration so config, "
            "model, P3, queue, causal event, initial-state, and latency identities "
            "are frozen before any arm is evaluated"
        )
    if args.native_exchange_book_root is not None and args.engine != "python":
        raise SystemExit("--native-exchange-book-root currently requires --engine python")
    wall_started = time.perf_counter()

    bt.configure_symbol(args.symbol)
    days = _normalize_days(args.days)
    if args.continuous:
        stamps = [_day_start_ts(day) for day in days]
        if any(b - a != 86400 for a, b in zip(stamps, stamps[1:], strict=False)):
            raise SystemExit(
                "--continuous requires contiguous UTC days; do not bridge missing days"
            )
    funding_history = (
        _load_funding_history(args.funding_history.expanduser(), symbol=args.symbol)
        if args.funding_history is not None else None
    )
    funding_by_day = {
        day: [row for row in funding_history
              if _day_start_ts(day) * 1000 <= int(row["fundingTime"])
              < (_day_start_ts(day) + 86400) * 1000]
        for day in days
    } if funding_history is not None else {}
    arms_by_name = _arm_map()
    for arm in _load_arm_spec_json(args.arm_spec_json):
        arms_by_name[arm.name] = arm
    grid_arms = _fill_cooldown_grid_arms(args.fill_cooldown_grid)
    for arm in grid_arms:
        arms_by_name[arm.name] = arm
    causal_post_fill_arm = (
        _causal_post_fill_stop_add_arm() if args.causal_post_fill_stop_add_arm else None
    )
    if causal_post_fill_arm is not None:
        arms_by_name[causal_post_fill_arm.name] = causal_post_fill_arm
    post_fill_quote_response_arms = (
        _post_fill_quote_response_arms() if args.post_fill_quote_response_arms else []
    )
    for arm in post_fill_quote_response_arms:
        arms_by_name[arm.name] = arm
    post_fill_q1_arm = (
        _post_fill_quote_response_q1_arm() if args.post_fill_q1_inventory_arm else None
    )
    if post_fill_q1_arm is not None:
        arms_by_name[post_fill_q1_arm.name] = post_fill_q1_arm
    post_fill_q4_arm = (
        _post_fill_quote_response_q4_arm() if args.post_fill_q4_backstop_arm else None
    )
    if post_fill_q4_arm is not None:
        arms_by_name[post_fill_q4_arm.name] = post_fill_q4_arm
    post_fill_fitted_a_arms = _post_fill_fitted_a_arms() if args.post_fill_fitted_a_arm else []
    for arm in post_fill_fitted_a_arms:
        arms_by_name[arm.name] = arm
    random_arms = _random_passive_arms(
        args.random_passive_trials,
        seed=args.random_passive_seed,
        side_mirror_prob=min(1.0, max(0.0, args.random_passive_side_mirror_prob)),
        timing_jitter_fraction=min(0.95, max(0.0, args.random_passive_timing_jitter_fraction)),
    )
    for arm in random_arms:
        arms_by_name[arm.name] = arm
    integrity_arms = _integrity_diagnostic_arms() if args.integrity_diagnostic_arms else []
    for arm in integrity_arms:
        arms_by_name[arm.name] = arm
    unknown = [name for name in args.arms if name not in arms_by_name]
    if unknown:
        raise SystemExit(f"Unknown arm(s): {', '.join(unknown)}")
    requested = list(args.arms)
    if grid_arms:
        requested.extend(arm.name for arm in grid_arms)
    if causal_post_fill_arm is not None:
        if "baseline" not in requested:
            requested.insert(0, "baseline")
        requested.append(causal_post_fill_arm.name)
    if post_fill_quote_response_arms:
        if "baseline" not in requested:
            requested.insert(0, "baseline")
        requested.extend(arm.name for arm in post_fill_quote_response_arms)
    if post_fill_q1_arm is not None:
        if "baseline" not in requested:
            requested.insert(0, "baseline")
        requested.append(post_fill_q1_arm.name)
    if post_fill_q4_arm is not None:
        if "baseline" not in requested:
            requested.insert(0, "baseline")
        requested.append(post_fill_q4_arm.name)
    if post_fill_fitted_a_arms:
        if "baseline" not in requested:
            requested.insert(0, "baseline")
        requested.extend(arm.name for arm in post_fill_fitted_a_arms)
    if random_arms:
        if "baseline" not in requested:
            requested.insert(0, "baseline")
        requested.extend(arm.name for arm in random_arms)
    if integrity_arms:
        if "baseline" not in requested:
            requested.insert(0, "baseline")
        requested.extend(arm.name for arm in integrity_arms)
    requested = list(dict.fromkeys(requested))
    if not requested:
        raise SystemExit("Provide --arms and/or --fill-cooldown-grid")
    arms = [arms_by_name[name] for name in requested]
    if args.replay_locator_projection is not None:
        locator_fields = {name.split(".", 1)[1] for name in REPLAY_LOCATOR_FIELDS}
        locator_fields.update({"resolved_model_dir", "_replay_locator_projection"})
        for arm in arms:
            forbidden = set(arm.overrides) & locator_fields
            if forbidden:
                raise ValueError(
                    f"locator-bound arm {arm.name!r} changes model/policy locations: "
                    + ", ".join(sorted(forbidden))
                )
    if args.engine == "cpp" and any(
        bool(arm.overrides.get("post_fill_quote_response_enabled", False)) for arm in arms
    ):
        raise SystemExit(
            "post-fill quote-response arms are Python replay-only until C++ "
            "discrete-price and lifecycle parity is implemented"
        )

    if args.config is not None:
        # 中文说明：campaign audit 可能由 parameter_racing_sweep 启动。
        # 公开仓库里的 live/config.yaml 只是 demo template；正式 live baseline
        # 必须显式传入私有 config，否则模型目录/风控参数会悄悄错位。
        base = load_tick_base_params(
            symbol=args.symbol,
            config_path=args.config,
            locator_projection_path=args.replay_locator_projection,
            configure_symbol=bt.configure_symbol,
            require_historical_bbo=True,
            queue_calibration_path=args.queue_calibration_path,
            strict_calibration=bool(args.strict_calibration),
        )
    else:
        base = smoke._base_params(args.symbol)
    base["trace_quotes_max"] = max(1, int(args.trace_quotes_max)) if args.save_quote_trace else 0
    base["trace_decisions_max"] = (
        max(1, int(args.trace_decisions_max)) if args.save_decision_trace else 0
    )
    base["trace_queue_events_max"] = 0
    base["trace_fills_max"] = int(args.trace_fills_max)
    base["execution_trade_source"] = str(args.execution_trade_source)
    if args.individual_trades_manifest_path is not None:
        base["individual_trades_manifest_path"] = str(
            args.individual_trades_manifest_path.expanduser().resolve()
        )
    if args.individual_trades_integrity_report_path is not None:
        base["individual_trades_integrity_report_path"] = str(
            args.individual_trades_integrity_report_path.expanduser().resolve()
        )
    if args.individual_trades_manifest_sha256:
        base["individual_trades_manifest_sha256"] = str(
            args.individual_trades_manifest_sha256
        ).lower()
    if args.individual_trades_integrity_report_sha256:
        base["individual_trades_integrity_report_sha256"] = str(
            args.individual_trades_integrity_report_sha256
        ).lower()
    base["market_context_warmup_days"] = max(
        0,
        int(args.market_context_warmup_days),
    )
    base["require_formal_l2"] = bool(args.replay_purpose == "formal")
    base["verify_formal_l2_hashes"] = bool(args.verify_formal_l2_hashes)
    if base["require_formal_l2"]:
        formal_l2_root = bt.BBO_DIR.parent.resolve()
        base["formal_l2_dataset_root"] = str(formal_l2_root)
        base["formal_l2_manifest_path"] = str(formal_l2_root / "manifest.json")
    base["sync_adjust_replay_mode"] = str(args.sync_adjust_replay_mode)
    base["sync_adjust_event_tape_path"] = (
        str(args.sync_adjust_event_tape.expanduser().resolve())
        if args.sync_adjust_event_tape is not None
        else ""
    )
    base["sync_adjust_event_tape_sha256"] = str(
        args.sync_adjust_event_tape_sha256 or ""
    ).lower()
    base["sync_adjust_event_environment"] = str(
        args.sync_adjust_event_environment or ""
    )
    base["sync_adjust_stress_seed"] = int(args.sync_adjust_stress_seed)
    base["sync_adjust_stress_interval_s"] = float(
        args.sync_adjust_stress_interval_s
    )
    if args.native_exchange_book_root is not None:
        base["exchange_book_queue_mode"] = str(args.native_exchange_book_mode)
        base["native_exchange_book_root"] = str(
            args.native_exchange_book_root.expanduser().resolve()
        )
        base["native_exchange_book_warmup_hours"] = max(
            0,
            int(args.native_exchange_book_warmup_hours),
        )
    if bool(base.get("dynamic_fill_hazard_action_enabled", False)):
        if args.engine != "python":
            raise SystemExit(
                "enabled BUY q90 action requires --engine python"
            )
        if args.native_exchange_book_root is None:
            raise SystemExit(
                "enabled BUY q90 action requires --native-exchange-book-root"
            )
        if args.native_exchange_book_mode != "strict":
            raise SystemExit(
                "enabled BUY q90 action requires --native-exchange-book-mode strict"
            )
    base["rng_seed"] = int(args.rng_seed)
    base["latency_seed"] = int(args.latency_seed)
    base["trace_campaign_repair_max"] = int(args.trace_campaign_repair_max)
    base["trace_post_fill_quote_response_max"] = int(args.trace_post_fill_quote_response_max)
    base["queue_regime_calibration_enabled"] = not bool(args.no_queue_regime_calibration)
    if args.live_like_replay_baseline:
        # 中文说明：这些不是策略参数晋级，只是让 replay 的挂单生命周期和
        # fill-selection 更接近最近 live 观测，避免在机制失真上评估 arm。
        base["fill_cooldown_consecutive_reset_policy"] = "opposite_fill_only"
        base["fill_cooldown_reset_consec_on_expiry"] = False
        legacy_queue_profile = {
            "queue_ahead_base_mult": 0.15,
            "queue_ahead_buy_exposure_mult": 0.50,
            "queue_ahead_buy_reducing_mult": 1.15,
            "queue_ahead_sell_exposure_mult": 1.45,
            "queue_ahead_sell_reducing_mult": 0.70,
        }
        artifact_queue_profile = base.get("queue_calibration_replay_params") or {}
        for key, value in legacy_queue_profile.items():
            base.setdefault(key, value)
        base["queue_profile_source"] = (
            "queue_calibration_artifact" if artifact_queue_profile else "legacy_live_like_fallback"
        )
    if args.live_perf_telemetry is not None:
        samples = bt._load_live_perf_latency_samples(
            args.live_perf_telemetry, mode=args.live_perf_latency_mode
        )
        base["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
        base["_cancel_order_latency_samples_ms"] = samples["cancel_order_latency_samples_ms"]
        if args.replay_purpose == "live_alignment":
            requote_clock = bt._load_live_requote_clock(args.live_perf_telemetry)
            base["_empirical_requote_ts_ms"] = requote_clock["requote_clock_ts_ms"]
            base["_empirical_requote_action"] = requote_clock["requote_clock_action"]
            base["empirical_requote_clock_stage_lag_mean_ms"] = float(
                np.mean(requote_clock["requote_clock_stage_lag_ms"])
            )
            base["empirical_requote_clock_timestamp_semantics"] = str(
                requote_clock["requote_clock_timestamp_semantics"]
            )
        base["live_perf_telemetry_path"] = str(args.live_perf_telemetry)
        base["live_perf_latency_mode"] = args.live_perf_latency_mode
    if args.exec_book_visibility_profile is not None:
        visibility = bt._load_exec_book_visibility_profile(args.exec_book_visibility_profile)
        base["_exec_book_visibility_delay_samples_ms"] = visibility.pop(
            "exec_book_visibility_delay_samples_ms"
        )
        for public_name, private_name in (
            ("exec_book_visibility_paired_ts_ms", "_exec_book_visibility_paired_ts_ms"),
            ("exec_book_visibility_paired_delay_ms", "_exec_book_visibility_paired_delay_ms"),
            ("exec_depth_visibility_paired_delay_ms", "_exec_depth_visibility_paired_delay_ms"),
            ("exec_trade_visibility_paired_delay_ms", "_exec_trade_visibility_paired_delay_ms"),
            ("exec_book_visibility_paired_mid", "_exec_book_visibility_paired_mid"),
        ):
            if public_name in visibility:
                base[private_name] = visibility.pop(public_name)
        base.update(visibility)
        base["exec_book_visibility_mode"] = str(args.exec_book_visibility_mode)
        base["exec_book_visibility_delay_profile_path"] = str(args.exec_book_visibility_profile)
        base["exec_book_visibility_delay_profile_id"] = str(args.exec_book_visibility_profile_id)
        base["exec_book_visibility_delay_seed"] = int(args.exec_book_visibility_delay_seed)
        base["exec_depth_visibility_source_offset_ms"] = int(
            args.exec_depth_visibility_source_offset_ms
        )
    runtime_timing_calibration: dict[str, Any] = {}
    if args.runtime_timing_samples is not None:
        runtime_timing_calibration = _apply_runtime_timing_samples(
            base,
            args.runtime_timing_samples,
            effective_time_assumption=args.runtime_effective_time_assumption,
            arms=arms,
            bulk_cancel_model=args.runtime_bulk_cancel_model,
            private_fill_model=args.runtime_private_fill_model,
        )

    configure_fixed_latency_distribution(
        base,
        scenario=args.latency_scenario,
        profile_id=args.latency_profile_id,
        environment=args.latency_environment,
        baseline_clip_quantile=args.latency_baseline_clip_quantile,
        stress_spike_probability=args.latency_stress_spike_probability,
        stress_spike_multiplier=args.latency_stress_spike_multiplier,
    )
    base["replay_purpose"] = args.replay_purpose
    if args.replay_purpose == "diagnostic":
        base.setdefault("replay_evidence_scope", "replay_diagnostic_only")
    base["replay_initial_state_mode"] = args.initial_state_mode
    base["replay_promotion_eligible"] = False
    if args.initial_state_mode == "frozen_standard":
        if args.standard_initial_state_json is None:
            raise SystemExit(
                "--initial-state-mode frozen_standard requires --standard-initial-state-json"
            )
        base.update(load_standard_initial_state(args.standard_initial_state_json))
    elif args.standard_initial_state_json is not None:
        raise SystemExit(
            "--standard-initial-state-json requires --initial-state-mode frozen_standard"
        )
    if args.replay_purpose == "formal":
        incompatible = []
        if args.initial_state_trades_csv is not None:
            incompatible.append("--initial-state-trades-csv")
        if args.initial_live_state_json is not None:
            incompatible.append("--initial-live-state-json")
        if args.exec_book_visibility_mode == "paired":
            incompatible.append("--exec-book-visibility-mode paired")
        if int(args.exec_depth_visibility_source_offset_ms) != 0:
            incompatible.append("--exec-depth-visibility-source-offset-ms")
        if incompatible:
            raise SystemExit(
                "formal replay cannot consume live-window identity: "
                + ", ".join(incompatible)
                + "; use --replay-purpose live_alignment"
            )
    if args.window_cache_dir:
        base["_window_cache_dir"] = args.window_cache_dir
    if args.refresh_window_cache:
        base["_refresh_window_cache"] = True

    active_multi_market = any(
        bool(arm.overrides.get("multi_market_policy_enabled", False)) for arm in arms
    )
    active_flow_quote_response = any(
        bool(arm.overrides.get("post_fill_quote_response_enabled", False))
        and str(arm.overrides.get("post_fill_quote_response_mode", "noop") or "noop").lower()
        in {"flow_add_widen", "hybrid"}
        and float(arm.overrides.get("post_fill_response_repair_probability_weight", 1.0) or 0.0)
        > 0.0
        for arm in arms
    )
    if (
        active_multi_market or active_flow_quote_response
    ) and args.campaign_repair_model_bundle is None:
        raise SystemExit(
            "active multi-market or flow quote-response arm requires --campaign-repair-model-bundle"
        )
    if active_multi_market and args.historical_global_flow_root is None:
        raise SystemExit("active multi-market arm requires --historical-global-flow-root")
    market_data_latency_profile_payload = None
    market_data_latency_profile_id = ""
    if args.market_data_latency_profile is not None:
        market_data_latency_profile_payload = json.loads(
            args.market_data_latency_profile.expanduser().read_text(encoding="utf-8")
        )
        market_data_latency_profile_id = MarketDataLatencySimulator(
            market_data_latency_profile_payload
        ).profile_id
        base["market_data_latency_profile_path"] = str(
            args.market_data_latency_profile.expanduser().resolve()
        )
        base["market_data_latency_profile_sha256"] = _sha256(
            args.market_data_latency_profile.expanduser()
        )
        base["market_data_latency_profile_id"] = market_data_latency_profile_id
        base["market_data_latency_environment"] = dict(
            market_data_latency_profile_payload.get("environment", {})
        )
        if args.runtime_timing_samples is not None:
            simulator = MarketDataLatencySimulator(market_data_latency_profile_payload)
            for event_type in ("book", "depth", "trade"):
                simulator.message_clock_arrays(
                    np.array([], dtype=np.int64), market_id=f"binance:perp:{args.symbol.upper()}",
                    event_type=event_type, transport="websocket", seed=args.market_data_latency_seed,
                )
            base["exec_book_visibility_mode"] = "message_schedule"
            base["exec_message_delivery_profile_path"] = base["market_data_latency_profile_path"]
            base["exec_book_visibility_delay_seed"] = int(args.market_data_latency_seed)
    base["market_data_latency_mode"] = args.market_data_latency_mode
    base["market_data_latency_seed"] = int(args.market_data_latency_seed)
    if (
        active_multi_market
        and args.market_data_latency_mode.startswith("profile_")
        and market_data_latency_profile_payload is None
    ):
        raise SystemExit("profile market-data latency mode requires --market-data-latency-profile")
    replay_contract: dict[str, Any] = {}
    if args.strict_calibration:
        calibration_params = dict(base)
        if (
            args.replay_purpose == "live_alignment"
            and str(calibration_params.get("replay_event_clock", "merged")).lower() == "empirical"
        ):
            calibration_params["replay_event_clock"] = "merged"
        validate_formal_replay_calibration(
            calibration_params,
            require_latency=True,
        )
        replay_contract = freeze_replay_contract(
            base,
            purpose=args.replay_purpose,
            initial_state_mode=args.initial_state_mode,
            initial_state_artifact=args.standard_initial_state_json,
            root=ROOT,
        )
        validate_frozen_replay_contract(base)
        base["strict_calibration_validated"] = True
        base["replay_evidence_scope"] = base.get("replay_evidence_scope") or (
            "formal_stress_only"
            if args.latency_scenario == "stress"
            else (
                "formal_frozen_contract"
                if args.replay_purpose == "formal"
                else "live_alignment_diagnostic_only"
            )
        )
    campaign_repair_models = _load_campaign_repair_models(
        args.campaign_repair_model_bundle,
        panel=args.campaign_repair_model_panel,
        days=days,
    )
    historical_global_flow_root = (
        str(args.historical_global_flow_root.expanduser())
        if args.historical_global_flow_root is not None
        else ""
    )

    cpp_parity_rows: list[dict[str, Any]] = []
    if args.engine == "cpp" and not args.no_cpp_baseline_parity_gate:
        try:
            cpp_parity_days = _resolve_cpp_parity_days(days, args.cpp_parity_days)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        model_dir_override = base.get("resolved_model_dir") or base.get("model_dir")
        bt.configure_symbol(args.symbol, model_dir_override=model_dir_override)
        parity_arm = next((arm for arm in arms if arm.name == "baseline"), None)
        parity_params = dict(base)
        if parity_arm is not None:
            parity_params.update(parity_arm.overrides)
        for gate_day in cpp_parity_days:
            print(f"Running C++ baseline parity gate on {gate_day} ...", flush=True)
            window = smoke._load_window(gate_day, parity_params)
            py_result = bt._simulate_tick_with_engine(
                "python",
                window["trades"],
                window["var_ts_ms"],
                window["var_ssq"],
                dict(parity_params),
                ml_data=window["ml_data"],
                bbo_data=window["bbo_data"],
                l2_data=window["l2_data"],
                var_ti=window["var_ti"],
                var_retsq=window["var_retsq"],
            )
            cpp_result = bt._simulate_tick_with_engine(
                "cpp",
                window["trades"],
                window["var_ts_ms"],
                window["var_ssq"],
                dict(parity_params),
                ml_data=window["ml_data"],
                bbo_data=window["bbo_data"],
                l2_data=window["l2_data"],
                var_ti=window["var_ti"],
                var_retsq=window["var_retsq"],
            )
            py_fills = float(py_result.get("fills_total", 0.0) or 0.0)
            cpp_fills = float(cpp_result.get("fills_total", 0.0) or 0.0)
            py_pnl = float(py_result.get("pnl", 0.0) or 0.0)
            cpp_pnl = float(cpp_result.get("pnl", 0.0) or 0.0)
            fill_diff_rate = abs(cpp_fills - py_fills) / max(py_fills, 1.0)
            pnl_diff = abs(cpp_pnl - py_pnl)
            passed = (
                fill_diff_rate <= float(args.cpp_parity_max_fill_diff_rate)
                and pnl_diff <= float(args.cpp_parity_max_pnl_diff)
            )
            cpp_parity_rows.append(
                {
                    "day": gate_day,
                    "arm": "baseline",
                    "arm_overrides": dict(parity_arm.overrides) if parity_arm else {},
                    "python_fills": py_fills,
                    "cpp_fills": cpp_fills,
                    "fill_diff_rate": fill_diff_rate,
                    "python_pnl": py_pnl,
                    "cpp_pnl": cpp_pnl,
                    "pnl_abs_diff": pnl_diff,
                    "passed": passed,
                }
            )
            print(
                f"  parity gate: py_fills={py_fills:.0f} cpp_fills={cpp_fills:.0f} "
                f"fill_diff={fill_diff_rate:.2%} pnl_diff={pnl_diff:.4f}",
                flush=True,
            )
            if not passed:
                raise SystemExit(
                    f"C++ baseline parity gate failed on {gate_day}; use --engine "
                    "python or fix C++ replay parity before running wide parameter "
                    "racing."
                )

    initial_states: dict[str, dict[str, float]] = {}
    if args.initial_state_trades_csv is not None:
        initial_states = _load_initial_states_from_trades_csv(args.initial_state_trades_csv, days)
        print(f"Loaded live day-start initial states from {args.initial_state_trades_csv}")
        for day in days:
            state = initial_states.get(day, {})
            print(
                f"  {day}: initial_inventory={float(state.get('initial_inventory', 0.0)):+.6f} "
                f"initial_entry_price={float(state.get('initial_entry_price', 0.0)):.2f}"
            )
    initial_live_states: dict[str, dict[str, Any]] = {}
    if args.initial_live_state_json is not None:
        if args.engine != "python":
            raise SystemExit(
                "--initial-live-state-json currently requires --engine python; "
                "C++ full live-state warm-start parity is not implemented"
            )
        initial_live_states = _load_initial_live_states_json(
            args.initial_live_state_json,
            days,
        )
        print(f"Loaded full live warm-start states from {args.initial_live_state_json}")
        for day in days:
            state = initial_live_states[day]
            if day not in initial_states:
                initial_states[day] = {
                    "initial_inventory": float(state.get("initial_inventory", 0.0) or 0.0),
                    "initial_entry_price": float(state.get("initial_entry_price", 0.0) or 0.0),
                }
            print(
                f"  {day}: initial_inventory="
                f"{float(initial_states[day].get('initial_inventory', 0.0)):+.6f} "
                f"active_orders={len(state.get('active_orders', []) or [])} "
                f"health_orders={state.get('health_orders', 'unknown')}"
            )

    campaign_rows_all: list[dict[str, Any]] = []
    label_rows_all: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    adaptive_hit_rows: list[dict[str, Any]] = []
    campaign_repair_rows: list[dict[str, Any]] = []
    post_fill_quote_response_rows: list[dict[str, Any]] = []
    quote_trace_rows: list[dict[str, Any]] = []
    fill_trace_rows: list[dict[str, Any]] = []
    decision_trace_rows: list[dict[str, Any]] = []
    native_exchange_book_identities: dict[str, dict[str, Any]] = {}
    funding_trace_rows: list[dict[str, Any]] = []
    out_dir = Path(os.environ.get("MM_RESULTS_DIR", str(bt.RESULTS_DIR))).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"campaign_outcome_replay_{args.tag}_{args.symbol.lower()}"

    print(
        f"Campaign outcome replay audit: symbol={args.symbol.upper()} days={','.join(days)} "
        f"arms={','.join(requested)}"
    )
    use_initial_state = bool(initial_states)

    def _merge_day_result(day_result: dict[str, Any]) -> None:
        for line in day_result.get("logs", []):
            print(line, flush=True)
        _write_partial_day_outputs(out_dir, stem, day_result)
        partial_day = (
            str(day_result.get("task_label") or day_result.get("day", "unknown"))
            .replace("/", "-")
            .replace(":", "-")
        )
        partial_daily_path = out_dir / f"{stem}.partial.{partial_day}.daily.csv"
        print(
            f"[main] Saved partial day outputs for {day_result.get('day', '')}: "
            f"{partial_daily_path}",
            flush=True,
        )
        campaign_rows_all.extend(day_result.get("campaign_rows", []))
        label_rows_all.extend(day_result.get("label_rows", []))
        daily_rows.extend(day_result.get("daily_rows", []))
        adaptive_hit_rows.extend(day_result.get("adaptive_hit_rows", []))
        campaign_repair_rows.extend(day_result.get("campaign_repair_rows", []))
        post_fill_quote_response_rows.extend(day_result.get("post_fill_quote_response_rows", []))
        quote_trace_rows.extend(day_result.get("quote_trace_rows", []))
        fill_trace_rows.extend(day_result.get("fill_trace_rows", []))
        funding_trace_rows.extend(day_result.get("funding_trace_rows", []))
        decision_trace_rows.extend(day_result.get("decision_trace_rows", []))
        identity = day_result.get("native_exchange_book_identity") or {}
        if identity:
            native_exchange_book_identities[str(day_result.get("day", ""))] = identity

    workers = max(1, int(args.workers or 1))
    arm_chunk_size = max(0, int(args.arm_chunk_size or 0))
    if workers <= 1:
        for day in (days[:1] if args.continuous else days):
            day_initial = initial_states.get(
                day, {"initial_inventory": 0.0, "initial_entry_price": 0.0}
            )
            _merge_day_result(
                _run_day_campaign_audit(
                    day=day,
                    symbol=args.symbol,
                    funding_events=(
                        [row for source_day in days for row in funding_by_day[source_day]]
                        if args.continuous and funding_history is not None
                        else funding_by_day.get(day)
                    ),
                    continuous_days=days if args.continuous else None,
                    base=base,
                    arms=arms,
                    engine=args.engine,
                    day_initial=day_initial,
                    day_live_state=initial_live_states.get(day),
                    use_initial_state=use_initial_state,
                    runtime_compute_calibration=(
                        runtime_timing_calibration if args.runtime_compute_clock else None
                    ),
                    runtime_compute_clock=args.runtime_compute_clock,
                    campaign_repair_model_payload=campaign_repair_models.get(day),
                    historical_global_flow_root=historical_global_flow_root,
                    market_data_latency_profile_payload=(market_data_latency_profile_payload),
                    market_data_latency_mode=args.market_data_latency_mode,
                    market_data_latency_seed=args.market_data_latency_seed,
                    market_data_latency_market_id=(args.market_data_latency_market_id),
                    save_quote_trace=bool(args.save_quote_trace),
                    save_fill_trace=bool(args.save_fill_trace),
                    save_decision_trace=bool(args.save_decision_trace),
                    native_exchange_book_root=(
                        str(args.native_exchange_book_root.expanduser())
                        if args.native_exchange_book_root is not None
                        else ""
                    ),
                    native_exchange_book_mode=args.native_exchange_book_mode,
                    native_exchange_book_warmup_hours=(args.native_exchange_book_warmup_hours),
                )
            )
    else:
        tasks: list[dict[str, Any]] = []
        order = 0
        for day in days:
            chunks = _arm_chunks(arms, arm_chunk_size)
            for chunk_idx, arm_chunk in enumerate(chunks):
                task_label = f"{day}.chunk{chunk_idx:03d}" if len(chunks) > 1 else ""
                tasks.append(
                    {
                        "day": day,
                        "arms": arm_chunk,
                        "task_label": task_label,
                        "task_order": order,
                        "day_initial": initial_states.get(
                            day,
                            {
                                "initial_inventory": 0.0,
                                "initial_entry_price": 0.0,
                            },
                        ),
                        "day_live_state": initial_live_states.get(day),
                        "campaign_repair_model_payload": campaign_repair_models.get(day),
                        "market_data_latency_profile_payload": (
                            market_data_latency_profile_payload
                        ),
                        "save_quote_trace": bool(args.save_quote_trace),
                        "save_fill_trace": bool(args.save_fill_trace),
                        "save_decision_trace": bool(args.save_decision_trace),
                        "native_exchange_book_root": (
                            str(args.native_exchange_book_root.expanduser())
                            if args.native_exchange_book_root is not None
                            else ""
                        ),
                    }
                )
                order += 1
        mode = f"day/arm chunks size={arm_chunk_size}" if arm_chunk_size > 0 else "day workers"
        print(
            f"Running campaign audit with {workers} workers ({mode}, tasks={len(tasks)}) ...",
            flush=True,
        )
        try:
            # macOS sandboxed runners can deny ``sysconf(SC_SEM_NSEMS_MAX)`` even
            # though process pools work.  Skip only that preflight check; worker
            # failures below still fail the run normally.
            import concurrent.futures.process as _cf_process

            _cf_process._check_system_limits()
        except PermissionError:
            _cf_process._check_system_limits = lambda: None
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            future_map = {}
            for task in tasks:
                future = pool.submit(
                    _run_day_campaign_audit,
                    day=task["day"],
                    symbol=args.symbol,
                    funding_events=funding_by_day.get(task["day"]),
                    base=base,
                    arms=task["arms"],
                    engine=args.engine,
                    day_initial=task["day_initial"],
                    day_live_state=task["day_live_state"],
                    use_initial_state=use_initial_state,
                    runtime_compute_calibration=(
                        runtime_timing_calibration if args.runtime_compute_clock else None
                    ),
                    runtime_compute_clock=args.runtime_compute_clock,
                    campaign_repair_model_payload=task["campaign_repair_model_payload"],
                    historical_global_flow_root=historical_global_flow_root,
                    market_data_latency_profile_payload=task["market_data_latency_profile_payload"],
                    market_data_latency_mode=args.market_data_latency_mode,
                    market_data_latency_seed=args.market_data_latency_seed,
                    market_data_latency_market_id=(args.market_data_latency_market_id),
                    task_label=task["task_label"],
                    task_order=task["task_order"],
                    save_quote_trace=task["save_quote_trace"],
                    save_fill_trace=task["save_fill_trace"],
                    save_decision_trace=task["save_decision_trace"],
                    native_exchange_book_root=task["native_exchange_book_root"],
                    native_exchange_book_mode=args.native_exchange_book_mode,
                    native_exchange_book_warmup_hours=(args.native_exchange_book_warmup_hours),
                )
                future_map[future] = task["task_label"] or task["day"]
            for future in concurrent.futures.as_completed(future_map):
                label = future_map[future]
                try:
                    result = future.result()
                except Exception as exc:
                    raise RuntimeError(f"Campaign audit worker failed for {label}: {exc}") from exc
                print(f"[main] Completed {label}", flush=True)
                _merge_day_result(result)

    campaigns_path = out_dir / f"{stem}.campaign_labels.csv"
    daily_path = out_dir / f"{stem}.daily.csv"
    rollup_path = out_dir / f"{stem}.rollup.csv"
    adaptive_hits_path = out_dir / f"{stem}.adaptive_add_hits.csv"
    campaign_repair_path = out_dir / f"{stem}.campaign_repair_sequence.csv"
    post_fill_quote_response_path = out_dir / f"{stem}.post_fill_quote_response.csv"
    quote_trace_path = out_dir / f"{stem}.quote_trace.csv"
    fill_trace_path = out_dir / f"{stem}.fill_trace.csv"
    decision_trace_path = out_dir / f"{stem}.decision_trace.csv"
    random_null_path = out_dir / f"{stem}.random_passive_null.csv"
    cpp_parity_path = out_dir / f"{stem}.cpp_baseline_parity.csv"
    md_path = out_dir / f"{stem}.md"
    json_path = out_dir / f"{stem}.json"
    replay_contract_path = out_dir / f"{stem}.replay_contract.json"
    if replay_contract:
        write_replay_contract(replay_contract, replay_contract_path)

    daily_df = pd.DataFrame(daily_rows)
    rollup_df = _rollup(daily_df)
    wall_runtime_s = time.perf_counter() - wall_started
    replay_runtime_s_sum = (
        float(
            pd.to_numeric(daily_df.get("runtime_s", pd.Series(dtype=float)), errors="coerce").sum()
        )
        if not daily_df.empty
        else 0.0
    )
    pd.DataFrame(campaign_rows_all).to_csv(campaigns_path, index=False)
    daily_df.to_csv(daily_path, index=False)
    if funding_history is not None:
        pd.DataFrame(funding_trace_rows).to_csv(out_dir / f"{stem}.funding.csv", index=False)
    rollup_df.to_csv(rollup_path, index=False)
    pd.DataFrame(adaptive_hit_rows).to_csv(adaptive_hits_path, index=False)
    pd.DataFrame(campaign_repair_rows).to_csv(campaign_repair_path, index=False)
    pd.DataFrame(post_fill_quote_response_rows).to_csv(
        post_fill_quote_response_path,
        index=False,
    )
    if args.save_quote_trace:
        pd.DataFrame(quote_trace_rows).to_csv(quote_trace_path, index=False)
    if args.save_fill_trace:
        pd.DataFrame(fill_trace_rows).to_csv(fill_trace_path, index=False)
    if args.save_decision_trace:
        pd.DataFrame(decision_trace_rows).to_csv(decision_trace_path, index=False)
    random_null_df = _random_passive_null_table(daily_df) if random_arms else pd.DataFrame()
    random_null_df.to_csv(random_null_path, index=False)
    if cpp_parity_rows:
        pd.DataFrame(cpp_parity_rows).to_csv(cpp_parity_path, index=False)
    queue_calibration_path = Path(str(base.get("queue_calibration_path", ""))).expanduser()
    if not str(base.get("queue_calibration_path", "")):
        queue_calibration_path = None
    cpp_module_path: Path | None = None
    if args.engine == "cpp":
        cpp_module = bt._load_cpp_tick_replay()
        module_file = getattr(cpp_module, "__file__", "")
        if module_file:
            cpp_module_path = Path(str(module_file)).expanduser().resolve()
    reported_timing_calibration = _runtime_timing_report(
        runtime_timing_calibration, daily_rows, compute_clock=args.runtime_compute_clock,
    )
    meta = {
        "symbol": args.symbol.upper(),
        "days": days,
        "arms": requested,
        "arm_definitions": [
            {
                "name": arm.name,
                "group": arm.group,
                "overrides": dict(arm.overrides),
                "note": arm.note,
            }
            for arm in arms
        ],
        "tag": args.tag,
        "engine": args.engine,
        "cpp_module_path": str(cpp_module_path or ""),
        "cpp_module_sha256": _sha256(cpp_module_path),
        "cpp_tick_replay_source_sha256": _sha256(
            ROOT / "cpp" / "narrowgate_cpp" / "tick_replay.cpp"
        ),
        "workers": workers,
        "wall_runtime_s": wall_runtime_s,
        "replay_runtime_s_sum": replay_runtime_s_sum,
        "model_dir": str(base.get("model_dir", "")),
        "resolved_model_dir": str(base.get("resolved_model_dir", "")),
        "features_dir": str(bt.FEATURES_DIR.resolve()),
        "window_cache_dir": str(args.window_cache_dir or ""),
        "window_cache_version": data_windows.WINDOW_CACHE_VERSION,
        "config_path": str(args.config.expanduser()) if args.config else "",
        "config_sha256": _sha256(args.config.expanduser() if args.config else None),
        "replay_locator_projection": base.get("_replay_locator_projection"),
        "live_like_replay_baseline": bool(args.live_like_replay_baseline),
        "replay_evidence_scope": str(base.get("replay_evidence_scope", "legacy_replay_diagnostic")),
        "runtime_timing_calibration": reported_timing_calibration,
        "runtime_compute_clock": args.runtime_compute_clock,
        "accounting_window": "continuous_segment" if args.continuous else "daily_fresh_start",
        "funding_history": {
            "path": str(args.funding_history) if args.funding_history else "",
            "sha256": _sha256(args.funding_history) if args.funding_history else "",
            "settlement_count": len(funding_history) if funding_history is not None else None,
            "mode": "frozen_settlement_tape" if funding_history is not None else "unmodeled",
            "risk_feedback": "not_applied_current_live_uses_trading_pnl",
            "same_ms_ordering": "settlement_before_equal_ms_fills",
        },
        **({
            "economic_pnl_complete": False,
            "economic_pnl_incomplete_day_arms": [
                {"day": row["day"], "arm": row["arm"],
                 "status": row.get("economic_pnl_status")}
                for row in daily_rows if row.get("economic_pnl_complete") is False
            ],
            "replay_promotion_eligible": False,
        } if any(row.get("economic_pnl_complete") is False for row in daily_rows) else {}),
        "live_perf_telemetry": str(args.live_perf_telemetry) if args.live_perf_telemetry else "",
        "live_perf_telemetry_sha256": _sha256(
            args.live_perf_telemetry.expanduser() if args.live_perf_telemetry else None
        ),
        "live_perf_latency_mode": args.live_perf_latency_mode if args.live_perf_telemetry else "",
        "exec_book_visibility_profile": (
            str(args.exec_book_visibility_profile) if args.exec_book_visibility_profile else ""
        ),
        "exec_book_visibility_profile_sha256": _sha256(
            args.exec_book_visibility_profile.expanduser()
            if args.exec_book_visibility_profile
            else None
        ),
        "exec_book_visibility_profile_id": str(
            base.get("exec_book_visibility_delay_profile_id", "")
        ),
        "exec_book_visibility_mode": str(base.get("exec_book_visibility_mode", "sampled")),
        "exec_book_visibility_delay_seed": int(
            base.get("exec_book_visibility_delay_seed", 20260718)
        ),
        "exec_depth_visibility_source_offset_ms": int(
            base.get("exec_depth_visibility_source_offset_ms", 0) or 0
        ),
        "exec_book_visibility_delay_sample_count": int(
            len(base.get("_exec_book_visibility_delay_samples_ms", []))
        ),
        "empirical_requote_clock_count": int(len(base.get("_empirical_requote_ts_ms", []))),
        "empirical_requote_ok_count": int(
            np.sum(np.asarray(base.get("_empirical_requote_action", [])) == 2)
        ),
        "empirical_requote_block_count": int(
            np.sum(np.asarray(base.get("_empirical_requote_action", [])) == 3)
        ),
        "empirical_requote_clock_stage_lag_mean_ms": float(
            base.get("empirical_requote_clock_stage_lag_mean_ms", 0.0) or 0.0
        ),
        "empirical_requote_clock_timestamp_semantics": str(
            base.get("empirical_requote_clock_timestamp_semantics", "observed")
        ),
        "initial_state_trades_csv": str(args.initial_state_trades_csv)
        if args.initial_state_trades_csv
        else "",
        "initial_live_state_json": str(args.initial_live_state_json)
        if args.initial_live_state_json
        else "",
        "initial_live_state_sha256": _sha256(
            args.initial_live_state_json.expanduser() if args.initial_live_state_json else None
        ),
        "full_live_warm_start": bool(initial_live_states),
        "strict_calibration": bool(args.strict_calibration),
        "replay_purpose": args.replay_purpose,
        "replay_contract_sha256": str(replay_contract.get("contract_sha256", "")),
        "replay_contract_path": (str(replay_contract_path) if replay_contract else ""),
        "initial_state_mode": args.initial_state_mode,
        "standard_initial_state_json": str(args.standard_initial_state_json or ""),
        "standard_initial_state_sha256": _sha256(
            args.standard_initial_state_json.expanduser()
            if args.standard_initial_state_json is not None
            else None
        ),
        "rng_seed": int(args.rng_seed),
        "latency_seed": int(args.latency_seed),
        "latency_profile_id": args.latency_profile_id,
        "latency_environment": args.latency_environment,
        "latency_scenario": args.latency_scenario,
        "latency_baseline_clip_quantile": float(args.latency_baseline_clip_quantile),
        "latency_stress_spike_probability": float(args.latency_stress_spike_probability),
        "latency_stress_spike_multiplier": float(args.latency_stress_spike_multiplier),
        "strict_calibration_base_validated": bool(base.get("strict_calibration_validated", False)),
        "strict_calibration_validated": bool(
            not daily_df.empty
            and daily_df.get("strict_calibration_validated", pd.Series(dtype=bool))
            .fillna(False)
            .all()
        ),
        "native_exchange_book_root": (
            str(args.native_exchange_book_root.expanduser())
            if args.native_exchange_book_root is not None
            else ""
        ),
        "native_exchange_book_mode": (
            args.native_exchange_book_mode
            if args.native_exchange_book_root is not None
            else "disabled"
        ),
        "native_exchange_book_warmup_hours": int(args.native_exchange_book_warmup_hours),
        "native_exchange_book_identities": native_exchange_book_identities,
        "sync_adjust_replay_mode": str(args.sync_adjust_replay_mode),
        "sync_adjust_event_tape": str(args.sync_adjust_event_tape or ""),
        "sync_adjust_event_tape_sha256": str(
            base.get("sync_adjust_event_tape_sha256", "")
        ),
        "sync_adjust_event_environment": str(
            base.get("sync_adjust_event_environment", "")
        ),
        "sync_adjust_stress_seed": int(args.sync_adjust_stress_seed),
        "sync_adjust_stress_interval_s": float(
            args.sync_adjust_stress_interval_s
        ),
        "queue_calibration_path": str(queue_calibration_path or ""),
        "queue_calibration_sha256": _sha256(
            queue_calibration_path
            if queue_calibration_path is not None and queue_calibration_path.exists()
            else None
        ),
        "queue_calibration_schema_version": str(base.get("queue_calibration_schema_version", "")),
        "queue_calibration_fit_days": list(base.get("queue_calibration_fit_days") or []),
        "queue_calibration_replay_params": dict(base.get("queue_calibration_replay_params") or {}),
        "queue_profile_source": str(base.get("queue_profile_source", "")),
        "individual_trades_manifest_path": str(base.get("individual_trades_manifest_path", "")),
        "individual_trades_manifest_sha256": str(
            replay_contract.get("artifacts", {})
            .get("individual_trades", {})
            .get("manifest_sha256", "")
        ),
        "individual_trades_integrity_report_path": str(
            base.get("individual_trades_integrity_report_path", "")
        ),
        "individual_trades_integrity_report_sha256": str(
            replay_contract.get("artifacts", {})
            .get("individual_trades", {})
            .get("integrity_report_sha256", "")
        ),
        "campaign_repair_model_bundle": str(args.campaign_repair_model_bundle or ""),
        "campaign_repair_model_bundle_sha256": _sha256(
            args.campaign_repair_model_bundle.expanduser()
            if args.campaign_repair_model_bundle
            else None
        ),
        "campaign_repair_model_panel": args.campaign_repair_model_panel,
        "historical_global_flow_root": historical_global_flow_root,
        "market_data_latency_profile": str(
            args.market_data_latency_profile.expanduser()
            if args.market_data_latency_profile is not None
            else ""
        ),
        "market_data_latency_profile_id": market_data_latency_profile_id,
        "market_data_latency_profile_sha256": _sha256(
            args.market_data_latency_profile.expanduser()
            if args.market_data_latency_profile is not None
            else None
        ),
        "market_data_latency_mode": args.market_data_latency_mode,
        "market_data_latency_seed": int(args.market_data_latency_seed),
        "market_data_latency_market_id": args.market_data_latency_market_id,
        "market_data_latency_environment": (
            market_data_latency_profile_payload.get("environment", {})
            if market_data_latency_profile_payload is not None
            else {}
        ),
        "random_passive_trials": int(args.random_passive_trials),
        "random_passive_seed": int(args.random_passive_seed),
        "random_passive_side_mirror_prob": float(
            args.random_passive_side_mirror_prob
        ),
        "random_passive_timing_jitter_fraction": float(
            args.random_passive_timing_jitter_fraction
        ),
        "cpp_baseline_parity_gate_enabled": bool(
            args.engine == "cpp" and not args.no_cpp_baseline_parity_gate
        ),
        "cpp_baseline_parity_days": [
            str(row["day"]) for row in cpp_parity_rows
        ],
        "cpp_baseline_parity_max_fill_diff_rate": float(
            args.cpp_parity_max_fill_diff_rate
        ),
        "cpp_baseline_parity_max_pnl_diff": float(args.cpp_parity_max_pnl_diff),
        "cpp_baseline_parity_results": cpp_parity_rows,
        "save_quote_trace": bool(args.save_quote_trace),
        "trace_quotes_max": int(base.get("trace_quotes_max", 0) or 0),
        "save_fill_trace": bool(args.save_fill_trace),
        "save_decision_trace": bool(args.save_decision_trace),
        "trace_decisions_max": int(base.get("trace_decisions_max", 0) or 0),
        "outputs": {
            "campaign_labels_csv": str(campaigns_path),
            "daily_csv": str(daily_path),
            "rollup_csv": str(rollup_path),
            "adaptive_add_hits_csv": str(adaptive_hits_path),
            "campaign_repair_sequence_csv": str(campaign_repair_path),
            "post_fill_quote_response_csv": str(post_fill_quote_response_path),
            "quote_trace_csv": str(quote_trace_path) if args.save_quote_trace else "",
            "fill_trace_csv": str(fill_trace_path) if args.save_fill_trace else "",
            "decision_trace_csv": (str(decision_trace_path) if args.save_decision_trace else ""),
            "random_passive_null_csv": str(random_null_path),
            "cpp_baseline_parity_csv": (
                str(cpp_parity_path) if cpp_parity_rows else ""
            ),
            "replay_contract_json": (str(replay_contract_path) if replay_contract else ""),
            "markdown": str(md_path),
        },
    }
    json_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    _write_markdown(md_path, daily_df, rollup_df, meta)
    print(f"Saved {campaigns_path}")
    print(f"Saved {daily_path}")
    print(f"Saved {rollup_path}")
    print(f"Saved {adaptive_hits_path}")
    print(f"Saved {campaign_repair_path}")
    print(f"Saved {post_fill_quote_response_path}")
    if args.save_quote_trace:
        print(f"Saved {quote_trace_path}")
    if args.save_fill_trace:
        print(f"Saved {fill_trace_path}")
    if args.save_decision_trace:
        print(f"Saved {decision_trace_path}")
    if random_arms:
        print(f"Saved {random_null_path}")
    if cpp_parity_rows:
        print(f"Saved {cpp_parity_path}")
    print(f"Saved {md_path}")
    print(
        f"Runtime: wall={wall_runtime_s:.1f}s replay_sum={replay_runtime_s_sum:.1f}s "
        f"workers={workers}"
    )


if __name__ == "__main__":
    main()
