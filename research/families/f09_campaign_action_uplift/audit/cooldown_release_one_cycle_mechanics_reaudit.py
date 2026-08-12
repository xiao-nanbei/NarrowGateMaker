#!/usr/bin/env python3
"""Reaudit the closed post-cooldown one-cycle action on the current stack.

This is a Development-only mechanics diagnostic.  It runs two observer-only
copies of the corrected wall-time baseline, one to locate BUY release
opportunities and one to locate SELL opportunities.  The observer threshold
is fixed so it can never block an order.  Core decision and order traces must
therefore remain byte-identical across the two runs.

The evaluator reports release masking, actual quote-action support, the fill
rate of the released order cycle, and a design MDE derived only from baseline
post-assignment outcome variance.  It never estimates a treatment contrast and
cannot register or authorize an action family.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import math
import shutil
import sys
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd

from models import backtest_tick as bt
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as full_path,
)
from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_runner as f10_runner,
)
from research.governance.paths import resolve_research_path

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "cooldown_release_one_cycle_mechanics_reaudit.v1"
IDENTITY = "cooldown_release_one_cycle_mechanics_reaudit_v1"
SIDES = ("BUY", "SELL")
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "cooldown_release_one_cycle_mechanics_reaudit_v1_spec_20260730.json"
)

DECISION_CORE_FIELDS = (
    "decision_id",
    "ts_ms",
    "side",
    "allow_post",
    "exposure_increasing",
    "reason_text",
    "action",
    "final_price",
    "final_size",
    "campaign_id",
    "fill_cooldown_elapsed_ms",
    "fill_cooldown_total_ms",
    "fill_cooldown_consecutive_units",
)
ORDER_CORE_FIELDS = (
    "order_id",
    "side",
    "campaign_id_at_submit",
    "inventory_role_at_submit",
    "submit_ts",
    "activate_ts",
    "quote_ts",
    "price",
    "quantity",
    "outcome",
    "outcome_ts",
    "cancel_reason",
    "fill_qty",
    "remaining",
)
OPPORTUNITY_COLUMNS = (
    "day",
    "side",
    "campaign_id",
    "release_ts_ms",
    "eligible_ts_ms",
    "release_to_eligible_ms",
    "release_first_blocker",
    "masked_at_release",
    "baseline_eligible_observed",
    "action_change_opportunity",
    "selected_cycle_order_id",
    "selected_cycle_order_submitted",
    "selected_cycle_any_fill",
    "selected_cycle_fill_qty_btc",
    "selected_cycle_outcome",
    "selected_cycle_lifetime_ms",
    "cooldown_total_ms",
    "consecutive_same_side_fill_units",
    "campaign_censored",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_sha256", None)
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    resolved = resolve_research_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return payload


def _require_identity(path: Path, expected: str, label: str) -> None:
    resolved = resolve_research_path(path, require_exists=False)
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} is missing: {resolved}")
    actual = sha256_file(resolved)
    if actual != str(expected):
        raise ValueError(
            f"{label} hash mismatch: expected {expected}, found {actual}"
        )


def validate_spec(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected cooldown-release reaudit schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected cooldown-release reaudit identity")
    frozen_hash = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(payload) != frozen_hash:
        raise ValueError("cooldown-release reaudit canonical hash mismatch")

    permissions = payload.get("permissions") or {}
    forbidden_true = (
        "treatment_contrast_read",
        "outcome_mean_or_sign_read",
        "validation_read",
        "sealed_holdout_read",
        "randomized_action_created",
        "action_experiment_authorized",
        "live_deployment_authorized",
    )
    if any(bool(permissions.get(key, False)) for key in forbidden_true):
        raise ValueError("diagnostic-only permissions were broadened")
    if not bool(permissions.get("outcome_variance_read_for_mde_only", False)):
        raise ValueError("the only permitted outcome read must remain MDE variance")

    overlap = payload.get("historical_action_overlap") or {}
    required_closed = {
        "first_add_marginal_order_value_v1",
        "sell_add_repair_trend_skip_causal_v4_v1",
        "recovery_event_rearm_v1",
    }
    if set(overlap) != required_closed:
        raise ValueError("historical one-cycle overlap registry is incomplete")
    if any(not bool(item.get("closed", False)) for item in overlap.values()):
        raise ValueError("all overlapping action identities must remain closed")

    diagnostic = payload.get("diagnostic_contract") or {}
    if diagnostic.get("classification") != "diagnostic_only":
        raise ValueError("reaudit must remain diagnostic_only")
    if diagnostic.get("candidate_action") != "skip_exactly_one_eligible_add_cycle":
        raise ValueError("reaudit action semantics drifted")
    if bool(diagnostic.get("may_register_randomized_action_on_pass", True)):
        raise ValueError("the closed one-cycle action cannot be re-registered")

    observer = payload.get("observer_contract") or {}
    threshold = float(observer.get("recovery_score_threshold", math.nan))
    component_epsilon = float(
        observer.get("recovery_component_epsilon", math.nan)
    )
    if not (0.0 < threshold < component_epsilon < 1.0):
        raise ValueError(
            "observer threshold must be positive and below the recovery "
            "component floor so every valid state exits immediately"
        )
    if not bool(observer.get("core_paths_must_be_identical", False)):
        raise ValueError("observer runs must preserve the baseline path")
    if int(observer.get("trace_decisions_max_per_day", 0) or 0) <= 0:
        raise ValueError("decision trace bound is invalid")
    if int(observer.get("trace_quotes_max_per_day", 0) or 0) <= 0:
        raise ValueError("order trace bound is invalid")

    panels = payload.get("panels") or {}
    days = [str(day) for day in panels.get("development_days", ())]
    if len(days) != 40 or days != sorted(days) or len(set(days)) != 40:
        raise ValueError("reaudit must retain the exact ordered 40-day Development")
    if panels.get("validation_days_read") or panels.get("sealed_holdout_days_read"):
        raise ValueError("later panels must remain unread")

    mde = payload.get("mde_contract") or {}
    if mde.get("estimand") != "balanced_two_arm_difference_in_means":
        raise ValueError("MDE estimand drifted")
    if not math.isclose(float(mde.get("alpha", 0.0)), 0.05):
        raise ValueError("MDE alpha drifted")
    if not math.isclose(float(mde.get("power", 0.0)), 0.80):
        raise ValueError("MDE power drifted")

    producer = payload.get("baseline_producer_identity") or {}
    producer_path = Path(str(producer.get("path", ""))).expanduser()
    _require_identity(producer_path, str(producer.get("sha256", "")), "F10 producer")
    f10_runner.load_producer_spec(producer_path)
    for label, identity in overlap.items():
        _require_identity(
            Path(str(identity.get("path", ""))),
            str(identity.get("sha256", "")),
            label,
        )
    lineage = payload.get("lineage_outcome_contract_identity") or {}
    _require_identity(
        Path(str(lineage.get("path", ""))),
        str(lineage.get("sha256", "")),
        "lineage outcome contract v2",
    )
    for relative, expected in (payload.get("implementation_identity") or {}).items():
        _require_identity(ROOT / str(relative), str(expected), str(relative))


def load_spec(path: Path = DEFAULT_SPEC_PATH) -> dict[str, Any]:
    payload = _load_json(path)
    validate_spec(payload)
    return payload


def bind_frozen_native_module(producer: Mapping[str, Any]) -> Path:
    """Bind the exact producer module before the deferred native import."""

    identity = producer.get("native_module_identity") or {}
    module_path = Path(str(identity.get("path", ""))).expanduser().resolve()
    expected = str(identity.get("sha256", ""))
    _require_identity(module_path, expected, "BUY q90 native module")

    loaded = sys.modules.get("narrowgate_cpp")
    if loaded is None:
        sys.path.insert(0, str(module_path.parent))
        importlib.invalidate_caches()
        loaded = importlib.import_module("narrowgate_cpp")
    loaded_path = Path(str(getattr(loaded, "__file__", ""))).resolve()
    if loaded_path != module_path:
        raise RuntimeError(
            "narrowgate_cpp was loaded from an unfrozen path: "
            f"expected {module_path}, found {loaded_path}"
        )
    _require_identity(loaded_path, expected, "loaded BUY q90 native module")
    return loaded_path


def _json_scalar(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("core trace contains a non-finite value")
        return value
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def core_trace_digest(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str:
    projected = [
        [_json_scalar(row.get(field)) for field in fields]
        for row in rows
    ]
    raw = json.dumps(
        projected,
        sort_keys=False,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _first_blocker(row: Mapping[str, Any]) -> str:
    reasons = str(row.get("reason_text", "") or "").strip()
    if not bool(int(row.get("allow_post", 0) or 0)):
        return reasons if reasons and reasons != "none" else "post_not_allowed"
    if bool(int(row.get("order_active_before", 0) or 0)):
        return "active_or_pending_order_present"
    if str(row.get("action", "")) != "place":
        return f"decision_action_{str(row.get('action', 'none') or 'none')}"
    return "none"


def _terminal_order_outcome(rows: pd.DataFrame) -> tuple[str, float, float]:
    if rows.empty:
        return "missing", 0.0, 0.0
    ordered = rows.sort_values(["outcome_ts", "order_id"], kind="stable")
    fills = ordered[ordered["outcome"].astype(str).eq("fill")]
    fill_qty = float(pd.to_numeric(fills.get("fill_qty"), errors="coerce").fillna(0.0).sum())
    terminal = str(ordered.iloc[-1].get("outcome", "unknown") or "unknown")
    lifetime = float(
        pd.to_numeric(ordered.get("lifetime_ms"), errors="coerce").fillna(0.0).max()
    )
    return terminal, fill_qty, lifetime


def extract_release_opportunities(
    day: str,
    side: str,
    decisions: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
    observer_rows: Sequence[Mapping[str, Any]],
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Reduce one observer path to one release row per campaign."""

    side = str(side).upper()
    decision_frame = pd.DataFrame(decisions)
    order_frame = pd.DataFrame(orders)
    observer_frame = pd.DataFrame(observer_rows)
    if decision_frame.empty:
        return pd.DataFrame(columns=OPPORTUNITY_COLUMNS), {
            "count": 0.0,
            "sum": 0.0,
            "sum_sq": 0.0,
        }

    numeric_columns = (
        "campaign_id",
        "campaign_active",
        "exposure_increasing",
        "fill_cooldown_total_ms",
        "fill_cooldown_elapsed_ms",
        "fill_cooldown_consecutive_units",
        "ts_ms",
    )
    for column in numeric_columns:
        decision_frame[column] = pd.to_numeric(
            decision_frame.get(column), errors="coerce"
        )
    eligible_release = (
        decision_frame["side"].astype(str).str.upper().eq(side)
        & decision_frame["campaign_active"].eq(1)
        & decision_frame["campaign_id"].gt(0)
        & decision_frame["exposure_increasing"].eq(1)
        & decision_frame["fill_cooldown_total_ms"].gt(0.0)
        & decision_frame["fill_cooldown_consecutive_units"].gt(0.0)
        & decision_frame["fill_cooldown_elapsed_ms"].ge(
            decision_frame["fill_cooldown_total_ms"]
        )
    )
    release_rows = (
        decision_frame.loc[eligible_release]
        .sort_values(["campaign_id", "ts_ms"], kind="stable")
        .groupby("campaign_id", sort=False, as_index=False)
        .first()
    )

    if observer_frame.empty:
        observer_by_campaign: dict[int, Mapping[str, Any]] = {}
    else:
        observer_frame = observer_frame[
            observer_frame["side"].astype(str).str.upper().eq(side)
        ].copy()
        if pd.to_numeric(
            observer_frame.get("action_effective"), errors="coerce"
        ).fillna(0).ne(0).any():
            raise RuntimeError("observer changed the post-cooldown action path")
        if pd.to_numeric(
            observer_frame.get("blocked_quote_cycles"), errors="coerce"
        ).fillna(0).ne(0).any():
            raise RuntimeError("observer blocked a quote cycle")
        observer_by_campaign = {
            int(row["campaign_id"]): row
            for row in observer_frame.to_dict("records")
        }

    if not order_frame.empty:
        for column in ("campaign_id_at_submit", "submit_ts", "order_id", "outcome_ts"):
            order_frame[column] = pd.to_numeric(order_frame.get(column), errors="coerce")
        order_frame = order_frame[
            order_frame["side"].astype(str).str.upper().eq(side)
        ]

    rows: list[dict[str, Any]] = []
    reward_values: list[float] = []
    for release in release_rows.to_dict("records"):
        campaign_id = int(release["campaign_id"])
        release_ts = int(release["ts_ms"])
        observer = observer_by_campaign.get(campaign_id)
        eligible_ts = int(observer.get("decision_ts_ms", 0) or 0) if observer else 0
        selected_order_id = -1
        selected_submitted = 0
        selected_fill = 0
        selected_fill_qty = 0.0
        selected_outcome = "none"
        selected_lifetime = 0.0
        if observer is not None:
            reward = float(observer.get("reward", math.nan))
            if not math.isfinite(reward):
                raise RuntimeError("observer MDE outcome is non-finite")
            reward_values.append(reward)
            if not order_frame.empty:
                submitted = order_frame[
                    order_frame["campaign_id_at_submit"].eq(campaign_id)
                    & order_frame["submit_ts"].eq(eligible_ts)
                    & order_frame["inventory_role_at_submit"].astype(str).eq("add")
                ]
                if not submitted.empty:
                    selected_order_id = int(submitted.iloc[0]["order_id"])
                    selected_rows = order_frame[
                        order_frame["order_id"].eq(selected_order_id)
                    ]
                    selected_outcome, selected_fill_qty, selected_lifetime = (
                        _terminal_order_outcome(selected_rows)
                    )
                    selected_submitted = 1
                    selected_fill = int(selected_fill_qty > 1e-12)

        rows.append(
            {
                "day": str(day),
                "side": side,
                "campaign_id": campaign_id,
                "release_ts_ms": release_ts,
                "eligible_ts_ms": eligible_ts,
                "release_to_eligible_ms": (
                    max(0, eligible_ts - release_ts) if eligible_ts > 0 else 0
                ),
                "release_first_blocker": _first_blocker(release),
                "masked_at_release": int(eligible_ts <= 0 or eligible_ts > release_ts),
                "baseline_eligible_observed": int(observer is not None),
                "action_change_opportunity": int(observer is not None and selected_submitted),
                "selected_cycle_order_id": selected_order_id,
                "selected_cycle_order_submitted": selected_submitted,
                "selected_cycle_any_fill": selected_fill,
                "selected_cycle_fill_qty_btc": selected_fill_qty,
                "selected_cycle_outcome": selected_outcome,
                "selected_cycle_lifetime_ms": selected_lifetime,
                "cooldown_total_ms": float(release["fill_cooldown_total_ms"]),
                "consecutive_same_side_fill_units": float(
                    release["fill_cooldown_consecutive_units"]
                ),
                "campaign_censored": int(
                    observer.get("campaign_censored", 0) if observer else 1
                ),
            }
        )

    rewards = np.asarray(reward_values, dtype=np.float64)
    sufficient = {
        "count": float(rewards.size),
        "sum": float(rewards.sum()) if rewards.size else 0.0,
        "sum_sq": float(np.square(rewards).sum()) if rewards.size else 0.0,
    }
    return pd.DataFrame(rows, columns=OPPORTUNITY_COLUMNS), sufficient


def _wilson_interval(successes: int, total: int, alpha: float = 0.05) -> tuple[float, float]:
    if total <= 0:
        return 0.0, 1.0
    z = NormalDist().inv_cdf(1.0 - alpha / 2.0)
    p = successes / total
    denom = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denom
    half = z * math.sqrt(p * (1.0 - p) / total + z * z / (4.0 * total * total)) / denom
    return max(0.0, center - half), min(1.0, center + half)


def mde_from_day_sufficient(
    daily: pd.DataFrame,
    *,
    alpha: float,
    power: float,
) -> float:
    """Balanced two-arm MDE using within-day outcome variance only."""

    if daily.empty:
        return math.inf
    count = pd.to_numeric(daily["mde_count"], errors="raise").to_numpy(float)
    sums = pd.to_numeric(daily["mde_sum"], errors="raise").to_numpy(float)
    sums_sq = pd.to_numeric(daily["mde_sum_sq"], errors="raise").to_numpy(float)
    valid = count > 1.0
    if not np.any(valid):
        return math.inf
    residual_ss = float(
        np.maximum(0.0, sums_sq[valid] - np.square(sums[valid]) / count[valid]).sum()
    )
    total = float(count[valid].sum())
    if total <= 1.0 or residual_ss <= 0.0:
        return math.inf
    null_se = 2.0 * math.sqrt(residual_ss) / total
    z_alpha = NormalDist().inv_cdf(1.0 - float(alpha) / 2.0)
    z_power = NormalDist().inv_cdf(float(power))
    return float((z_alpha + z_power) * null_se)


def summarize_opportunities(
    opportunities: pd.DataFrame,
    daily: pd.DataFrame,
    spec: Mapping[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    near_noop_ucb = float(
        spec["diagnostic_contract"]["near_noop_fill_rate_ucb_threshold"]
    )
    mde_contract = spec["mde_contract"]
    for side in SIDES:
        frame = opportunities[opportunities["side"].astype(str).eq(side)]
        side_daily = daily[daily["side"].astype(str).eq(side)]
        releases = int(len(frame))
        eligible = int(frame["baseline_eligible_observed"].sum()) if releases else 0
        changes = int(frame["action_change_opportunity"].sum()) if releases else 0
        fills = int(frame["selected_cycle_any_fill"].sum()) if releases else 0
        submitted = int(frame["selected_cycle_order_submitted"].sum()) if releases else 0
        fill_lcb, fill_ucb = _wilson_interval(fills, submitted)
        rows.append(
            {
                "side": side,
                "release_episode_count": releases,
                "release_days": int(frame["day"].nunique()) if releases else 0,
                "baseline_eligible_count": eligible,
                "baseline_eligible_rate": eligible / max(releases, 1),
                "masked_at_release_count": int(frame["masked_at_release"].sum()) if releases else 0,
                "masked_at_release_rate": float(frame["masked_at_release"].mean()) if releases else 0.0,
                "action_change_opportunity_count": changes,
                "action_change_opportunity_rate": changes / max(releases, 1),
                "selected_cycle_order_count": submitted,
                "selected_cycle_fill_count": fills,
                "selected_cycle_fill_rate": fills / max(submitted, 1),
                "selected_cycle_fill_rate_lcb95": fill_lcb,
                "selected_cycle_fill_rate_ucb95": fill_ucb,
                "near_noop_mechanics_classification": bool(fill_ucb < near_noop_ucb),
                "mde_80pct_power_two_sided_usdc": mde_from_day_sufficient(
                    side_daily,
                    alpha=float(mde_contract["alpha"]),
                    power=float(mde_contract["power"]),
                ),
                "mde_outcome_rows": int(side_daily["mde_count"].sum()) if len(side_daily) else 0,
            }
        )
    return pd.DataFrame(rows)


def _configure_observer(
    producer: Mapping[str, Any],
    base: Mapping[str, Any],
    spec: Mapping[str, Any],
    day: str,
    side: str,
) -> dict[str, Any]:
    params = full_path._configure_params(base, day)
    observer = spec["observer_contract"]
    params.update(
        {
            "fill_cooldown_clock_mode": "wall_time",
            "variance_time_lineage_randomized_enabled": False,
            "trace_variance_time_lineage_max": 0,
            "state_conditioned_rearm_enabled": True,
            "state_conditioned_rearm_sides": (str(side),),
            "state_conditioned_rearm_probabilities": {
                "baseline_rearm": 0.5,
                "continue_block_until_recovery": 0.5,
            },
            "state_conditioned_rearm_seed": int(observer["observer_seed_base"])
            + int(str(day).replace("-", "")),
            "state_conditioned_rearm_min_elapsed_s": 0.0,
            "state_conditioned_rearm_min_followup_s": 0.0,
            "state_conditioned_rearm_policy_version": "composite_recovery_v2",
            "recovery_event_rearm_score_threshold": float(
                observer["recovery_score_threshold"]
            ),
            "recovery_event_rearm_component_epsilon": float(
                observer["recovery_component_epsilon"]
            ),
            "recovery_event_rearm_max_book_age_ms": 2_000.0,
            "trace_state_conditioned_rearm_max": int(
                observer["trace_observer_rows_max_per_day"]
            ),
            "trace_decisions_max": int(observer["trace_decisions_max_per_day"]),
            "trace_quotes_max": int(observer["trace_quotes_max_per_day"]),
            "trace_fills_max": 0,
            "decision_trace_profile": "mechanics_only",
            "collect_curves": False,
            "window_cache_write_enabled": False,
            "replay_purpose": "mechanics_preflight",
            "replay_initial_state_mode": "fresh_start",
            "replay_promotion_eligible": False,
            "dynamic_fill_hazard_cpp_parity_enabled": True,
            "dynamic_fill_hazard_cpp_parity_strict": True,
            "dynamic_fill_hazard_cpp_parity_trace_max": int(
                producer["replay_contract"]["q90_mismatch_trace_max"]
            ),
        }
    )
    return params


def _run_observer(
    producer: Mapping[str, Any],
    base: Mapping[str, Any],
    spec: Mapping[str, Any],
    day: str,
    side: str,
    window: Mapping[str, Any],
) -> dict[str, Any]:
    params = _configure_observer(producer, base, spec, day, side)
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(base["source_identity"]["native_orderbook_root"]),
        day=str(day),
        symbol="BTCUSDC",
        tick_size=float(params.get("tick_size", bt.TICK)),
        warmup_hours=int(base["replay_contract"]["native_warmup_hours"]),
        strict_complete=True,
    )
    result = bt._simulate_tick_with_engine(
        "python",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        params,
        ml_data=None,
        bbo_data=window.get("bbo_data"),
        l2_data=window.get("l2_data"),
        var_ti=window.get("var_ti"),
        var_retsq=window.get("var_retsq"),
        exchange_book_event_tape=tape,
    )
    for key in ("_decision_trace", "_quote_trace", "_state_conditioned_rearm_trace"):
        if len(result.get(key, ())) >= int(
            spec["observer_contract"][
                {
                    "_decision_trace": "trace_decisions_max_per_day",
                    "_quote_trace": "trace_quotes_max_per_day",
                    "_state_conditioned_rearm_trace": "trace_observer_rows_max_per_day",
                }[key]
            ]
        ):
            raise RuntimeError(f"{key} reached its per-day bound on {day} {side}")
    if int(result.get("exchange_book_source_gap_events", 0) or 0) != 0:
        raise RuntimeError(f"native source gap encountered on {day} {side}")
    if int(result.get("exchange_book_invalid_sequence_messages", 0) or 0) != 0:
        raise RuntimeError(f"native sequence failure encountered on {day} {side}")
    if not bool(result.get("dynamic_fill_hazard_cpp_parity_passed", False)):
        raise RuntimeError(f"BUY q90 Python/C++ parity failed on {day} {side}")
    if int(result.get("dynamic_fill_hazard_cpp_mismatch_count", 0) or 0) != 0:
        raise RuntimeError(f"BUY q90 mismatch was nonzero on {day} {side}")
    expected_module = str(producer["native_module_identity"]["sha256"])
    actual_module = str(
        (result.get("dynamic_fill_hazard_cpp_identity") or {}).get(
            "native_module_sha256", ""
        )
    )
    if actual_module != expected_module:
        raise RuntimeError(
            f"BUY q90 native module drifted: expected {expected_module}, found {actual_module}"
        )
    return result


def run_day(spec_path: Path, day: str) -> dict[str, Any]:
    started = time.monotonic()
    spec = load_spec(spec_path)
    producer_path = resolve_research_path(
        spec["baseline_producer_identity"]["path"]
    )
    producer, _, base, _ = f10_runner.load_producer_spec(producer_path)
    bind_frozen_native_module(producer)
    if str(day) not in set(spec["panels"]["development_days"]):
        raise ValueError(f"day is outside frozen Development: {day}")
    template = _configure_observer(producer, base, spec, str(day), "BUY")
    window = full_path._load_window(base, str(day), template)
    results = {
        side: _run_observer(producer, base, spec, str(day), side, window)
        for side in SIDES
    }

    decision_digests = {
        side: core_trace_digest(results[side]["_decision_trace"], DECISION_CORE_FIELDS)
        for side in SIDES
    }
    order_digests = {
        side: core_trace_digest(results[side]["_quote_trace"], ORDER_CORE_FIELDS)
        for side in SIDES
    }
    if len(set(decision_digests.values())) != 1 or len(set(order_digests.values())) != 1:
        raise RuntimeError(f"observer instrumentation changed the baseline path on {day}")
    if any(
        int(results[side].get("fills_bid", 0) or 0)
        != int(results[SIDES[0]].get("fills_bid", 0) or 0)
        or int(results[side].get("fills_ask", 0) or 0)
        != int(results[SIDES[0]].get("fills_ask", 0) or 0)
        for side in SIDES
    ):
        raise RuntimeError(f"observer fill path differs on {day}")

    opportunities: list[pd.DataFrame] = []
    daily_rows: list[dict[str, Any]] = []
    for side in SIDES:
        frame, sufficient = extract_release_opportunities(
            str(day),
            side,
            results[side]["_decision_trace"],
            results[side]["_quote_trace"],
            results[side]["_state_conditioned_rearm_trace"],
        )
        opportunities.append(frame)
        daily_rows.append(
            {
                "day": str(day),
                "side": side,
                "release_episode_count": int(len(frame)),
                "baseline_eligible_count": int(frame["baseline_eligible_observed"].sum()),
                "action_change_opportunity_count": int(frame["action_change_opportunity"].sum()),
                "selected_cycle_fill_count": int(frame["selected_cycle_any_fill"].sum()),
                "mde_count": int(sufficient["count"]),
                "mde_sum": float(sufficient["sum"]),
                "mde_sum_sq": float(sufficient["sum_sq"]),
                "decision_core_sha256": decision_digests[side],
                "order_core_sha256": order_digests[side],
                "observer_path_identical": True,
            }
        )
    return {
        "day": str(day),
        "runtime_s": float(time.monotonic() - started),
        "opportunities": pd.concat(opportunities, ignore_index=True),
        "daily": pd.DataFrame(daily_rows),
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _checkpoint_day(
    output: Path,
    result: Mapping[str, Any],
    spec_sha256: str,
) -> dict[str, Any]:
    day = str(result["day"])
    day_dir = output / "days"
    day_dir.mkdir(parents=True, exist_ok=True)
    opportunity_path = day_dir / f"{day}.opportunities.parquet"
    daily_path = day_dir / f"{day}.daily.parquet"
    audit_path = day_dir / f"{day}.json"
    _atomic_parquet(opportunity_path, result["opportunities"])
    _atomic_parquet(daily_path, result["daily"])
    payload = {
        "day": day,
        "runtime_s": float(result["runtime_s"]),
        "spec_sha256": str(spec_sha256),
        "opportunity_path": str(opportunity_path),
        "opportunity_sha256": sha256_file(opportunity_path),
        "daily_path": str(daily_path),
        "daily_sha256": sha256_file(daily_path),
        "release_episode_count": int(len(result["opportunities"])),
        "observer_path_identical": bool(result["daily"]["observer_path_identical"].all()),
    }
    _atomic_json(audit_path, payload)
    return payload


def _load_checkpoint(
    output: Path,
    day: str,
    spec_sha256: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]] | None:
    audit_path = output / "days" / f"{day}.json"
    if not audit_path.is_file():
        return None
    audit = _load_json(audit_path)
    if audit.get("spec_sha256") != spec_sha256:
        raise ValueError(f"checkpoint spec drifted on {day}")
    opportunity_path = Path(audit["opportunity_path"])
    daily_path = Path(audit["daily_path"])
    _require_identity(opportunity_path, audit["opportunity_sha256"], day)
    _require_identity(daily_path, audit["daily_sha256"], day)
    if not bool(audit.get("observer_path_identical", False)):
        raise ValueError(f"checkpoint observer path failed on {day}")
    return pd.read_parquet(opportunity_path), pd.read_parquet(daily_path), audit


def _run_day_for_pool(spec_path: str, day: str) -> dict[str, Any]:
    return run_day(Path(spec_path), str(day))


def _write_markdown_report(path: Path, report: Mapping[str, Any]) -> None:
    rows = report["side_summary"]
    lines = [
        "# Cooldown Release One-Cycle Mechanics Reaudit v1",
        "",
        "Last materially modified: 2026-07-30",
        "",
        "## Decision",
        "",
        str(report["decision"]),
        "",
        "This is a Development-only diagnostic of an already closed action. It",
        "does not register C0/C1, read Validation/holdout, or grant action/live",
        "authority. F04 receive-time accumulation remains a background branch.",
        "",
        "## Current-Stack Mechanics",
        "",
        "| Side | Release episodes | Eligible | Action-change opportunities | Cycle fills | Fill rate (95% Wilson) | MDE USDC |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {side} | {release_episode_count:,} | {baseline_eligible_count:,} "
            "| {action_change_opportunity_count:,} | {selected_cycle_fill_count:,} "
            "| {selected_cycle_fill_rate:.2%} [{selected_cycle_fill_rate_lcb95:.2%}, "
            "{selected_cycle_fill_rate_ucb95:.2%}] | {mde_80pct_power_two_sided_usdc:.6f} |".format(**row)
        )
    lines.extend(
        [
            "",
            "`MDE` uses only within-day baseline post-assignment outcome variance;",
            "no action contrast, outcome mean, sign, policy score, or threshold was",
            "estimated. The exact one-cycle action remains closed regardless of this",
            "mechanics classification.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--dates", nargs="*")
    parser.add_argument("--allow-partial-diagnostic", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    spec = load_spec(spec_path)
    spec_hash = sha256_file(spec_path)
    expected_days = tuple(spec["panels"]["development_days"])
    requested = tuple(args.dates or expected_days)
    unknown = sorted(set(requested) - set(expected_days))
    if unknown:
        raise ValueError(f"requested dates are outside frozen Development: {unknown}")
    complete = set(requested) == set(expected_days)
    if not complete and not args.allow_partial_diagnostic:
        raise ValueError("formal reaudit requires all 40 Development days")

    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    free_gib = shutil.disk_usage(output).free / (1024**3)
    required_free = float(spec["storage_gate"]["required_free_gib"])
    if free_gib < required_free:
        raise RuntimeError(
            f"reaudit storage gate failed: {free_gib:.2f} < {required_free:.2f} GiB"
        )

    opportunities_by_day: dict[str, pd.DataFrame] = {}
    daily_by_day: dict[str, pd.DataFrame] = {}
    audits: dict[str, dict[str, Any]] = {}
    pending: list[str] = []
    for day in requested:
        checkpoint = _load_checkpoint(output, day, spec_hash)
        if checkpoint is None:
            pending.append(day)
        else:
            opportunities_by_day[day], daily_by_day[day], audits[day] = checkpoint

    workers = max(1, min(int(args.workers), len(pending) or 1))
    if workers == 1:
        iterator = (_run_day_for_pool(str(spec_path), day) for day in pending)
        for result in iterator:
            day = str(result["day"])
            audits[day] = _checkpoint_day(output, result, spec_hash)
            opportunities_by_day[day] = result["opportunities"]
            daily_by_day[day] = result["daily"]
            print(
                f"{day}: releases={len(result['opportunities'])} "
                f"runtime={result['runtime_s']:.1f}s",
                flush=True,
            )
    elif pending:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_run_day_for_pool, str(spec_path), day): day
                for day in pending
            }
            for future in as_completed(futures):
                result = future.result()
                day = str(result["day"])
                audits[day] = _checkpoint_day(output, result, spec_hash)
                opportunities_by_day[day] = result["opportunities"]
                daily_by_day[day] = result["daily"]
                print(
                    f"{day}: releases={len(result['opportunities'])} "
                    f"runtime={result['runtime_s']:.1f}s",
                    flush=True,
                )

    opportunities = pd.concat(
        [opportunities_by_day[day] for day in requested], ignore_index=True
    )
    daily = pd.concat([daily_by_day[day] for day in requested], ignore_index=True)
    if not bool(daily["observer_path_identical"].all()):
        raise RuntimeError("observer path identity failed")
    summary = summarize_opportunities(opportunities, daily, spec)
    blocker_counts = (
        opportunities.groupby(["side", "release_first_blocker"], sort=True)
        .size()
        .rename("count")
        .reset_index()
    )
    summary_rows = summary.to_dict("records")
    both_near_noop = bool(
        len(summary) == 2
        and summary["near_noop_mechanics_classification"].astype(bool).all()
    )
    decision = (
        "historical_one_cycle_near_noop_mechanics_reconfirmed_current_stack"
        if both_near_noop
        else "historical_one_cycle_mechanics_not_near_noop_or_uncertain_but_action_remains_closed"
    )

    opportunity_path = output / "release_opportunities.parquet"
    daily_path = output / "daily_mechanics.parquet"
    summary_path = output / "side_summary.csv"
    blockers_path = output / "release_first_blockers.csv"
    _atomic_parquet(opportunity_path, opportunities)
    _atomic_parquet(daily_path, daily)
    summary.to_csv(summary_path, index=False)
    blocker_counts.to_csv(blockers_path, index=False)
    report = {
        "schema_version": "cooldown_release_one_cycle_mechanics_reaudit_report.v1",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "formal_development" if complete else "partial_diagnostic_only",
        "decision": decision,
        "side_summary": summary_rows,
        "observer_path_identical_all_days": True,
        "historical_action_remains_closed": True,
        "randomized_action_registration_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
        "mde_only_outcome_variance_read": True,
        "treatment_contrast_read": False,
        "outcome_mean_or_sign_read": False,
        "artifacts": {
            "release_opportunities": {
                "path": str(opportunity_path),
                "sha256": sha256_file(opportunity_path),
            },
            "daily_mechanics": {
                "path": str(daily_path),
                "sha256": sha256_file(daily_path),
            },
            "side_summary": {
                "path": str(summary_path),
                "sha256": sha256_file(summary_path),
            },
            "release_first_blockers": {
                "path": str(blockers_path),
                "sha256": sha256_file(blockers_path),
            },
        },
    }
    report_path = output / "report.json"
    report_md_path = output / "report.md"
    _atomic_json(report_path, report)
    _write_markdown_report(report_md_path, report)
    manifest = {
        "schema_version": "cooldown_release_one_cycle_mechanics_reaudit_manifest.v1",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {
            "path": str(spec_path),
            "sha256": spec_hash,
            "canonical_spec_sha256": spec["canonical_spec_sha256"],
        },
        "requested_days": list(requested),
        "complete_development": complete,
        "day_audits": [audits[day] for day in requested],
        "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        "report_markdown": {
            "path": str(report_md_path),
            "sha256": sha256_file(report_md_path),
        },
        "decision": decision,
        "diagnostic_only": True,
        "historical_action_remains_closed": True,
        "randomized_action_registration_allowed": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    _atomic_json(output / "manifest.json", manifest)
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
