#!/usr/bin/env python3
# ruff: noqa: E402
"""Run the first local-only randomized action family in Python tick replay.

The behavior policy intervenes at most once per inventory campaign and only on
an exposure-increasing add-side order that the baseline would newly place with
no active or pending order on that side.  This keeps queue/reset cost equal to
zero by construction while the selected order still traverses real replay
latency, queue, cancel, fill, cooldown, and future inventory mechanics.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from research.families.f01_fixed_parameter_racing import daily_smoke_sweep as smoke  # noqa: E402
from models.audit.evidence_split import load_evidence_panel  # noqa: E402
from models.audit.experiment_manifest import (  # noqa: E402
    build_manifest,
    git_workspace_identity,
    write_code_checkpoint,
    write_manifest,
)
from research.families.f07_active_order_continuation.audit.local_order_value_panel import (  # noqa: E402
    add_competing_risk_labels,
    validate_randomized_action_panel,
)
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (  # noqa: E402
    OPEConfig,
    evaluate_offline_policy,
)
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
    write_outputs as write_ope_outputs,
)
from research.families.f07_active_order_continuation.audit.queue_value_models import (  # noqa: E402
    EVENT_COLUMNS,
    NATIVE_EXCHANGE_EVENT_COLUMNS,
    QueueReactiveHawkesArtifact,
    QueueValueModelBundle,
)
from research.families.f07_active_order_continuation.audit.queue_value_competing_risk import (  # noqa: E402
    CompetingRiskBundle,
)
from research.families.f09_campaign_action_uplift.audit.toxic_fill_selectivity import (  # noqa: E402
    randomized_panel_selectivity,
)
from models.backtest_config import (  # noqa: E402
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from research.families.f09_campaign_action_uplift.causal_path_features import CAUSAL_PATH_FEATURE_COLUMNS  # noqa: E402
from models.exchange_book_replay import CryptoHFTExchangeBookTape  # noqa: E402
from models.replay_policies import (  # noqa: E402
    CAMPAIGN_STOP_ADD_ACTIONS,
    LOCAL_ACTIONS,
    QUEUE_VALUE_CANCEL_REENTER_ACTIONS,
    QUEUE_VALUE_KEEP_CANCEL_ACTIONS,
    SELL_ADD_SKIP_ACTIONS,
    normalize_action_probabilities,
    normalize_campaign_stop_add_probabilities,
    normalize_queue_value_cancel_reenter_probabilities,
    normalize_queue_value_probabilities,
    normalize_sell_add_skip_probabilities,
)

SCHEMA_VERSION = "local_action_uplift.v2"
QUEUE_VALUE_ACTION_FAMILIES = {
    "queue_value_keep_cancel",
    "queue_value_cancel_reenter",
    "queue_value_net_keep_cancel",
}
ADD_SKIP_ACTION_FAMILIES = {
    "sell_add_skip",
    "first_add_skip",
    "campaign_stop_add",
}
FIRST_ADD_SKIP_FAMILY_ID = "buy_first_add_skip_marginal_value_v1"
CAMPAIGN_STOP_ADD_FAMILY_ID = "sell_campaign_add_permission_v1"
QUEUE_VALUE_NET_FAMILY_ID = "queue_value_net_hazard_keep_cancel_v2"
NATIVE_ACTION_MIN_SEED_SUPPORT = 0.98
NATIVE_ACTION_MIN_PATH_SUPPORT = 0.98
OPE_FEATURES = (
    "side",
    "inventory",
    "inventory_ratio",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_mae_so_far",
    "campaign_add_count_so_far",
    "toxicity",
    "markout_ema",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    *CAUSAL_PATH_FEATURE_COLUMNS,
    "quote_distance_bps",
    "quote_distance_ticks",
    "quote_delta_to_bbo_ticks",
    "exact_l2_spread_bps",
    "baseline_visible_queue_ahead",
    "baseline_estimated_queue_ahead",
    "mid",
    "best_bid",
    "best_ask",
)
QUEUE_VALUE_OPE_FEATURES = (
    "side",
    "inventory",
    "inventory_ratio",
    "campaign_age_s",
    "campaign_pnl_so_far",
    "campaign_mae_so_far",
    "campaign_add_count_so_far",
    "order_age_ms",
    "quote_distance_ticks",
    "queue_init",
    "queue_left",
    "queue_fraction_left",
    "queue_local_rank",
    "spread_ticks",
    "book_imbalance",
    "microprice_shift_bps",
    "l2_book_cancel_ratio",
    "l2_book_refresh_ratio",
    "l2_quote_flip_rate",
    "toxicity",
    "markout_ema",
    "maker_expected_ticks",
    "empirical_adverse_probability",
    "empirical_favorable_probability",
    "market_order_intensity",
    "cancel_intensity",
    "refill_intensity",
    "adverse_to_refill_ratio",
    "queue_state_key",
    "microprice_state_key",
)
QUEUE_VALUE_NET_OPE_FEATURES = (
    *QUEUE_VALUE_OPE_FEATURES,
    "queue_recovery_probability",
    "keep_value_bps",
    "cancel_reenter_value_bps",
    "cancel_advantage_bps",
    "queue_reset_option_cost_bps",
    "hazard_favorable_fill_per_s",
    "hazard_adverse_fill_per_s",
    "hazard_cancel_per_s",
    "hazard_adverse_price_jump_per_s",
    "hazard_campaign_repair_per_s",
    "hazard_queue_recovery_per_s",
    "probability_favorable_fill",
    "probability_adverse_fill",
    "probability_cancel",
    "probability_adverse_price_jump",
    "probability_campaign_repair",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _live_like_params(base: dict[str, Any]) -> None:
    base["fill_cooldown_reset_consec_on_expiry"] = False
    base["queue_regime_calibration_enabled"] = True
    base["queue_profile_source"] = (
        "queue_calibration_artifact"
        if base.get("queue_calibration_replay_params")
        else "config_without_queue_artifact"
    )


def validate_action_panel(
    frame: pd.DataFrame,
    *,
    actions: tuple[str, ...] = LOCAL_ACTIONS,
    require_zero_queue_cost: bool = True,
    require_price_bound: bool = True,
) -> None:
    if frame.empty:
        raise ValueError("randomized replay produced no local action interventions")
    required = {
        "day",
        "decision_id",
        "campaign_id",
        "side",
        "inventory_role",
        "action",
        "behavior_propensity",
        "reward",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "reward_identity_error",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"local action panel missing columns: {missing}")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be unique")
    if frame.groupby(["day", "campaign_id"], sort=False).size().max() != 1:
        raise ValueError("each campaign may contain at most one intervention")
    if set(frame["inventory_role"].astype(str)) != {"add"}:
        raise ValueError("the v1 action family may intervene only on add-side orders")
    if set(frame["action"].astype(str)) - set(actions):
        raise ValueError("the panel contains an unregistered local action")
    behavior = frame[[f"behavior_prob_{action}" for action in actions]].apply(
        pd.to_numeric, errors="coerce"
    )
    if not np.isfinite(behavior.to_numpy(dtype=float)).all():
        raise ValueError("behavior probabilities must be finite")
    if not np.allclose(behavior.sum(axis=1), 1.0, atol=1e-10, rtol=0.0):
        raise ValueError("behavior probabilities must sum to one")
    action_index = {action: idx for idx, action in enumerate(actions)}
    logged_indices = np.asarray(
        [action_index[action] for action in frame["action"].astype(str)], dtype=int
    )
    logged_probability = behavior.to_numpy(dtype=float)[np.arange(len(frame)), logged_indices]
    supplied_propensity = pd.to_numeric(frame["behavior_propensity"], errors="coerce").to_numpy(
        dtype=float
    )
    if not np.isfinite(supplied_propensity).all() or not np.allclose(
        supplied_propensity,
        logged_probability,
        atol=1e-10,
        rtol=0.0,
    ):
        raise ValueError("behavior_propensity must match the selected action's exact probability")
    if require_price_bound and "action_delta_ticks" in frame:
        delta_ticks = pd.to_numeric(frame["action_delta_ticks"], errors="coerce")
        if delta_ticks.isna().any() or (delta_ticks.abs() > 1.0 + 1e-8).any():
            raise ValueError("local action price changes must be bounded to one tick")
        baseline_delta = delta_ticks[frame["action"].astype(str) == "baseline"]
        if not np.allclose(baseline_delta, 0.0, atol=1e-10, rtol=0.0):
            raise ValueError("baseline action must be an exact no-op")
    identity_error = pd.to_numeric(frame["reward_identity_error"], errors="coerce").abs()
    if identity_error.isna().any() or float(identity_error.max()) > 1e-9:
        raise ValueError("reward != fill_value - campaign_cost - queue_cost")
    if (
        require_zero_queue_cost
        and not (pd.to_numeric(frame["queue_cost"], errors="coerce") == 0.0).all()
    ):
        raise ValueError("v1 requires zero queue/reset cost by construction")


def annotate_native_action_support(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Mark native queue support without exposing simulator state as features.

    Seed support is known at the intervention. Path support can be lost later
    through a snapshot reset or unresolved same-millisecond ordering, so it is
    reported separately and is not silently dropped from the randomized panel.
    """

    required = {
        "simulator_queue_source",
        "exchange_book_queue_status",
        "exchange_book_queue_path_valid",
        "exchange_book_queue_ambiguous",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            f"native queue action panel is missing support fields: {missing}"
        )
    out = frame.copy()
    native_source = (
        out["simulator_queue_source"].astype(str) == "native_exchange_book"
    )
    seed_status = out["exchange_book_queue_status"].astype(str)
    seed_supported = native_source & seed_status.isin(
        {"exact", "known_zero"}
    )
    if "native_exchange_seed_supported_at_decision" in out.columns:
        seed_supported &= (
            pd.to_numeric(
                out["native_exchange_seed_supported_at_decision"],
                errors="coerce",
            )
            .fillna(0)
            .astype(bool)
        )
    campaign_predecision_supported = pd.Series(
        True,
        index=out.index,
        dtype=bool,
    )
    if "native_campaign_predecision_supported" in out.columns:
        campaign_predecision_supported = (
            pd.to_numeric(
                out["native_campaign_predecision_supported"],
                errors="coerce",
            )
            .fillna(0)
            .astype(bool)
        )
        seed_supported &= campaign_predecision_supported
    path_valid = (
        pd.to_numeric(
            out["exchange_book_queue_path_valid"],
            errors="coerce",
        )
        .fillna(0)
        .astype(bool)
    )
    ambiguous = (
        pd.to_numeric(
            out["exchange_book_queue_ambiguous"],
            errors="coerce",
        )
        .fillna(0)
        .astype(bool)
    )
    outcome_supported = seed_supported & path_valid & ~ambiguous
    invalidated_reason = out.get(
        "exchange_book_queue_invalidated_reason",
        pd.Series("", index=out.index, dtype=object),
    ).fillna("").astype(str)
    support_reason = np.where(
        ~native_source,
        "non_native_simulator_queue",
        np.where(
            ~seed_status.isin({"exact", "known_zero"}),
            "native_seed_unknown",
            np.where(
                ~campaign_predecision_supported,
                "native_campaign_predecision_path_invalid",
                np.where(
                    ambiguous,
                    np.where(
                        invalidated_reason.ne(""),
                        invalidated_reason,
                        "native_path_ambiguous",
                    ),
                    np.where(
                        ~path_valid,
                        np.where(
                            invalidated_reason.ne(""),
                            invalidated_reason,
                            "native_path_invalid",
                        ),
                        "supported",
                    ),
                ),
            ),
        ),
    )
    out["native_exchange_seed_supported"] = seed_supported.astype(np.uint8)
    out["native_exchange_outcome_supported"] = outcome_supported.astype(
        np.uint8
    )
    out["native_exchange_support_reason"] = support_reason

    rows = int(len(out))
    seed_rows = int(seed_supported.sum())
    outcome_rows = int(outcome_supported.sum())
    reason_counts = {
        str(key): int(value)
        for key, value in (
            pd.Series(support_reason).value_counts(dropna=False).items()
        )
    }
    summary = {
        "rows": rows,
        "seed_supported_rows": seed_rows,
        "seed_support_ratio": float(seed_rows / max(rows, 1)),
        "outcome_supported_rows": outcome_rows,
        "outcome_support_ratio": float(outcome_rows / max(rows, 1)),
        "ambiguous_rows": int(ambiguous.sum()),
        "invalid_path_rows": int((seed_supported & ~path_valid).sum()),
        "support_reason_counts": reason_counts,
        "seed_gate": float(seed_rows / max(rows, 1))
        >= NATIVE_ACTION_MIN_SEED_SUPPORT,
        "path_gate": float(outcome_rows / max(rows, 1))
        >= NATIVE_ACTION_MIN_PATH_SUPPORT,
        "simulator_only_not_policy_features": True,
    }
    return out, summary


def native_censoring_reward_bounds(
    frame: pd.DataFrame,
    *,
    actions: tuple[str, ...],
    reward_clip_usdc: tuple[float, float],
    bootstrap_trials: int = 500,
    random_seed: int = 20260720,
) -> dict[str, Any]:
    """Bound randomized arm value without conditioning on path completion."""

    lower_clip, upper_clip = map(float, reward_clip_usdc)
    if not np.isfinite([lower_clip, upper_clip]).all() or lower_clip >= upper_clip:
        raise ValueError("native censoring reward clip must be finite and ordered")
    required = {
        "day",
        "side",
        "action",
        "reward",
        "native_exchange_outcome_supported",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"native censoring bounds missing fields: {missing}")
    baseline_action = str(actions[0])
    candidates = [str(action) for action in actions[1:]]

    def arm_bounds(scope: pd.DataFrame, action: str) -> dict[str, Any]:
        selected = scope.loc[scope["action"].astype(str).eq(action)]
        supported = (
            pd.to_numeric(
                selected["native_exchange_outcome_supported"],
                errors="coerce",
            )
            .fillna(0)
            .astype(bool)
        )
        observed = pd.to_numeric(
            selected.loc[supported, "reward"],
            errors="coerce",
        )
        if observed.isna().any():
            raise ValueError("supported native outcome has missing reward")
        clipped = observed.clip(lower=lower_clip, upper=upper_clip)
        rows = int(len(selected))
        supported_rows = int(supported.sum())
        missing_rows = rows - supported_rows
        observed_sum = float(clipped.sum())
        return {
            "rows": rows,
            "supported_rows": supported_rows,
            "censored_rows": missing_rows,
            "support_ratio": float(supported_rows / max(rows, 1)),
            "observed_clipped_mean": (
                float(clipped.mean()) if supported_rows else math.nan
            ),
            "value_lower": (
                float((observed_sum + missing_rows * lower_clip) / rows)
                if rows
                else math.nan
            ),
            "value_upper": (
                float((observed_sum + missing_rows * upper_clip) / rows)
                if rows
                else math.nan
            ),
        }

    def scope_report(scope: pd.DataFrame) -> dict[str, Any]:
        arm_reports = {
            action: arm_bounds(scope, action) for action in actions
        }
        contrasts: dict[str, Any] = {}
        for candidate in candidates:
            candidate_report = arm_reports[candidate]
            baseline_report = arm_reports[baseline_action]
            lower = float(candidate_report["value_lower"]) - float(
                baseline_report["value_upper"]
            )
            upper = float(candidate_report["value_upper"]) - float(
                baseline_report["value_lower"]
            )
            contrasts[f"{candidate}_minus_{baseline_action}"] = {
                "uplift_lower": lower,
                "uplift_upper": upper,
            }

        days = sorted(scope["day"].astype(str).unique())
        rng = np.random.default_rng(int(random_seed))
        bootstrap: dict[str, list[float]] = {
            key: [] for key in contrasts
        }
        if days and bootstrap_trials > 0:
            day_frames = {
                day: scope.loc[scope["day"].astype(str).eq(day)]
                for day in days
            }
            for _ in range(int(bootstrap_trials)):
                sampled = rng.choice(days, size=len(days), replace=True)
                resampled = pd.concat(
                    [day_frames[str(day)] for day in sampled],
                    ignore_index=True,
                )
                sampled_arms = {
                    action: arm_bounds(resampled, action)
                    for action in actions
                }
                for candidate in candidates:
                    key = f"{candidate}_minus_{baseline_action}"
                    if (
                        sampled_arms[candidate]["rows"] <= 0
                        or sampled_arms[baseline_action]["rows"] <= 0
                    ):
                        continue
                    bootstrap[key].append(
                        float(sampled_arms[candidate]["value_lower"])
                        - float(sampled_arms[baseline_action]["value_upper"])
                    )
        for key, values in bootstrap.items():
            array = np.asarray(values, dtype=float)
            contrasts[key]["lower_bound_bootstrap_p025"] = (
                float(np.quantile(array, 0.025))
                if array.size
                else math.nan
            )
            contrasts[key]["lower_bound_bootstrap_p50"] = (
                float(np.quantile(array, 0.50))
                if array.size
                else math.nan
            )
            contrasts[key]["strict_positive_lower_gate"] = bool(
                array.size
                and float(contrasts[key]["uplift_lower"]) > 0.0
                and float(np.quantile(array, 0.025)) > 0.0
            )
        return {
            "rows": int(len(scope)),
            "days": days,
            "arms": arm_reports,
            "contrasts": contrasts,
        }

    scopes = {"pooled": frame}
    for side in ("BUY", "SELL"):
        scopes[side.lower()] = frame.loc[
            frame["side"].astype(str).str.upper().eq(side)
        ]
    return {
        "schema_version": "native_censoring_reward_bounds.v1",
        "estimand": "clipped_decision_to_terminal_campaign_reward",
        "reward_clip_usdc": [lower_clip, upper_clip],
        "missing_outcome_treatment": (
            "Manski arm bounds; no complete-case filtering"
        ),
        "bootstrap_unit": "UTC_day",
        "bootstrap_trials": int(bootstrap_trials),
        "scopes": {
            name: scope_report(scope)
            for name, scope in scopes.items()
            if not scope.empty
        },
    }


def _clean_summary(result: dict[str, Any], prefix: str) -> dict[str, Any]:
    fields = (
        "pnl",
        "fills_total",
        "fills_bid",
        "fills_ask",
        "n_requotes",
        "decision_place_count",
        "decision_replace_count",
        "decision_keep_count",
        "decision_pause_count",
        "final_inventory",
        "max_inventory",
        "abs_inventory_time_s",
        "campaign_count",
        "campaign_closed_count",
        "campaign_open_count",
        "campaign_max_abs_inventory",
        "campaign_max_duration_s",
        "campaign_max_adverse_excursion",
        "exchange_book_queue_lookup_count",
        "exchange_book_queue_exact_count",
        "exchange_book_queue_known_zero_count",
        "exchange_book_queue_missing_count",
        "exchange_book_queue_invalidated_order_count",
        "exchange_book_queue_ambiguous_event_count",
        "exchange_book_events_consumed",
        "exchange_book_events_accepted",
        "exchange_book_events_rejected",
        "exchange_book_delta_bootstrap_events",
        "exchange_book_source_gap_events",
        "exchange_book_invalid_sequence_messages",
        "exchange_book_sequence_gaps",
        "exchange_book_message_time_reversals",
        "exchange_book_cancel_trade_ambiguous_order_count",
        "exchange_book_cancel_book_ambiguous_order_count",
        "exchange_book_transaction_timestamp_events",
        "exchange_book_event_timestamp_fallback_events",
        "exchange_book_receive_timestamp_fallback_events",
        "exchange_book_unknown_timestamp_source_events",
    )
    return {f"{prefix}_{field}": result.get(field, 0) for field in fields}


def _exchange_book_tape(
    base: dict[str, Any],
    *,
    day: str,
    symbol: str,
) -> CryptoHFTExchangeBookTape | None:
    raw_root = str(base.get("_exchange_book_raw_root", "") or "")
    if not raw_root:
        return None
    return CryptoHFTExchangeBookTape(
        raw_root=Path(raw_root),
        day=day,
        symbol=symbol,
        tick_size=float(base.get("_exchange_book_tick_size", 0.1)),
        exchange=str(
            base.get("_exchange_book_exchange", "binance_futures")
        ),
        warmup_hours=int(base.get("_exchange_book_warmup_hours", 24)),
        strict_complete=bool(
            base.get("_exchange_book_strict_complete", True)
        ),
    )


def _exchange_book_identities(
    base: dict[str, Any],
    *,
    days: list[str],
    symbol: str,
) -> tuple[list[dict[str, Any]], list[Path]]:
    identities: list[dict[str, Any]] = []
    paths: dict[str, Path] = {}
    for day in days:
        tape = _exchange_book_tape(base, day=day, symbol=symbol)
        if tape is None:
            continue
        identities.append(tape.identity(include_sha256=False))
        paths.update({str(path): path for path in tape.source_paths})
    return identities, [paths[key] for key in sorted(paths)]


def _run_day(task: tuple[str, str, dict[str, Any]]) -> dict[str, Any]:
    day, symbol, raw_base = task
    base = dict(raw_base)
    action_family = str(base.get("_randomized_action_family", "local_quote"))
    if action_family in ADD_SKIP_ACTION_FAMILIES:
        base["sell_add_skip_ope_seed"] = int(base.get("sell_add_skip_ope_seed", 20260718)) + int(
            day.replace("-", "")
        )
    elif action_family in QUEUE_VALUE_ACTION_FAMILIES:
        if (
            action_family != "queue_value_net_keep_cancel"
            and base.get("queue_value_competing_risk_bundle_path")
        ):
            raise SystemExit(
                "--queue-competing-risk-bundle is valid only for "
                "queue_value_net_keep_cancel"
            )
        base["queue_value_keep_cancel_seed"] = int(
            base.get("queue_value_keep_cancel_seed", 20260718)
        ) + int(day.replace("-", ""))
    else:
        base["local_action_ope_seed"] = int(base.get("local_action_ope_seed", 20260713)) + int(
            day.replace("-", "")
        )
    model_dir = base.get("resolved_model_dir") or base.get("model_dir")
    bt.configure_symbol(symbol, model_dir_override=model_dir)
    if base.get("_historical_bbo_dir"):
        bt.BBO_DIR = Path(str(base["_historical_bbo_dir"])).resolve()
    if base.get("_historical_l2_dir"):
        bt.L2_DIR = Path(str(base["_historical_l2_dir"])).resolve()
    started = time.perf_counter()
    window = smoke._load_window(day, base)

    control_params = dict(base)
    control_params["local_action_ope_enabled"] = False
    control_params["trace_local_action_ope_max"] = 0
    control_params["sell_add_skip_ope_enabled"] = False
    control_params["trace_sell_add_skip_ope_max"] = 0
    control_params["queue_value_keep_cancel_enabled"] = False
    control_params["trace_queue_value_keep_cancel_max"] = 0
    control = bt._simulate_tick_with_engine(
        "python",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        control_params,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
        exchange_book_event_tape=_exchange_book_tape(
            base,
            day=day,
            symbol=symbol,
        ),
    )
    randomized = bt._simulate_tick_with_engine(
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
        exchange_book_event_tape=_exchange_book_tape(
            base,
            day=day,
            symbol=symbol,
        ),
    )
    trace_key = (
        "_sell_add_skip_ope_trace"
        if action_family in ADD_SKIP_ACTION_FAMILIES
        else (
            "_queue_value_keep_cancel_trace"
            if action_family in QUEUE_VALUE_ACTION_FAMILIES
            else "_local_action_ope_trace"
        )
    )
    actions = [{"day": day, **row} for row in randomized[trace_key]]
    daily = {
        "day": day,
        "runtime_s": time.perf_counter() - started,
        "interventions": len(actions),
        **_clean_summary(control, "control"),
        **_clean_summary(randomized, "randomized"),
    }
    daily["pnl_delta"] = float(daily["randomized_pnl"]) - float(daily["control_pnl"])
    daily["fills_delta"] = int(daily["randomized_fills_total"]) - int(daily["control_fills_total"])
    return {"day": day, "actions": actions, "daily": daily}


def _parse_probabilities(raw: str) -> dict[str, float]:
    return normalize_action_probabilities(json.loads(raw) if raw else None)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", nargs="+", default=[])
    parser.add_argument("--days-file", type=Path, default=None)
    parser.add_argument("--evidence-split-manifest", type=Path, default=None)
    parser.add_argument("--panel-access-decision", type=Path, default=None)
    parser.add_argument(
        "--evidence-panel",
        choices=("development", "validation", "sealed_holdout"),
        default=None,
    )
    parser.add_argument("--allow-sealed-holdout", action="store_true")
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--window-cache-dir", type=Path, default=None)
    parser.add_argument("--refresh-window-cache", action="store_true")
    parser.add_argument("--refresh-partials", action="store_true")
    parser.add_argument("--strict-calibration", action="store_true")
    parser.add_argument(
        "--queue-calibration-artifact",
        type=Path,
        default=None,
        help=(
            "Explicit queue-calibration v3 artifact. Native formal replay "
            "requires a frozen path."
        ),
    )
    parser.add_argument("--live-perf-telemetry", type=Path, default=None)
    parser.add_argument("--live-perf-latency-mode", choices=("avg", "max", "sum"), default="avg")
    parser.add_argument("--latency-profile-id", default="")
    parser.add_argument("--exec-book-visibility-profile", type=Path, default=None)
    parser.add_argument("--exec-book-visibility-profile-id", default="")
    parser.add_argument(
        "--exec-book-visibility-delay-seed",
        type=int,
        default=20260718,
    )
    parser.add_argument("--trace-max", type=int, default=50_000)
    parser.add_argument("--random-seed", type=int, default=20260713)
    parser.add_argument("--queue-model-bundle", type=Path, default=None)
    parser.add_argument(
        "--queue-competing-risk-bundle",
        type=Path,
        default=None,
    )
    parser.add_argument("--queue-hawkes-artifact", type=Path, default=None)
    parser.add_argument("--queue-microprice-artifact", type=Path, default=None)
    parser.add_argument("--queue-fill-horizon-ms", type=int, default=1_000)
    parser.add_argument("--queue-price-jump-ticks", type=float, default=1.0)
    parser.add_argument(
        "--execution-trade-source",
        choices=("aggTrades", "trades"),
        default="trades",
    )
    parser.add_argument("--bbo-dir", type=Path, default=None)
    parser.add_argument("--l2-dir", type=Path, default=None)
    parser.add_argument("--exchange-book-raw-root", type=Path, default=None)
    parser.add_argument(
        "--exchange-book-mode",
        choices=("disabled", "diagnostic", "strict"),
        default="disabled",
    )
    parser.add_argument(
        "--exchange-book-exchange",
        default="binance_futures",
    )
    parser.add_argument(
        "--exchange-book-warmup-hours",
        type=int,
        default=24,
    )
    parser.add_argument(
        "--allow-missing-exchange-book-hours",
        action="store_true",
    )
    parser.add_argument(
        "--panel-role",
        choices=(
            "smoke",
            "development",
            "validation",
            "embargo",
            "later",
            "sealed_holdout",
        ),
        default="smoke",
    )
    parser.add_argument(
        "--action-family",
        choices=(
            "local_quote",
            "sell_add_skip",
            "first_add_skip",
            "campaign_stop_add",
            "queue_value_cancel_reenter",
            "queue_value_keep_cancel",
            "queue_value_net_keep_cancel",
        ),
        default=None,
        help=(
            "Explicit family for an unfrozen smoke run. Formal development, "
            "validation, and holdout runs must obtain the family from an "
            "evidence-split manifest."
        ),
    )
    parser.add_argument(
        "--eligible-sides",
        default="",
        help=(
            "Comma-separated BUY/SELL sides for an unfrozen smoke run. "
            "Formal runs obtain this registry from the evidence split."
        ),
    )
    parser.add_argument(
        "--action-probabilities-json",
        default="",
        help=(
            "Complete JSON mapping; a frozen split supplies its registered vector, "
            "otherwise default is baseline=0.90 and 0.10 split equally."
        ),
    )
    parser.add_argument("--evaluate-ope", action="store_true")
    parser.add_argument("--min-train-days", type=int, default=50)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--min-action-rows", type=int, default=50)
    parser.add_argument("--min-effective-sample-size", type=float, default=100.0)
    parser.add_argument("--bootstrap-trials", type=int, default=500)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.trace_max <= 0:
        raise SystemExit("--trace-max must be positive")
    if args.live_perf_telemetry is not None and not args.latency_profile_id.strip():
        raise SystemExit("--latency-profile-id is required with empirical latency")
    if (args.bbo_dir is None) != (args.l2_dir is None):
        raise SystemExit("--bbo-dir and --l2-dir must be supplied together")
    if (args.exchange_book_raw_root is not None) != (
        args.exchange_book_mode != "disabled"
    ):
        raise SystemExit(
            "--exchange-book-raw-root and a non-disabled "
            "--exchange-book-mode are required together"
        )
    if args.exchange_book_warmup_hours < 0:
        raise SystemExit("--exchange-book-warmup-hours must be non-negative")
    if (
        args.strict_calibration
        and args.exchange_book_mode != "disabled"
        and args.queue_calibration_artifact is None
    ):
        raise SystemExit(
            "native strict replay requires --queue-calibration-artifact"
        )
    evidence_identity: dict[str, Any] = {}
    action_family = "local_quote"
    action_registry = LOCAL_ACTIONS
    if args.evidence_split_manifest is not None:
        if args.action_family is not None or args.eligible_sides.strip():
            raise SystemExit(
                "--action-family/--eligible-sides cannot override a frozen "
                "evidence split"
            )
        if args.days or args.days_file is not None:
            raise SystemExit("--evidence-split-manifest cannot be combined with --days/--days-file")
        if args.evidence_panel is None:
            raise SystemExit("--evidence-panel is required with an evidence split")
        try:
            requested_days, evidence_identity = load_evidence_panel(
                args.evidence_split_manifest,
                args.evidence_panel,
                allow_sealed_holdout=bool(args.allow_sealed_holdout),
                access_decision_path=args.panel_access_decision,
                queue_model_bundle_path=(
                    args.queue_competing_risk_bundle
                    or args.queue_model_bundle
                ),
            )
        except (OSError, ValueError, PermissionError) as exc:
            raise SystemExit(str(exc)) from exc
        frozen_actions = tuple(evidence_identity["actions"])
        if set(frozen_actions) == set(CAMPAIGN_STOP_ADD_ACTIONS):
            if (
                str(evidence_identity.get("family_id", ""))
                != CAMPAIGN_STOP_ADD_FAMILY_ID
            ):
                raise SystemExit(
                    "campaign stop-add actions require the registered family identity"
                )
            action_family = "campaign_stop_add"
            action_registry = CAMPAIGN_STOP_ADD_ACTIONS
            frozen_probabilities = normalize_campaign_stop_add_probabilities(
                evidence_identity["behavior_probabilities"]
            )
        elif set(frozen_actions) == set(SELL_ADD_SKIP_ACTIONS):
            action_family = (
                "first_add_skip"
                if str(evidence_identity.get("family_id", ""))
                == FIRST_ADD_SKIP_FAMILY_ID
                else "sell_add_skip"
            )
            action_registry = SELL_ADD_SKIP_ACTIONS
            frozen_probabilities = normalize_sell_add_skip_probabilities(
                evidence_identity["behavior_probabilities"]
            )
        elif set(frozen_actions) == set(QUEUE_VALUE_CANCEL_REENTER_ACTIONS):
            action_family = "queue_value_cancel_reenter"
            action_registry = QUEUE_VALUE_CANCEL_REENTER_ACTIONS
            frozen_probabilities = (
                normalize_queue_value_cancel_reenter_probabilities(
                    evidence_identity["behavior_probabilities"]
                )
            )
        elif set(frozen_actions) == set(QUEUE_VALUE_KEEP_CANCEL_ACTIONS):
            action_family = (
                "queue_value_net_keep_cancel"
                if str(evidence_identity.get("family_id", ""))
                == QUEUE_VALUE_NET_FAMILY_ID
                else "queue_value_keep_cancel"
            )
            action_registry = QUEUE_VALUE_KEEP_CANCEL_ACTIONS
            frozen_probabilities = normalize_queue_value_probabilities(
                evidence_identity["behavior_probabilities"]
            )
        elif set(frozen_actions).issubset(set(LOCAL_ACTIONS)):
            frozen_raw = {
                action: float(evidence_identity["behavior_probabilities"].get(action, 0.0))
                for action in LOCAL_ACTIONS
            }
            frozen_probabilities = normalize_action_probabilities(
                frozen_raw,
                allow_zero_support=True,
            )
        else:
            raise SystemExit(f"unsupported frozen replay action family: {frozen_actions}")
        if args.action_probabilities_json:
            requested_raw = json.loads(args.action_probabilities_json)
            if action_family == "campaign_stop_add":
                requested_probabilities = normalize_campaign_stop_add_probabilities(
                    requested_raw
                )
            elif action_family in ADD_SKIP_ACTION_FAMILIES:
                requested_probabilities = normalize_sell_add_skip_probabilities(requested_raw)
            elif action_family == "queue_value_cancel_reenter":
                requested_probabilities = (
                    normalize_queue_value_cancel_reenter_probabilities(
                        requested_raw
                    )
                )
            elif action_family in {
                "queue_value_keep_cancel",
                "queue_value_net_keep_cancel",
            }:
                requested_probabilities = normalize_queue_value_probabilities(requested_raw)
            else:
                requested_probabilities = normalize_action_probabilities(
                    {action: float(requested_raw.get(action, 0.0)) for action in LOCAL_ACTIONS},
                    allow_zero_support=True,
                )
        else:
            requested_probabilities = frozen_probabilities
        if requested_probabilities != frozen_probabilities:
            raise SystemExit("runtime action probabilities differ from the frozen action family")
        probabilities = frozen_probabilities
        panel_role = str(args.evidence_panel)
    else:
        if (
            args.evidence_panel is not None
            or args.allow_sealed_holdout
            or args.panel_access_decision is not None
        ):
            raise SystemExit(
                "--evidence-panel/--allow-sealed-holdout/"
                "--panel-access-decision require --evidence-split-manifest"
            )
        requested_days = list(args.days)
        if args.days_file is not None:
            day_frame = pd.read_csv(args.days_file.expanduser().resolve())
            if "day" not in day_frame:
                raise SystemExit("--days-file must contain a day column")
            requested_days.extend(day_frame["day"].astype(str).tolist())
        if not requested_days:
            raise SystemExit("provide --days/--days-file or a frozen evidence split")
        action_family = str(args.action_family or "local_quote")
        if args.action_family is not None and args.panel_role != "smoke":
            raise SystemExit(
                "unfrozen --action-family is smoke-only; formal panels require "
                "--evidence-split-manifest"
            )
        requested_sides = sorted(
            {
                value.strip().upper()
                for value in str(args.eligible_sides).split(",")
                if value.strip()
            }
        )
        if not requested_sides:
            requested_sides = (
                ["SELL"]
                if action_family in {"sell_add_skip", "campaign_stop_add"}
                else (
                    ["BUY"]
                    if action_family in {
                        "first_add_skip",
                        "queue_value_cancel_reenter",
                    }
                    else ["BUY", "SELL"]
                )
            )
        if set(requested_sides) - {"BUY", "SELL"}:
            raise SystemExit("--eligible-sides accepts only BUY and SELL")
        raw_probabilities = (
            json.loads(args.action_probabilities_json)
            if args.action_probabilities_json
            else None
        )
        if action_family == "campaign_stop_add":
            action_registry = CAMPAIGN_STOP_ADD_ACTIONS
            probabilities = normalize_campaign_stop_add_probabilities(
                raw_probabilities
            )
        elif action_family in ADD_SKIP_ACTION_FAMILIES:
            action_registry = SELL_ADD_SKIP_ACTIONS
            probabilities = normalize_sell_add_skip_probabilities(
                raw_probabilities
            )
        elif action_family == "queue_value_cancel_reenter":
            action_registry = QUEUE_VALUE_CANCEL_REENTER_ACTIONS
            probabilities = normalize_queue_value_cancel_reenter_probabilities(
                raw_probabilities
            )
        elif action_family in {
            "queue_value_keep_cancel",
            "queue_value_net_keep_cancel",
        }:
            action_registry = QUEUE_VALUE_KEEP_CANCEL_ACTIONS
            probabilities = normalize_queue_value_probabilities(
                raw_probabilities
            )
        else:
            probabilities = _parse_probabilities(
                args.action_probabilities_json
            )
        evidence_identity = {
            "family_id": "unfrozen_smoke_only",
            "actions": list(action_registry),
            "behavior_probabilities": dict(probabilities),
            "sides": requested_sides,
            "panel": "smoke",
            "sealed_access": False,
        }
        panel_role = str(args.panel_role)
    if (
        action_family in QUEUE_VALUE_ACTION_FAMILIES
        and args.exchange_book_mode == "disabled"
    ):
        raise SystemExit(
            "new queue-value action evidence requires "
            "complete native snapshot/delta simulator state"
        )
    if (
        action_family in QUEUE_VALUE_ACTION_FAMILIES
        and args.allow_missing_exchange_book_hours
    ):
        raise SystemExit(
            "new queue-value action evidence cannot allow missing native "
            "exchange-book hours"
        )
    days = smoke._normalize_days(requested_days)
    config = args.config.expanduser().resolve()
    output_prefix = args.output_prefix.expanduser().resolve()
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    code_identity = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        output_prefix.parent / f"{output_prefix.name}.code_checkpoint",
        repo_root=ROOT,
        code_identity=code_identity,
    )

    bt.configure_symbol(args.symbol)
    base = load_tick_base_params(
        symbol=args.symbol,
        config_path=config,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=args.queue_calibration_artifact,
        strict_calibration=bool(args.strict_calibration),
    )
    _live_like_params(base)
    queue_bundle_path: Path | None = None
    competing_bundle_path: Path | None = None
    competing_bundle: CompetingRiskBundle | None = None
    queue_hawkes_path: Path | None = None
    queue_microprice_path: Path | None = None
    queue_runtime_event_source_expected = ""
    base.update(
        {
            "trace_quotes_max": 0,
            "trace_decisions_max": 0,
            "trace_queue_events_max": 0,
            "trace_fills_max": 0,
            "trace_safe_add_rearm_max": 0,
            "_randomized_action_family": action_family,
            "queue_value_keep_cancel_enabled": False,
            "trace_queue_value_keep_cancel_max": 0,
            "execution_trade_source": str(args.execution_trade_source),
            "exchange_book_queue_mode": str(args.exchange_book_mode),
        }
    )
    if args.bbo_dir is not None:
        base["_historical_bbo_dir"] = str(args.bbo_dir.expanduser().resolve())
        base["_historical_l2_dir"] = str(args.l2_dir.expanduser().resolve())
    if args.exchange_book_raw_root is not None:
        base["_exchange_book_raw_root"] = str(
            args.exchange_book_raw_root.expanduser().resolve()
        )
        base["_exchange_book_tick_size"] = float(
            base.get("tick_size", 0.1)
        )
        base["_exchange_book_exchange"] = str(
            args.exchange_book_exchange
        )
        base["_exchange_book_warmup_hours"] = int(
            args.exchange_book_warmup_hours
        )
        base["_exchange_book_strict_complete"] = not bool(
            args.allow_missing_exchange_book_hours
        )
    if action_family in ADD_SKIP_ACTION_FAMILIES:
        required_sides = (
            ["BUY"] if action_family == "first_add_skip" else ["SELL"]
        )
        if list(evidence_identity.get("sides") or ()) != required_sides:
            raise SystemExit(
                f"{action_family} replay requires a {required_sides[0]}-only split"
            )
        base.update(
            {
                "local_action_ope_enabled": False,
                "trace_local_action_ope_max": 0,
                "sell_add_skip_ope_enabled": True,
                "trace_sell_add_skip_ope_max": int(args.trace_max),
                "sell_add_skip_ope_probabilities": probabilities,
                "sell_add_skip_ope_seed": int(args.random_seed),
                "sell_add_skip_ope_sides": required_sides,
                "sell_add_skip_ope_family_id": str(
                    evidence_identity.get("family_id", "")
                ),
                "sell_add_skip_ope_mode": (
                    "until_flat"
                    if action_family == "campaign_stop_add"
                    else "one_cycle"
                ),
                "sell_add_skip_min_followup_s": (
                    0.0 if action_family == "first_add_skip" else 1_800.0
                ),
            }
        )
    elif action_family in QUEUE_VALUE_ACTION_FAMILIES:
        if (
            action_family == "queue_value_cancel_reenter"
            and list(evidence_identity.get("sides") or ()) != ["BUY"]
        ):
            raise SystemExit(
                "queue_value_cancel_reenter requires a BUY-only frozen split"
            )
        if action_family == "queue_value_net_keep_cancel":
            if args.queue_competing_risk_bundle is None:
                raise SystemExit(
                    "queue_value_net_keep_cancel requires "
                    "--queue-competing-risk-bundle"
                )
            if any(
                value is not None
                for value in (
                    args.queue_model_bundle,
                    args.queue_hawkes_artifact,
                    args.queue_microprice_artifact,
                )
            ):
                raise SystemExit(
                    "--queue-competing-risk-bundle cannot be combined with "
                    "legacy queue-value artifacts"
                )
            competing_bundle_path = (
                args.queue_competing_risk_bundle.expanduser().resolve()
            )
            competing_bundle = CompetingRiskBundle.load(
                competing_bundle_path
            )
            if competing_bundle.family_id != QUEUE_VALUE_NET_FAMILY_ID:
                raise SystemExit(
                    "competing-risk bundle has the wrong frozen family id"
                )
            event_mappings = {
                tuple(
                    sorted(
                        artifact.runtime_queue_artifact.event_columns.items()
                    )
                )
                for artifact in competing_bundle.sides.values()
            }
            if event_mappings != {
                tuple(sorted(NATIVE_EXCHANGE_EVENT_COLUMNS.items()))
            }:
                raise SystemExit(
                    "competing-risk bundle must use native exact-level "
                    "runtime queue events"
                )
            queue_runtime_event_source_expected = (
                "native_exchange_exact_level"
            )
            if args.evidence_split_manifest is not None:
                expected_evidence_hash = str(
                    evidence_identity["manifest_sha256"]
                )
                if (
                    competing_bundle.evidence_split_sha256
                    != expected_evidence_hash
                ):
                    raise SystemExit(
                        "competing-risk bundle is not bound to the frozen "
                        "evidence split"
                    )
                if max(competing_bundle.calibration_days) >= min(
                    requested_days
                ):
                    raise SystemExit(
                        "competing-risk calibration is not strictly earlier "
                        "than the requested evidence panel"
                    )
        elif args.queue_model_bundle is not None:
            if args.queue_hawkes_artifact is not None or args.queue_microprice_artifact is not None:
                raise SystemExit(
                    "--queue-model-bundle cannot be combined with pooled queue artifacts"
                )
            queue_bundle_path = args.queue_model_bundle.expanduser().resolve()
            bundle = QueueValueModelBundle.load(queue_bundle_path)
            bundle_event_mappings = {
                tuple(sorted(model.queue_artifact.event_columns.items()))
                for model in bundle.sides.values()
            }
            if len(bundle_event_mappings) != 1:
                raise SystemExit(
                    "queue-value bundle sides use different runtime event sources"
                )
            bundle_event_columns = dict(next(iter(bundle_event_mappings)))
            if bundle_event_columns == NATIVE_EXCHANGE_EVENT_COLUMNS:
                queue_runtime_event_source_expected = (
                    "native_exchange_exact_level"
                )
            elif bundle_event_columns == EVENT_COLUMNS:
                queue_runtime_event_source_expected = (
                    "policy_visible_top_book"
                )
            else:
                raise SystemExit(
                    "queue-value bundle uses an unsupported runtime event "
                    f"mapping: {bundle_event_columns}"
                )
            if args.evidence_split_manifest is not None:
                expected_evidence_hash = str(
                    evidence_identity["manifest_sha256"]
                )
                split_payload = json.loads(
                    args.evidence_split_manifest.expanduser()
                    .resolve()
                    .read_text(encoding="utf-8")
                )
                expected_source_hash = str(
                    split_payload["source_manifest_sha256"]
                )
                if (
                    bundle.evidence_split_sha256
                    != expected_evidence_hash
                ):
                    raise SystemExit(
                        "queue-value bundle is not bound to the frozen "
                        "evidence split"
                    )
                if (
                    bundle.source_manifest_sha256
                    != expected_source_hash
                ):
                    raise SystemExit(
                        "queue-value bundle is not bound to the frozen "
                        "native source universe"
                    )
                if max(bundle.calibration_days) >= min(requested_days):
                    raise SystemExit(
                        "queue-value calibration is not strictly earlier "
                        "than the requested evidence panel"
                    )
        else:
            if args.queue_hawkes_artifact is None:
                raise SystemExit(
                    "queue-value family requires --queue-model-bundle or --queue-hawkes-artifact"
                )
            if args.queue_microprice_artifact is None:
                raise SystemExit("pooled queue-value fitting requires --queue-microprice-artifact")
            queue_hawkes_path = args.queue_hawkes_artifact.expanduser().resolve()
            queue_microprice_path = args.queue_microprice_artifact.expanduser().resolve()
            queue_artifact = QueueReactiveHawkesArtifact.load(queue_hawkes_path)
            if queue_artifact.event_columns == NATIVE_EXCHANGE_EVENT_COLUMNS:
                queue_runtime_event_source_expected = (
                    "native_exchange_exact_level"
                )
            elif queue_artifact.event_columns == EVENT_COLUMNS:
                queue_runtime_event_source_expected = (
                    "policy_visible_top_book"
                )
            else:
                raise SystemExit(
                    "queue-value artifact uses an unsupported runtime event "
                    f"mapping: {queue_artifact.event_columns}"
                )
        base.update(
            {
                "local_action_ope_enabled": False,
                "trace_local_action_ope_max": 0,
                "sell_add_skip_ope_enabled": False,
                "trace_sell_add_skip_ope_max": 0,
                "queue_value_keep_cancel_enabled": True,
                "queue_value_action_family": action_family,
                "queue_value_keep_cancel_sides": list(
                    evidence_identity.get("sides") or ("BUY", "SELL")
                ),
                "trace_queue_value_keep_cancel_max": int(args.trace_max),
                "queue_value_keep_cancel_probabilities": probabilities,
                "queue_value_keep_cancel_seed": int(args.random_seed),
                "queue_value_model_bundle_path": str(queue_bundle_path or ""),
                "queue_value_competing_risk_bundle_path": str(
                    competing_bundle_path or ""
                ),
                "queue_value_expected_evidence_split_sha256": str(
                    evidence_identity.get("manifest_sha256", "")
                ),
                "queue_value_expected_source_manifest_sha256": (
                    str(bundle.source_manifest_sha256)
                    if queue_bundle_path is not None
                    else ""
                ),
                "queue_value_hawkes_artifact_path": str(queue_hawkes_path or ""),
                "queue_value_microprice_artifact_path": str(queue_microprice_path or ""),
                "queue_value_require_calibration_passed": True,
                "queue_value_fill_horizon_ms": int(args.queue_fill_horizon_ms),
                "queue_value_price_jump_ticks": float(args.queue_price_jump_ticks),
                "replay_event_clock": "merged",
                "replay_clock_interval_ms": 100,
            }
        )
    else:
        base.update(
            {
                "local_action_ope_enabled": True,
                "trace_local_action_ope_max": int(args.trace_max),
                "local_action_ope_probabilities": probabilities,
                "local_action_ope_allow_zero_support": bool(
                    any(value == 0.0 for value in probabilities.values())
                ),
                "local_action_ope_sides": list(evidence_identity.get("sides") or ("BUY", "SELL")),
                "local_action_ope_seed": int(args.random_seed),
                "sell_add_skip_ope_enabled": False,
                "trace_sell_add_skip_ope_max": 0,
            }
        )
    if args.live_perf_telemetry is not None:
        telemetry = args.live_perf_telemetry.expanduser().resolve()
        samples = bt._load_live_perf_latency_samples(telemetry, mode=args.live_perf_latency_mode)
        base["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
        base["_cancel_order_latency_samples_ms"] = samples["cancel_order_latency_samples_ms"]
        base["live_perf_telemetry_path"] = str(telemetry)
        base["live_perf_latency_mode"] = args.live_perf_latency_mode
    if args.exec_book_visibility_profile is not None:
        if not args.exec_book_visibility_profile_id.strip():
            raise SystemExit(
                "--exec-book-visibility-profile-id is required with an "
                "execution-book visibility profile"
            )
        visibility_profile = (
            args.exec_book_visibility_profile.expanduser().resolve()
        )
        visibility = bt._load_exec_book_visibility_profile(visibility_profile)
        base["_exec_book_visibility_delay_samples_ms"] = visibility.pop(
            "exec_book_visibility_delay_samples_ms"
        )
        base.update(visibility)
        base["exec_book_visibility_delay_profile_path"] = str(
            visibility_profile
        )
        base["exec_book_visibility_delay_profile_id"] = str(
            args.exec_book_visibility_profile_id
        )
        base["exec_book_visibility_delay_seed"] = int(
            args.exec_book_visibility_delay_seed
        )
    if args.strict_calibration:
        validate_formal_replay_calibration(base, require_latency=True)
    if args.window_cache_dir:
        base["_window_cache_dir"] = str(args.window_cache_dir.expanduser().resolve())
    if args.refresh_window_cache:
        base["_refresh_window_cache"] = True
    exchange_book_identities, exchange_book_source_paths = (
        _exchange_book_identities(
            base,
            days=days,
            symbol=args.symbol,
        )
    )

    partial_dir = output_prefix.parent / f"{output_prefix.name}.partial"
    partial_dir.mkdir(parents=True, exist_ok=True)
    identity_path = partial_dir / "run_identity.json"
    run_identity = {
        "schema_version": SCHEMA_VERSION,
        "workspace_sha256": code_identity["workspace_sha256"],
        "config_sha256": _sha256(config),
        "days": days,
        "action_family": action_family,
        "probabilities": probabilities,
        "latency_profile_id": args.latency_profile_id,
        "exec_book_visibility_profile_id": str(
            base.get("exec_book_visibility_delay_profile_id", "")
        ),
        "exec_book_visibility_profile_path": str(
            base.get("exec_book_visibility_delay_profile_path", "")
        ),
        "exec_book_visibility_profile_sha256": (
            _sha256(
                Path(
                    str(base["exec_book_visibility_delay_profile_path"])
                )
            )
            if base.get("exec_book_visibility_delay_profile_path")
            else ""
        ),
        "exec_book_visibility_delay_seed": int(
            base.get("exec_book_visibility_delay_seed", 20260718)
        ),
        "random_seed": int(args.random_seed),
        "queue_calibration_path": str(
            Path(str(base.get("queue_calibration_path", ""))).resolve()
            if base.get("queue_calibration_path")
            else ""
        ),
        "queue_calibration_sha256": (
            _sha256(Path(str(base["queue_calibration_path"])))
            if base.get("queue_calibration_path")
            else ""
        ),
        "panel_role": panel_role,
        "evidence_split": evidence_identity,
        "queue_model_bundle": str(queue_bundle_path or ""),
        "queue_model_bundle_sha256": (
            _sha256(queue_bundle_path) if queue_bundle_path is not None else ""
        ),
        "queue_competing_risk_bundle": str(competing_bundle_path or ""),
        "queue_competing_risk_bundle_sha256": (
            _sha256(competing_bundle_path)
            if competing_bundle_path is not None
            else ""
        ),
        "queue_hawkes_artifact": str(queue_hawkes_path or ""),
        "queue_hawkes_sha256": (
            _sha256(queue_hawkes_path) if queue_hawkes_path is not None else ""
        ),
        "queue_microprice_artifact": str(queue_microprice_path or ""),
        "queue_microprice_sha256": (
            _sha256(queue_microprice_path) if queue_microprice_path is not None else ""
        ),
        "queue_fill_horizon_ms": int(args.queue_fill_horizon_ms),
        "queue_price_jump_ticks": float(args.queue_price_jump_ticks),
        "execution_trade_source": str(args.execution_trade_source),
        "exchange_book_mode": str(args.exchange_book_mode),
        "exchange_book_raw_root": (
            str(args.exchange_book_raw_root.expanduser().resolve())
            if args.exchange_book_raw_root is not None
            else ""
        ),
        "exchange_book_exchange": str(args.exchange_book_exchange),
        "exchange_book_warmup_hours": int(
            args.exchange_book_warmup_hours
        ),
        "exchange_book_strict_complete": not bool(
            args.allow_missing_exchange_book_hours
        ),
        "exchange_book_artifacts": exchange_book_identities,
        "bbo_dir": str(base.get("_historical_bbo_dir", bt.BBO_DIR)),
        "l2_dir": str(base.get("_historical_l2_dir", bt.L2_DIR)),
    }
    if identity_path.exists() and not args.refresh_partials:
        existing = json.loads(identity_path.read_text(encoding="utf-8"))
        if existing != run_identity:
            raise RuntimeError("partial output identity differs; use a new prefix or refresh")
    identity_path.write_text(
        json.dumps(run_identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in days:
        action_path = partial_dir / f"{day}.actions.csv"
        daily_path = partial_dir / f"{day}.daily.csv"
        if not args.refresh_partials and action_path.exists() and daily_path.exists():
            actions = pd.read_csv(action_path).to_dict("records")
            daily = pd.read_csv(daily_path).iloc[0].to_dict()
            results.append({"day": day, "actions": actions, "daily": daily})
            print(f"{day}: reused interventions={len(actions)}", flush=True)
        else:
            pending.append(day)

    tasks = [(day, args.symbol, base) for day in pending]
    workers = max(1, min(int(args.workers), max(len(tasks), 1)))
    if workers == 1:
        iterator = map(_run_day, tasks)
        for item in iterator:
            results.append(item)
            pd.DataFrame(item["actions"]).to_csv(
                partial_dir / f"{item['day']}.actions.csv", index=False
            )
            pd.DataFrame([item["daily"]]).to_csv(
                partial_dir / f"{item['day']}.daily.csv", index=False
            )
            print(
                f"{item['day']}: interventions={len(item['actions'])} "
                f"fills_delta={item['daily']['fills_delta']} "
                f"pnl_delta={item['daily']['pnl_delta']:+.4f}",
                flush=True,
            )
    elif tasks:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_run_day, task): task[0] for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                item = future.result()
                results.append(item)
                pd.DataFrame(item["actions"]).to_csv(
                    partial_dir / f"{item['day']}.actions.csv", index=False
                )
                pd.DataFrame([item["daily"]]).to_csv(
                    partial_dir / f"{item['day']}.daily.csv", index=False
                )
                print(f"{item['day']}: interventions={len(item['actions'])}", flush=True)

    results.sort(key=lambda item: item["day"])
    panel = pd.DataFrame([row for item in results for row in item["actions"]])
    daily = pd.DataFrame([item["daily"] for item in results])
    validate_action_panel(
        panel,
        actions=action_registry,
        require_zero_queue_cost=action_family not in QUEUE_VALUE_ACTION_FAMILIES,
        require_price_bound=action_family not in QUEUE_VALUE_ACTION_FAMILIES,
    )
    local_order_value_path: Path | None = None
    native_censoring_bounds_path: Path | None = None
    if action_family in QUEUE_VALUE_ACTION_FAMILIES:
        panel = add_competing_risk_labels(panel)
        validate_randomized_action_panel(
            panel,
            actions=action_registry,
        )
        if "queue_runtime_event_source" not in panel.columns:
            raise ValueError(
                "queue-value action panel is missing queue_runtime_event_source"
            )
        queue_runtime_event_sources_observed = sorted(
            {
                str(value)
                for value in panel["queue_runtime_event_source"]
                .fillna("")
                .astype(str)
                if str(value)
            }
        )
        if queue_runtime_event_sources_observed != [
            queue_runtime_event_source_expected
        ]:
            raise ValueError(
                "queue-value runtime event source does not match the frozen "
                "model artifact: "
                f"expected={queue_runtime_event_source_expected!r}, "
                f"observed={queue_runtime_event_sources_observed!r}"
            )
        panel, native_support = annotate_native_action_support(panel)
        frozen_clip = (
            (evidence_identity.get("action_family") or {}).get(
                "native_censoring_reward_clip_usdc",
                (-50.0, 50.0),
            )
        )
        if not isinstance(frozen_clip, (list, tuple)) or len(frozen_clip) != 2:
            raise ValueError(
                "frozen native censoring reward clip must have two values"
            )
        native_censoring_bounds = native_censoring_reward_bounds(
            panel,
            actions=tuple(action_registry),
            reward_clip_usdc=(
                float(frozen_clip[0]),
                float(frozen_clip[1]),
            ),
            bootstrap_trials=int(args.bootstrap_trials),
            random_seed=int(args.random_seed),
        )
        native_censoring_bounds_path = output_prefix.with_suffix(
            ".native_censoring_bounds.json"
        )
        native_censoring_bounds_path.write_text(
            json.dumps(
                native_censoring_bounds,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        local_order_value_path = output_prefix.with_suffix(".local_order_value_panel.parquet")
        panel.to_parquet(local_order_value_path, index=False)
    else:
        native_support = {}
        native_censoring_bounds = {}
        queue_runtime_event_sources_observed = []

    native_source_integrity = {
        field: int(
            pd.to_numeric(
                daily.get(f"randomized_{field}", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )
        for field in (
            "exchange_book_source_gap_events",
            "exchange_book_invalid_sequence_messages",
            "exchange_book_sequence_gaps",
            "exchange_book_message_time_reversals",
            "exchange_book_receive_timestamp_fallback_events",
            "exchange_book_unknown_timestamp_source_events",
        )
    }
    native_source_integrity["passed"] = not any(
        native_source_integrity[field]
        for field in (
            "exchange_book_source_gap_events",
            "exchange_book_invalid_sequence_messages",
            "exchange_book_sequence_gaps",
            "exchange_book_message_time_reversals",
            "exchange_book_receive_timestamp_fallback_events",
            "exchange_book_unknown_timestamp_source_events",
        )
    )
    ope_block_reason = ""
    promotion_status = "not_evaluated"
    if action_family in QUEUE_VALUE_ACTION_FAMILIES:
        if not bool(native_source_integrity["passed"]):
            ope_block_reason = "native_exchange_source_integrity_failed"
            promotion_status = "diagnostic_only_native_source_integrity"
        elif not bool(native_support.get("seed_gate", False)):
            ope_block_reason = "native_exchange_seed_support_below_gate"
            promotion_status = "diagnostic_only_native_seed_support"
        elif not bool(native_support.get("path_gate", False)):
            ope_block_reason = "native_exchange_path_support_below_gate"
            promotion_status = "diagnostic_only_native_path_support"
        elif int(native_support.get("outcome_supported_rows", 0)) < int(
            native_support.get("rows", 0)
        ):
            # Path support is observed after treatment. Filtering these rows
            # would condition on an action-dependent post-treatment variable,
            # so ordinary DR is not identified without censoring bounds/IPCW.
            ope_block_reason = "action_dependent_native_path_censoring"
            promotion_status = "diagnostic_only_native_path_censoring"
    logged_selectivity: dict[str, Any] = {}
    logged_selectivity_path: Path | None = None
    if action_family in QUEUE_VALUE_ACTION_FAMILIES:
        logged_selectivity = randomized_panel_selectivity(
            panel,
            candidate_action=(
                "cancel_then_baseline_reenter"
                if action_family == "queue_value_cancel_reenter"
                else "cancel_until_state_exit"
            ),
            baseline_action="keep",
            bootstrap_trials=max(100, int(args.bootstrap_trials)),
            random_seed=int(args.random_seed),
        )
        logged_selectivity_path = output_prefix.with_suffix(
            ".toxic_fill_selectivity.json"
        )
        logged_selectivity_path.write_text(
            json.dumps(logged_selectivity, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    panel_path = output_prefix.with_suffix(".action_panel.csv")
    daily_path = output_prefix.with_suffix(".daily.csv")
    metadata_path = output_prefix.with_suffix(".metadata.json")
    days_path = output_prefix.with_suffix(".days.csv")
    panel.to_csv(panel_path, index=False)
    daily.to_csv(daily_path, index=False)
    pd.DataFrame({"day": days}).to_csv(days_path, index=False)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "engine": "python_authoritative_randomized_replay",
        "action_family": action_family,
        "days": days,
        "rows": int(len(panel)),
        "campaigns": int(panel[["day", "campaign_id"]].drop_duplicates().shape[0]),
        "action_counts": {
            str(key): int(value)
            for key, value in panel["action"].value_counts().sort_index().items()
        },
        "behavior_probabilities": probabilities,
        "registered_actions": [
            action for action, probability in probabilities.items() if probability > 0.0
        ],
        "eligible_sides": list(evidence_identity.get("sides") or ("BUY", "SELL")),
        "one_intervention_per_campaign": True,
        "inventory_role": "add_only",
        "reducing_side_modified": False,
        "order_size_modified": False,
        "inventory_limit_modified": False,
        "external_reference_used": False,
        "native_exchange_book_scope": (
            "strategy_independent_exchange_time_simulator_only"
            if args.exchange_book_mode != "disabled"
            else "disabled"
        ),
        "policy_feature_clock": "delayed_strategy_visible_bbo_l2_flow",
        "exchange_book_artifacts": exchange_book_identities,
        "queue_reset_cost": (
            "estimated_value_of_existing_queue_priority_at_decision"
            if action_family in QUEUE_VALUE_ACTION_FAMILIES
            else "zero_by_empty_side_order_eligibility"
        ),
        "reward": "fill_value - campaign_cost - queue_cost",
        "campaign_cost": "accounting residual; not separately identified",
        "reward_target": "decision_to_flat_or_day_end_mtm_with_censoring",
        "censored_rows": int(pd.to_numeric(panel["campaign_censored"]).sum()),
        "reward_identity_error_max": float(
            pd.to_numeric(panel["reward_identity_error"]).abs().max()
        ),
        "config_path": str(config),
        "config_sha256": _sha256(config),
        "execution_trade_source": str(args.execution_trade_source),
        "bbo_dir": str(base.get("_historical_bbo_dir", bt.BBO_DIR)),
        "l2_dir": str(base.get("_historical_l2_dir", bt.L2_DIR)),
        "latency_profile_id": args.latency_profile_id,
        "latency_source": str(args.live_perf_telemetry or "configured_constant"),
        "latency_source_sha256": (
            _sha256(args.live_perf_telemetry.expanduser().resolve())
            if args.live_perf_telemetry is not None
            else ""
        ),
        "queue_calibration_path": str(
            base.get("queue_calibration_path", "")
        ),
        "queue_calibration_sha256": (
            _sha256(Path(str(base["queue_calibration_path"])))
            if base.get("queue_calibration_path")
            else ""
        ),
        "exec_book_visibility_profile_id": str(
            base.get("exec_book_visibility_delay_profile_id", "")
        ),
        "exec_book_visibility_profile_path": str(
            base.get("exec_book_visibility_delay_profile_path", "")
        ),
        "exec_book_visibility_profile_sha256": (
            _sha256(
                Path(
                    str(base["exec_book_visibility_delay_profile_path"])
                )
            )
            if base.get("exec_book_visibility_delay_profile_path")
            else ""
        ),
        "exec_book_visibility_delay_seed": int(
            base.get("exec_book_visibility_delay_seed", 20260718)
        ),
        "exec_book_visibility_delay_sample_count": int(
            len(base.get("_exec_book_visibility_delay_samples_ms", []))
        ),
        "code_checkpoint": checkpoint,
        "workspace_sha256": code_identity["workspace_sha256"],
        "strategy_evidence": False,
        "panel_role": panel_role,
        "evidence_split": evidence_identity,
        "promotion_status": promotion_status,
        "native_action_support": native_support,
        "native_censoring_reward_bounds": native_censoring_bounds,
        "logged_toxic_fill_selectivity": logged_selectivity,
        "native_source_integrity": native_source_integrity,
        "queue_runtime_event_source_expected": (
            queue_runtime_event_source_expected
        ),
        "queue_runtime_event_sources_observed": (
            queue_runtime_event_sources_observed
        ),
        "ope_block_reason": ope_block_reason,
        "queue_model_bundle": str(queue_bundle_path or ""),
        "queue_model_bundle_sha256": (
            _sha256(queue_bundle_path) if queue_bundle_path is not None else ""
        ),
        "queue_hawkes_artifact": str(queue_hawkes_path or ""),
        "queue_hawkes_sha256": (
            _sha256(queue_hawkes_path) if queue_hawkes_path is not None else ""
        ),
        "queue_microprice_artifact": str(queue_microprice_path or ""),
        "queue_microprice_sha256": (
            _sha256(queue_microprice_path) if queue_microprice_path is not None else ""
        ),
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    ope_paths: dict[str, dict[str, str]] = {}
    if args.evaluate_ope and not ope_block_reason:
        for scope, scoped_panel in (
            ("pooled", panel),
            ("buy", panel[panel["side"].astype(str).str.upper() == "BUY"]),
            ("sell", panel[panel["side"].astype(str).str.upper() == "SELL"]),
        ):
            if scoped_panel.empty:
                continue
            for candidate in action_registry:
                candidate_panel = scoped_panel.copy()
                candidate_panel["candidate_action"] = candidate
                rows, folds, actions, summary = evaluate_offline_policy(
                    candidate_panel,
                    feature_names=(
                        QUEUE_VALUE_NET_OPE_FEATURES
                        if action_family == "queue_value_net_keep_cancel"
                        else QUEUE_VALUE_OPE_FEATURES
                        if action_family in QUEUE_VALUE_ACTION_FAMILIES
                        else OPE_FEATURES
                    ),
                    config=OPEConfig(
                        split_mode="chronological",
                        min_train_days=int(args.min_train_days),
                        test_days=int(args.test_days),
                        embargo_days=int(args.embargo_days),
                        min_train_rows=max(500, int(args.min_action_rows) * 8),
                        min_action_rows=int(args.min_action_rows),
                        min_effective_sample_size=float(args.min_effective_sample_size),
                        bootstrap_trials=int(args.bootstrap_trials),
                        random_seed=int(args.random_seed),
                    ),
                )
                key = f"{scope}.{candidate}"
                ope_paths[key] = write_ope_outputs(
                    output_prefix.parent / f"{output_prefix.name}_{scope}_{candidate}",
                    rows,
                    folds,
                    actions,
                    summary,
                )

    artifact_paths = [panel_path, daily_path, metadata_path, days_path]
    if local_order_value_path is not None:
        artifact_paths.append(local_order_value_path)
    if native_censoring_bounds_path is not None:
        artifact_paths.append(native_censoring_bounds_path)
    if logged_selectivity_path is not None:
        artifact_paths.append(logged_selectivity_path)
    manifest = build_manifest(
        {
            "experiment_id": output_prefix.name,
            "engine": "python_authoritative_randomized_replay",
            "config_path": str(config),
            "dataset_manifest_path": str(days_path),
            "feature_schema_version": "local_exact_l2_queue_flow.v1",
            "model_versions": {
                "quote_model_dir": str(base.get("resolved_model_dir", "")),
                "ope": "doubly_robust.v1",
                "queue_value_bundle": str(base.get("queue_value_model_bundle_path", "")),
                "queue_value_competing_risk_bundle": str(
                    base.get("queue_value_competing_risk_bundle_path", "")
                ),
                "queue_hawkes": str(base.get("queue_value_hawkes_artifact_path", "")),
                "empirical_microprice": str(base.get("queue_value_microprice_artifact_path", "")),
                "native_exchange_book": (
                    "native_exchange_book_tape.v1"
                    if args.exchange_book_mode != "disabled"
                    else "disabled"
                ),
            },
            "label_versions": {
                "reward": "decision_to_flat_or_day_end_mtm_with_censoring.v2",
                "fill_value": (
                    f"maker_signed_{int(args.queue_fill_horizon_ms)}ms_usdc.v1"
                    if action_family in QUEUE_VALUE_ACTION_FAMILIES
                    else "maker_signed_30s_usdc.v1"
                ),
                "campaign_cost": "accounting_residual_not_causal.v2",
            },
            "splits": {panel_role: days},
            "baseline_definition": {
                "name": "frozen_operational_baseline_from_config",
                "config_sha256": _sha256(config),
                "latency_profile_id": args.latency_profile_id,
                "exec_book_visibility_profile_id": str(
                    base.get("exec_book_visibility_delay_profile_id", "")
                ),
            },
            "action_definition": {
                "family": action_family,
                "actions": list(action_registry),
                "probabilities": probabilities,
                "eligibility": (
                    "first active exposure-increasing add order with frozen "
                    "V_cancel/reenter minus V_keep above its side-specific "
                    "entry threshold per campaign"
                    if action_family == "queue_value_net_keep_cancel"
                    else "first active exposure-increasing add order in a "
                    "local queue-value adverse state per campaign"
                    if action_family in QUEUE_VALUE_ACTION_FAMILIES
                    else "first empty-order exposure-increasing add opportunity per campaign"
                ),
            },
            "input_paths": [
                str(path)
                for path in (
                    args.live_perf_telemetry,
                    args.exec_book_visibility_profile,
                    args.evidence_split_manifest,
                    args.panel_access_decision,
                    args.queue_calibration_artifact,
                    queue_bundle_path,
                    competing_bundle_path,
                    queue_hawkes_path,
                    queue_microprice_path,
                    *exchange_book_source_paths,
                )
                if path is not None
            ],
            "artifact_paths": [str(path) for path in artifact_paths],
            "metrics": metadata,
            "promotion_status": promotion_status,
            "notes": (
                "No live policy wiring. No external reference features. "
                "Native exchange-time snapshot/delta is hidden simulator state; "
                "the action model remains restricted to delayed policy-visible "
                "BBO/L2/flow features."
            ),
        },
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest_path = output_prefix.with_suffix(".experiment_manifest.json")
    if manifest_path.exists():
        raise FileExistsError(f"refusing to overwrite manifest: {manifest_path}")
    write_manifest(manifest_path, manifest)
    print(
        json.dumps(
            {
                "panel": str(panel_path),
                "daily": str(daily_path),
                "metadata": str(metadata_path),
                "manifest": str(manifest_path),
                "ope": ope_paths,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
