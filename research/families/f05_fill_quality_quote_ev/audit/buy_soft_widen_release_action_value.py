#!/usr/bin/env python3
"""Identify the action value of releasing one BUY soft-widen decision.

This successor does not load the retired BUY fill-selection model. It samples
pre-decision BUY opener/add opportunities where the current baseline is
actually widened, forks the authoritative path once, then returns to the
unchanged baseline policy. The outcome is day-end MTM, which carries all
inventory, queue, cooldown, and campaign continuation caused by that action.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit import (
    causal_v12_toxicity_conditional_p3_reach_gate as replay_support,
)
from strategy.policy_guards import (
    POLICY_REASON_BURST,
    POLICY_REASON_DEFENSE,
    POLICY_REASON_FILL_COOLDOWN,
    POLICY_REASON_INV_LIMIT,
    POLICY_REASON_STALE_HARD,
)


ROOT = replay_support.ROOT
DATA_ROOT = replay_support.DATA_ROOT
IDENTITY = "buy_soft_widen_release_single_decision_action_value_v1"
SCHEMA_VERSION = "narrowgate_buy_soft_widen_release_action_value.v1"
CREATED_DATE = "2026-08-04"
OUTPUT_ROOT = DATA_ROOT / f"reports/{IDENTITY}_20260804"
TRACE_DIR = OUTPUT_ROOT / "opportunity_trace"
OUTCOME_DIR = OUTPUT_ROOT / "single_action_outcomes"
SPEC_PATH = (
    ROOT
    / "research/families/f05_fill_quality_quote_ev/docs/"
    "buy_soft_widen_release_single_decision_action_value_v1_spec_20260804.json"
)

ROLES = ("opener", "add")
SAMPLES_PER_ROLE_DAY = 12
SAMPLING_SEED = 20260804
SPREAD_MULT_CAP = 1.0
ECONOMIC_THRESHOLD_USDC = 0.0001
RIDGE_ALPHA = 10.0
TRAIN_DAYS_INITIAL = 16
OOF_FOLD_DAYS = 6
OOF_FOLDS = 4
BOOTSTRAP_DRAWS = 20_000
BOOTSTRAP_SEED = 2026080451
MIN_MECHANICS_ROWS_PER_ROLE = 100
MIN_MECHANICS_DAYS_PER_ROLE = 20
MIN_SELECTED_ROWS_PER_ROLE = 30
MIN_SELECTED_DAYS_PER_ROLE = 12
HARD_REASON_MASK = (
    POLICY_REASON_FILL_COOLDOWN
    | POLICY_REASON_STALE_HARD
    | POLICY_REASON_DEFENSE
    | POLICY_REASON_BURST
    | POLICY_REASON_INV_LIMIT
)

MODEL_FEATURES = (
    "toxicity_score",
    "toxicity_other_side",
    "pred_direction",
    "pred_return",
    "pred_volatility",
    "inventory_units",
    "baseline_distance_ticks",
    "side_policy_spread_mult",
    "bbo_spread_ticks",
    "bar_spread_bps",
    "l2_microprice_offset_bps",
    "l2_imbalance_l1",
    "l2_imbalance_l5",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_quote_flip_rate",
    "log1p_l2_near_depth_total",
    "price_change_5s",
    "price_change_30s",
    "micro_ret_std",
    "taker_quote_imbalance_5s",
    "taker_quote_imbalance_30s",
    "vpin_30s",
)
ML_FEATURES = (
    "bar_spread_bps",
    "l2_microprice_offset_bps",
    "l2_imbalance_l1",
    "l2_imbalance_l5",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_quote_flip_rate",
    "l2_near_depth_total",
    "price_change_5s",
    "price_change_30s",
    "micro_ret_std",
    "taker_quote_imbalance_5s",
    "taker_quote_imbalance_30s",
    "vpin_30s",
)


def sha256_file(path: Path) -> str:
    return replay_support.sha256_file(path)


def canonical_sha256(payload: Any) -> str:
    return replay_support.canonical_sha256(payload)


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    replay_support.atomic_json(path, payload)


def atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    replay_support.atomic_parquet(path, frame)


def _artifact(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    return {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "bytes": resolved.stat().st_size,
    }


def _stable_key(day: str, decision_ts_ms: int, role: str) -> str:
    raw = f"{SAMPLING_SEED}|{day}|{decision_ts_ms}|BUY|{role}".encode("ascii")
    return hashlib.sha256(raw).hexdigest()


def _extract_features(trace: pd.DataFrame, ml_data: Sequence[Any]) -> pd.DataFrame:
    ml_ts = np.asarray(ml_data[0], dtype=np.int64)
    indexes = np.searchsorted(
        ml_ts,
        trace["prediction_ts_ms"].to_numpy(dtype=np.int64),
        side="left",
    )
    if np.any(indexes >= len(ml_ts)) or not np.array_equal(
        ml_ts[indexes], trace["prediction_ts_ms"].to_numpy(dtype=np.int64)
    ):
        raise RuntimeError("opportunity prediction timestamp is absent from ML overlay")
    feature_payload = ml_data[-1]
    if not isinstance(feature_payload, dict):
        raise TypeError("v12 ML overlay lacks the causal feature dictionary")
    missing = sorted(set(ML_FEATURES) - set(feature_payload))
    if missing:
        raise KeyError(f"v12 ML overlay lacks frozen action-value features: {missing}")

    output = trace.copy()
    output["pred_direction"] = np.asarray(ml_data[1], dtype=float)[indexes]
    output["pred_volatility"] = np.asarray(ml_data[2], dtype=float)[indexes]
    output["pred_return"] = np.asarray(ml_data[3], dtype=float)[indexes]
    output["toxicity_other_side"] = np.asarray(ml_data[5], dtype=float)[indexes]
    for feature in ML_FEATURES:
        output[feature] = np.asarray(feature_payload[feature], dtype=float)[indexes]
    output["inventory_units"] = (
        output["inventory_btc"].to_numpy(dtype=float) / 0.001
    )
    output["bbo_spread_ticks"] = np.rint(
        (output["best_ask"].to_numpy(dtype=float) - output["best_bid"].to_numpy(dtype=float))
        / 0.1
    )
    output["log1p_l2_near_depth_total"] = np.log1p(
        np.maximum(output["l2_near_depth_total"].to_numpy(dtype=float), 0.0)
    )
    return output


def _opportunity_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt

    day = str(payload["day"])
    output_path = Path(str(payload["output_path"])).resolve()
    config_path = Path(str(payload["config_path"])).resolve()
    params = replay_support.build_params(day, config_path)
    params.update(
        {
            "trace_p3_reach_decisions_max": 25_000,
            "conditional_p3_reach_gate_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_soft_widen_release_probe_enabled": False,
            "replay_purpose": "buy_soft_widen_release_opportunity_census",
            "replay_promotion_eligible": False,
        }
    )
    window = replay_support.load_window(day, params)
    started = time.perf_counter()
    result = bt._simulate_tick_with_engine(
        "cpp",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        ml_data=window.ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
    )
    trace = pd.DataFrame(result.get("_p3_reach_decision_trace") or [])
    required = {
        "decision_ts_ms",
        "prediction_ts_ms",
        "side",
        "role",
        "exposure_increasing",
        "baseline_eligible",
        "inventory_btc",
        "toxicity_score",
        "best_bid",
        "best_ask",
        "baseline_price",
        "side_policy_spread_mult",
        "side_policy_allow_post",
        "side_policy_allow_exposure_increase",
        "side_policy_reason_mask",
        "baseline_distance_ticks",
    }
    if trace.empty or not required.issubset(trace.columns):
        raise RuntimeError(f"{day} opportunity trace is empty or incomplete")
    trace.insert(0, "day", day)
    trace = trace.loc[
        trace["side"].eq("BUY")
        & trace["role"].isin(ROLES)
        & trace["exposure_increasing"].astype(bool)
        & trace["baseline_eligible"].astype(bool)
        & trace["side_policy_allow_post"].astype(bool)
        & trace["side_policy_allow_exposure_increase"].astype(bool)
        & (
            trace["side_policy_reason_mask"].astype(np.int64)
            & int(HARD_REASON_MASK)
        ).eq(0)
        & (trace["side_policy_spread_mult"].astype(float) > 1.0 + 1e-12)
    ].copy()
    if trace.duplicated(["decision_ts_ms", "side"]).any():
        raise RuntimeError(f"{day} opportunity trace duplicates a BUY decision")
    trace = _extract_features(trace, window.ml_data)
    trace["sampling_key"] = [
        _stable_key(day, int(ts), str(role))
        for ts, role in zip(trace["decision_ts_ms"], trace["role"])
    ]
    atomic_parquet(output_path, trace)
    return {
        "day": day,
        "rows": int(len(trace)),
        "opener_rows": int(trace["role"].eq("opener").sum()),
        "add_rows": int(trace["role"].eq("add").sum()),
        "path": str(output_path),
        "sha256": sha256_file(output_path),
        "runtime_s": time.perf_counter() - started,
        "economic_outcomes_read": False,
    }


def prepare_opportunities(*, workers: int) -> None:
    if OUTCOME_DIR.exists() and any(OUTCOME_DIR.glob("*.parquet")):
        raise RuntimeError("economic outcomes already exist; opportunity identity is frozen")
    baseline = replay_support.validate_current_baseline()
    days, grade_a, grade_b = replay_support.load_panel()
    TRACE_DIR.mkdir(parents=True, exist_ok=True)
    tasks = [
        {
            "day": day,
            "config_path": str(baseline["config_path"]),
            "output_path": str(TRACE_DIR / f"day={day}.parquet"),
        }
        for day in days
    ]
    summaries: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_opportunity_day_task, task): task["day"] for task in tasks}
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            summaries.append(row)
            print(
                f"opportunity {len(summaries)}/{len(tasks)} {row['day']} "
                f"opener={row['opener_rows']} add={row['add_rows']} "
                f"runtime={row['runtime_s']:.2f}s",
                flush=True,
            )
    summaries.sort(key=lambda row: str(row["day"]))
    trace_manifest = pd.DataFrame(summaries)
    atomic_parquet(OUTPUT_ROOT / "opportunity_trace_manifest.parquet", trace_manifest)

    frame = pd.concat(
        [pd.read_parquet(TRACE_DIR / f"day={day}.parquet") for day in days],
        ignore_index=True,
    )
    sampled = (
        frame.sort_values(["day", "role", "sampling_key"])
        .groupby(["day", "role"], observed=True, sort=False)
        .head(SAMPLES_PER_ROLE_DAY)
        .sort_values(["day", "decision_ts_ms"])
        .reset_index(drop=True)
    )
    sampled.insert(0, "opportunity_id", [f"BSWR-{i:06d}" for i in range(len(sampled))])
    if sampled.empty:
        raise RuntimeError("no frozen BUY soft-widen opportunities")
    atomic_parquet(OUTPUT_ROOT / "opportunity_manifest.parquet", sampled)
    counts = (
        sampled.groupby("role", observed=True)
        .agg(rows=("opportunity_id", "size"), days=("day", "nunique"))
        .reset_index()
        .to_dict(orient="records")
    )
    report = {
        "schema_version": f"{SCHEMA_VERSION}.opportunity",
        "identity": IDENTITY,
        "stage": "outcome_blind_opportunity_freeze",
        "development_days": days,
        "grade_a_days": sorted(grade_a),
        "grade_b_days": sorted(grade_b),
        "census_rows": int(len(frame)),
        "sampled_rows": int(len(sampled)),
        "sampled_counts": counts,
        "samples_per_role_day": SAMPLES_PER_ROLE_DAY,
        "sampling_seed": SAMPLING_SEED,
        "model_features": list(MODEL_FEATURES),
        "trace_manifest": _artifact(OUTPUT_ROOT / "opportunity_trace_manifest.parquet"),
        "opportunity_manifest": _artifact(OUTPUT_ROOT / "opportunity_manifest.parquet"),
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_or_live_authority": False,
    }
    atomic_json(OUTPUT_ROOT / "opportunity_report.json", report)


def _implementation_paths() -> tuple[Path, ...]:
    return (
        Path(__file__).resolve(),
        ROOT / "strategy/buy_soft_widen_release.py",
        ROOT / "models/backtest_tick.py",
        ROOT / "cpp/narrowgate_cpp/tick_replay.hpp",
        ROOT / "cpp/narrowgate_cpp/tick_replay.cpp",
        ROOT / "cpp/narrowgate_cpp/bindings.cpp",
        ROOT / "tests/test_buy_soft_widen_release_action_value.py",
    )


def freeze_spec() -> None:
    if SPEC_PATH.exists():
        raise FileExistsError(f"frozen Spec already exists: {SPEC_PATH}")
    if OUTCOME_DIR.exists() and any(OUTCOME_DIR.glob("*.parquet")):
        raise RuntimeError("cannot freeze Spec after economic outcomes")
    baseline = replay_support.validate_current_baseline()
    days, grade_a, grade_b = replay_support.load_panel()
    opportunity_report = OUTPUT_ROOT / "opportunity_report.json"
    opportunity_manifest = OUTPUT_ROOT / "opportunity_manifest.parquet"
    replay_support.require_file(opportunity_report)
    replay_support.require_file(opportunity_manifest)
    folds = []
    for fold in range(OOF_FOLDS):
        test_start = TRAIN_DAYS_INITIAL + fold * OOF_FOLD_DAYS
        folds.append(
            {
                "fold": fold + 1,
                "train_days": days[:test_start],
                "test_days": days[test_start : test_start + OOF_FOLD_DAYS],
            }
        )
    payload: dict[str, Any] = {
        "schema_version": f"{SCHEMA_VERSION}.spec",
        "identity": IDENTITY,
        "created_date": CREATED_DATE,
        "frozen_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "frozen_before_single_action_economic_outcomes",
        "research_family": "F05_fill_quality_quote_ev",
        "historical_context": {
            "retired_selector_estimand": "P(good_outcome_given_filled_x_pi0)",
            "successor_estimand": (
                "Q_pi0(x,release_one_buy_soft_widen_decision)-"
                "Q_pi0(x,current_baseline_decision)"
            ),
            "legacy_selector_model_or_threshold_used": False,
            "legacy_score_0_44_used": False,
        },
        "baseline": {
            "pointer": _artifact(replay_support.BASELINE_POINTER),
            "identity": _artifact(baseline["identity_path"]),
            "config": _artifact(baseline["config_path"]),
            "baseline_id": str(baseline["pointer"]["baseline_id"]),
            "ml_enabled": True,
            "q90_action_enabled": False,
            "buy_fill_selection_action_enabled": False,
        },
        "action": {
            "side": "BUY",
            "roles_evaluated_independently": list(ROLES),
            "assignment_unit": "one_pre_frozen_canonical_10s_quote_decision",
            "spread_mult_mapping": "min(current_side_policy_spread_mult,1.0)",
            "spread_mult_cap": SPREAD_MULT_CAP,
            "action_duration": "one_quote_decision_only",
            "post_action_policy": "return_to_current_operational_baseline",
            "queue_lifecycle_inventory_cooldown_campaign_regenerated": True,
            "reducing_quote_changed": False,
            "sell_quote_changed_directly": False,
            "candidate_requires_existing_soft_widen": True,
            "old_selector_loaded": False,
        },
        "outcome": {
            "primary": "candidate_minus_baseline_day_end_terminal_mtm_usdc",
            "ownership_start": "target_quote_decision_ts",
            "ownership_end": "UTC_day_end_with_open_inventory_MTM",
            "single_action_per_fork": True,
            "pre_assignment_pnl_cancels_by_identical_path": True,
            "economic_threshold_usdc_per_action": ECONOMIC_THRESHOLD_USDC,
        },
        "development_panel": {
            "days": days,
            "grade_a_days": sorted(grade_a),
            "grade_b_days": sorted(grade_b),
            "historical_development_reuse": True,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "opportunity_sampling": {
            "samples_per_role_day": SAMPLES_PER_ROLE_DAY,
            "seed": SAMPLING_SEED,
            "stable_hash_fields": ["day", "decision_ts_ms", "side", "role"],
            "opportunity_manifest": _artifact(opportunity_manifest),
            "opportunity_report": _artifact(opportunity_report),
        },
        "direct_value_model": {
            "model": "fixed_ridge",
            "ridge_alpha": RIDGE_ALPHA,
            "features": list(MODEL_FEATURES),
            "missing_policy": "training_median",
            "scale_policy": "training_IQR_then_clip_plus_minus_8",
            "roles_fitted_separately": True,
            "oof_folds": folds,
            "selector_rule": f"predicted_delta_usdc >= {ECONOMIC_THRESHOLD_USDC}",
            "hyperparameter_search": False,
        },
        "gates": {
            "mechanics_minimum_rows_per_role": MIN_MECHANICS_ROWS_PER_ROLE,
            "mechanics_minimum_days_per_role": MIN_MECHANICS_DAYS_PER_ROLE,
            "minimum_selected_rows_per_role": MIN_SELECTED_ROWS_PER_ROLE,
            "minimum_selected_days_per_role": MIN_SELECTED_DAYS_PER_ROLE,
            "research_supported": {
                "selected_mean_delta_lcb_gt_economic_threshold": True,
                "selected_day_sum_lcb_gt_zero": True,
                "selected_positive_day_rate_min": 0.55,
            },
            "owner_progression": {
                "selected_mean_delta_point_gt_economic_threshold": True,
                "selected_day_sum_point_gt_zero": True,
                "selected_positive_day_rate_min": 0.50,
                "original_hard_gate_failures_preserved": True,
            },
            "either_path_only_unblocks_new_full_path_policy_identity": True,
            "this_identity_cannot_authorize_action_or_live": True,
        },
        "bootstrap": {
            "unit": "UTC_day",
            "draws": BOOTSTRAP_DRAWS,
            "seed": BOOTSTRAP_SEED,
        },
        "implementation_identities": {
            str(path.relative_to(ROOT)): _artifact(path)
            for path in _implementation_paths()
        },
        "permissions_at_freeze": {
            "prediction_authority": False,
            "action_authority": False,
            "live_authority": False,
        },
    }
    payload["canonical_spec_identity_sha256"] = canonical_sha256(payload)
    atomic_json(SPEC_PATH, payload)


def load_spec() -> dict[str, Any]:
    spec = json.loads(replay_support.require_file(SPEC_PATH).read_text(encoding="utf-8"))
    expected = str(spec.get("canonical_spec_identity_sha256", ""))
    content = dict(spec)
    content.pop("canonical_spec_identity_sha256", None)
    if expected != canonical_sha256(content):
        raise ValueError("frozen action-value Spec identity drift")
    for artifact in spec["implementation_identities"].values():
        replay_support.require_file(Path(str(artifact["path"])), str(artifact["sha256"]))
    opportunity = spec["opportunity_sampling"]["opportunity_manifest"]
    replay_support.require_file(Path(str(opportunity["path"])), str(opportunity["sha256"]))
    return spec


def _outcome_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt

    day = str(payload["day"])
    output_path = Path(str(payload["output_path"])).resolve()
    config_path = Path(str(payload["config_path"])).resolve()
    opportunities = pd.read_parquet(Path(str(payload["opportunity_path"]))).loc[
        lambda frame: frame["day"].eq(day)
    ].copy()
    if opportunities.empty:
        raise RuntimeError(f"{day} has no frozen opportunities")
    params = replay_support.build_params(day, config_path)
    params.update(
        {
            "trace_p3_reach_decisions_max": 0,
            "conditional_p3_reach_gate_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_soft_widen_release_probe_enabled": False,
            "collect_curves": False,
            "replay_purpose": "buy_soft_widen_release_single_action_value",
            "replay_promotion_eligible": False,
        }
    )
    window = replay_support.load_window(day, params)

    def simulate(run_params: Mapping[str, Any]) -> dict[str, Any]:
        return bt._simulate_tick_with_engine(
            "cpp",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            dict(run_params),
            ml_data=window.ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
        )

    started = time.perf_counter()
    baseline = simulate(params)
    rows: list[dict[str, Any]] = []
    for index, opportunity in opportunities.iterrows():
        candidate_params = dict(params)
        candidate_params.update(
            {
                "buy_soft_widen_release_probe_enabled": True,
                "buy_soft_widen_release_probe_apply_candidate": True,
                "buy_soft_widen_release_target_ts_ms": int(opportunity["decision_ts_ms"]),
                "buy_soft_widen_release_target_role": str(opportunity["role"]),
                "buy_soft_widen_release_spread_mult_cap": SPREAD_MULT_CAP,
            }
        )
        candidate = simulate(candidate_params)
        if int(candidate["buy_soft_widen_release_target_reached_count"]) != 1:
            raise RuntimeError(f"{day} target was not reached: {opportunity['opportunity_id']}")
        if int(candidate["buy_soft_widen_release_eligible_count"]) != 1:
            raise RuntimeError(f"{day} target eligibility drift: {opportunity['opportunity_id']}")
        if str(candidate["buy_soft_widen_release_role_observed"]) != str(opportunity["role"]):
            raise RuntimeError(f"{day} target role drift: {opportunity['opportunity_id']}")
        row = opportunity.to_dict()
        row.update(
            {
                "baseline_pnl_usdc": float(baseline["pnl"]),
                "candidate_pnl_usdc": float(candidate["pnl"]),
                "delta_pnl_usdc": float(candidate["pnl"] - baseline["pnl"]),
                "baseline_fills": int(baseline.get("n_fills", 0)),
                "candidate_fills": int(candidate.get("n_fills", 0)),
                "delta_fills": int(candidate.get("n_fills", 0) - baseline.get("n_fills", 0)),
                "baseline_abs_inventory_time_s": float(baseline.get("abs_inventory_time_s", 0.0)),
                "candidate_abs_inventory_time_s": float(candidate.get("abs_inventory_time_s", 0.0)),
                "delta_abs_inventory_time_s": float(
                    candidate.get("abs_inventory_time_s", 0.0)
                    - baseline.get("abs_inventory_time_s", 0.0)
                ),
                "baseline_max_inventory_btc": float(baseline.get("max_inventory", 0.0)),
                "candidate_max_inventory_btc": float(candidate.get("max_inventory", 0.0)),
                "baseline_max_drawdown_usdc": float(baseline.get("max_drawdown", 0.0)),
                "candidate_max_drawdown_usdc": float(candidate.get("max_drawdown", 0.0)),
                "effective_mult": bool(candidate["buy_soft_widen_release_effective_mult_count"]),
                "effective_price": bool(candidate["buy_soft_widen_release_effective_price_count"]),
                "baseline_spread_mult_observed": float(
                    candidate["buy_soft_widen_release_baseline_spread_mult"]
                ),
                "selected_spread_mult_observed": float(
                    candidate["buy_soft_widen_release_selected_spread_mult"]
                ),
                "baseline_bid_price_observed": float(
                    candidate["buy_soft_widen_release_baseline_bid_price"]
                ),
                "candidate_bid_price_observed": float(
                    candidate["buy_soft_widen_release_candidate_bid_price"]
                ),
            }
        )
        rows.append(row)
        if (len(rows) % 6) == 0:
            print(f"{day} outcomes {len(rows)}/{len(opportunities)}", flush=True)
    output = pd.DataFrame(rows)
    atomic_parquet(output_path, output)
    return {
        "day": day,
        "rows": int(len(output)),
        "effective_price_rows": int(output["effective_price"].sum()),
        "path": str(output_path),
        "sha256": sha256_file(output_path),
        "runtime_s": time.perf_counter() - started,
    }


def run_outcomes(*, workers: int) -> None:
    spec = load_spec()
    baseline = replay_support.validate_current_baseline()
    opportunity_path = Path(
        str(spec["opportunity_sampling"]["opportunity_manifest"]["path"])
    )
    opportunities = pd.read_parquet(opportunity_path)
    days = [str(day) for day in spec["development_panel"]["days"]]
    OUTCOME_DIR.mkdir(parents=True, exist_ok=True)
    tasks = [
        {
            "day": day,
            "config_path": str(baseline["config_path"]),
            "opportunity_path": str(opportunity_path),
            "output_path": str(OUTCOME_DIR / f"day={day}.parquet"),
        }
        for day in days
        if opportunities["day"].eq(day).any()
    ]
    summaries: list[dict[str, Any]] = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_outcome_day_task, task): task["day"] for task in tasks}
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            summaries.append(row)
            print(
                f"outcome {len(summaries)}/{len(tasks)} {row['day']} "
                f"rows={row['rows']} effective={row['effective_price_rows']} "
                f"runtime={row['runtime_s']:.1f}s",
                flush=True,
            )
    summaries.sort(key=lambda row: str(row["day"]))
    manifest = pd.DataFrame(summaries)
    if int(manifest["rows"].sum()) != len(opportunities):
        raise RuntimeError("single-action outcome denominator drift")
    atomic_parquet(OUTPUT_ROOT / "single_action_outcome_manifest.parquet", manifest)


def _training_transform(
    train: pd.DataFrame, test: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    train_x = train.loc[:, MODEL_FEATURES].to_numpy(dtype=float)
    test_x = test.loc[:, MODEL_FEATURES].to_numpy(dtype=float)
    median = np.nanmedian(train_x, axis=0)
    median = np.where(np.isfinite(median), median, 0.0)
    q25 = np.nanquantile(train_x, 0.25, axis=0)
    q75 = np.nanquantile(train_x, 0.75, axis=0)
    scale = q75 - q25
    scale = np.where(np.isfinite(scale) & (scale > 1e-12), scale, 1.0)
    train_x = np.where(np.isfinite(train_x), train_x, median)
    test_x = np.where(np.isfinite(test_x), test_x, median)
    train_x = np.clip((train_x - median) / scale, -8.0, 8.0)
    test_x = np.clip((test_x - median) / scale, -8.0, 8.0)
    return train_x, test_x, {
        "median": median.tolist(),
        "scale_iqr": scale.tolist(),
    }


def _cluster_interval(
    frame: pd.DataFrame,
    *,
    value_column: str,
    all_days: Sequence[str],
    per_action: bool,
) -> tuple[float, list[float]]:
    sums = frame.groupby("day", observed=True)[value_column].sum().reindex(all_days, fill_value=0.0)
    counts = frame.groupby("day", observed=True).size().reindex(all_days, fill_value=0)
    if per_action:
        point = float(sums.sum() / max(int(counts.sum()), 1))
    else:
        point = float(sums.mean())
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    picks = rng.integers(0, len(all_days), size=(BOOTSTRAP_DRAWS, len(all_days)))
    sum_values = sums.to_numpy(dtype=float)
    count_values = counts.to_numpy(dtype=float)
    draw_sums = sum_values[picks].sum(axis=1)
    if per_action:
        draw_counts = count_values[picks].sum(axis=1)
        draws = draw_sums / np.maximum(draw_counts, 1.0)
    else:
        draws = draw_sums / len(all_days)
    return point, [float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))]


def evaluate_oof() -> None:
    from sklearn.linear_model import Ridge

    spec = load_spec()
    manifest_path = OUTPUT_ROOT / "single_action_outcome_manifest.parquet"
    replay_support.require_file(manifest_path)
    manifest = pd.read_parquet(manifest_path)
    frames = [
        pd.read_parquet(
            replay_support.require_file(Path(str(row.path)), str(row.sha256))
        )
        for row in manifest.itertuples(index=False)
    ]
    outcomes = pd.concat(frames, ignore_index=True)
    if len(outcomes) != int(pd.read_parquet(
        Path(str(spec["opportunity_sampling"]["opportunity_manifest"]["path"]))
    ).shape[0]):
        raise RuntimeError("OOF outcome denominator drift")
    days = [str(day) for day in spec["development_panel"]["days"]]
    oof_parts: list[pd.DataFrame] = []
    model_identities: list[dict[str, Any]] = []
    for fold in spec["direct_value_model"]["oof_folds"]:
        train_days = [str(day) for day in fold["train_days"]]
        test_days = [str(day) for day in fold["test_days"]]
        for role in ROLES:
            train = outcomes.loc[
                outcomes["day"].isin(train_days)
                & outcomes["role"].eq(role)
                & outcomes["effective_price"].astype(bool)
            ].copy()
            test = outcomes.loc[
                outcomes["day"].isin(test_days)
                & outcomes["role"].eq(role)
                & outcomes["effective_price"].astype(bool)
            ].copy()
            if len(train) < 50 or test.empty:
                raise RuntimeError(
                    f"fold {fold['fold']} {role} lacks direct-value support: "
                    f"train={len(train)} test={len(test)}"
                )
            train_x, test_x, transform = _training_transform(train, test)
            model = Ridge(alpha=RIDGE_ALPHA, fit_intercept=True)
            model.fit(train_x, train["delta_pnl_usdc"].to_numpy(dtype=float))
            part = test.copy()
            part["fold"] = int(fold["fold"])
            part["predicted_delta_pnl_usdc"] = model.predict(test_x)
            part["selected"] = (
                part["predicted_delta_pnl_usdc"] >= ECONOMIC_THRESHOLD_USDC
            )
            oof_parts.append(part)
            model_identities.append(
                {
                    "fold": int(fold["fold"]),
                    "role": role,
                    "train_rows": int(len(train)),
                    "test_rows": int(len(test)),
                    "intercept": float(model.intercept_),
                    "coefficients": [float(value) for value in model.coef_],
                    "transform": transform,
                }
            )
    oof = pd.concat(oof_parts, ignore_index=True).sort_values(
        ["day", "decision_ts_ms", "role"]
    )
    atomic_parquet(OUTPUT_ROOT / "direct_action_value_oof.parquet", oof)
    atomic_json(
        OUTPUT_ROOT / "direct_action_value_oof_models.json",
        {
            "identity": IDENTITY,
            "features": list(MODEL_FEATURES),
            "ridge_alpha": RIDGE_ALPHA,
            "models": model_identities,
        },
    )

    oof_days = days[TRAIN_DAYS_INITIAL:]
    role_reports: dict[str, Any] = {}
    unblocked_research: list[str] = []
    unblocked_owner: list[str] = []
    for role in ROLES:
        role_all = outcomes.loc[
            outcomes["role"].eq(role) & outcomes["effective_price"].astype(bool)
        ].copy()
        role_oof = oof.loc[oof["role"].eq(role)].copy()
        selected = role_oof.loc[role_oof["selected"].astype(bool)].copy()
        mean_delta, mean_ci = _cluster_interval(
            selected,
            value_column="delta_pnl_usdc",
            all_days=oof_days,
            per_action=True,
        )
        daily_delta, daily_ci = _cluster_interval(
            selected,
            value_column="delta_pnl_usdc",
            all_days=oof_days,
            per_action=False,
        )
        daily = (
            selected.groupby("day", observed=True)["delta_pnl_usdc"]
            .sum()
            .reindex(oof_days, fill_value=0.0)
        )
        mechanics = bool(
            len(role_all) >= MIN_MECHANICS_ROWS_PER_ROLE
            and role_all["day"].nunique() >= MIN_MECHANICS_DAYS_PER_ROLE
        )
        selection_support = bool(
            len(selected) >= MIN_SELECTED_ROWS_PER_ROLE
            and selected["day"].nunique() >= MIN_SELECTED_DAYS_PER_ROLE
        )
        positive_rate = float((daily > 0.0).mean())
        hard_gates = {
            "mechanics_support": mechanics,
            "selection_support": selection_support,
            "selected_mean_delta_lcb_gt_economic_threshold": bool(
                mean_ci[0] > ECONOMIC_THRESHOLD_USDC
            ),
            "selected_day_sum_lcb_gt_zero": bool(daily_ci[0] > 0.0),
            "selected_positive_day_rate": bool(positive_rate >= 0.55),
        }
        owner_gates = {
            "mechanics_support": mechanics,
            "selection_support": selection_support,
            "selected_mean_delta_point_gt_economic_threshold": bool(
                mean_delta > ECONOMIC_THRESHOLD_USDC
            ),
            "selected_day_sum_point_gt_zero": bool(daily_delta > 0.0),
            "selected_positive_day_rate": bool(positive_rate >= 0.50),
        }
        research_pass = all(hard_gates.values())
        owner_pass = all(owner_gates.values())
        if research_pass:
            unblocked_research.append(role)
        if owner_pass:
            unblocked_owner.append(role)
        role_reports[role] = {
            "mechanics_rows": int(len(role_all)),
            "mechanics_days": int(role_all["day"].nunique()),
            "oof_rows": int(len(role_oof)),
            "selected_rows": int(len(selected)),
            "selected_days": int(selected["day"].nunique()),
            "selection_rate": float(len(selected) / max(len(role_oof), 1)),
            "selected_mean_delta_usdc_per_action": mean_delta,
            "selected_mean_delta_ci95_day_cluster": mean_ci,
            "selected_delta_usdc_per_oof_day": daily_delta,
            "selected_daily_delta_ci95_day_cluster": daily_ci,
            "selected_positive_day_rate": positive_rate,
            "all_action_mean_delta_usdc": float(role_oof["delta_pnl_usdc"].mean()),
            "hard_gates": hard_gates,
            "owner_gates": owner_gates,
            "research_supported_progression": research_pass,
            "owner_risk_accepted_progression": owner_pass,
        }
    if unblocked_research:
        decision = "register_new_full_path_policy_for_research_supported_roles"
    elif unblocked_owner:
        decision = "owner_may_register_new_full_path_policy_with_permanent_risk_label"
    else:
        decision = "close_buy_soft_widen_release_action_value_successor_development"
    report = {
        "schema_version": f"{SCHEMA_VERSION}.development",
        "identity": IDENTITY,
        "spec": _artifact(SPEC_PATH),
        "outcome_manifest": _artifact(manifest_path),
        "oof_predictions": _artifact(OUTPUT_ROOT / "direct_action_value_oof.parquet"),
        "oof_models": _artifact(OUTPUT_ROOT / "direct_action_value_oof_models.json"),
        "roles": role_reports,
        "research_supported_roles": unblocked_research,
        "owner_risk_accepted_roles": unblocked_owner,
        "decision": decision,
        "legacy_selector_reopened": False,
        "old_selector_model_or_threshold_used": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authority": False,
        "live_authority": False,
    }
    atomic_json(OUTPUT_ROOT / "development_report.json", report)
    atomic_json(
        OUTPUT_ROOT / "development_manifest.json",
        {
            "identity": IDENTITY,
            "spec": _artifact(SPEC_PATH),
            "report": _artifact(OUTPUT_ROOT / "development_report.json"),
            "oof_predictions": _artifact(OUTPUT_ROOT / "direct_action_value_oof.parquet"),
            "oof_models": _artifact(OUTPUT_ROOT / "direct_action_value_oof_models.json"),
            "outcome_manifest": _artifact(manifest_path),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "stage",
        choices=("prepare-opportunities", "freeze-spec", "run-outcomes", "evaluate-oof"),
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.stage == "prepare-opportunities":
        prepare_opportunities(workers=args.workers)
    elif args.stage == "freeze-spec":
        freeze_spec()
    elif args.stage == "run-outcomes":
        run_outcomes(workers=args.workers)
    else:
        evaluate_oof()


if __name__ == "__main__":
    main()
