#!/usr/bin/env python3
"""Development-only randomized full-path replay for the fixed SELL-add price curve."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import time
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models import backtest_tick as bt
from models.audit.experiment_scorecard import (
    CANONICAL_EVIDENCE_SCHEMA_VERSION,
    score_canonical_evidence,
    score_profile_contract,
)
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f09_campaign_action_uplift.audit import (
    lineage_randomized_outcome_contract as outcome_contract,
)
from research.families.f09_campaign_action_uplift.audit import (
    post_cooldown_incremental_inventory_budget_feasibility as budget_execution,
)
from research.families.f09_campaign_action_uplift.audit import (
    randomized_action_contrast as contrast,
)
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as full_path,
)
from research.families.f09_campaign_action_uplift.audit.sell_add_inventory_price_penalty import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "sell_add_inventory_price_penalty_randomized_replay.v1"
FAMILY_ID = "sell_add_inventory_price_penalty_randomized_replay_v1"

OUTCOMES = (
    "reward",
    "negative_terminal_protection",
    "q10_shortfall_protection",
    "campaign_mae_avoidance",
    "repair_event",
    "repair_time_avoidance_s",
    "censoring_avoidance",
    "queue_reset_value",
    "latency_adjusted_value",
    "multilevel_short_loss_protection",
    "max_inventory_avoidance_btc",
    "inventory_time_avoidance_btc_s",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_identity_sha256", None)
    return canonical_sha256(normalized)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_spec(path: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    spec = _load_json(path)
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected SELL-add price-penalty spec schema")
    if spec.get("family_id") != FAMILY_ID:
        raise ValueError("unexpected SELL-add price-penalty family id")
    if canonical_spec_sha256(spec) != str(
        spec.get("canonical_spec_identity_sha256", "")
    ):
        raise ValueError("SELL-add price-penalty canonical spec hash mismatch")
    permissions = spec.get("permissions") or {}
    if not bool(permissions.get("development_outcome_read", False)):
        raise ValueError("Development outcome read was not preregistered")
    for forbidden in (
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden, False)):
            raise ValueError(f"Development spec cannot grant {forbidden}")

    base_identity = spec["base_full_path_contract"]
    base_path = Path(str(base_identity["path"])).expanduser()
    budget_execution.require_identity(
        base_path,
        str(base_identity["sha256"]),
        "base full-path contract",
    )
    base = budget_execution._relocate_value(_load_json(base_path))
    if list(spec["panels"]["development_days"]) != list(
        base["panels"]["development_days"]
    ):
        raise ValueError("SELL-add Development denominator differs from F09 base")
    if len(spec["panels"]["development_days"]) != 40:
        raise ValueError("SELL-add v1 requires exactly 40 Development days")
    grade_a = set(str(day) for day in spec["panels"]["grade_a_days"])
    grade_b = set(str(day) for day in spec["panels"]["grade_b_days"])
    development = set(str(day) for day in spec["panels"]["development_days"])
    if grade_a & grade_b or grade_a | grade_b != development:
        raise ValueError("Grade A/B must be a disjoint partition of Development")
    if spec.get("scorecard_profile") != score_profile_contract(
        "action_execution_v1"
    ):
        raise ValueError("action_execution_v1 score profile was not frozen exactly")
    if spec["behavior_policy"].get("probabilities") != {
        CONTROL_ACTION: 0.5,
        CANDIDATE_ACTION: 0.5,
    }:
        raise ValueError("behavior policy must be exact 0.5/0.5")

    foundation_identity = spec["lineage_outcome_foundation"]
    foundation_path = Path(str(foundation_identity["path"])).expanduser()
    budget_execution.require_identity(
        foundation_path,
        str(foundation_identity["sha256"]),
        "lineage outcome foundation",
    )
    foundation = _load_json(foundation_path)
    outcome_contract.validate_foundation_contract(foundation)
    outcome_contract.validate_action_registration(
        spec["action_registration"], foundation
    )
    return spec, base, foundation


def _validate_identities(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    implementation = spec["implementation_identity"]
    budget_execution.require_identity(
        Path(__file__).resolve(),
        str(implementation["evaluator_sha256"]),
        "SELL-add randomized evaluator",
    )
    for relative, expected in implementation["source_sha256"].items():
        budget_execution.require_identity(
            ROOT / str(relative), str(expected), str(relative)
        )
    for key, label in (
        ("q90_off_mechanics_spec", "q90-OFF mechanics spec"),
        ("q90_off_mechanics_report", "q90-OFF mechanics report"),
        ("q90_off_mechanics_postrun_audit", "q90-OFF mechanics postrun audit"),
    ):
        identity = spec["predecessor_identity"][key]
        budget_execution.require_identity(
            Path(str(identity["path"])), str(identity["sha256"]), label
        )
    for identity, label in (
        (base["operational_config_identity"], "operational config"),
        (base["execution_trade_identity"]["manifest"], "trade manifest"),
        (base["execution_trade_identity"]["quality_report"], "trade quality"),
        (base["source_identity"]["normalized_l2_manifest"], "L2 manifest"),
        (base["source_identity"]["normalized_l2_quality"], "L2 quality"),
        (base["source_identity"]["queue_calibration"], "queue calibration"),
        (base["source_identity"]["p3_artifact"], "P3 artifact"),
        (base["latency_identity"]["samples"], "latency samples"),
        (base["panels"]["source_split"], "source split"),
    ):
        budget_execution.require_identity(
            Path(str(identity["path"])), str(identity["sha256"]), label
        )

    market_manifest = full_path.build_market_source_manifest(base)
    market_identity = budget_execution._market_source_manifest_identity(
        spec, market_manifest
    )
    if not bool(market_identity["rehash_performed"]):
        raise ValueError("formal randomized replay requires source rehash")

    junit_identity = spec["test_identity"]["junit_xml"]
    junit_path = Path(str(junit_identity["path"])).expanduser()
    budget_execution.require_identity(
        junit_path, str(junit_identity["sha256"]), "action contract JUnit"
    )
    junit = full_path.read_junit(junit_path)
    missing = sorted(
        set(spec["test_identity"]["required_test_names"])
        - set(junit["test_names"])
    )
    if not bool(junit["passed"]) or missing:
        raise ValueError(f"SELL-add action contract tests are incomplete: {missing}")
    return market_manifest, junit


def storage_gate(output: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    gate = spec["storage_gate"]
    probe = output.parent if output.parent.exists() else ROOT
    usage = shutil.disk_usage(probe)
    free_gib = float(usage.free) / (1024.0**3)
    estimated = float(gate["estimated_new_output_gib"])
    required = max(
        float(gate["absolute_minimum_free_gib"]),
        float(gate["reserve_free_gib"])
        + float(gate["output_multiple"]) * estimated,
    )
    if free_gib < required:
        raise RuntimeError(
            f"storage gate failed: free={free_gib:.2f} GiB required={required:.2f} GiB"
        )
    return {
        "free_gib_before": free_gib,
        "estimated_new_output_gib": estimated,
        "required_free_gib": required,
        "passed": True,
    }


def _configure_params(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    params = full_path._configure_params(base, day)
    actions = spec["actions"]
    replay = spec["replay_contract"]
    params.update(
        {
            "fill_cooldown_clock_mode": "wall_time",
            "decision_trace_profile": "mechanics_only",
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "sell_add_inventory_price_penalty_enabled": True,
            "sell_add_inventory_price_penalty_seed": int(
                spec["behavior_policy"]["random_seed"]
            ),
            "sell_add_inventory_price_penalty_family_id": FAMILY_ID,
            "sell_add_inventory_price_penalty_probabilities": dict(
                spec["behavior_policy"]["probabilities"]
            ),
            "sell_add_inventory_price_penalty_unit_btc": float(
                actions["candidate"]["inventory_unit_btc"]
            ),
            "sell_add_inventory_price_penalty_step_bps": float(
                actions["candidate"]["step_bps_per_short_unit"]
            ),
            "sell_add_inventory_price_penalty_max_bps": float(
                actions["candidate"]["maximum_penalty_bps"]
            ),
            "trace_sell_add_inventory_price_penalty_max": int(
                replay["trace_campaigns_max_per_day"]
            ),
            "lineage_randomized_outcome_contract_version": "v2",
            "lineage_randomized_family_id": FAMILY_ID,
            "post_cooldown_incremental_inventory_budget_enabled": False,
            "variance_time_lineage_randomized_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "dynamic_fill_hazard_mechanics_telemetry_enabled": False,
            "replay_purpose": "formal",
            "replay_promotion_eligible": False,
        }
    )
    return params


def _run_day(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
    foundation: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    started = time.monotonic()
    params = _configure_params(spec, base, day)
    window = full_path._load_window(base, day, params)
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(str(base["source_identity"]["native_orderbook_root"])),
        day=day,
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
    budget_execution._assert_q90_off_result(result)
    raw_trace = pd.DataFrame(
        result.get("_sell_add_inventory_price_penalty_trace", ())
    )
    raw_events = pd.DataFrame(
        result.get("_sell_add_inventory_price_penalty_event_journal", ())
    )
    if raw_trace.empty:
        raise RuntimeError(f"{day}: no SELL-add action assignments")
    validated = outcome_contract.validate_native_lineage_trace(
        raw_trace,
        foundation,
        event_journal=raw_events,
        producer_audit=result[
            "_sell_add_inventory_price_penalty_trace_audit"
        ],
    )
    source_gap_events = int(result.get("exchange_book_source_gap_events", 0) or 0)
    invalid_sequence = int(
        result.get("exchange_book_invalid_sequence_messages", 0) or 0
    )
    sync_censored = bool(result.get("sync_adjust_censored", False))
    if source_gap_events > 0 or invalid_sequence > 0 or sync_censored:
        validated["support_valid"] = 0
        reasons: list[str] = []
        if source_gap_events > 0:
            reasons.append("native_source_gap")
        if invalid_sequence > 0:
            reasons.append("native_invalid_sequence")
        if sync_censored:
            reasons.append("sync_adjust_unsupported_censor")
        validated["unsupported_reasons"] = "|".join(reasons)
    return {
        "day": str(day),
        "runtime_s": float(time.monotonic() - started),
        "rows": validated.to_dict("records"),
        "events": raw_events.to_dict("records"),
        "summary": {
            "pnl": float(result.get("pnl", 0.0) or 0.0),
            "fills_total": int(result.get("fills_bid", 0) or 0)
            + int(result.get("fills_ask", 0) or 0),
            "campaign_count": int(result.get("campaign_count", 0) or 0),
            "action_assignments": int(len(validated)),
            "control_assignments": int(
                result.get(
                    "sell_add_inventory_price_penalty_control_assignments", 0
                )
                or 0
            ),
            "candidate_assignments": int(
                result.get(
                    "sell_add_inventory_price_penalty_candidate_assignments", 0
                )
                or 0
            ),
            "source_gap_events": source_gap_events,
            "invalid_sequence_messages": invalid_sequence,
            "sync_censored": int(sync_censored),
            "q90_evaluations": int(
                result.get("dynamic_fill_hazard_eval_count", 0) or 0
            ),
            "full_cpp_tick_replay_authority": int(
                bool(
                    result.get(
                        "sell_add_inventory_price_penalty_full_cpp_tick_replay_authority",
                        False,
                    )
                )
            ),
        },
    }


def validate_panel(frame: pd.DataFrame, spec: Mapping[str, Any]) -> None:
    required = {
        "day",
        "decision_id",
        "campaign_id",
        "action",
        "behavior_propensity",
        "decision_to_campaign_terminal_value_usdc",
        "lineage_mae",
        "lineage_max_abs_inventory",
        "inventory_time_btc_s",
        "repair_event",
        "campaign_censored",
        "decision_to_terminal_s",
        "sell_add_fill_count",
        "intervention_fill_count",
        "actual_final_action_change_count",
        "candidate_quote_count",
        "cap_truncation_count",
        "full_cap_truncation_count",
        "requested_penalty_bps_sum",
        "realized_penalty_bps_sum",
        "queue_reset_count",
        "replace_cancel_request_count",
        "multilevel_short_terminal_value_usdc",
        "support_valid",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("SELL-add randomized panel is missing: " + ", ".join(missing))
    if frame.empty or frame.duplicated(["day", "campaign_id"]).any():
        raise ValueError("SELL-add randomized campaign denominator is invalid")
    if set(frame["action"].astype(str)) != {CONTROL_ACTION, CANDIDATE_ACTION}:
        raise ValueError("both SELL-add actions require support")
    if not np.allclose(
        pd.to_numeric(frame["behavior_propensity"], errors="coerce"),
        0.5,
        atol=1e-12,
    ):
        raise ValueError("SELL-add behavior propensity differs from 0.5")
    development = set(str(day) for day in spec["panels"]["development_days"])
    if set(frame["day"].astype(str)) != development:
        raise ValueError("SELL-add run did not cover exact Development")


def derive_outcomes(frame: pd.DataFrame, *, baseline_q10: float) -> pd.DataFrame:
    out = frame.copy()

    def numeric(column: str) -> pd.Series:
        values = pd.to_numeric(out[column], errors="coerce").astype(float)
        if values.isna().any() or not np.isfinite(values.to_numpy()).all():
            raise ValueError(f"non-finite randomized outcome: {column}")
        return values

    reward = numeric("decision_to_campaign_terminal_value_usdc")
    repair = numeric("repair_event").clip(0.0, 1.0)
    censored = numeric("campaign_censored").clip(0.0, 1.0)
    duration = numeric("decision_to_terminal_s").clip(0.0, 1_800.0)
    out["reward"] = reward
    out["negative_terminal_protection"] = reward.clip(upper=0.0)
    out["q10_shortfall_protection"] = (reward - baseline_q10).clip(upper=0.0)
    out["campaign_mae_avoidance"] = numeric("lineage_mae")
    out["repair_event"] = repair
    out["repair_time_avoidance_s"] = -np.where(
        repair > 0.5, duration, 1_800.0
    )
    out["censoring_avoidance"] = -censored
    out["queue_reset_value"] = 0.0
    out["latency_adjusted_value"] = reward
    out["multilevel_short_loss_protection"] = numeric(
        "multilevel_short_terminal_value_usdc"
    )
    out["max_inventory_avoidance_btc"] = -numeric(
        "lineage_max_abs_inventory"
    )
    out["inventory_time_avoidance_btc_s"] = -numeric(
        "inventory_time_btc_s"
    )
    return out


def _binary_day_interval(
    frame: pd.DataFrame,
    column: str,
    *,
    seed: int,
    draws: int,
) -> dict[str, float]:
    daily = frame.groupby("day", sort=True)[column].agg(["sum", "count"])
    estimate = float(daily["sum"].sum() / max(daily["count"].sum(), 1))
    rng = np.random.default_rng(int(seed))
    values = daily.to_numpy(dtype=float)
    samples = np.empty(int(draws), dtype=float)
    for index in range(int(draws)):
        sampled = values[rng.integers(0, len(values), size=len(values))]
        samples[index] = sampled[:, 0].sum() / max(sampled[:, 1].sum(), 1.0)
    return {
        "estimate": estimate,
        "lcb95": float(np.quantile(samples, 0.025)),
        "ucb95": float(np.quantile(samples, 0.975)),
    }


def _retention(frame: pd.DataFrame, column: str) -> float:
    means = frame.groupby("action")[column].mean()
    return float(
        means.get(CANDIDATE_ACTION, math.nan)
        / max(float(means.get(CONTROL_ACTION, 0.0)), 1e-12)
    )


def _arm_tail(frame: pd.DataFrame, column: str) -> dict[str, dict[str, float]]:
    rows: dict[str, dict[str, float]] = {}
    for action in (CONTROL_ACTION, CANDIDATE_ACTION):
        values = pd.to_numeric(
            frame.loc[frame["action"].astype(str).eq(action), column],
            errors="coerce",
        ).dropna()
        q10 = float(values.quantile(0.10))
        rows[action] = {
            "q10": q10,
            "cvar10": float(values[values <= q10].mean()),
            "mean": float(values.mean()),
            "rows": int(len(values)),
        }
    return rows


def _daily_policy_value(
    frame: pd.DataFrame,
    *,
    seed: int,
    draws: int,
) -> dict[str, float]:
    daily_rows: list[tuple[float, float]] = []
    for _, day_frame in frame.groupby("day", sort=True):
        means = day_frame.groupby("action")["reward"].mean()
        if {CONTROL_ACTION, CANDIDATE_ACTION}.issubset(means.index):
            uplift = float(means[CANDIDATE_ACTION] - means[CONTROL_ACTION])
            daily_rows.append((uplift * float(len(day_frame)), float(len(day_frame))))
    values = np.asarray(daily_rows, dtype=float)
    estimate = float(values[:, 0].mean())
    rng = np.random.default_rng(int(seed))
    samples = np.empty(int(draws), dtype=float)
    for index in range(int(draws)):
        selected = values[rng.integers(0, len(values), size=len(values))]
        samples[index] = float(selected[:, 0].mean())
    return {
        "estimate_usdc_per_day": estimate,
        "lcb95_usdc_per_day": float(np.quantile(samples, 0.025)),
        "ucb95_usdc_per_day": float(np.quantile(samples, 0.975)),
        "mean_assignments_per_day": float(values[:, 1].mean()),
    }


def evaluate_scope(
    frame: pd.DataFrame,
    *,
    scope_id: str,
    spec: Mapping[str, Any],
    primary: bool,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    if frame.empty:
        raise ValueError(f"empty evaluation scope: {scope_id}")
    baseline_q10 = float(
        pd.to_numeric(
            frame.loc[
                frame["action"].astype(str).eq(CONTROL_ACTION),
                "decision_to_campaign_terminal_value_usdc",
            ],
            errors="coerce",
        ).quantile(0.10)
    )
    derived = derive_outcomes(frame, baseline_q10=baseline_q10)
    bootstrap = spec["bootstrap"]
    outcomes = {
        outcome: contrast.randomized_itt_contrast(
            derived,
            outcome=outcome,
            baseline_action=CONTROL_ACTION,
            candidate_action=CANDIDATE_ACTION,
            bootstrap_trials=int(bootstrap["draws"]),
            random_seed=int(bootstrap["seed"]) + index,
        )
        for index, outcome in enumerate(OUTCOMES)
    }
    candidate = derived[
        derived["action"].astype(str).eq(CANDIDATE_ACTION)
    ].copy()
    candidate["action_changed"] = (
        pd.to_numeric(
            candidate["actual_final_action_change_count"], errors="coerce"
        ).fillna(0)
        > 0
    ).astype(int)
    action_change = _binary_day_interval(
        candidate,
        "action_changed",
        seed=int(bootstrap["seed"]) + 101,
        draws=int(bootstrap["draws"]),
    )
    fill_retention = _retention(derived, "sell_add_fill_count")
    activity_retention = _retention(derived, "intervention_fill_count")
    candidate_quote_count = float(
        pd.to_numeric(candidate["candidate_quote_count"], errors="coerce").sum()
    )
    cap_truncation_rate = float(
        pd.to_numeric(candidate["cap_truncation_count"], errors="coerce").sum()
        / max(candidate_quote_count, 1.0)
    )
    full_cap_truncation_rate = float(
        pd.to_numeric(
            candidate["full_cap_truncation_count"], errors="coerce"
        ).sum()
        / max(candidate_quote_count, 1.0)
    )
    requested_bps = float(
        pd.to_numeric(
            candidate["requested_penalty_bps_sum"], errors="coerce"
        ).sum()
    )
    realized_to_requested = float(
        pd.to_numeric(
            candidate["realized_penalty_bps_sum"], errors="coerce"
        ).sum()
        / max(requested_bps, 1e-12)
    )
    reward = outcomes["reward"]
    support_valid = pd.to_numeric(
        derived["support_valid"], errors="coerce"
    ).fillna(0)
    ess = min(
        float(value["effective_sample_size"])
        for value in reward["arms"].values()
    )
    gates = spec["family_gates"]
    family_failures: list[str] = []
    if action_change["lcb95"] <= float(
        gates["actual_action_change_rate_lcb_min"]
    ):
        family_failures.append("actual_action_change_rate_lcb_below_gate")
    if fill_retention < float(gates["minimum_sell_add_fill_retention"]):
        family_failures.append("sell_add_fill_retention_below_gate")
    if activity_retention < float(gates["minimum_activity_retention"]):
        family_failures.append("activity_retention_below_gate")
    if cap_truncation_rate > float(gates["maximum_cap_truncation_rate"]):
        family_failures.append("cap_truncation_rate_above_gate")
    if full_cap_truncation_rate > float(
        gates["maximum_full_cap_truncation_rate"]
    ):
        family_failures.append("full_cap_truncation_rate_above_gate")
    if realized_to_requested < float(
        gates["minimum_realized_to_requested_penalty_ratio"]
    ):
        family_failures.append("realized_penalty_ratio_below_gate")
    if (
        float(outcomes["multilevel_short_loss_protection"]["interval"]["p025"])
        <= float(gates["multilevel_short_loss_lcb_must_exceed"])
    ):
        family_failures.append("multilevel_short_loss_lcb_not_positive")
    if (
        float(outcomes["max_inventory_avoidance_btc"]["interval"]["p025"])
        < float(gates["max_inventory_avoidance_lcb_min"])
    ):
        family_failures.append("max_inventory_increase_not_excluded")

    report = {
        "scope_id": scope_id,
        "primary": bool(primary),
        "rows": int(len(derived)),
        "days": int(derived["day"].nunique()),
        "baseline_q10_usdc": baseline_q10,
        "candidate_assignment_rate": float(
            derived["action"].astype(str).eq(CANDIDATE_ACTION).mean()
        ),
        "actual_action_change": action_change,
        "sell_add_fill_retention": fill_retention,
        "activity_retention": activity_retention,
        "cap_diagnostics": {
            "candidate_quote_count": int(candidate_quote_count),
            "cap_truncation_rate": cap_truncation_rate,
            "full_cap_truncation_rate": full_cap_truncation_rate,
            "realized_to_requested_penalty_ratio": realized_to_requested,
        },
        "outcomes": outcomes,
        "tail": _arm_tail(derived, "reward"),
        "policy_value": _daily_policy_value(
            derived,
            seed=int(bootstrap["seed"]) + 202,
            draws=int(bootstrap["draws"]),
        ),
        "churn": {
            column: {
                action: float(value)
                for action, value in derived.groupby("action")[column]
                .mean()
                .to_dict()
                .items()
            }
            for column in (
                "queue_reset_count",
                "replace_cancel_request_count",
                "order_submit_count",
            )
        },
        "family_gate_failures": family_failures,
    }
    if not primary:
        return report, None, None

    metric_aliases = {
        "conditional_net_value": "reward",
        "negative_terminal_protection": "negative_terminal_protection",
        "q10_shortfall_protection": "q10_shortfall_protection",
        "campaign_mae_avoidance": "campaign_mae_avoidance",
        "repair_event": "repair_event",
        "repair_time_avoidance_s": "repair_time_avoidance_s",
        "censoring_avoidance": "censoring_avoidance",
        "queue_reset_value": "queue_reset_value",
        "latency_adjusted_value": "latency_adjusted_value",
    }
    metrics = {
        metric: {
            "estimate": float(outcomes[outcome]["uplift"]),
            "lower_bound": float(outcomes[outcome]["interval"]["p025"]),
            "upper_bound": float(outcomes[outcome]["interval"]["p975"]),
            "daily_positive_rate": float(
                outcomes[outcome]["daily_positive_rate"]
            ),
            "source": f"campaign_randomized_itt.{outcome}",
        }
        for metric, outcome in metric_aliases.items()
    }
    metrics["fills_retention"] = {
        "estimate": fill_retention,
        "source": "candidate_vs_control_mean_sell_add_fill_count",
    }
    evidence = {
        "schema_version": CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": FAMILY_ID,
        "family_id": FAMILY_ID,
        "panel_role": "development",
        "score_profile_contract": dict(spec["scorecard_profile"]),
        "input_identity": {
            "spec_canonical_identity_sha256": str(
                spec["canonical_spec_identity_sha256"]
            ),
            "scope_id": scope_id,
            "grade_a_primary_days": list(spec["panels"]["grade_a_days"]),
        },
        "validity_failures": [],
        "support": {
            "n_rows": int(len(derived)),
            "n_days": int(derived["day"].nunique()),
            "effective_sample_size": float(ess),
            "minimum_behavior_propensity": 0.5,
            "unsupported_mass": float(1.0 - support_valid.mean()),
            "overlap_violations": 0,
            "failures": [],
        },
        "candidate_rate": float(
            derived["action"].astype(str).eq(CANDIDATE_ACTION).mean()
        ),
        "invariant_violations": [],
        "family_gate_failures": family_failures,
        "metrics": metrics,
    }
    scorecard = score_canonical_evidence(
        evidence,
        profile_id="action_execution_v1",
        require_frozen_profile=True,
    )
    return report, evidence, scorecard


def _quality_summary(
    spec: Mapping[str, Any], panel: pd.DataFrame
) -> dict[str, Any]:
    return {
        "formal_days": int(panel["day"].nunique()),
        "grade_a_days": int(len(spec["panels"]["grade_a_days"])),
        "grade_b_days": int(len(spec["panels"]["grade_b_days"])),
        "grade_b_used_in_primary_scorecard": False,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec, base, foundation = _load_spec(spec_path)
    market_manifest, junit = _validate_identities(spec, base)
    disk = storage_gate(output, spec)
    days = [str(day) for day in spec["panels"]["development_days"]]
    if len(days) != 40:
        raise ValueError("formal SELL-add action identity requires exactly 40 days")
    if output.exists() and not args.resume:
        raise FileExistsError(f"output directory already exists: {output}")
    output.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output / "day_checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    spec_hash = sha256_file(spec_path)
    (output / "market_source_manifest.json").write_text(
        json.dumps(market_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in days:
        checkpoint = checkpoint_dir / f"{day}.json"
        if args.resume and checkpoint.is_file():
            payload = _load_json(checkpoint)
            if payload.get("spec_sha256") != spec_hash:
                raise ValueError(f"checkpoint spec mismatch: {checkpoint}")
            results.append(payload["result"])
        else:
            pending.append(day)
    workers = max(1, min(int(args.workers), len(pending) or 1))
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    if workers == 1:
        for day in pending:
            result = _run_day(spec, base, foundation, day)
            results.append(result)
            (checkpoint_dir / f"{day}.json").write_text(
                json.dumps(
                    {"spec_sha256": spec_hash, "result": result},
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_run_day, spec, base, foundation, day): day
                for day in pending
            }
            for future in as_completed(futures):
                day = futures[future]
                result = future.result()
                results.append(result)
                (checkpoint_dir / f"{day}.json").write_text(
                    json.dumps(
                        {"spec_sha256": spec_hash, "result": result},
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                print(
                    json.dumps(
                        {"completed_day": day, "runtime_s": result["runtime_s"]}
                    )
                )

    results.sort(key=lambda row: str(row["day"]))
    panel = pd.DataFrame(
        [row for result in results for row in result.get("rows", ())]
    ).sort_values(["day", "campaign_id"], kind="stable")
    events = pd.DataFrame(
        [row for result in results for row in result.get("events", ())]
    ).sort_values(["day", "lineage_id", "event_seq"], kind="stable")
    daily = pd.DataFrame(
        [
            {"day": result["day"], "runtime_s": result["runtime_s"], **result["summary"]}
            for result in results
        ]
    ).sort_values("day", kind="stable")
    validate_panel(panel, spec)
    if set(daily["day"].astype(str)) != set(days):
        raise ValueError("formal run did not complete all frozen Development days")
    if int(daily["q90_evaluations"].sum()) != 0:
        raise RuntimeError("q90 OFF contract emitted evaluations")
    if int(daily["full_cpp_tick_replay_authority"].sum()) != 0:
        raise RuntimeError("full C++ tick replay authority must remain false")

    grade_a = panel[panel["day"].isin(spec["panels"]["grade_a_days"])].copy()
    grade_b = panel[panel["day"].isin(spec["panels"]["grade_b_days"])].copy()
    grade_a_report, evidence, scorecard = evaluate_scope(
        grade_a,
        scope_id="grade_a_primary",
        spec=spec,
        primary=True,
    )
    grade_b_report, _, _ = evaluate_scope(
        grade_b,
        scope_id="grade_b_sensitivity",
        spec=spec,
        primary=False,
    )
    all_report, _, _ = evaluate_scope(
        panel,
        scope_id="all_40_descriptive",
        spec=spec,
        primary=False,
    )
    if evidence is None or scorecard is None:
        raise RuntimeError("Grade A primary scorecard was not generated")
    sensitivity = spec["grade_b_sensitivity_gates"]
    grade_b_failures: list[str] = []
    if float(grade_b_report["outcomes"]["reward"]["uplift"]) < float(
        sensitivity["minimum_reward_point_estimate"]
    ):
        grade_b_failures.append("grade_b_reward_point_estimate_negative")
    if float(
        grade_b_report["outcomes"]["multilevel_short_loss_protection"][
            "uplift"
        ]
    ) < float(sensitivity["minimum_multilevel_loss_point_estimate"]):
        grade_b_failures.append("grade_b_multilevel_loss_point_estimate_negative")
    if grade_b_report["sell_add_fill_retention"] < float(
        sensitivity["minimum_sell_add_fill_retention"]
    ):
        grade_b_failures.append("grade_b_sell_add_fill_retention_below_gate")

    primary_pass = bool(scorecard["ranking_eligible"])
    decision = (
        "development_passed_validation_locked"
        if primary_pass and not grade_b_failures
        else "close_sell_add_inventory_price_penalty_on_development"
    )

    panel_path = output / "campaign_randomized_panel.parquet"
    events_path = output / "campaign_event_journal.parquet"
    daily_path = output / "daily_summary.csv"
    evidence_path = output / "canonical_evidence.json"
    scorecard_path = output / "scorecard.json"
    report_path = output / "report.json"
    panel.to_parquet(panel_path, index=False)
    events.to_parquet(events_path, index=False)
    daily.to_csv(daily_path, index=False)
    evidence_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    scorecard_path.write_text(
        json.dumps(scorecard, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path), "sha256": spec_hash},
        "panel_role": "development",
        "development_days": days,
        "quality": _quality_summary(spec, panel),
        "storage_gate": disk,
        "grade_a_primary": grade_a_report,
        "grade_b_sensitivity": grade_b_report,
        "all_40_descriptive": all_report,
        "grade_b_gate_failures": grade_b_failures,
        "scorecard": {
            "ranking_eligible": bool(scorecard["ranking_eligible"]),
            "ranking_score": scorecard["ranking_score"],
            "promotion_status": scorecard["promotion_status"],
            "hard_gate_failures": scorecard["hard_gates"]["failures"],
            "support_failures": scorecard["support"]["failures"],
        },
        "decision": decision,
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
            "full_cpp_tick_replay_authority": False,
        },
        "test_evidence": junit,
        "artifacts": {},
    }
    artifacts = {
        "panel": panel_path,
        "events": events_path,
        "daily": daily_path,
        "canonical_evidence": evidence_path,
        "scorecard": scorecard_path,
        "market_source_manifest": output / "market_source_manifest.json",
    }
    report["artifacts"] = {
        name: {"path": str(path), "sha256": sha256_file(path)}
        for name, path in artifacts.items()
    }
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest = {
        "schema_version": "sell_add_inventory_price_penalty_manifest.v1",
        "family_id": FAMILY_ID,
        "decision": decision,
        "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        "permissions": report["permissions"],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"decision": decision, "report": str(report_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
