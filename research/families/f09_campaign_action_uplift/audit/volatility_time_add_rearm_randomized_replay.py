#!/usr/bin/env python3
"""Development-only randomized full-path replay for variance-time add rearm.

The behavior action is assigned at the first eligible exposure-increasing fill
of a same-side cooldown lineage. Assignment precedes every downstream quote,
cancel, ACK, queue, and fill outcome and remains fixed until an opposite-side
fill, explicit reset, or the daily fresh-start censor.
"""

from __future__ import annotations

import argparse
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
    randomized_action_contrast as contrast,
)
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as full_path,
)
from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "volatility_time_add_rearm_randomized_replay.v1"
FAMILY_ID = "volatility_time_add_rearm_randomized_replay_v1"
SIDES = ("BUY", "SELL")
OUTCOMES = (
    "reward",
    "terminal_campaign_value",
    "negative_terminal_protection",
    "q10_shortfall_protection",
    "campaign_mae_avoidance",
    "repair_event",
    "repair_time_avoidance_s",
    "censoring_avoidance",
    "queue_reset_value",
    "latency_adjusted_value",
    "inventory_time_avoidance_btc_s",
)


def canonical_spec_identity_sha256(spec: Mapping[str, Any]) -> str:
    """Hash the frozen contract without requiring an impossible file self-hash."""

    payload = {
        key: value
        for key, value in spec.items()
        if key != "canonical_spec_identity_sha256"
    }
    return full_path.canonical_sha256(payload)


def _strict_bool_series(values: pd.Series, *, label: str) -> pd.Series:
    if pd.api.types.is_bool_dtype(values.dtype):
        return values.astype(bool)
    normalized = values.astype(str).str.strip().str.lower()
    mapping = {
        "1": True,
        "true": True,
        "yes": True,
        "0": False,
        "false": False,
        "no": False,
    }
    unknown = sorted(set(normalized) - set(mapping))
    if unknown:
        raise ValueError(f"{label} contains invalid booleans: {unknown}")
    return normalized.map(mapping).astype(bool)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_spec(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    spec = _load_json(path)
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected randomized replay spec schema")
    expected_spec_identity = str(spec.get("canonical_spec_identity_sha256", ""))
    actual_spec_identity = canonical_spec_identity_sha256(spec)
    if expected_spec_identity != actual_spec_identity:
        raise ValueError(
            "randomized spec canonical identity mismatch: "
            f"expected={expected_spec_identity}, actual={actual_spec_identity}"
        )
    permissions = spec.get("permissions") or {}
    forbidden = (
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
        "aws_receive_time_transport_supported",
        "full_cpp_tick_replay_authority",
    )
    enabled = [key for key in forbidden if bool(permissions.get(key, False))]
    if enabled:
        raise ValueError("randomized Development spec exceeds authority: " + ", ".join(enabled))
    if spec.get("scorecard_profile") != score_profile_contract("action_execution_v1"):
        raise ValueError("action_execution_v1 score profile was not frozen exactly")
    behavior = spec.get("behavior_policy") or {}
    probabilities = behavior.get("probabilities") or {}
    if probabilities != {
        LINEAGE_CONTROL_ACTION: 0.5,
        LINEAGE_CANDIDATE_ACTION: 0.5,
    }:
        raise ValueError("behavior policy must be exact 0.5/0.5")
    base_identity = spec["base_full_path_contract"]
    base_path = Path(base_identity["path"])
    full_path.require_identity(base_path, base_identity["sha256"], "base full-path spec")
    base = _load_json(base_path)
    if list(spec["panels"]["development_days"]) != list(
        base["panels"]["development_days"]
    ):
        raise ValueError("randomized Development denominator differs from F09 base")
    if len(spec["panels"]["development_days"]) != 40:
        raise ValueError("randomized v1 requires exactly 40 frozen Development days")
    return spec, base


def _validate_identities(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    implementation = spec["implementation_identity"]
    full_path.require_identity(
        Path(__file__).resolve(), implementation["evaluator_sha256"], "evaluator"
    )
    for relative, expected in implementation["source_sha256"].items():
        full_path.require_identity(ROOT / relative, expected, relative)
    for key in (
        "predecessor_cpp_q90_report",
        "predecessor_cpp_q90_postrun_audit",
        "quality_ledger",
    ):
        identity = spec[key]
        full_path.require_identity(Path(identity["path"]), identity["sha256"], key)
    for identity, label in (
        (base["operational_config_identity"], "operational config"),
        (base["execution_trade_identity"]["manifest"], "trade manifest"),
        (base["execution_trade_identity"]["quality_report"], "trade quality report"),
        (base["source_identity"]["normalized_l2_manifest"], "normalized L2 manifest"),
        (base["source_identity"]["normalized_l2_quality"], "normalized L2 quality"),
        (base["source_identity"]["queue_calibration"], "queue calibration"),
        (base["source_identity"]["p3_artifact"], "P3 artifact"),
        (base["latency_identity"]["samples"], "latency samples"),
        (base["buy_q90_identity"]["model"], "BUY q90 model"),
        (base["buy_q90_identity"]["policy"], "BUY q90 policy"),
        (base["panels"]["source_split"], "source split"),
    ):
        full_path.require_identity(Path(identity["path"]), identity["sha256"], label)
    market_manifest = full_path.build_market_source_manifest(base)
    actual_market_hash = full_path.canonical_sha256(market_manifest)
    expected_market_hash = str(
        base["source_identity"]["market_source_manifest_canonical_sha256"]
    )
    if actual_market_hash != expected_market_hash:
        raise ValueError("market source manifest changed after randomization freeze")
    junit_identity = spec["test_identity"]["junit_xml"]
    junit_path = Path(junit_identity["path"])
    full_path.require_identity(junit_path, junit_identity["sha256"], "contract JUnit")
    junit = full_path.read_junit(junit_path)
    required = set(spec["test_identity"]["required_test_names"])
    missing = sorted(required - set(junit["test_names"]))
    if not junit["passed"] or missing:
        raise ValueError(f"randomized lineage contract tests are incomplete: {missing}")
    return market_manifest, junit


def storage_gate(output: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    gate = spec["storage_gate"]
    usage = shutil.disk_usage(output.parent if output.parent.exists() else ROOT)
    free_gib = float(usage.free) / (1024.0**3)
    estimated_gib = float(gate["estimated_new_output_gib"])
    required_gib = max(
        float(gate["absolute_minimum_free_gib"]),
        float(gate["reserve_free_gib"])
        + float(gate["output_multiple"]) * estimated_gib,
    )
    if free_gib < required_gib:
        raise RuntimeError(
            f"storage gate failed: free={free_gib:.2f} GiB required={required_gib:.2f} GiB"
        )
    return {
        "free_gib_before": free_gib,
        "estimated_new_output_gib": estimated_gib,
        "required_free_gib": required_gib,
        "passed": True,
    }


def _configure_params(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    params = full_path._configure_params(base, day)
    clock = base["variance_clock"]
    behavior = spec["behavior_policy"]
    params.update(
        {
            "fill_cooldown_clock_mode": "randomized_lineage",
            "variance_time_lineage_randomized_enabled": True,
            "variance_time_lineage_probabilities": dict(behavior["probabilities"]),
            "variance_time_lineage_seed": int(behavior["random_seed_base"])
            + int(day.replace("-", "")),
            "trace_variance_time_lineage_max": int(
                spec["replay_contract"]["trace_lineages_max_per_day"]
            ),
            "variance_time_lineage_markout_horizon_ms": int(
                spec["reward_contract"]["fill_value_horizon_ms"]
            ),
            "variance_time_lineage_markout_max_age_ms": int(
                spec["reward_contract"]["markout_max_book_age_ms"]
            ),
            "variance_time_lineage_fail_on_q90_pre_ack_fill": True,
            "variance_time_reference_rate_buy_bps2_per_s": float(
                clock["reference_rate_bps2_per_s"]["BUY"]
            ),
            "variance_time_reference_rate_sell_bps2_per_s": float(
                clock["reference_rate_bps2_per_s"]["SELL"]
            ),
            "variance_time_minimum_wall_time_ms": int(clock["minimum_wall_time_ms"]),
            "variance_time_maximum_wall_time_ms": int(clock["maximum_wall_time_ms"]),
            "variance_time_max_feature_age_ms": int(clock["max_feature_age_ms"]),
            "dynamic_fill_hazard_cpp_parity_enabled": True,
            "dynamic_fill_hazard_cpp_parity_strict": True,
            "dynamic_fill_hazard_cpp_parity_trace_max": int(
                spec["replay_contract"]["cpp_q90_mismatch_trace_max"]
            ),
            "decision_trace_profile": "mechanics_only",
            "window_cache_write_enabled": False,
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "replay_purpose": "formal",
            "replay_promotion_eligible": False,
        }
    )
    return params


def _run_day(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    started = time.monotonic()
    params = _configure_params(spec, base, day)
    window = full_path._load_window(base, day, params)
    variance = full_path._variance_time_data(base, day)
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(base["source_identity"]["native_orderbook_root"]),
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
        variance_time_data=variance,
    )
    if not bool(result.get("dynamic_fill_hazard_cpp_parity_passed", False)):
        raise RuntimeError(f"BUY q90 C++ lockstep failed on {day}")
    if int(result.get("dynamic_fill_hazard_cpp_mismatch_count", 0) or 0) != 0:
        raise RuntimeError(f"BUY q90 C++ mismatch was nonzero on {day}")
    if bool(result.get("dynamic_fill_hazard_full_cpp_tick_replay_authority", True)):
        raise RuntimeError("full C++ tick replay authority must remain false")
    expected_module = str(spec["native_module_identity"]["sha256"])
    actual_module = str(
        (result.get("dynamic_fill_hazard_cpp_identity") or {}).get(
            "native_module_sha256", ""
        )
    )
    if actual_module != expected_module:
        raise RuntimeError(
            f"native q90 module identity changed: expected {expected_module}, found {actual_module}"
        )
    source_gap_events = int(result.get("exchange_book_source_gap_events", 0) or 0)
    invalid_sequence = int(
        result.get("exchange_book_invalid_sequence_messages", 0) or 0
    )
    sync_censored = bool(result.get("sync_adjust_censored", False))
    rows = []
    for raw in result.get("_variance_time_lineage_trace", ()):
        row = {"day": day, **dict(raw)}
        if source_gap_events > 0 or invalid_sequence > 0 or sync_censored:
            row["support_valid"] = 0
            reasons = set(
                value
                for value in str(row.get("unsupported_reasons", "")).split("|")
                if value
            )
            if source_gap_events > 0:
                reasons.add("native_source_gap")
            if invalid_sequence > 0:
                reasons.add("native_invalid_sequence")
            if sync_censored:
                reasons.add("sync_adjust_unsupported_censor")
            row["unsupported_reasons"] = "|".join(sorted(reasons))
        rows.append(row)
    return {
        "day": day,
        "runtime_s": float(time.monotonic() - started),
        "lineages": rows,
        "summary": {
            "fills_total": int(result.get("fills_bid", 0) or 0)
            + int(result.get("fills_ask", 0) or 0),
            "campaign_count": int(result.get("campaign_count", 0) or 0),
            "pnl": float(result.get("pnl", 0.0) or 0.0),
            "source_gap_events": source_gap_events,
            "invalid_sequence_messages": invalid_sequence,
            "sync_censored": int(sync_censored),
            "q90_pre_ack_fill_count": int(
                result.get("dynamic_fill_hazard_pre_ack_fill_count", 0) or 0
            ),
            "q90_mismatch_count": int(
                result.get("dynamic_fill_hazard_cpp_mismatch_count", 0) or 0
            ),
            "full_cpp_tick_replay_authority": int(
                bool(result.get("dynamic_fill_hazard_full_cpp_tick_replay_authority", False))
            ),
        },
    }


def validate_lineage_panel(frame: pd.DataFrame, spec: Mapping[str, Any]) -> None:
    required = {
        "day",
        "decision_id",
        "lineage_id",
        "campaign_id",
        "side",
        "action",
        "behavior_propensity",
        "assignment_before_downstream_path",
        "assignment_fixed_within_lineage",
        "trigger_fill_excluded_from_reward",
        "reward",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "reward_identity_error",
        "terminal_campaign_pnl",
        "campaign_mae",
        "decision_to_terminal_s",
        "repair_event",
        "campaign_censored",
        "intervention_fill_count",
        "inventory_time_btc_s",
        "actual_final_action_change_count",
        "support_valid",
        "full_cpp_tick_replay_authority",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("randomized lineage panel is missing: " + ", ".join(missing))
    if frame.empty or frame.duplicated(["day", "decision_id"]).any():
        raise ValueError("randomized lineage denominator is empty or duplicated")
    if frame.duplicated(["day", "lineage_id"]).any():
        raise ValueError("randomized lineage id is not unique within day")
    if set(frame["action"].astype(str)) != {
        LINEAGE_CONTROL_ACTION,
        LINEAGE_CANDIDATE_ACTION,
    }:
        raise ValueError("both frozen behavior actions require support")
    propensity = pd.to_numeric(frame["behavior_propensity"], errors="coerce")
    if propensity.isna().any() or not np.allclose(propensity, 0.5, atol=1e-12):
        raise ValueError("logged propensity differs from exact 0.5")
    for column in (
        "assignment_before_downstream_path",
        "assignment_fixed_within_lineage",
        "trigger_fill_excluded_from_reward",
    ):
        if not pd.to_numeric(frame[column], errors="coerce").eq(1).all():
            raise ValueError(f"lineage invariant failed: {column}")
    identity_error = pd.to_numeric(frame["reward_identity_error"], errors="coerce")
    if identity_error.isna().any() or float(identity_error.abs().max()) > 1e-9:
        raise ValueError("lineage reward accounting identity failed")
    if frame["full_cpp_tick_replay_authority"].astype(bool).any():
        raise ValueError("lineage panel incorrectly claims full C++ authority")
    development = set(str(day) for day in spec["panels"]["development_days"])
    observed = set(frame["day"].astype(str))
    if not observed.issubset(development):
        raise ValueError("lineage panel read outside frozen Development")


def derive_outcomes(frame: pd.DataFrame, *, development_q10: float) -> pd.DataFrame:
    out = frame.copy()

    def numeric(name: str) -> pd.Series:
        return pd.to_numeric(out[name], errors="coerce").astype(float)

    terminal = numeric("terminal_campaign_pnl")
    repair = numeric("repair_event").clip(0.0, 1.0)
    censored = numeric("campaign_censored").clip(0.0, 1.0)
    duration = numeric("decision_to_terminal_s").clip(0.0, 1_800.0)
    out["reward"] = numeric("reward")
    out["terminal_campaign_value"] = terminal
    out["negative_terminal_protection"] = terminal.clip(upper=0.0)
    out["q10_shortfall_protection"] = (terminal - development_q10).clip(upper=0.0)
    out["campaign_mae_avoidance"] = numeric("campaign_mae")
    out["repair_event"] = repair
    out["repair_time_avoidance_s"] = -np.where(repair > 0.5, duration, 1_800.0)
    out["censoring_avoidance"] = -censored
    out["queue_reset_value"] = -numeric("queue_cost")
    out["latency_adjusted_value"] = out["reward"]
    out["inventory_time_avoidance_btc_s"] = -numeric("inventory_time_btc_s")
    if not np.isfinite(out[list(OUTCOMES)].to_numpy(dtype=float)).all():
        raise ValueError("randomized lineage outcomes contain non-finite values")
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
    if len(daily) < 2:
        return {"estimate": estimate, "lcb95": estimate, "ucb95": estimate}
    rng = np.random.default_rng(seed)
    values = daily.to_numpy(dtype=float)
    samples = np.empty(draws, dtype=float)
    for index in range(draws):
        selected = values[rng.integers(0, len(values), size=len(values))]
        samples[index] = selected[:, 0].sum() / max(selected[:, 1].sum(), 1.0)
    return {
        "estimate": estimate,
        "lcb95": float(np.quantile(samples, 0.025)),
        "ucb95": float(np.quantile(samples, 0.975)),
    }


def _retention(frame: pd.DataFrame) -> float:
    means = frame.groupby("action")["intervention_fill_count"].mean()
    return float(
        means.get(LINEAGE_CANDIDATE_ACTION, math.nan)
        / max(float(means.get(LINEAGE_CONTROL_ACTION, 0.0)), 1e-12)
    )


def evaluate_side(
    frame: pd.DataFrame,
    *,
    side: str,
    spec: Mapping[str, Any],
    development_q10: float,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    scoped = derive_outcomes(
        frame.loc[frame["side"].astype(str).str.upper().eq(side)].copy(),
        development_q10=development_q10,
    )
    if scoped.empty:
        raise ValueError(f"no randomized lineage support for {side}")
    bootstrap = spec["bootstrap"]
    outcomes = {
        outcome: contrast.randomized_itt_contrast(
            scoped,
            outcome=outcome,
            baseline_action=LINEAGE_CONTROL_ACTION,
            candidate_action=LINEAGE_CANDIDATE_ACTION,
            bootstrap_trials=int(bootstrap["draws"]),
            random_seed=int(bootstrap["seed"]) + 100 * SIDES.index(side) + index,
        )
        for index, outcome in enumerate(OUTCOMES)
    }
    actual_candidate = scoped[
        scoped["action"].astype(str).eq(LINEAGE_CANDIDATE_ACTION)
    ].copy()
    actual_candidate["actual_effective"] = (
        pd.to_numeric(
            actual_candidate["actual_final_action_change_count"], errors="coerce"
        ).fillna(0)
        > 0
    ).astype(int)
    actual_effect = _binary_day_interval(
        actual_candidate,
        "actual_effective",
        seed=int(bootstrap["seed"]) + 1_000 + SIDES.index(side),
        draws=int(bootstrap["draws"]),
    )
    support_valid = pd.to_numeric(scoped["support_valid"], errors="coerce").fillna(0)
    reward_support = outcomes["reward"]["arms"]
    ess = min(float(value["effective_sample_size"]) for value in reward_support.values())
    retention = _retention(scoped)
    candidate_rate = float(
        scoped["action"].astype(str).eq(LINEAGE_CANDIDATE_ACTION).mean()
    )
    inventory = outcomes["inventory_time_avoidance_btc_s"]
    family_failures: list[str] = []
    gates = spec["family_gates"]
    if actual_effect["lcb95"] <= float(gates["actual_action_change_rate_lcb_min"]):
        family_failures.append("actual_final_action_change_lcb_not_positive")
    action_days = int(
        actual_candidate.loc[actual_candidate["actual_effective"] > 0, "day"].nunique()
    )
    if action_days < int(gates["minimum_actual_action_change_days_per_side"]):
        family_failures.append("actual_final_action_change_days_below_gate")
    if float(inventory["interval"]["p025"]) < 0.0:
        family_failures.append("inventory_time_budget_increase_not_excluded")
    if retention < float(gates["minimum_fills_retention"]):
        family_failures.append("lineage_fills_retention_below_gate")
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
            "daily_positive_rate": float(outcomes[outcome]["daily_positive_rate"]),
            "source": f"lineage_randomized_itt.{outcome}",
        }
        for metric, outcome in metric_aliases.items()
    }
    metrics["fills_retention"] = {
        "estimate": retention,
        "source": "candidate_vs_control_mean_downstream_fill_count",
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
            "side": side,
            "development_days": list(spec["panels"]["development_days"]),
        },
        "validity_failures": [],
        "support": {
            "n_rows": int(len(scoped)),
            "n_days": int(scoped["day"].nunique()),
            "effective_sample_size": float(ess),
            "minimum_behavior_propensity": 0.5,
            "importance_weight_clipped_rows": 0,
            "unsupported_mass": float(1.0 - support_valid.mean()),
            "overlap_violations": 0,
            "failures": [],
        },
        "candidate_rate": candidate_rate,
        "invariant_violations": [],
        "family_gate_failures": family_failures,
        "metrics": metrics,
    }
    scorecard = score_canonical_evidence(
        evidence,
        profile_id="action_execution_v1",
        require_frozen_profile=True,
    )
    report = {
        "side": side,
        "support": evidence["support"],
        "candidate_assignment_rate": candidate_rate,
        "actual_final_action_change": {
            **actual_effect,
            "days": action_days,
        },
        "fills_retention": retention,
        "outcomes": outcomes,
        "family_gate_failures": family_failures,
    }
    return report, evidence, scorecard


def _quality_summary(spec: Mapping[str, Any]) -> dict[str, Any]:
    quality = pd.read_csv(spec["quality_ledger"]["path"], dtype={"day": str})
    selected = quality[
        quality["day"].isin(spec["panels"]["development_days"])
    ].copy()
    if len(selected) != len(spec["panels"]["development_days"]):
        raise ValueError("quality ledger does not cover all 40 Development days")
    grade_counts = {
        str(key): int(value)
        for key, value in selected["quality_grade"].value_counts().sort_index().items()
    }
    native_eligible = bool(
        _strict_bool_series(
            selected["native_sequence_eligible"],
            label="native_sequence_eligible",
        ).all()
    )
    normalized_eligible = bool(
        _strict_bool_series(
            selected["normalized_formal_eligible"],
            label="normalized_formal_eligible",
        ).all()
    )
    contract = spec["quality_contract"]
    if grade_counts != dict(contract["expected_quality_grade_counts"]):
        raise ValueError(
            "Development quality-grade identity changed: "
            f"expected={contract['expected_quality_grade_counts']}, "
            f"actual={grade_counts}"
        )
    if bool(contract["require_all_native_sequence_eligible"]) and not native_eligible:
        raise ValueError("Development contains a native-sequence-ineligible day")
    if (
        bool(contract["require_all_normalized_formal_eligible"])
        and not normalized_eligible
    ):
        raise ValueError("Development contains a normalized-formal-ineligible day")
    return {
        "rows": int(len(selected)),
        "quality_grade_counts": grade_counts,
        "grade_b_days": sorted(
            selected.loc[selected["quality_grade"].eq("B"), "day"].astype(str)
        ),
        "all_native_sequence_eligible": native_eligible,
        "all_normalized_formal_eligible": normalized_eligible,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--days", nargs="*")
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec, base = _load_spec(spec_path)
    market_manifest, junit = _validate_identities(spec, base)
    disk = storage_gate(output, spec)
    development_days = [str(day) for day in spec["panels"]["development_days"]]
    selected_days = list(args.days or development_days)
    if sorted(selected_days) != sorted(development_days) and not spec.get(
        "diagnostic_subset_allowed", False
    ):
        raise ValueError("formal run must use all 40 Development days")
    unknown = sorted(set(selected_days) - set(development_days))
    if unknown:
        raise ValueError(f"requested dates are outside Development: {unknown}")
    if output.exists() and not args.resume:
        raise FileExistsError(f"output directory already exists: {output}")
    output.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output / "day_checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    spec_hash = full_path.sha256_file(spec_path)
    (output / "market_source_manifest.json").write_text(
        json.dumps(market_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in selected_days:
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
        iterator = ((day, _run_day(spec, base, day)) for day in pending)
        for day, result in iterator:
            results.append(result)
            (checkpoint_dir / f"{day}.json").write_text(
                json.dumps({"spec_sha256": spec_hash, "result": result}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run_day, spec, base, day): day for day in pending}
            for future in as_completed(futures):
                day = futures[future]
                result = future.result()
                results.append(result)
                (checkpoint_dir / f"{day}.json").write_text(
                    json.dumps({"spec_sha256": spec_hash, "result": result}, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))

    results.sort(key=lambda row: str(row["day"]))
    panel = pd.DataFrame(
        [row for result in results for row in result.get("lineages", ())]
    ).sort_values(["day", "lineage_id"], kind="stable")
    validate_lineage_panel(panel, spec)
    daily = pd.DataFrame(
        [
            {"day": result["day"], "runtime_s": result["runtime_s"], **result["summary"]}
            for result in results
        ]
    ).sort_values("day", kind="stable")
    if set(daily["day"].astype(str)) != set(development_days):
        raise ValueError("formal randomized run did not complete all 40 days")
    if int(daily["q90_pre_ack_fill_count"].sum()) != 0:
        raise RuntimeError("historical q90 fill-before-ACK branch lacks lockstep authority")
    if int(daily["q90_mismatch_count"].sum()) != 0:
        raise RuntimeError("q90 C++ lockstep mismatch is nonzero")
    if int(daily["full_cpp_tick_replay_authority"].sum()) != 0:
        raise RuntimeError("full C++ replay authority must remain false")

    baseline_terminal = pd.to_numeric(
        panel.loc[
            panel["action"].astype(str).eq(LINEAGE_CONTROL_ACTION),
            "terminal_campaign_pnl",
        ],
        errors="coerce",
    )
    development_q10 = float(baseline_terminal.quantile(0.10))
    side_reports: dict[str, Any] = {}
    evidences: dict[str, Any] = {}
    scorecards: dict[str, Any] = {}
    for side in SIDES:
        side_report, evidence, scorecard = evaluate_side(
            panel,
            side=side,
            spec=spec,
            development_q10=development_q10,
        )
        side_reports[side] = side_report
        evidences[side] = evidence
        scorecards[side] = scorecard
    both_pass = all(
        bool(scorecards[side]["ranking_eligible"]) for side in SIDES
    )
    decision = (
        "development_passed_validation_locked"
        if both_pass
        else "close_variance_time_add_rearm_action_on_development"
    )
    quality = _quality_summary(spec)

    panel_path = output / "randomized_lineage_panel.parquet"
    daily_path = output / "daily_summary.csv"
    evidence_path = output / "canonical_evidence.json"
    scorecard_path = output / "scorecards.json"
    report_path = output / "report.json"
    panel.to_parquet(panel_path, index=False)
    daily.to_csv(daily_path, index=False)
    evidence_path.write_text(json.dumps(evidences, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    scorecard_path.write_text(json.dumps(scorecards, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {"path": str(spec_path), "sha256": spec_hash},
        "panel_role": "development",
        "development_days": development_days,
        "evaluated_days": daily["day"].astype(str).tolist(),
        "development_q10_usdc": development_q10,
        "quality": quality,
        "storage_gate": disk,
        "scorecard_profile": dict(spec["scorecard_profile"]),
        "side_reports": side_reports,
        "side_scorecard_status": {
            side: {
                "ranking_eligible": bool(scorecards[side]["ranking_eligible"]),
                "ranking_score": scorecards[side]["ranking_score"],
                "promotion_status": scorecards[side]["promotion_status"],
                "hard_gate_failures": scorecards[side]["hard_gates"]["failures"],
                "support_failures": scorecards[side]["support"]["failures"],
            }
            for side in SIDES
        },
        "decision": decision,
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
            "aws_receive_time_transport_supported": False,
            "full_cpp_tick_replay_authority": False,
        },
        "test_evidence": junit,
        "artifacts": {},
    }
    artifacts = {
        "panel": panel_path,
        "daily": daily_path,
        "canonical_evidence": evidence_path,
        "scorecards": scorecard_path,
        "market_source_manifest": output / "market_source_manifest.json",
    }
    report["artifacts"] = {
        name: {"path": str(path), "sha256": full_path.sha256_file(path)}
        for name, path in artifacts.items()
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest = {
        "schema_version": "volatility_time_add_rearm_randomized_manifest.v1",
        "family_id": FAMILY_ID,
        "decision": decision,
        "report": {"path": str(report_path), "sha256": full_path.sha256_file(report_path)},
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
