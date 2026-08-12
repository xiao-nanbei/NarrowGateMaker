#!/usr/bin/env python3
"""Development-only full-path replay for the cross-venue fair quote center."""

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
from research.families.f04_external_market_alpha.audit.cross_venue_causal_fair_price import (
    load_common_support_variants,
)
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
from research.families.f09_campaign_action_uplift.audit.cross_venue_fair_center_shift import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "cross_venue_fair_center_shift_randomized_replay.v1"
FAMILY_ID = "cross_venue_fair_center_shift_randomized_replay_v1"
VARIANTS = (
    "all_venues",
    "leave_bitget_out",
    "leave_bybit_out",
    "leave_okx_out",
)
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
        raise ValueError("unexpected fair-center randomized replay schema")
    if spec.get("family_id") != FAMILY_ID:
        raise ValueError("unexpected fair-center randomized replay family")
    if canonical_spec_sha256(spec) != str(
        spec.get("canonical_spec_identity_sha256", "")
    ):
        raise ValueError("fair-center randomized replay spec hash mismatch")
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
    days = list(spec["panels"]["development_days"])
    if days != list(base["panels"]["development_days"]) or len(days) != 40:
        raise ValueError("fair-center replay requires the exact 40-day F09 panel")
    grade_a = set(str(day) for day in spec["panels"]["grade_a_days"])
    grade_b = set(str(day) for day in spec["panels"]["grade_b_days"])
    if grade_a & grade_b or grade_a | grade_b != set(days):
        raise ValueError("Grade A/B must partition the 40 Development days")
    if spec.get("scorecard_profile") != score_profile_contract("action_alpha_v1"):
        raise ValueError("action_alpha_v1 score profile was not frozen exactly")
    if spec["behavior_policy"].get("probabilities") != {
        CONTROL_ACTION: 0.5,
        CANDIDATE_ACTION: 0.5,
    }:
        raise ValueError("fair-center propensity must be exactly 0.5/0.5")

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
) -> tuple[list[dict[str, Any]], dict[str, Any], pd.DataFrame]:
    implementation = spec["implementation_identity"]
    budget_execution.require_identity(
        Path(__file__).resolve(),
        str(implementation["evaluator_sha256"]),
        "fair-center randomized evaluator",
    )
    for relative, expected in implementation["source_sha256"].items():
        budget_execution.require_identity(
            ROOT / str(relative), str(expected), str(relative)
        )
    source_identity = spec["fair_price_source_identity"]
    for key in ("cache_universe_manifest", "cache_variants", "common_support"):
        identity = source_identity[key]
        budget_execution.require_identity(
            Path(str(identity["path"])),
            str(identity["sha256"]),
            f"fair-price {key}",
        )
    cache_variants = pd.read_csv(source_identity["cache_variants"]["path"])
    if len(cache_variants) != 160:
        raise ValueError("fair-price cache identity must contain 40x4 rows")

    for identity, label in (
        (spec["operational_config_identity"], "operational config"),
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
        raise ValueError("formal fair-center replay requires market-source rehash")

    junit_identity = spec["test_identity"]["junit_xml"]
    junit_path = Path(str(junit_identity["path"])).expanduser()
    budget_execution.require_identity(
        junit_path, str(junit_identity["sha256"]), "fair-center contract JUnit"
    )
    junit = full_path.read_junit(junit_path)
    missing = sorted(
        set(spec["test_identity"]["required_test_names"])
        - set(junit["test_names"])
    )
    if not bool(junit["passed"]) or missing:
        raise ValueError(f"fair-center action tests are incomplete: {missing}")
    return market_manifest, junit, cache_variants


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


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        frame.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _persist_day_result(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
    foundation: Mapping[str, Any],
    cache_variants: pd.DataFrame,
    day: str,
    checkpoint_dir: Path,
    spec_sha256: str,
) -> dict[str, Any]:
    """Run one day and persist large rows before crossing the process pipe."""

    result = _run_day(spec, base, foundation, cache_variants, day)
    rows = pd.DataFrame(result.pop("rows", ()))
    events = pd.DataFrame(result.pop("events", ()))
    if rows.empty or events.empty:
        raise RuntimeError(f"{day}: persisted randomized result is empty")
    rows_path = checkpoint_dir / f"{day}.rows.parquet"
    events_path = checkpoint_dir / f"{day}.events.parquet"
    checkpoint_path = checkpoint_dir / f"{day}.json"
    _atomic_parquet(rows, rows_path)
    _atomic_parquet(events, events_path)
    payload = {
        "schema_version": "cross_venue_fair_center_day_checkpoint.v1",
        "spec_sha256": str(spec_sha256),
        "day": str(day),
        "result": result,
        "rows": {
            "path": str(rows_path),
            "sha256": sha256_file(rows_path),
            "count": int(len(rows)),
        },
        "events": {
            "path": str(events_path),
            "sha256": sha256_file(events_path),
            "count": int(len(events)),
        },
    }
    _atomic_json(payload, checkpoint_path)
    return payload


def _load_day_checkpoint(path: Path, *, spec_sha256: str) -> dict[str, Any]:
    payload = _load_json(path)
    if payload.get("schema_version") != "cross_venue_fair_center_day_checkpoint.v1":
        raise ValueError(f"checkpoint schema mismatch: {path}")
    if payload.get("spec_sha256") != str(spec_sha256):
        raise ValueError(f"checkpoint spec mismatch: {path}")
    for key in ("rows", "events"):
        identity = payload[key]
        artifact = Path(str(identity["path"]))
        if not artifact.is_file() or sha256_file(artifact) != str(identity["sha256"]):
            raise ValueError(f"checkpoint {key} identity mismatch: {path}")
    return payload


def _configure_params(
    spec: Mapping[str, Any],
    base: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    execution_base = dict(base)
    execution_base["operational_config_identity"] = dict(
        spec["operational_config_identity"]
    )
    params = full_path._configure_params(execution_base, day)
    required_semantics = spec["operational_config_identity"]["required_semantics"]
    actual_semantics = {
        "ml_enabled": bool(params.get("ml_enabled", True)),
        "fill_cooldown_s": float(params.get("fill_cooldown", 0.0) or 0.0),
        "consecutive_reset": str(
            params.get("fill_cooldown_consecutive_reset_policy", "")
        ),
        "reducing_cooldown_s": float(
            params.get("fill_cooldown_reducing", 0.0) or 0.0
        ),
        "max_consecutive_losses": int(
            params.get("max_consecutive_losses", 0) or 0
        ),
        "loss_cooldown_s": float(
            params.get("cooldown_after_loss", 0.0) or 0.0
        ),
        "markout_side_asymmetry_sign": float(
            params.get("markout_side_asymmetry_sign", 0.0) or 0.0
        ),
        "buy_q90_enabled_in_source": bool(
            params.get("dynamic_fill_hazard_action_enabled", False)
        ),
    }
    if actual_semantics != required_semantics:
        raise ValueError(
            "fair-center operational config semantics drifted: "
            f"expected={required_semantics}, actual={actual_semantics}"
        )
    replay = spec["replay_contract"]
    params.update(
        {
            "fill_cooldown_clock_mode": "wall_time",
            "decision_trace_profile": "mechanics_only",
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "cross_venue_fair_center_shift_enabled": True,
            "cross_venue_fair_center_shift_seed": int(
                spec["behavior_policy"]["random_seed"]
            ),
            "cross_venue_fair_center_shift_family_id": FAMILY_ID,
            "cross_venue_fair_center_shift_probabilities": dict(
                spec["behavior_policy"]["probabilities"]
            ),
            "cross_venue_fair_center_max_state_age_ms": float(
                spec["actions"]["candidate"]["max_state_age_ms"]
            ),
            "trace_cross_venue_fair_center_shift_max": int(
                replay["trace_campaigns_max_per_day"]
            ),
            "lineage_randomized_outcome_contract_version": "v2",
            "lineage_randomized_family_id": FAMILY_ID,
            "post_cooldown_incremental_inventory_budget_enabled": False,
            "multi_short_reducing_buy_aggression_enabled": False,
            "sell_add_inventory_price_penalty_enabled": False,
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
    cache_variants: pd.DataFrame,
    day: str,
) -> dict[str, Any]:
    started = time.monotonic()
    params = _configure_params(spec, base, day)
    fair_data, fair_support = load_common_support_variants(cache_variants, day)
    if not bool(fair_support["identical_validity_denominator"]):
        raise RuntimeError(f"{day}: LOO common-support mask drifted")
    window = full_path._load_window(base, day, params)
    rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for variant in VARIANTS:
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
            dict(params),
            ml_data=None,
            bbo_data=window.get("bbo_data"),
            l2_data=window.get("l2_data"),
            var_ti=window.get("var_ti"),
            var_retsq=window.get("var_retsq"),
            historical_fair_price_data=fair_data[variant],
            exchange_book_event_tape=tape,
        )
        budget_execution._assert_q90_off_result(result)
        raw_trace = pd.DataFrame(
            result.get("_cross_venue_fair_center_shift_trace", ())
        )
        raw_events = pd.DataFrame(
            result.get("_cross_venue_fair_center_shift_event_journal", ())
        )
        if raw_trace.empty:
            raise RuntimeError(f"{day}/{variant}: no fair-center assignments")
        validated = outcome_contract.validate_native_lineage_trace(
            raw_trace,
            foundation,
            event_journal=raw_events,
            producer_audit=result[
                "_cross_venue_fair_center_shift_trace_audit"
            ],
        )
        validated["variant"] = variant
        raw_events["variant"] = variant
        source_gap_events = int(
            result.get("exchange_book_source_gap_events", 0) or 0
        )
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
        rows.extend(validated.to_dict("records"))
        events.extend(raw_events.to_dict("records"))
        summaries.append(
            {
                "day": str(day),
                "variant": variant,
                "pnl": float(result.get("pnl", 0.0) or 0.0),
                "fills_total": int(result.get("fills_bid", 0) or 0)
                + int(result.get("fills_ask", 0) or 0),
                "campaign_count": int(result.get("campaign_count", 0) or 0),
                "action_assignments": int(len(validated)),
                "control_assignments": int(
                    result.get(
                        "cross_venue_fair_center_shift_control_assignments", 0
                    )
                    or 0
                ),
                "candidate_assignments": int(
                    result.get(
                        "cross_venue_fair_center_shift_candidate_assignments", 0
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
                            "cross_venue_fair_center_shift_full_cpp_tick_replay_authority",
                            False,
                        )
                    )
                ),
                "common_valid_fraction": float(
                    fair_support["common_valid_fraction"]
                ),
            }
        )
    return {
        "day": str(day),
        "runtime_s": float(time.monotonic() - started),
        "rows": rows,
        "events": events,
        "summaries": summaries,
        "fair_support": fair_support,
    }


def validate_panel(frame: pd.DataFrame, spec: Mapping[str, Any]) -> None:
    required = {
        "variant",
        "day",
        "decision_id",
        "campaign_id",
        "side",
        "opener_side",
        "campaign_started",
        "action",
        "behavior_propensity",
        "decision_to_campaign_terminal_value_usdc",
        "lineage_mae",
        "lineage_max_abs_inventory",
        "inventory_time_btc_s",
        "campaign_censored",
        "assignment_ts_ms",
        "campaign_terminal_ts_ms",
        "campaign_terminal_reason",
        "fill_count",
        "buy_fill_count",
        "sell_fill_count",
        "actual_final_action_change_count",
        "candidate_coordinate_change_count",
        "maker_violation_count",
        "action_generated_ioc_or_taker_count",
        "queue_reset_count",
        "replace_cancel_request_count",
        "order_submit_count",
        "support_valid",
        "transport_supported",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("fair-center randomized panel is missing: " + ", ".join(missing))
    if frame.empty or frame.duplicated(["variant", "day", "campaign_id"]).any():
        raise ValueError("fair-center campaign denominator is invalid")
    if set(frame["variant"].astype(str)) != set(VARIANTS):
        raise ValueError("fair-center LOO variants are incomplete")
    development = set(str(day) for day in spec["panels"]["development_days"])
    for variant in VARIANTS:
        subset = frame[frame["variant"].astype(str).eq(variant)]
        if set(subset["day"].astype(str)) != development:
            raise ValueError(f"{variant}: exact Development denominator drifted")
        if set(subset["action"].astype(str)) != {
            CONTROL_ACTION,
            CANDIDATE_ACTION,
        }:
            raise ValueError(f"{variant}: both randomized actions need support")
    if not np.allclose(
        pd.to_numeric(frame["behavior_propensity"], errors="coerce"),
        0.5,
        atol=1e-12,
    ):
        raise ValueError("fair-center behavior propensity differs from 0.5")
    if not pd.to_numeric(frame["transport_supported"], errors="coerce").eq(0).all():
        raise ValueError("historical trade-bar replay claimed live transport")
    if int(pd.to_numeric(frame["maker_violation_count"], errors="coerce").sum()) != 0:
        raise ValueError("fair-center candidate emitted a non-maker quote")
    if int(
        pd.to_numeric(
            frame["action_generated_ioc_or_taker_count"], errors="coerce"
        ).sum()
    ) != 0:
        raise ValueError("fair-center candidate emitted IOC/taker activity")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce").astype(float)
    if values.isna().any() or not np.isfinite(values.to_numpy()).all():
        raise ValueError(f"non-finite fair-center outcome: {column}")
    return values


def derive_outcomes(frame: pd.DataFrame, *, baseline_q10: float) -> pd.DataFrame:
    out = frame.copy()
    reward = _numeric(out, "decision_to_campaign_terminal_value_usdc")
    started = _numeric(out, "campaign_started").gt(0.5)
    reason = out["campaign_terminal_reason"].astype(str)
    flat = reason.eq("flat")
    duration = (
        _numeric(out, "campaign_terminal_ts_ms")
        - _numeric(out, "assignment_ts_ms")
    ).clip(lower=0.0) / 1_000.0
    out["reward"] = reward
    out["negative_terminal_protection"] = reward.clip(upper=0.0)
    out["q10_shortfall_protection"] = (reward - baseline_q10).clip(upper=0.0)
    out["campaign_mae_avoidance"] = _numeric(out, "lineage_mae")
    out["repair_event"] = ((~started) | flat).astype(float)
    out["repair_time_avoidance_s"] = -np.where(
        ~started,
        0.0,
        np.where(flat, duration.clip(upper=1_800.0), 1_800.0),
    )
    out["censoring_avoidance"] = -(
        started & _numeric(out, "campaign_censored").gt(0.5)
    ).astype(float)
    out["queue_reset_value"] = 0.0
    out["latency_adjusted_value"] = reward
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
    values = daily.to_numpy(dtype=float)
    rng = np.random.default_rng(int(seed))
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


def _arm_tail(frame: pd.DataFrame) -> dict[str, dict[str, float]]:
    rows: dict[str, dict[str, float]] = {}
    for action in (CONTROL_ACTION, CANDIDATE_ACTION):
        values = _numeric(
            frame[frame["action"].astype(str).eq(action)], "reward"
        )
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
    daily_values: list[float] = []
    for _, day_frame in frame.groupby("day", sort=True):
        candidate = float(
            day_frame.loc[
                day_frame["action"].astype(str).eq(CANDIDATE_ACTION), "reward"
            ].sum()
        )
        control = float(
            day_frame.loc[
                day_frame["action"].astype(str).eq(CONTROL_ACTION), "reward"
            ].sum()
        )
        daily_values.append(2.0 * candidate - 2.0 * control)
    values = np.asarray(daily_values, dtype=float)
    rng = np.random.default_rng(int(seed))
    samples = np.empty(int(draws), dtype=float)
    for index in range(int(draws)):
        samples[index] = float(
            rng.choice(values, size=len(values), replace=True).mean()
        )
    return {
        "estimate_usdc_per_day": float(values.mean()),
        "lcb95_usdc_per_day": float(np.quantile(samples, 0.025)),
        "ucb95_usdc_per_day": float(np.quantile(samples, 0.975)),
        "days": int(len(values)),
        "estimator": "day_level_Horvitz_Thompson_exact_half_propensity",
    }


def evaluate_scope(
    frame: pd.DataFrame,
    *,
    scope_id: str,
    spec: Mapping[str, Any],
    primary: bool,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    if frame.empty:
        raise ValueError(f"empty fair-center evaluation scope: {scope_id}")
    control_reward = _numeric(
        frame[frame["action"].astype(str).eq(CONTROL_ACTION)],
        "decision_to_campaign_terminal_value_usdc",
    )
    baseline_q10 = float(control_reward.quantile(0.10))
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
        _numeric(candidate, "actual_final_action_change_count") > 0.0
    ).astype(int)
    action_change = _binary_day_interval(
        candidate,
        "action_changed",
        seed=int(bootstrap["seed"]) + 101,
        draws=int(bootstrap["draws"]),
    )
    fill_retention = _retention(derived, "fill_count")
    activity_retention = _retention(derived, "order_submit_count")
    effective_candidate_rate = float(0.5 * action_change["estimate"])
    tails = _arm_tail(derived)
    policy_value = _daily_policy_value(
        derived,
        seed=int(bootstrap["seed"]) + 202,
        draws=int(bootstrap["draws"]),
    )
    side_results: dict[str, Any] = {}
    for side_index, side in enumerate(("BUY", "SELL")):
        side_frame = derived[derived["side"].astype(str).eq(side)]
        if set(side_frame["action"].astype(str)) != {
            CONTROL_ACTION,
            CANDIDATE_ACTION,
        }:
            side_results[side] = {"supported": False}
            continue
        side_results[side] = {
            "supported": True,
            **contrast.randomized_itt_contrast(
                side_frame,
                outcome="reward",
                baseline_action=CONTROL_ACTION,
                candidate_action=CANDIDATE_ACTION,
                bootstrap_trials=int(bootstrap["draws"]),
                random_seed=int(bootstrap["seed"]) + 300 + side_index,
            ),
        }

    gates = spec["family_gates"]
    failures: list[str] = []
    if action_change["lcb95"] <= float(gates["actual_action_change_rate_lcb_min"]):
        failures.append("actual_action_change_rate_lcb_below_gate")
    if activity_retention < float(gates["minimum_activity_retention"]):
        failures.append("activity_retention_below_gate")
    if policy_value["lcb95_usdc_per_day"] <= 0.0:
        failures.append("policy_value_lcb_not_positive")
    nonharm_margin = float(gates["side_nonharm_margin_usdc_per_assignment"])
    for side, result in side_results.items():
        if not bool(result.get("supported", False)):
            failures.append(f"{side.lower()}_side_contrast_unsupported")
        elif float(result["interval"]["p025"]) < -nonharm_margin:
            failures.append(f"{side.lower()}_side_material_harm_not_excluded")
    tail_margin = float(gates["tail_nonharm_margin_usdc_per_assignment"])
    control_tail = tails[CONTROL_ACTION]
    candidate_tail = tails[CANDIDATE_ACTION]
    if candidate_tail["q10"] < control_tail["q10"] - tail_margin:
        failures.append("candidate_q10_materially_worse")
    if candidate_tail["cvar10"] < control_tail["cvar10"] - tail_margin:
        failures.append("candidate_cvar10_materially_worse")

    reward = outcomes["reward"]
    support_valid = _numeric(derived, "support_valid")
    ess = min(
        float(value["effective_sample_size"])
        for value in reward["arms"].values()
    )
    report = {
        "scope_id": scope_id,
        "primary": bool(primary),
        "rows": int(len(derived)),
        "days": int(derived["day"].nunique()),
        "baseline_q10_usdc": baseline_q10,
        "candidate_assignment_rate": float(
            derived["action"].astype(str).eq(CANDIDATE_ACTION).mean()
        ),
        "effective_policy_candidate_rate": effective_candidate_rate,
        "actual_action_change": action_change,
        "fill_retention": fill_retention,
        "activity_retention": activity_retention,
        "outcomes": outcomes,
        "tail": tails,
        "side_nonharm": side_results,
        "policy_value": policy_value,
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
        "family_gate_failures": failures,
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
        "source": "candidate_vs_control_mean_post_assignment_fill_count",
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
        "candidate_rate": effective_candidate_rate,
        "invariant_violations": [],
        "family_gate_failures": failures,
        "metrics": metrics,
    }
    scorecard = score_canonical_evidence(
        evidence,
        profile_id="action_alpha_v1",
        require_frozen_profile=True,
    )
    return report, evidence, scorecard


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
    market_manifest, junit, cache_variants = _validate_identities(spec, base)
    disk = storage_gate(output, spec)
    days = [str(day) for day in spec["panels"]["development_days"]]
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

    checkpoints: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in days:
        checkpoint = checkpoint_dir / f"{day}.json"
        if args.resume and checkpoint.is_file():
            checkpoints.append(
                _load_day_checkpoint(checkpoint, spec_sha256=spec_hash)
            )
        else:
            pending.append(day)
    workers = max(1, min(int(args.workers), len(pending) or 1))
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    if workers == 1:
        for day in pending:
            payload = _persist_day_result(
                spec,
                base,
                foundation,
                cache_variants,
                day,
                checkpoint_dir,
                spec_hash,
            )
            checkpoints.append(payload)
            print(
                json.dumps(
                    {
                        "completed_day": day,
                        "runtime_s": payload["result"]["runtime_s"],
                    }
                ),
                flush=True,
            )
    else:
        with ProcessPoolExecutor(
            max_workers=workers,
            max_tasks_per_child=1,
        ) as pool:
            futures = {
                pool.submit(
                    _persist_day_result,
                    spec,
                    base,
                    foundation,
                    cache_variants,
                    day,
                    checkpoint_dir,
                    spec_hash,
                ): day
                for day in pending
            }
            for future in as_completed(futures):
                day = futures[future]
                payload = future.result()
                checkpoints.append(payload)
                print(
                    json.dumps(
                        {
                            "completed_day": day,
                            "runtime_s": payload["result"]["runtime_s"],
                        }
                    ),
                    flush=True,
                )

    checkpoints.sort(key=lambda row: str(row["day"]))
    panel = pd.concat(
        [pd.read_parquet(row["rows"]["path"]) for row in checkpoints],
        ignore_index=True,
    ).sort_values(["variant", "day", "campaign_id"], kind="stable")
    events = pd.concat(
        [pd.read_parquet(row["events"]["path"]) for row in checkpoints],
        ignore_index=True,
    ).sort_values(["variant", "day", "lineage_id", "event_seq"], kind="stable")
    daily = pd.DataFrame(
        [
            row
            for checkpoint in checkpoints
            for row in checkpoint["result"].get("summaries", ())
        ]
    ).sort_values(["variant", "day"], kind="stable")
    fair_support = pd.DataFrame(
        [checkpoint["result"]["fair_support"] for checkpoint in checkpoints]
    ).sort_values("day", kind="stable")
    validate_panel(panel, spec)
    if int(daily["q90_evaluations"].sum()) != 0:
        raise RuntimeError("q90 OFF contract emitted evaluations")
    if int(daily["full_cpp_tick_replay_authority"].sum()) != 0:
        raise RuntimeError("full C++ tick replay authority must remain false")

    grade_a_days = set(spec["panels"]["grade_a_days"])
    grade_b_days = set(spec["panels"]["grade_b_days"])
    variant_reports: dict[str, Any] = {}
    evidence = None
    scorecard = None
    for variant in VARIANTS:
        variant_panel = panel[panel["variant"].astype(str).eq(variant)]
        grade_a = variant_panel[variant_panel["day"].isin(grade_a_days)].copy()
        grade_b = variant_panel[variant_panel["day"].isin(grade_b_days)].copy()
        grade_a_report, current_evidence, current_scorecard = evaluate_scope(
            grade_a,
            scope_id=f"{variant}.grade_a_primary",
            spec=spec,
            primary=variant == "all_venues",
        )
        grade_b_report, _, _ = evaluate_scope(
            grade_b,
            scope_id=f"{variant}.grade_b_sensitivity",
            spec=spec,
            primary=False,
        )
        all_report, _, _ = evaluate_scope(
            variant_panel,
            scope_id=f"{variant}.all_40_descriptive",
            spec=spec,
            primary=False,
        )
        variant_reports[variant] = {
            "grade_a_primary": grade_a_report,
            "grade_b_sensitivity": grade_b_report,
            "all_40_descriptive": all_report,
        }
        if variant == "all_venues":
            evidence = current_evidence
            scorecard = current_scorecard
    if evidence is None or scorecard is None:
        raise RuntimeError("all-venues Grade A scorecard was not generated")

    loo_failures: list[str] = []
    loo_gate = spec["leave_one_venue_out_gates"]
    for variant in VARIANTS[1:]:
        report = variant_reports[variant]["grade_a_primary"]
        if float(report["outcomes"]["reward"]["uplift"]) <= 0.0:
            loo_failures.append(f"{variant}_reward_direction_nonpositive")
        if float(report["policy_value"]["estimate_usdc_per_day"]) <= 0.0:
            loo_failures.append(f"{variant}_policy_value_direction_nonpositive")
        if float(report["fill_retention"]) < float(
            loo_gate["minimum_fill_retention"]
        ):
            loo_failures.append(f"{variant}_fill_retention_below_gate")
    grade_b_report = variant_reports["all_venues"]["grade_b_sensitivity"]
    grade_b_failures: list[str] = []
    if float(grade_b_report["outcomes"]["reward"]["uplift"]) < 0.0:
        grade_b_failures.append("grade_b_reward_direction_negative")
    if float(grade_b_report["policy_value"]["estimate_usdc_per_day"]) < 0.0:
        grade_b_failures.append("grade_b_policy_value_direction_negative")
    if float(grade_b_report["fill_retention"]) < float(
        spec["grade_b_sensitivity_gates"]["minimum_fill_retention"]
    ):
        grade_b_failures.append("grade_b_fill_retention_below_gate")

    primary_pass = bool(scorecard["ranking_eligible"])
    decision = (
        "development_passed_validation_locked"
        if primary_pass and not loo_failures and not grade_b_failures
        else "close_cross_venue_fair_center_shift_on_development"
    )

    panel_path = output / "campaign_randomized_panel.parquet"
    events_path = output / "campaign_event_journal.parquet"
    daily_path = output / "daily_summary.csv"
    support_path = output / "fair_price_common_support.csv"
    evidence_path = output / "canonical_evidence.json"
    scorecard_path = output / "scorecard.json"
    report_path = output / "report.json"
    panel.to_parquet(panel_path, index=False)
    events.to_parquet(events_path, index=False)
    daily.to_csv(daily_path, index=False)
    fair_support.to_csv(support_path, index=False)
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
        "fair_price_support": {
            "common_valid_fraction": float(
                fair_support["common_valid_rows"].sum()
                / fair_support["rows"].sum()
            ),
            "minimum_daily_common_valid_fraction": float(
                fair_support["common_valid_fraction"].min()
            ),
            "fallback_outside_common_support": "baseline_local_quote_center",
            "historical_transport_supported": False,
        },
        "variants": variant_reports,
        "leave_one_venue_out_gate_failures": loo_failures,
        "grade_b_gate_failures": grade_b_failures,
        "storage_gate": disk,
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
            "historical_trade_bar_live_transport_authority": False,
        },
        "test_evidence": junit,
        "artifacts": {},
    }
    artifacts = {
        "panel": panel_path,
        "events": events_path,
        "daily": daily_path,
        "fair_price_common_support": support_path,
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
        "schema_version": "cross_venue_fair_center_shift_manifest.v1",
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
