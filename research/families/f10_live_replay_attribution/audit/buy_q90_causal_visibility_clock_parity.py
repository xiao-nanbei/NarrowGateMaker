#!/usr/bin/env python3
"""Audit BUY q90 exchange-truth and causal-visibility clock parity.

This Development-only runner deliberately excludes PnL, markout, campaign
outcomes, and policy promotion. It compares shadow paths so the visibility
clock cannot alter exchange truth, then runs one corrected apply path for the
stateful Python/C++ q90 contract.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from models import backtest_tick as bt
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f10_live_replay_attribution.audit import (
    buy_q90_portfolio_path_attribution as historical_q90,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "buy_q90_causal_visibility_clock_parity.v1_1"
IDENTITY = "buy_q90_causal_visibility_clock_parity_v1_1"
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "buy_q90_causal_visibility_clock_parity_v1_1_spec_20260731.json"
)

MODE_IDS = (
    "legacy_shadow",
    "provider_receive_shadow",
    "aws_profile_shadow",
    "aws_profile_apply",
)

MECHANICS_RESULT_KEYS = (
    "dynamic_fill_hazard_action_enabled",
    "dynamic_fill_hazard_action_application",
    "dynamic_fill_hazard_replay_authority",
    "dynamic_fill_hazard_visibility_clock_mode",
    "dynamic_fill_hazard_visibility_profile_identity",
    "dynamic_fill_hazard_visibility_stats",
    "dynamic_fill_hazard_visibility_book_stats",
    "dynamic_fill_hazard_truth_state_fingerprint",
    "dynamic_fill_hazard_visibility_state_fingerprint",
    "dynamic_fill_hazard_visible_trade_enqueued_count",
    "dynamic_fill_hazard_visible_trade_delivered_count",
    "dynamic_fill_hazard_visible_trade_pending_count",
    "dynamic_fill_hazard_visible_book_trade_tie_count",
    "dynamic_fill_hazard_provider_receive_missing_count",
    "dynamic_fill_hazard_invalid_reason_counts",
    "dynamic_fill_hazard_invalid_attribution_closed",
    "dynamic_fill_hazard_future_feature_time_count",
    "dynamic_fill_hazard_eval_by_role",
    "dynamic_fill_hazard_action_by_role",
    "dynamic_fill_hazard_valid_score_quantiles",
    "dynamic_fill_hazard_shadow_cancel_signal_count",
    "dynamic_fill_hazard_eval_count",
    "dynamic_fill_hazard_valid_eval_count",
    "dynamic_fill_hazard_invalid_eval_count",
    "dynamic_fill_hazard_keep_count",
    "dynamic_fill_hazard_cancel_request_count",
    "dynamic_fill_hazard_cancel_ack_count",
    "dynamic_fill_hazard_pre_ack_fill_count",
    "dynamic_fill_hazard_recovery_count",
    "dynamic_fill_hazard_reentry_count",
    "dynamic_fill_hazard_blocked_quote_count",
    "dynamic_fill_hazard_retain_invalid_count",
    "dynamic_fill_hazard_hold_active_end",
    "dynamic_fill_hazard_cpp_parity_enabled",
    "dynamic_fill_hazard_cpp_parity_scope",
    "dynamic_fill_hazard_full_cpp_tick_replay_authority",
    "dynamic_fill_hazard_cpp_parity_passed",
    "dynamic_fill_hazard_cpp_identity",
    "dynamic_fill_hazard_cpp_book_event_count",
    "dynamic_fill_hazard_cpp_activation_count",
    "dynamic_fill_hazard_cpp_evaluation_count",
    "dynamic_fill_hazard_cpp_lifecycle_count",
    "dynamic_fill_hazard_cpp_mismatch_count",
    "dynamic_fill_hazard_cpp_mismatch_by_stage",
    "dynamic_fill_hazard_cpp_sequence_stats",
    "dynamic_fill_hazard_model_family_id",
    "dynamic_fill_hazard_model_sha256",
    "dynamic_fill_hazard_policy_id",
    "dynamic_fill_hazard_policy_sha256",
    "exchange_book_events_consumed",
    "exchange_book_events_accepted",
    "exchange_book_events_rejected",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_snapshot_events",
    "exchange_book_delta_events",
    "exchange_book_delta_bootstrap_events",
    "exchange_book_sequence_gaps",
    "exchange_book_message_time_reversals",
    "exchange_book_transaction_timestamp_events",
    "exchange_book_event_timestamp_fallback_events",
    "exchange_book_receive_timestamp_fallback_events",
    "exchange_book_unknown_timestamp_source_events",
    "exchange_book_queue_cancel_ahead_event_count",
    "exchange_book_queue_cancel_ahead_qty",
    "exchange_book_queue_ambiguous_event_count",
    "exchange_book_cancel_trade_ambiguous_order_count",
    "exchange_book_cancel_book_ambiguous_order_count",
    "fills_bid",
    "fills_ask",
    "final_inventory",
)

TRUTH_INVARIANCE_KEYS = (
    "dynamic_fill_hazard_truth_state_fingerprint",
    "exchange_book_events_consumed",
    "exchange_book_events_accepted",
    "exchange_book_events_rejected",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_snapshot_events",
    "exchange_book_delta_events",
    "exchange_book_delta_bootstrap_events",
    "exchange_book_sequence_gaps",
    "exchange_book_message_time_reversals",
    "exchange_book_transaction_timestamp_events",
    "exchange_book_event_timestamp_fallback_events",
    "exchange_book_receive_timestamp_fallback_events",
    "exchange_book_unknown_timestamp_source_events",
    "exchange_book_queue_cancel_ahead_event_count",
    "exchange_book_queue_cancel_ahead_qty",
    "exchange_book_queue_ambiguous_event_count",
    "exchange_book_cancel_trade_ambiguous_order_count",
    "exchange_book_cancel_book_ambiguous_order_count",
    "fills_bid",
    "fills_ask",
    "final_inventory",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def canonical_spec_sha256(spec: Mapping[str, Any]) -> str:
    payload = dict(spec)
    payload.pop("canonical_spec_sha256", None)
    return canonical_sha256(payload)


def _resolve(path: str | Path) -> Path:
    candidate = resolve_portable_path(path, root=ROOT)
    return candidate.resolve() if candidate.is_absolute() else (ROOT / candidate).resolve()


def _require_identity(identity: Mapping[str, Any], label: str) -> Path:
    path = _resolve(str(identity["path"]))
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    expected = str(identity["sha256"]).strip().lower()
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(
            f"{label} hash mismatch: expected={expected} actual={actual}"
        )
    return path


def load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("q90 causal visibility spec must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected q90 causal visibility schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected q90 causal visibility identity")
    if payload.get("status") != "frozen_before_mechanics_output_read":
        raise ValueError("q90 causal visibility status drifted")
    frozen_hash = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(payload) != frozen_hash:
        raise ValueError("q90 causal visibility canonical spec hash mismatch")
    if payload.get("day") != "2026-07-25" or payload.get("quality_grade") != "B":
        raise ValueError("q90 causal visibility must remain the frozen Grade-B day")
    if tuple(payload.get("mode_order") or ()) != MODE_IDS:
        raise ValueError("q90 causal visibility mode order drifted")
    if not bool(payload.get("economic_outputs_prohibited", False)):
        raise ValueError("q90 causal visibility cannot read economics")
    permissions = payload.get("permissions") or {}
    if not permissions or any(bool(value) for value in permissions.values()):
        raise ValueError("q90 causal visibility cannot grant permissions")
    return payload


def validate_frozen_inputs(spec_path: Path, spec: Mapping[str, Any]) -> None:
    implementation = spec.get("implementation_identity") or {}
    for relative, expected in implementation.items():
        _require_identity(
            {"path": relative, "sha256": expected},
            f"implementation {relative}",
        )
    for key, identity in (spec.get("source_identity") or {}).items():
        if isinstance(identity, Mapping) and "path" in identity:
            _require_identity(identity, f"source {key}")
    for key, identity in (spec.get("test_identity") or {}).items():
        if isinstance(identity, Mapping) and "path" in identity:
            _require_identity(identity, f"test {key}")
    _require_identity(spec["native_module_identity"], "native q90 module")
    _require_identity(spec["latency_profile_identity"], "AWS latency profile")
    if canonical_spec_sha256(spec) != str(spec["canonical_spec_sha256"]):
        raise ValueError(f"spec changed while validating inputs: {spec_path}")


def _mode_params(
    base: Mapping[str, Any],
    spec: Mapping[str, Any],
    mode_id: str,
) -> dict[str, Any]:
    mode = (spec.get("modes") or {}).get(mode_id)
    if not isinstance(mode, Mapping):
        raise ValueError(f"missing frozen q90 mode={mode_id}")
    params = copy.deepcopy(dict(base))
    params.update(
        {
            "trace_fills_max": 0,
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_campaign_repair_max": 0,
            "trace_first_add_decision_to_terminal_max": 0,
            "trace_first_opener_decision_to_terminal_max": 0,
            "collect_curves": False,
            "window_cache_write_enabled": False,
            "replay_promotion_eligible": False,
            "dynamic_fill_hazard_mechanics_telemetry_enabled": True,
            "dynamic_fill_hazard_action_enabled": True,
            "dynamic_fill_hazard_action_application": str(mode["action_application"]),
            "dynamic_fill_hazard_visibility_clock_mode": str(mode["clock_mode"]),
            "dynamic_fill_hazard_cpp_parity_enabled": bool(mode["cpp_parity"]),
            "dynamic_fill_hazard_cpp_parity_strict": True,
            "dynamic_fill_hazard_cpp_parity_trace_max": int(
                spec["replay_contract"]["cpp_mismatch_trace_max"]
            ),
            "dynamic_fill_hazard_visibility_seed": int(
                spec["replay_contract"]["visibility_seed"]
            ),
            "dynamic_fill_hazard_provider_feature_latency_ms": float(
                spec["replay_contract"]["provider_feature_latency_ms"]
            ),
            "dynamic_fill_hazard_provider_trade_delay_ms": float(
                spec["replay_contract"]["provider_trade_delay_ms"]
            ),
        }
    )
    if str(mode["clock_mode"]) == "aws_profile":
        profile = spec["latency_profile_identity"]
        params.update(
            {
                "dynamic_fill_hazard_visibility_profile_path": str(
                    _resolve(str(profile["path"]))
                ),
                "dynamic_fill_hazard_visibility_profile_sha256": str(
                    profile["sha256"]
                ),
                "dynamic_fill_hazard_visibility_profile_id": str(
                    profile["profile_id"]
                ),
                "dynamic_fill_hazard_visibility_profile_market_id": (
                    "binance:perp:BTCUSDC"
                ),
                "dynamic_fill_hazard_visibility_profile_trade_market_id": (
                    "binance:perp:BTCUSDC"
                ),
                "dynamic_fill_hazard_visibility_profile_transport": "websocket",
                "dynamic_fill_hazard_visibility_profile_mode": str(
                    spec["replay_contract"]["profile_mode"]
                ),
            }
        )
    return params


def _mechanics_only(result: Mapping[str, Any], elapsed_hours: float) -> dict[str, Any]:
    output = {key: result.get(key) for key in MECHANICS_RESULT_KEYS}
    evaluations = int(output["dynamic_fill_hazard_eval_count"] or 0)
    valid = int(output["dynamic_fill_hazard_valid_eval_count"] or 0)
    cancels = int(output["dynamic_fill_hazard_cancel_request_count"] or 0)
    output["rates"] = {
        "evaluations_per_hour": evaluations / max(elapsed_hours, 1e-12),
        "valid_fraction": valid / evaluations if evaluations else math.nan,
        "cancel_requests_per_hour": cancels / max(elapsed_hours, 1e-12),
        "cancel_requests_per_valid_evaluation": (
            cancels / valid if valid else math.nan
        ),
    }
    return output


def _run_mode(
    *,
    mode_id: str,
    base_params: Mapping[str, Any],
    spec: Mapping[str, Any],
    window: Mapping[str, Any],
    tape: CryptoHFTExchangeBookTape,
    elapsed_hours: float,
) -> dict[str, Any]:
    params = _mode_params(base_params, spec, mode_id)
    started = time.monotonic()
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
    output = {
        "mode_id": mode_id,
        "runtime_s": float(time.monotonic() - started),
        "mechanics": _mechanics_only(result, elapsed_hours),
    }
    print(
        json.dumps(
            {
                "mode": mode_id,
                "runtime_s": output["runtime_s"],
                "evaluations": output["mechanics"]["dynamic_fill_hazard_eval_count"],
                "valid": output["mechanics"]["dynamic_fill_hazard_valid_eval_count"],
                "cancels": output["mechanics"]["dynamic_fill_hazard_cancel_request_count"],
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return output


def _distribution(counts: Mapping[str, Any], keys: Iterable[str]) -> dict[str, float]:
    values = {key: max(0.0, float(counts.get(key, 0) or 0)) for key in keys}
    total = sum(values.values())
    return {
        key: (value / total if total > 0.0 else 0.0)
        for key, value in values.items()
    }


def _distribution_tv(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    keys: Iterable[str],
) -> float:
    key_tuple = tuple(keys)
    lhs = _distribution(left, key_tuple)
    rhs = _distribution(right, key_tuple)
    return 0.5 * sum(abs(lhs[key] - rhs[key]) for key in key_tuple)


def _ratio(value: float, reference: float) -> float:
    return value / reference if value > 0.0 and reference > 0.0 else math.nan


def _truth_invariance(modes: Mapping[str, Any]) -> dict[str, Any]:
    reference = modes["legacy_shadow"]["mechanics"]
    mismatches: dict[str, dict[str, Any]] = {}
    for mode_id in ("provider_receive_shadow", "aws_profile_shadow"):
        candidate = modes[mode_id]["mechanics"]
        for key in TRUTH_INVARIANCE_KEYS:
            if candidate.get(key) != reference.get(key):
                mismatches[f"{mode_id}:{key}"] = {
                    "legacy_shadow": reference.get(key),
                    mode_id: candidate.get(key),
                }
    return {
        "passed": not mismatches,
        "compared_keys": list(TRUTH_INVARIANCE_KEYS),
        "mismatches": mismatches,
    }


def _same_date_live_parity(
    replay: Mapping[str, Any],
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    live = spec["known_live_metrics_before_freeze"]
    tolerances = spec["same_date_parity_tolerances"]
    mechanics = replay["mechanics"]
    replay_roles = mechanics.get("dynamic_fill_hazard_eval_by_role") or {}
    replay_role_all = {
        role: int((counts or {}).get("evaluations", 0) or 0)
        for role, counts in replay_roles.items()
    }
    replay_role_valid = {
        role: int((counts or {}).get("valid", 0) or 0)
        for role, counts in replay_roles.items()
    }
    role_keys = ("opener", "add", "reducing")
    eval_rate_ratio = _ratio(
        float(mechanics["rates"]["evaluations_per_hour"]),
        float(live["shadow_rows"]) / float(live["elapsed_hours"]),
    )
    valid_fraction_delta = abs(
        float(mechanics["rates"]["valid_fraction"])
        - float(live["valid_rows"]) / float(live["shadow_rows"])
    )
    all_role_tv = _distribution_tv(
        replay_role_all,
        live["roles_all"],
        role_keys,
    )
    valid_role_tv = _distribution_tv(
        replay_role_valid,
        live["roles_valid"],
        role_keys,
    )
    score_ratios: dict[str, float] = {}
    replay_scores = mechanics.get("dynamic_fill_hazard_valid_score_quantiles") or {}
    for name in ("p10", "p50", "p90"):
        score_ratios[name] = _ratio(
            float(replay_scores.get(name, math.nan)),
            float(live["score_quantiles"][name]),
        )
    cancel_hour_ratio = _ratio(
        float(mechanics["rates"]["cancel_requests_per_hour"]),
        float(live["cancel_requests"]) / float(live["elapsed_hours"]),
    )
    cancel_valid_ratio = _ratio(
        float(mechanics["rates"]["cancel_requests_per_valid_evaluation"]),
        float(live["cancel_requests"]) / float(live["valid_rows"]),
    )
    replay_actions = mechanics.get("dynamic_fill_hazard_action_by_role") or {}
    replay_cancel_roles = {
        role: int((counts or {}).get("cancel_signal", 0) or 0)
        for role, counts in replay_actions.items()
    }
    cancel_role_tv = _distribution_tv(
        replay_cancel_roles,
        live["cancel_roles"],
        ("opener", "add"),
    )
    cancel_count = int(mechanics["dynamic_fill_hazard_cancel_request_count"] or 0)
    reentry_ratio = (
        int(mechanics["dynamic_fill_hazard_reentry_count"] or 0) / cancel_count
        if cancel_count > 0
        else math.nan
    )
    live_reentry_ratio = float(live["reentries"]) / float(live["cancel_requests"])
    reentry_ratio_delta = abs(reentry_ratio - live_reentry_ratio)

    ratio_low = float(tolerances["rate_ratio_low"])
    ratio_high = float(tolerances["rate_ratio_high"])
    score_low = float(tolerances["score_ratio_low"])
    score_high = float(tolerances["score_ratio_high"])
    checks = {
        "evaluation_rate": bool(ratio_low <= eval_rate_ratio <= ratio_high),
        "valid_fraction": bool(
            valid_fraction_delta <= float(tolerances["valid_fraction_abs"])
        ),
        "role_all": bool(all_role_tv <= float(tolerances["role_tv"])),
        "role_valid": bool(valid_role_tv <= float(tolerances["role_tv"])),
        "score": bool(
            all(score_low <= value <= score_high for value in score_ratios.values())
        ),
        "cancel_per_hour": bool(ratio_low <= cancel_hour_ratio <= ratio_high),
        "cancel_per_valid": bool(ratio_low <= cancel_valid_ratio <= ratio_high),
        "cancel_role": bool(
            cancel_role_tv <= float(tolerances["cancel_role_tv"])
        ),
        "lifecycle_reentry": bool(
            reentry_ratio_delta <= float(tolerances["reentry_ratio_abs"])
        ),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "evaluation_rate_ratio": eval_rate_ratio,
        "valid_fraction_abs_delta": valid_fraction_delta,
        "all_role_total_variation": all_role_tv,
        "valid_role_total_variation": valid_role_tv,
        "score_ratios": score_ratios,
        "cancel_per_hour_ratio": cancel_hour_ratio,
        "cancel_per_valid_ratio": cancel_valid_ratio,
        "cancel_role_total_variation": cancel_role_tv,
        "reentry_ratio": reentry_ratio,
        "live_reentry_ratio": live_reentry_ratio,
        "reentry_ratio_abs_delta": reentry_ratio_delta,
    }


def _evaluate_gates(
    modes: Mapping[str, Any],
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    causal_shadow = modes["aws_profile_shadow"]["mechanics"]
    causal_apply = modes["aws_profile_apply"]["mechanics"]
    truth = _truth_invariance(modes)
    profile = causal_apply.get("dynamic_fill_hazard_visibility_profile_identity") or {}
    expected_profile = spec["latency_profile_identity"]
    profile_identity_passed = bool(
        profile.get("profile_id") == expected_profile["profile_id"]
        and profile.get("sha256") == expected_profile["sha256"]
        and profile.get("profile_market_id") == "binance:perp:BTCUSDC"
        and profile.get("profile_trade_market_id") == "binance:perp:BTCUSDC"
        and profile.get("trade_latency_proxy") is False
        and profile.get("provider_clock_authority") is False
    )
    invalid_closed = bool(
        causal_shadow.get("dynamic_fill_hazard_invalid_attribution_closed")
        and causal_apply.get("dynamic_fill_hazard_invalid_attribution_closed")
    )
    future_feature_zero = bool(
        int(causal_shadow.get("dynamic_fill_hazard_future_feature_time_count", 0) or 0)
        == 0
        and int(causal_apply.get("dynamic_fill_hazard_future_feature_time_count", 0) or 0)
        == 0
    )
    native_truth_valid = bool(
        all(
            int(causal_apply.get(key, 0) or 0) == 0
            for key in (
                "exchange_book_source_gap_events",
                "exchange_book_invalid_sequence_messages",
                "exchange_book_sequence_gaps",
                "exchange_book_message_time_reversals",
                "exchange_book_receive_timestamp_fallback_events",
                "exchange_book_unknown_timestamp_source_events",
            )
        )
    )
    cpp_parity = bool(
        causal_apply.get("dynamic_fill_hazard_cpp_parity_passed")
        and int(causal_apply.get("dynamic_fill_hazard_cpp_mismatch_count", 0) or 0)
        == 0
    )
    live_parity = _same_date_live_parity(modes["aws_profile_apply"], spec)
    deep_event_path_available = bool(
        spec["latency_profile_identity"]["deep_event_path_recorded"]
    )
    mechanics_checks = {
        "truth_and_fill_invariance": bool(truth["passed"]),
        "future_feature_time_zero": future_feature_zero,
        "invalid_reason_attribution_closed": invalid_closed,
        "native_truth_valid": native_truth_valid,
        "profile_identity_valid": profile_identity_passed,
        "python_cpp_q90_parity": cpp_parity,
        "same_date_live_mechanics_parity": bool(live_parity["passed"]),
    }
    mechanics_supported = all(mechanics_checks.values())
    exact_aws_transport_supported = bool(
        mechanics_supported
        and deep_event_path_available
        and spec["quality_grade"] == "A"
    )
    fully_passed = bool(mechanics_supported and exact_aws_transport_supported)
    return {
        "mechanics_checks": mechanics_checks,
        "mechanics_supported": mechanics_supported,
        "truth_invariance": truth,
        "same_date_live_parity": live_parity,
        "deep_event_path_available": deep_event_path_available,
        "exact_aws_transport_supported": exact_aws_transport_supported,
        "fully_passed": fully_passed,
        "f07_v2_registration_unblocked": fully_passed,
        "decision": (
            "mechanics_and_exact_aws_transport_passed_f07_v2_registration_unblocked"
            if fully_passed
            else "mechanics_contract_passed_transport_sensitivity_only"
            if mechanics_supported
            else "mechanics_contract_failed"
        ),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_checkpoint(
    checkpoint_dir: Path,
    *,
    spec: Mapping[str, Any],
    mode_id: str,
    result: Mapping[str, Any],
) -> None:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    payload = _json_safe(
        {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "canonical_spec_sha256": spec["canonical_spec_sha256"],
            "mode_id": mode_id,
            "economic_outputs_read": False,
            "result": result,
        }
    )
    destination = checkpoint_dir / f"{mode_id}.json"
    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)


def run(
    spec_path: Path,
    *,
    checkpoint_dir: Path | None = None,
) -> dict[str, Any]:
    spec = load_spec(spec_path)
    validate_frozen_inputs(spec_path, spec)
    parent_path = _require_identity(
        spec["parent_historical_q90_spec"],
        "parent historical q90 spec",
    )
    parent = historical_q90._load_json(parent_path)
    historical_q90.validate_spec(parent)
    source = copy.deepcopy(historical_q90._runtime_source_contract(parent))

    l2_path = _resolve(str(spec["source_identity"]["normalized_l2"]["path"]))
    normalized_root = l2_path.parents[1]
    source["source_identity"]["normalized_l2_root"] = str(normalized_root)
    bt.BBO_DIR = normalized_root / "bbo"
    bt.L2_DIR = normalized_root / "l2"
    day = str(spec["day"])
    base_params = historical_q90.full_path._configure_params(source, day)
    window = historical_q90.full_path._load_window(source, day, base_params)
    base_params = historical_q90._configure_arm_params(
        source,
        day,
        parent,
        q90_enabled=True,
    )
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(source["source_identity"]["native_orderbook_root"]),
        day=day,
        symbol="BTCUSDC",
        tick_size=float(base_params.get("tick_size", bt.TICK)),
        warmup_hours=int(source["replay_contract"]["native_warmup_hours"]),
        strict_complete=True,
    )
    trade_ts = pd.to_numeric(window["trades"]["transact_time"], errors="raise")
    elapsed_hours = max(
        1e-12,
        float(trade_ts.iloc[-1] - trade_ts.iloc[0]) / 3_600_000.0,
    )
    modes: dict[str, Any] = {}
    for mode_id in MODE_IDS:
        modes[mode_id] = _run_mode(
            mode_id=mode_id,
            base_params=base_params,
            spec=spec,
            window=window,
            tape=tape,
            elapsed_hours=elapsed_hours,
        )
        if checkpoint_dir is not None:
            _write_checkpoint(
                checkpoint_dir,
                spec=spec,
                mode_id=mode_id,
                result=modes[mode_id],
            )
    gates = _evaluate_gates(modes, spec)
    return _json_safe(
        {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "day": day,
            "quality_grade": spec["quality_grade"],
            "elapsed_hours": elapsed_hours,
            "mode_order": list(MODE_IDS),
            "modes": modes,
            "gates": gates,
            "economic_outputs_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "permissions": dict(spec["permissions"]),
        }
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    destination = Path(args.output).expanduser().resolve()
    output = run(
        args.spec,
        checkpoint_dir=destination.parent / "checkpoints",
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(output, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(output["gates"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
