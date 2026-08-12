#!/usr/bin/env python3
"""Development-only mechanics audit for post-cooldown inventory budgets."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import shutil
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import LEGACY_MARKETDATA_ROOT, relocate_marketdata_path
from models import backtest_tick as bt
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f09_campaign_action_uplift.audit.post_cooldown_incremental_inventory_budget import (
    outcome_blind_budget_grid,
)
from research.families.f09_campaign_action_uplift.audit.volatility_time_add_rearm_full_path_preflight import (
    _configure_params,
    _load_window,
    build_market_source_manifest,
    read_junit,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "post_cooldown_incremental_inventory_budget_execution.v1"
FAMILY_ID = "post_cooldown_incremental_inventory_budget_feasibility_v1_2"
SIDES = ("BUY", "SELL")

TRACE_FIELDS = (
    "schema_version",
    "episode_id",
    "arm_id",
    "side",
    "budget_units",
    "assignment_ts_ms",
    "scheduled_release_ts_ms",
    "release_observed_delay_ms",
    "trigger_fill_ts_ms",
    "assignment_consecutive_same_side_fill_units",
    "assignment_inventory_units",
    "campaign_id",
    "masked_decision_count_before_assignment",
    "admission_attempt_count",
    "admitted_order_count",
    "blocked_submission_count",
    "blocked_planned_units",
    "fill_count",
    "filled_quantity_btc",
    "released_units",
    "reservation_release_count",
    "max_reserved_units",
    "max_consumed_units",
    "max_abs_inventory_units",
    "reducing_order_budget_bypass_count",
    "one_order_overshoot_count",
    "assignment_preexisting_exposure_order_count",
    "terminal_ts_ms",
    "terminal_reason",
    "censored",
    "terminal_inventory_units",
    "consumed_units",
    "reserved_units",
    "available_units",
    "reserved_order_count",
    "budget_hit",
    "final_quote_action_changed",
    "supported",
)

MECHANICS_RESULT_FIELDS = (
    "fills_bid",
    "fills_ask",
    "quote_attempts",
    "n_requotes",
    "gtx_rejects",
    "fills_while_pending_cancel",
    "dynamic_fill_hazard_cancel_request_count",
    "dynamic_fill_hazard_cancel_ack_count",
    "dynamic_fill_hazard_pre_ack_fill_count",
    "dynamic_fill_hazard_recovery_count",
    "dynamic_fill_hazard_reentry_count",
    "dynamic_fill_hazard_blocked_quote_count",
    "dynamic_fill_hazard_eval_count",
    "dynamic_fill_hazard_valid_eval_count",
    "dynamic_fill_hazard_invalid_eval_count",
    "consecutive_loss_cooldown_trigger_count",
    "consecutive_loss_cooldown_expiry_count",
    "consecutive_loss_cooldown_block_count",
    "consecutive_loss_cooldown_cancel_count",
    "sync_adjust_event_count",
    "exchange_book_events_consumed",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "post_cooldown_incremental_inventory_budget_assignment_count",
    "post_cooldown_incremental_inventory_budget_block_count",
    "post_cooldown_incremental_inventory_budget_unsupported_count",
    "post_cooldown_incremental_inventory_budget_conservation_checks",
    "post_cooldown_incremental_inventory_budget_conservation_failures",
)

Q90_ACTION_COUNTER_FIELDS = tuple(
    field
    for field in MECHANICS_RESULT_FIELDS
    if field.startswith("dynamic_fill_hazard_")
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


def _market_source_manifest_identity(
    spec: Mapping[str, Any],
    manifest: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Describe the frozen market identity without inventing an empty manifest."""

    source = spec["source_identity"]
    declared_sha256 = str(source["market_manifest_canonical_sha256"])
    declared_rows = int(source["market_manifest_rows"])
    declared_bytes = int(source["market_manifest_bytes"])
    if manifest is None:
        return {
            "canonical_sha256": declared_sha256,
            "rows": declared_rows,
            "bytes": declared_bytes,
            "rehash_performed": False,
            "entries_materialized": False,
            "verification_status": "declared_frozen_identity_not_rehashed",
        }

    actual_sha256 = canonical_sha256(manifest)
    actual_rows = len(manifest)
    actual_bytes = sum(int(row["bytes"]) for row in manifest)
    mismatches: list[str] = []
    if actual_sha256 != declared_sha256:
        mismatches.append(f"sha256 expected {declared_sha256}, found {actual_sha256}")
    if actual_rows != declared_rows:
        mismatches.append(f"rows expected {declared_rows}, found {actual_rows}")
    if actual_bytes != declared_bytes:
        mismatches.append(f"bytes expected {declared_bytes}, found {actual_bytes}")
    if mismatches:
        raise ValueError("relocated market source manifest changed: " + "; ".join(mismatches))
    return {
        "canonical_sha256": actual_sha256,
        "rows": actual_rows,
        "bytes": actual_bytes,
        "rehash_performed": True,
        "entries_materialized": True,
        "verification_status": "rehashed_and_matched_frozen_identity",
    }


def require_identity(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    actual = sha256_file(path)
    if actual != str(expected_sha256):
        raise ValueError(
            f"{label} hash mismatch: expected {expected_sha256}, found {actual}"
        )


def _relocate_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _relocate_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_relocate_value(item) for item in value]
    if isinstance(value, str) and value.startswith(str(LEGACY_MARKETDATA_ROOT)):
        return str(relocate_marketdata_path(value))
    return value


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_execution_spec(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected inventory-budget execution spec schema")
    permissions = payload.get("permissions") or {}
    forbidden_true = (
        "development_reward_or_pnl_read",
        "markout_read",
        "validation_read",
        "sealed_holdout_read",
        "randomized_action_identity_created",
        "action_experiment_authorized",
        "live_deployment_authorized",
    )
    enabled = [key for key in forbidden_true if bool(permissions.get(key, False))]
    if enabled:
        raise ValueError("mechanics identity has forbidden permissions: " + ", ".join(enabled))
    if not bool(permissions.get("development_mechanics_execution_allowed", False)):
        raise ValueError("Development mechanics execution is not enabled")
    if str(payload.get("identity") or "") != FAMILY_ID:
        raise ValueError(
            f"inventory-budget evaluator requires {FAMILY_ID}; "
            f"found {payload.get('identity')!r}"
        )
    if str((payload.get("replay_contract") or {}).get("buy_q90_action") or "") != (
        "off_both_arms"
    ):
        raise ValueError("v1.2 mechanics requires BUY q90 OFF in both arms")
    panels = payload.get("panels") or {}
    development = [str(day) for day in panels.get("development_days", ())]
    grade_a = [str(day) for day in panels.get("grade_a_days", ())]
    grade_b = [str(day) for day in panels.get("grade_b_days", ())]
    if not grade_a or not grade_b:
        raise ValueError("v1.2 mechanics requires explicit Grade-A and Grade-B day lists")
    if set(grade_a) & set(grade_b):
        raise ValueError("Grade-A and Grade-B Development day lists overlap")
    if sorted(grade_a + grade_b) != sorted(development):
        raise ValueError("Grade-A and Grade-B days do not partition Development")
    return payload


def _runtime_baseline_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    identity = spec["baseline_contract_identity"]
    baseline_path = Path(identity["path"])
    require_identity(baseline_path, identity["sha256"], "baseline replay contract")
    baseline = _relocate_value(_load_json(baseline_path))
    expected_days = [str(day) for day in spec["panels"]["development_days"]]
    actual_days = [str(day) for day in baseline["panels"]["development_days"]]
    if expected_days != actual_days:
        raise ValueError("execution Development days drifted from baseline contract")
    return baseline


def _validate_nonmarket_identities(
    spec: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> None:
    implementation = spec["implementation_identity"]
    require_identity(Path(__file__).resolve(), implementation["evaluator_sha256"], "evaluator")
    for relative, expected in implementation["source_sha256"].items():
        require_identity(ROOT / relative, expected, relative)
    state_contract = spec["state_machine_contract_identity"]
    require_identity(Path(state_contract["path"]), state_contract["sha256"], "state contract")
    quality = spec["panels"]["quality_ledger"]
    require_identity(
        Path(_relocate_value(quality["path"])),
        quality["sha256"],
        "Development quality ledger",
    )
    required_sections = [
        (baseline["operational_config_identity"], "operational config"),
        (baseline["execution_trade_identity"]["manifest"], "trade manifest"),
        (baseline["execution_trade_identity"]["quality_report"], "trade quality report"),
        (baseline["source_identity"]["normalized_l2_manifest"], "normalized L2 manifest"),
        (baseline["source_identity"]["normalized_l2_quality"], "normalized L2 quality"),
        (baseline["source_identity"]["queue_calibration"], "queue calibration"),
        (baseline["source_identity"]["p3_artifact"], "P3 artifact"),
        (baseline["latency_identity"]["samples"], "latency samples"),
        (baseline["panels"]["source_split"], "source split"),
    ]
    if str(spec["replay_contract"]["buy_q90_action"]) != "off_both_arms":
        required_sections.extend(
            (
                (baseline["buy_q90_identity"]["model"], "BUY q90 model"),
                (baseline["buy_q90_identity"]["policy"], "BUY q90 policy"),
            )
        )
    for section, label in required_sections:
        require_identity(Path(section["path"]), section["sha256"], label)

    mde = spec.get("mde_context") or {}
    if mde:
        require_identity(Path(mde["source_path"]), mde["source_sha256"], "MDE context")
    test_identity = spec["test_identity"]
    test_path = Path(test_identity["junit_xml"]["path"])
    require_identity(
        test_path,
        test_identity["junit_xml"]["sha256"],
        "inventory-budget contract tests",
    )
    junit = read_junit(test_path)
    required_names = set(test_identity["required_test_names"])
    missing_names = sorted(required_names - set(junit["test_names"]))
    if not junit["passed"] or missing_names:
        raise ValueError(f"inventory-budget contract tests are incomplete: {missing_names}")


def _validate_market_identity(
    spec: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> list[dict[str, Any]]:
    market_manifest = build_market_source_manifest(baseline)
    _market_source_manifest_identity(spec, market_manifest)
    return market_manifest


def _validate_identities(
    spec_path: Path,
    spec: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> list[dict[str, Any]]:
    del spec_path
    _validate_nonmarket_identities(spec, baseline)
    return _validate_market_identity(spec, baseline)


def _validate_source_rehash_mode(*, diagnostic_subset: bool, skip_source_rehash: bool) -> None:
    if skip_source_rehash and not diagnostic_subset:
        raise ValueError("formal 40-day Development run cannot skip source rehash")


def _check_storage(spec: Mapping[str, Any], output: Path, baseline: Mapping[str, Any]) -> None:
    gate = spec["storage_gate"]
    output_parent = output.parent
    output_parent.mkdir(parents=True, exist_ok=True)
    external_free = shutil.disk_usage(output_parent).free
    required_external = int(gate["minimum_external_free_bytes_after_estimate"]) + int(
        gate["estimated_output_bytes"]
    )
    if external_free < required_external:
        raise OSError(
            f"external evidence volume has {external_free} free bytes; "
            f"requires at least {required_external}"
        )
    cache_dir = Path(baseline["source_identity"]["window_cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_free = shutil.disk_usage(cache_dir).free
    if cache_free < int(gate["minimum_internal_cache_free_bytes"]):
        raise OSError(
            f"internal cache volume has {cache_free} free bytes; "
            f"requires at least {gate['minimum_internal_cache_free_bytes']}"
        )


def _finite_or_none(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _finite_or_none(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite_or_none(item) for item in value]
    return value


def _budget_trace(result: Mapping[str, Any], day: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in result.get("_post_cooldown_incremental_inventory_budget_trace", ()):
        row = {field: _finite_or_none(source.get(field)) for field in TRACE_FIELDS}
        row["day"] = str(day)
        rows.append(row)
    return rows


def _mechanics_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: _finite_or_none(result.get(field, 0))
        for field in MECHANICS_RESULT_FIELDS
    }


def _order_path_counter(rows: Iterable[Mapping[str, Any]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in rows:
        key = (
            str(row.get("side", "")),
            int(row.get("submit_ts", row.get("quote_ts", 0)) or 0),
            round(float(row.get("price", 0.0) or 0.0), 10),
            str(row.get("outcome", "")),
            int(row.get("outcome_ts", 0) or 0),
            round(float(row.get("fill_qty", 0.0) or 0.0), 10),
            round(float(row.get("remaining", 0.0) or 0.0), 10),
        )
        counter[json.dumps(key, separators=(",", ":"))] += 1
    return counter


def _path_stats(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    materialized = list(rows)
    output: dict[str, dict[str, Any]] = {}
    for side in SIDES:
        side_rows = [row for row in materialized if str(row.get("side", "")) == side]
        order_ids = {int(row.get("order_id", -1)) for row in side_rows}
        fills = [row for row in side_rows if str(row.get("outcome", "")) == "fill"]
        output[side] = {
            "order_count": int(len(order_ids)),
            "outcome_count": int(len(side_rows)),
            "fill_event_count": int(len(fills)),
            "filled_quantity_btc": float(
                sum(float(row.get("fill_qty", 0.0) or 0.0) for row in fills)
            ),
            "cancel_event_count": int(
                sum(str(row.get("outcome", "")) == "cancel" for row in side_rows)
            ),
            "gtx_reject_count": int(
                sum(str(row.get("outcome", "")) == "gtx_reject" for row in side_rows)
            ),
        }
    return output


def _path_difference(
    control_counter: Mapping[str, int],
    candidate_counter: Mapping[str, int],
    *,
    side: str,
) -> dict[str, int]:
    prefix = json.dumps([str(side)], separators=(",", ":"))[:-1] + ","
    control = Counter(
        {key: int(value) for key, value in control_counter.items() if key.startswith(prefix)}
    )
    candidate = Counter(
        {key: int(value) for key, value in candidate_counter.items() if key.startswith(prefix)}
    )
    return {
        "candidate_only_order_outcomes": int(sum((candidate - control).values())),
        "control_only_order_outcomes": int(sum((control - candidate).values())),
    }


def _configure_budget_params(
    baseline: Mapping[str, Any],
    spec: Mapping[str, Any],
    day: str,
    *,
    budget_units: float,
    target_side: str,
    trace_limit: int,
) -> dict[str, Any]:
    params = _configure_params(baseline, day)
    if str(spec["replay_contract"]["buy_q90_action"]) != "off_both_arms":
        raise ValueError("inventory-budget v1.2 only supports q90 OFF in both arms")
    params.update(
        {
            "fill_cooldown_clock_mode": "wall_time",
            "decision_trace_profile": "mechanics_only",
            "trace_decisions_max": 0,
            "post_cooldown_incremental_inventory_budget_enabled": True,
            "post_cooldown_incremental_inventory_budget_units": float(budget_units),
            "post_cooldown_incremental_inventory_budget_target_side": str(target_side),
            "post_cooldown_incremental_inventory_budget_arm_id": (
                "control_infinity"
                if math.isinf(budget_units)
                else f"{str(target_side).lower()}_budget_{int(budget_units)}_units"
            ),
            "trace_post_cooldown_incremental_inventory_budget_max": int(trace_limit),
            "replay_purpose": "post_cooldown_inventory_budget_mechanics",
            "replay_promotion_eligible": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "dynamic_fill_hazard_mechanics_telemetry_enabled": False,
        }
    )
    return params


def _assert_q90_off_result(result: Mapping[str, Any]) -> None:
    unexpected_q90: dict[str, Any] = {
        field: int(result.get(field, 0) or 0)
        for field in Q90_ACTION_COUNTER_FIELDS
        if int(result.get(field, 0) or 0) != 0
    }
    if bool(result.get("dynamic_fill_hazard_action_enabled", False)):
        unexpected_q90["dynamic_fill_hazard_action_enabled"] = True
    if unexpected_q90:
        raise RuntimeError(
            "q90 OFF contract emitted evaluations or actions: "
            + json.dumps(unexpected_q90, sort_keys=True)
        )


def _run_arm(
    baseline: Mapping[str, Any],
    day: str,
    window: Mapping[str, Any],
    params: Mapping[str, Any],
) -> dict[str, Any]:
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(baseline["source_identity"]["native_orderbook_root"]),
        day=day,
        symbol="BTCUSDC",
        tick_size=float(params.get("tick_size", bt.TICK)),
        warmup_hours=int(baseline["replay_contract"]["native_warmup_hours"]),
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
        exchange_book_event_tape=tape,
    )
    _assert_q90_off_result(result)
    return result


def _run_disabled_baseline(
    baseline: Mapping[str, Any],
    spec: Mapping[str, Any],
    day: str,
    window: Mapping[str, Any],
) -> dict[str, Any]:
    params = _configure_params(baseline, day)
    if str(spec["replay_contract"]["buy_q90_action"]) != "off_both_arms":
        raise ValueError("inventory-budget v1.2 only supports q90 OFF in both arms")
    params.update(
        {
            "fill_cooldown_clock_mode": "wall_time",
            "decision_trace_profile": "mechanics_only",
            "trace_decisions_max": 0,
            "replay_purpose": "post_cooldown_inventory_budget_control_equivalence",
            "replay_promotion_eligible": False,
            "post_cooldown_incremental_inventory_budget_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "dynamic_fill_hazard_mechanics_telemetry_enabled": False,
        }
    )
    return _run_arm(baseline, day, window, params)


def run_control_day(
    baseline: Mapping[str, Any],
    spec: Mapping[str, Any],
    day: str,
) -> dict[str, Any]:
    started = time.monotonic()
    trace_limit = int(spec["replay_contract"]["budget_trace_max"])
    params = _configure_budget_params(
        baseline,
        spec,
        day,
        budget_units=math.inf,
        target_side="BOTH",
        trace_limit=trace_limit,
    )
    window = _load_window(baseline, day, params)
    control = _run_arm(baseline, day, window, params)
    quote_rows = list(control.get("_quote_trace", ()))
    payload: dict[str, Any] = {
        "day": str(day),
        "runtime_s": float(time.monotonic() - started),
        "mechanics": _mechanics_result(control),
        "budget_trace": _budget_trace(control, day),
        "path_stats": _path_stats(quote_rows),
        "path_counter": dict(_order_path_counter(quote_rows)),
        "control_equivalence_checked": False,
        "control_equivalence_passed": None,
    }
    equivalence_contract = spec["q90_off_reference_equivalence_days"]
    equivalence_days = {
        str(value)
        for panel in ("grade_a_primary", "grade_b_sensitivity")
        for value in equivalence_contract[panel]
    }
    if day in equivalence_days:
        disabled = _run_disabled_baseline(baseline, spec, day, window)
        disabled_rows = list(disabled.get("_quote_trace", ()))
        same_path = _order_path_counter(disabled_rows) == _order_path_counter(quote_rows)
        same_mechanics = all(
            disabled.get(field, 0) == control.get(field, 0)
            for field in (
                "fills_bid",
                "fills_ask",
                "quote_attempts",
                "n_requotes",
                "gtx_rejects",
                "fills_while_pending_cancel",
            )
        )
        payload["control_equivalence_checked"] = True
        payload["control_equivalence_passed"] = bool(same_path and same_mechanics)
    return payload


def run_candidate_day(
    baseline: Mapping[str, Any],
    spec: Mapping[str, Any],
    day: str,
    candidate_grid: Mapping[str, list[int]],
    control_checkpoint: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    control = _load_json(control_checkpoint)["result"]
    trace_limit = int(spec["replay_contract"]["budget_trace_max"])
    seed_params = _configure_budget_params(
        baseline,
        spec,
        day,
        budget_units=1.0,
        target_side="SELL",
        trace_limit=trace_limit,
    )
    window = _load_window(baseline, day, seed_params)
    arms: list[dict[str, Any]] = []
    for side in SIDES:
        for budget_units in candidate_grid.get(side, ()):
            params = _configure_budget_params(
                baseline,
                spec,
                day,
                budget_units=float(budget_units),
                target_side=side,
                trace_limit=trace_limit,
            )
            result = _run_arm(baseline, day, window, params)
            quote_rows = list(result.get("_quote_trace", ()))
            candidate_counter = dict(_order_path_counter(quote_rows))
            arms.append(
                {
                    "day": str(day),
                    "target_side": side,
                    "budget_units": int(budget_units),
                    "mechanics": _mechanics_result(result),
                    "budget_trace": _budget_trace(result, day),
                    "path_stats": _path_stats(quote_rows),
                    "path_difference": _path_difference(
                        control["path_counter"],
                        candidate_counter,
                        side=side,
                    ),
                }
            )
    return {
        "day": str(day),
        "runtime_s": float(time.monotonic() - started),
        "arms": arms,
    }


def _run_candidate_day_from_checkpoints(
    baseline: Mapping[str, Any],
    spec: Mapping[str, Any],
    candidate_grid: Mapping[str, list[int]],
    control_checkpoint_dir: Path,
    day: str,
) -> dict[str, Any]:
    return run_candidate_day(
        baseline,
        spec,
        day,
        candidate_grid,
        control_checkpoint_dir / f"{day}.json",
    )


def _write_checkpoint(path: Path, spec_sha256: str, result: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            {"spec_sha256": spec_sha256, "result": _finite_or_none(dict(result))},
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _control_equivalence_summary(
    controls: Iterable[Mapping[str, Any]],
    *,
    frozen_days: Iterable[str],
    evaluated_days: Iterable[str],
) -> dict[str, Any]:
    """Evaluate equivalence only for frozen check days present in this run."""

    frozen = [str(day) for day in frozen_days]
    evaluated = {str(day) for day in evaluated_days}
    required = [day for day in frozen if day in evaluated]
    checked_by_day = {
        str(row["day"]): row
        for row in controls
        if bool(row.get("control_equivalence_checked", False))
    }
    checked = [day for day in required if day in checked_by_day]
    missing = [day for day in required if day not in checked_by_day]
    failed = [
        day
        for day in checked
        if not bool(checked_by_day[day].get("control_equivalence_passed", False))
    ]
    passed: bool | None
    if not required:
        passed = None
    else:
        passed = not missing and not failed
    return {
        "frozen_days": frozen,
        "required_days_for_run": required,
        "checked_days": checked,
        "missing_required_days": missing,
        "failed_days": failed,
        "passed": passed,
        "scope": "evaluated_frozen_equivalence_days",
    }


def _run_stage(
    days: list[str],
    *,
    workers: int,
    checkpoint_dir: Path,
    spec_sha256: str,
    resume: bool,
    submit: Any,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in days:
        checkpoint = checkpoint_dir / f"{day}.json"
        if resume and checkpoint.is_file():
            payload = _load_json(checkpoint)
            if payload.get("spec_sha256") != spec_sha256:
                raise ValueError(f"checkpoint spec mismatch: {checkpoint}")
            results.append(payload["result"])
        else:
            pending.append(day)
    active_workers = max(1, min(int(workers), len(pending) or 1))
    if active_workers == 1:
        for day in pending:
            result = submit(day)
            results.append(result)
            _write_checkpoint(checkpoint_dir / f"{day}.json", spec_sha256, result)
            print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))
    else:
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        with ProcessPoolExecutor(max_workers=active_workers) as pool:
            futures = {pool.submit(submit, day): day for day in pending}
            for future in as_completed(futures):
                day = futures[future]
                result = future.result()
                results.append(result)
                _write_checkpoint(checkpoint_dir / f"{day}.json", spec_sha256, result)
                print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))
    return sorted(results, key=lambda row: str(row["day"]))


def _derive_grid(
    controls: Iterable[Mapping[str, Any]],
    maximum_units: int,
) -> dict[str, list[int]]:
    traces = [row for result in controls for row in result.get("budget_trace", ())]
    grid: dict[str, list[int]] = {}
    for side in SIDES:
        values = [
            float(row.get("consumed_units") or 0.0)
            for row in traces
            if row.get("side") == side and int(row.get("supported") or 0) == 1
        ]
        grid[side] = list(
            outcome_blind_budget_grid(values, maximum_units=int(maximum_units))
        )
    return grid


def _ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return float(numerator / denominator)


def _summarize_candidates(
    controls: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    grid: Mapping[str, list[int]],
    spec: Mapping[str, Any],
) -> pd.DataFrame:
    control_by_day = {str(row["day"]): row for row in controls}
    arm_rows = [arm for day in candidates for arm in day.get("arms", ())]
    gates = spec["mechanics_gates"]
    summary: list[dict[str, Any]] = []
    for side in SIDES:
        for budget in grid.get(side, ()):
            selected = [
                arm
                for arm in arm_rows
                if arm["target_side"] == side and int(arm["budget_units"]) == int(budget)
            ]
            traces = [row for arm in selected for row in arm.get("budget_trace", ())]
            supported = [row for row in traces if int(row.get("supported") or 0) == 1]
            unsupported = [row for row in traces if int(row.get("supported") or 0) != 1]
            hits = [row for row in supported if int(row.get("budget_hit") or 0) == 1]
            control_fills = sum(
                int(control_by_day[arm["day"]]["path_stats"][side]["fill_event_count"])
                for arm in selected
            )
            candidate_fills = sum(
                int(arm["path_stats"][side]["fill_event_count"]) for arm in selected
            )
            control_orders = sum(
                int(control_by_day[arm["day"]]["path_stats"][side]["order_count"])
                for arm in selected
            )
            candidate_orders = sum(
                int(arm["path_stats"][side]["order_count"]) for arm in selected
            )
            unsupported_rate = _ratio(len(unsupported), len(traces))
            action_change_rate = _ratio(len(hits), len(supported))
            fill_retention = _ratio(candidate_fills, control_fills)
            activity_retention = _ratio(candidate_orders, control_orders)
            conservation_failures = sum(
                int(
                    arm["mechanics"].get(
                        "post_cooldown_incremental_inventory_budget_conservation_failures",
                        0,
                    )
                    or 0
                )
                for arm in selected
            )
            overshoots = sum(int(row.get("one_order_overshoot_count") or 0) for row in traces)
            support_pass = bool(
                len(supported) >= int(gates["minimum_supported_episodes_per_side_budget"])
                and len({row["day"] for row in supported})
                >= int(gates["minimum_supported_days_per_side_budget"])
            )
            action_pass = bool(
                float(gates["minimum_action_change_rate"])
                <= action_change_rate
                <= float(gates["maximum_action_change_rate"])
            )
            retention_pass = bool(
                fill_retention >= float(gates["minimum_fill_retention"])
                and activity_retention >= float(gates["minimum_activity_retention"])
            )
            integrity_pass = bool(
                unsupported_rate <= float(gates["maximum_unsupported_rate"])
                and conservation_failures == 0
                and overshoots == 0
            )
            summary.append(
                {
                    "side": side,
                    "budget_units": int(budget),
                    "evaluated_days": int(len(selected)),
                    "supported_episodes": int(len(supported)),
                    "supported_days": int(len({row["day"] for row in supported})),
                    "unsupported_episodes": int(len(unsupported)),
                    "unsupported_rate": unsupported_rate,
                    "censored_supported_rate": _ratio(
                        sum(int(row.get("censored") or 0) for row in supported),
                        len(supported),
                    ),
                    "budget_hit_episodes": int(len(hits)),
                    "action_change_days": int(len({row["day"] for row in hits})),
                    "final_action_change_rate": action_change_rate,
                    "control_fill_events": int(control_fills),
                    "candidate_fill_events": int(candidate_fills),
                    "fill_retention": fill_retention,
                    "control_orders": int(control_orders),
                    "candidate_orders": int(candidate_orders),
                    "activity_retention": activity_retention,
                    "candidate_only_order_outcomes": int(
                        sum(
                            int(arm["path_difference"]["candidate_only_order_outcomes"])
                            for arm in selected
                        )
                    ),
                    "control_only_order_outcomes": int(
                        sum(
                            int(arm["path_difference"]["control_only_order_outcomes"])
                            for arm in selected
                        )
                    ),
                    "consumed_units": float(
                        sum(float(row.get("consumed_units") or 0.0) for row in supported)
                    ),
                    "blocked_planned_units": float(
                        sum(float(row.get("blocked_planned_units") or 0.0) for row in supported)
                    ),
                    "maximum_abs_inventory_units": float(
                        max(
                            (float(row.get("max_abs_inventory_units") or 0.0) for row in supported),
                            default=0.0,
                        )
                    ),
                    "reducing_budget_bypass_count": int(
                        sum(
                            int(row.get("reducing_order_budget_bypass_count") or 0)
                            for row in supported
                        )
                    ),
                    "conservation_failures": int(conservation_failures),
                    "one_order_overshoot_count": int(overshoots),
                    "support_gate_passed": support_pass,
                    "action_leverage_gate_passed": action_pass,
                    "retention_gate_passed": retention_pass,
                    "integrity_gate_passed": integrity_pass,
                    "mechanics_region_passed": bool(
                        support_pass and action_pass and retention_pass and integrity_pass
                    ),
                }
            )
    return pd.DataFrame(summary)


def _results_for_days(
    rows: Iterable[Mapping[str, Any]],
    days: Iterable[str],
) -> list[dict[str, Any]]:
    allowed = {str(day) for day in days}
    return [dict(row) for row in rows if str(row.get("day")) in allowed]


def _panel_summary(
    controls: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    grid: Mapping[str, list[int]],
    spec: Mapping[str, Any],
    *,
    panel: str,
    days: Iterable[str],
) -> pd.DataFrame:
    selected_controls = _results_for_days(controls, days)
    selected_candidates = _results_for_days(candidates, days)
    summary = _summarize_candidates(selected_controls, selected_candidates, grid, spec)
    if not summary.empty:
        summary.insert(0, "panel", str(panel))
    return summary


def _report_markdown(report: Mapping[str, Any], summary: pd.DataFrame) -> str:
    lines = [
        "# Post-Cooldown Incremental Inventory Budget Feasibility v1.2",
        "",
        "Development-only mechanics. No reward, PnL, markout, Validation, or holdout was read.",
        "",
        f"- decision: `{report['decision']}`",
        f"- evaluated Development days: `{len(report['evaluated_days'])}`",
        f"- candidate grid: `{json.dumps(report['candidate_grid'], sort_keys=True)}`",
        "- q90-OFF reference equivalence, Grade A: "
        f"`{report['q90_off_reference_equivalence']['grade_a_primary']['passed']}`",
        "- SELL is the primary mechanics slice; BUY is a separate negative control.",
        "- This audit cannot create an action identity or authorize live deployment.",
        "",
        "| Panel | Side | Budget | Episodes | Change rate | Fill retention | Activity retention | Unsupported | Pass |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in summary.to_dict("records"):
        lines.append(
            "| {panel} | {side} | {budget_units} | {supported_episodes} | "
            "{final_action_change_rate:.2%} | {fill_retention:.2%} | "
            "{activity_retention:.2%} | {unsupported_rate:.2%} | "
            "{mechanics_region_passed} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--days", nargs="*")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-source-rehash", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec = _load_execution_spec(spec_path)
    baseline = _runtime_baseline_spec(spec)
    _check_storage(spec, output, baseline)
    if output.exists() and any(output.iterdir()) and not args.resume:
        raise FileExistsError(f"output directory already contains files: {output}")
    output.mkdir(parents=True, exist_ok=True)

    development_days = [str(day) for day in spec["panels"]["development_days"]]
    selected_days = list(args.days or development_days)
    unknown = sorted(set(selected_days) - set(development_days))
    if unknown:
        raise ValueError(f"days outside frozen Development: {unknown}")
    diagnostic_subset = selected_days != development_days
    _validate_source_rehash_mode(
        diagnostic_subset=diagnostic_subset,
        skip_source_rehash=bool(args.skip_source_rehash),
    )
    _validate_nonmarket_identities(spec, baseline)
    if args.skip_source_rehash:
        if not bool(spec["source_identity"].get("diagnostic_skip_rehash_allowed", False)):
            raise ValueError("source rehash skipping is not permitted by this spec")
        market_manifest: list[dict[str, Any]] | None = None
    else:
        market_manifest = _validate_market_identity(spec, baseline)
    market_manifest_identity = _market_source_manifest_identity(spec, market_manifest)
    spec_sha256 = sha256_file(spec_path)
    selected_set = set(selected_days)
    grade_a_days = [
        str(day) for day in spec["panels"]["grade_a_days"] if str(day) in selected_set
    ]
    grade_b_days = [
        str(day) for day in spec["panels"]["grade_b_days"] if str(day) in selected_set
    ]

    control_dir = output / "control_checkpoints"
    candidate_dir = output / "candidate_checkpoints"
    workers = max(1, int(args.workers))
    controls = _run_stage(
        selected_days,
        workers=workers,
        checkpoint_dir=control_dir,
        spec_sha256=spec_sha256,
        resume=args.resume,
        submit=partial(run_control_day, baseline, spec),
    )
    grade_a_controls = _results_for_days(controls, grade_a_days)
    grid = _derive_grid(
        grade_a_controls,
        maximum_units=int(spec["candidate_grid"]["maximum_candidate_units"]),
    )
    minimum_candidates = int(spec["candidate_grid"]["minimum_distinct_nonzero_candidates"])
    grid_supported = {side: len(grid.get(side, ())) >= minimum_candidates for side in SIDES}

    candidates: list[dict[str, Any]] = []
    if any(grid.values()):
        candidates = _run_stage(
            selected_days,
            workers=workers,
            checkpoint_dir=candidate_dir,
            spec_sha256=spec_sha256,
            resume=args.resume,
            submit=partial(
                _run_candidate_day_from_checkpoints,
                baseline,
                spec,
                grid,
                control_dir,
            ),
        )

    primary_summary = _panel_summary(
        controls,
        candidates,
        grid,
        spec,
        panel="grade_a_primary",
        days=grade_a_days,
    )
    sensitivity_summary = _panel_summary(
        controls,
        candidates,
        grid,
        spec,
        panel="grade_b_sensitivity",
        days=grade_b_days,
    )
    pooled_summary = _panel_summary(
        controls,
        candidates,
        grid,
        spec,
        panel="all_40_diagnostic",
        days=selected_days,
    )
    summary = pd.concat(
        [primary_summary, sensitivity_summary, pooled_summary],
        ignore_index=True,
    )
    control_traces = pd.DataFrame(
        [row for result in controls for row in result.get("budget_trace", ())]
    )
    candidate_traces = pd.DataFrame(
        [
            {**row, "target_side": arm["target_side"], "candidate_budget_units": arm["budget_units"]}
            for result in candidates
            for arm in result.get("arms", ())
            for row in arm.get("budget_trace", ())
        ]
    )
    daily_rows: list[dict[str, Any]] = []
    for result in controls:
        daily_rows.append(
            {
                "day": result["day"],
                "stage": "control_infinity",
                "target_side": "BOTH",
                "budget_units": math.inf,
                "runtime_s": result["runtime_s"],
                "trace_episodes": len(result.get("budget_trace", ())),
                **result["mechanics"],
            }
        )
    for result in candidates:
        arm_runtime = float(result["runtime_s"]) / max(len(result.get("arms", ())), 1)
        for arm in result.get("arms", ()):
            daily_rows.append(
                {
                    "day": arm["day"],
                    "stage": "candidate",
                    "target_side": arm["target_side"],
                    "budget_units": arm["budget_units"],
                    "runtime_s": arm_runtime,
                    "trace_episodes": len(arm.get("budget_trace", ())),
                    **arm["mechanics"],
                    **arm["path_difference"],
                }
            )
    daily = pd.DataFrame(daily_rows)

    equivalence_contract = spec["q90_off_reference_equivalence_days"]
    primary_equivalence = _control_equivalence_summary(
        controls,
        frozen_days=equivalence_contract["grade_a_primary"],
        evaluated_days=grade_a_days,
    )
    sensitivity_equivalence = _control_equivalence_summary(
        controls,
        frozen_days=equivalence_contract["grade_b_sensitivity"],
        evaluated_days=grade_b_days,
    )
    sell_pass = bool(
        grid_supported["SELL"]
        and not primary_summary.empty
        and primary_summary.loc[
            primary_summary["side"] == "SELL", "mechanics_region_passed"
        ].any()
    )
    if diagnostic_subset:
        decision = "diagnostic_subset_only_no_family_decision"
    elif primary_equivalence["passed"] is not True:
        decision = "close_inventory_budget_mechanics_q90_off_reference_not_reproduced"
    elif not grid_supported["SELL"]:
        decision = "close_inventory_budget_mechanics_insufficient_sell_grid_resolution"
    elif sell_pass:
        decision = "mechanics_region_supported_no_action_identity_created"
    else:
        decision = "close_inventory_budget_mechanics_no_supported_sell_region"

    manifest_path = output / "market_source_manifest.json"
    manifest_payload: Any
    if market_manifest is None:
        manifest_payload = {
            "schema_version": "market_source_manifest_reference.v1",
            "entries_materialized": False,
            "identity": market_manifest_identity,
            "reason": "diagnostic source rehash was explicitly skipped",
        }
    else:
        manifest_payload = market_manifest
    manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    control_trace_path = output / "control_inventory_budget_trace.parquet"
    candidate_trace_path = output / "candidate_inventory_budget_trace.parquet"
    daily_path = output / "daily_mechanics.csv"
    summary_path = output / "side_budget_mechanics.csv"
    report_path = output / "report.json"
    markdown_path = output / "report.md"
    control_traces.to_parquet(control_trace_path, index=False)
    candidate_traces.to_parquet(candidate_trace_path, index=False)
    daily.to_csv(daily_path, index=False)
    summary.to_csv(summary_path, index=False)

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "family_id": str(spec["identity"]),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "diagnostic_subset": diagnostic_subset,
        "evaluated_days": selected_days,
        "development_days": development_days,
        "validation_days_read": [],
        "sealed_holdout_days_read": [],
        "economics_read": False,
        "candidate_grid": grid,
        "candidate_grid_supported": grid_supported,
        "primary_side": "SELL",
        "negative_control_side": "BUY",
        "side_pooling_used": False,
        "q90_off_reference_equivalence": {
            "grade_a_primary": primary_equivalence,
            "grade_b_sensitivity": sensitivity_equivalence,
            "interpretation": "finite-budget infinity versus disabled-budget equivalence under the frozen q90-OFF reference; this is not current live-baseline equivalence",
        },
        "quality_panels": {
            "grade_a_primary_days": grade_a_days,
            "grade_b_sensitivity_days": grade_b_days,
            "primary_decision_uses_grade_b": False,
            "candidate_grid_source": "grade_a_primary_unlimited_control_only",
        },
        "mechanics_summary": summary.to_dict("records"),
        "sell_mechanics_region_supported": sell_pass,
        "mde_context": {
            **(spec.get("mde_context") or {}),
            "decision_gate": False,
            "interpretation": "bound contextual design scale, not an effect estimate",
        },
        "test_identity": spec["test_identity"],
        "python_cpp_scope": {
            "python_authoritative_full_path": True,
            "cpp_state_machine_or_full_path_authority": False,
            "cpp_development_allowed_only_after_mechanics_region": sell_pass,
        },
        "buy_q90_action": "off_both_arms",
        "permissions": {
            "development_reward_or_pnl_read": False,
            "markout_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "randomized_action_identity_created": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
        "spec": {"path": str(spec_path), "sha256": spec_sha256},
        "baseline_contract": spec["baseline_contract_identity"],
        "state_machine_contract": spec["state_machine_contract_identity"],
        "market_source_manifest": {
            "path": str(manifest_path),
            **market_manifest_identity,
            "artifact_sha256": sha256_file(manifest_path),
        },
        "artifacts": {
            "control_trace": str(control_trace_path),
            "candidate_trace": str(candidate_trace_path),
            "daily_mechanics": str(daily_path),
            "side_budget_mechanics": str(summary_path),
        },
    }
    report["report_payload_sha256"] = canonical_sha256(report)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_report_markdown(report, summary), encoding="utf-8")
    artifact_paths = (
        control_trace_path,
        candidate_trace_path,
        daily_path,
        summary_path,
        report_path,
        markdown_path,
        manifest_path,
    )
    artifact_manifest = {
        "schema_version": "post_cooldown_incremental_inventory_budget_manifest.v1",
        "spec_sha256": spec_sha256,
        "artifacts": [
            {"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size}
            for path in artifact_paths
        ],
    }
    (output / "manifest.json").write_text(
        json.dumps(artifact_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output_dir": str(output),
                "decision": decision,
                "candidate_grid": grid,
                "sell_mechanics_region_supported": sell_pass,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
