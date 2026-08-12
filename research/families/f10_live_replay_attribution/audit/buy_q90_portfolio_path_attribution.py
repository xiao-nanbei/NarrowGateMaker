#!/usr/bin/env python3
"""Paired full-path attribution for the operational BUY q90 policy.

The experiment replays each frozen Development day twice on the same causal
market, latency, and system-event path.  The only arm difference is whether
the frozen BUY exposure-increasing q90 cancel/re-entry policy is enabled.
This is evidence about the replayed policy mechanism, not live or action
authorization.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import time
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import LEGACY_MARKETDATA_ROOT, relocate_marketdata_path
from models import backtest_tick as bt
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as full_path,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "buy_q90_portfolio_path_attribution.v1"
IDENTITY = "buy_q90_portfolio_path_attribution_v1"
ARMS = ("q90_off", "q90_on")
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "buy_q90_portfolio_path_attribution_v1_spec_20260731.json"
)

IMPLEMENTATION_PATHS = (
    "research/families/f10_live_replay_attribution/audit/buy_q90_portfolio_path_attribution.py",
    "research/families/f09_campaign_action_uplift/audit/volatility_time_add_rearm_full_path_preflight.py",
    "tests/test_buy_q90_portfolio_path_attribution.py",
    "tests/test_dynamic_fill_hazard_cpp_parity.py",
    "tests/test_volatility_time_add_rearm_full_path_preflight.py",
    "tests/test_replay_path_dependent_controls.py",
    "tests/test_post_cooldown_incremental_inventory_budget.py",
    "tests/test_post_cooldown_incremental_inventory_budget_feasibility.py",
    "tests/test_cpp_tick_replay_parity.py",
    "models/backtest_tick.py",
    "models/backtest_config.py",
    "models/data_windows.py",
    "models/exchange_book_replay.py",
    "models/replay_contract.py",
    "data_paths.py",
    "live/config.py",
    "cpp/narrowgate_cpp/dynamic_fill_hazard.cpp",
    "cpp/narrowgate_cpp/dynamic_fill_hazard.hpp",
    "strategy/dynamic_fill_hazard_model.py",
    "strategy/fill_cooldown.py",
    "strategy/policy_guards.py",
    "strategy/quote_core.py",
    "strategy/replay_controls.py",
    "strategy/signal.py",
)

PAIRED_METRICS = (
    "terminal_mtm_pnl_usdc",
    "closed_campaign_value_usdc",
    "buy_exposure_fills_per_hour",
    "sell_exposure_fills_per_hour",
    "exposure_side_imbalance_per_hour",
    "short_campaign_share",
    "multi_level_short_share_of_all",
    "multi_level_short_rate_given_short",
    "single_level_short_value_usdc",
    "multi_level_short_value_usdc",
    "abs_inventory_time_s",
    "sq_inventory_time_s",
    "fills_total",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
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
    normalized.pop("canonical_spec_sha256", None)
    return canonical_sha256(normalized)


def _relocate_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _relocate_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_relocate_value(item) for item in value]
    if isinstance(value, str) and value.startswith(str(LEGACY_MARKETDATA_ROOT)):
        return str(relocate_marketdata_path(value))
    return value


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return payload


def _require_identity(path: Path, expected: str, label: str) -> None:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} is missing: {resolved}")
    actual = sha256_file(resolved)
    if actual != str(expected):
        raise ValueError(
            f"{label} hash mismatch: expected {expected}, found {actual}"
        )


def _read_junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    names: list[str] = []
    for suite in suites:
        for key in totals:
            totals[key] += int(suite.attrib.get(key, 0) or 0)
        names.extend(str(case.attrib.get("name", "")) for case in suite.findall("testcase"))
    return {
        **totals,
        "test_names": sorted(set(names)),
        "passed": bool(
            totals["tests"] > 0
            and totals["failures"] == 0
            and totals["errors"] == 0
        ),
    }


def validate_spec(spec: Mapping[str, Any]) -> None:
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected BUY q90 attribution schema")
    if spec.get("identity") != IDENTITY:
        raise ValueError("unexpected BUY q90 attribution identity")
    if spec.get("status") != "frozen_before_development_pair_outcome_read":
        raise ValueError("BUY q90 attribution status drifted")
    frozen_hash = str(spec.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(spec) != frozen_hash:
        raise ValueError("BUY q90 attribution canonical spec hash mismatch")

    panels = spec.get("panels") or {}
    development = tuple(map(str, panels.get("development_days") or ()))
    grade_a = tuple(
        map(str, panels.get("development_primary_grade_a_days") or ())
    )
    grade_b = tuple(
        map(str, panels.get("development_sensitivity_grade_b_days") or ())
    )
    if len(development) != 40 or development != tuple(sorted(development)):
        raise ValueError("BUY q90 attribution requires the frozen 40-day Development panel")
    if (
        len(grade_a) != 24
        or len(grade_b) != 16
        or set(grade_a) & set(grade_b)
        or set(grade_a) | set(grade_b) != set(development)
        or grade_a != tuple(sorted(grade_a))
        or grade_b != tuple(sorted(grade_b))
        or panels.get("grade_b_policy")
        != "sensitivity_only_never_pooled_into_primary_decision"
    ):
        raise ValueError("BUY q90 Grade-A/Grade-B Development identity drifted")
    later = tuple(
        map(
            str,
            (
                *(panels.get("validation_days_not_read") or ()),
                *(panels.get("sealed_holdout_days_not_read") or ()),
            ),
        )
    )
    if set(development) & set(later):
        raise ValueError("BUY q90 Development overlaps a locked later panel")

    treatment = spec.get("treatment_contract") or {}
    expected_treatment = {
        "control": "q90_off",
        "candidate": "q90_on",
        "changed_mechanism": "BUY_exposure_increasing_active_order_cancel_reenter_only",
        "sell_unchanged": True,
        "buy_reducing_unchanged": True,
        "future_fills_reused_between_arms": False,
    }
    for key, expected in expected_treatment.items():
        if treatment.get(key) != expected:
            raise ValueError(f"BUY q90 treatment contract drifted: {key}")

    replay = spec.get("replay_contract") or {}
    required_replay = {
        "engine": "python_authoritative",
        "native_queue": "strict_snapshot_delta_exact_level",
        "initial_state": "daily_fresh_start",
        "fill_cooldown_clock": "wall_time_85n",
        "ml_enabled": False,
        "maker_fill_prob": 1.0,
        "sync_adjust_mode": "disabled_primary",
        "latency_path": "shared_between_arms",
        "rng_path": "shared_between_arms",
        "q90_on_cpp_scope": "native_book_and_buy_q90_kernel_lockstep_only",
        "full_cpp_tick_replay_authority": False,
    }
    for key, expected in required_replay.items():
        if replay.get(key) != expected:
            raise ValueError(f"BUY q90 replay contract drifted: {key}")
    if int(replay.get("trace_fills_max_per_arm_day", 0) or 0) <= 0:
        raise ValueError("BUY q90 fill-trace bound is invalid")

    inference = spec.get("inference_contract") or {}
    if (
        tuple(inference.get("paired_metrics") or ()) != PAIRED_METRICS
        or inference.get("cluster_unit") != "UTC_day"
        or inference.get("interval") != "paired_day_bootstrap_95pct"
        or int(inference.get("minimum_evaluated_days", 0) or 0) != 40
        or int(inference.get("minimum_primary_grade_a_days", 0) or 0) != 24
        or int(inference.get("minimum_sensitivity_grade_b_days", 0) or 0) != 16
    ):
        raise ValueError("BUY q90 inference contract drifted")

    permissions = spec.get("permissions") or {}
    required_false = {
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
        "automatic_live_rollback_authorized",
    }
    if not bool(permissions.get("development_pair_execution_allowed", False)):
        raise ValueError("BUY q90 Development execution is not authorized")
    if any(bool(permissions.get(key, False)) for key in required_false):
        raise ValueError("BUY q90 attribution cannot grant later-panel or live authority")


def _runtime_source_contract(spec: Mapping[str, Any]) -> dict[str, Any]:
    identity = spec["source_contract_identity"]
    source_path = Path(str(identity["path"])).expanduser().resolve()
    _require_identity(source_path, str(identity["sha256"]), "source replay contract")
    source = _relocate_value(_load_json(source_path))
    expected_days = list(map(str, spec["panels"]["development_days"]))
    if list(map(str, source["panels"]["development_days"])) != expected_days:
        raise ValueError("BUY q90 Development denominator drifted from source contract")
    return source


def validate_frozen_identities(
    spec_path: Path,
    spec: Mapping[str, Any],
    source: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validate_spec(spec)
    implementation = spec.get("implementation_identity") or {}
    if set(implementation) != set(IMPLEMENTATION_PATHS):
        raise ValueError("BUY q90 implementation identity is incomplete")
    for relative, expected in implementation.items():
        _require_identity(ROOT / relative, str(expected), relative)
    identities = (
        (source["operational_config_identity"], "operational config"),
        (source["execution_trade_identity"]["manifest"], "execution trade manifest"),
        (source["execution_trade_identity"]["quality_report"], "execution trade quality"),
        (source["source_identity"]["normalized_l2_manifest"], "normalized L2 manifest"),
        (source["source_identity"]["normalized_l2_quality"], "normalized L2 quality"),
        (source["source_identity"]["queue_calibration"], "queue calibration"),
        (source["source_identity"]["p3_artifact"], "P3 artifact"),
        (source["latency_identity"]["samples"], "latency samples"),
        (source["buy_q90_identity"]["model"], "BUY q90 model"),
        (source["buy_q90_identity"]["policy"], "BUY q90 policy"),
        (source["panels"]["source_split"], "source split"),
    )
    for identity, label in identities:
        _require_identity(Path(str(identity["path"])), str(identity["sha256"]), label)

    native = spec["native_module_identity"]
    _require_identity(Path(str(native["path"])), str(native["sha256"]), "q90 native module")
    tests = spec["test_identity"]
    junit_path = Path(str(tests["junit_xml"]["path"]))
    _require_identity(junit_path, str(tests["junit_xml"]["sha256"]), "q90 contract tests")
    junit = _read_junit(junit_path)
    missing = sorted(set(tests["required_test_names"]) - set(junit["test_names"]))
    if not junit["passed"] or missing:
        raise ValueError(f"BUY q90 contract tests are incomplete: {missing}")

    market_manifest = full_path.build_market_source_manifest(source)
    actual_manifest = canonical_sha256(market_manifest)
    expected_manifest = str(spec["source_manifest_identity"]["canonical_sha256"])
    if actual_manifest != expected_manifest:
        raise ValueError(
            "BUY q90 market source manifest drifted: "
            f"expected {expected_manifest}, found {actual_manifest}"
        )
    return market_manifest, junit


def reconstruct_campaigns(
    fills: pd.DataFrame,
    *,
    day: str,
    arm: str,
    inventory_unit_btc: float,
    terminal_mark_price: float,
    expected_campaign_count: int | None = None,
    expected_closed_count: int | None = None,
) -> pd.DataFrame:
    """Reconstruct flat-to-flat campaigns from the authoritative fill path."""

    required = {
        "side",
        "fill_ts",
        "quote_px",
        "fill_qty",
        "fill_fee_usdc",
        "inventory_before_fill",
        "inventory_after_fill",
    }
    missing = sorted(required - set(fills.columns))
    if missing:
        raise ValueError("fill trace is missing campaign fields: " + ", ".join(missing))
    if arm not in ARMS:
        raise ValueError(f"unknown q90 arm: {arm}")
    unit = float(inventory_unit_btc)
    if not math.isfinite(unit) or unit <= 0.0:
        raise ValueError("inventory unit must be positive")
    terminal_mark = float(terminal_mark_price)
    if not math.isfinite(terminal_mark) or terminal_mark <= 0.0:
        raise ValueError("terminal mark price must be positive")

    ordered = fills.reset_index(drop=False).rename(columns={"index": "_trace_order"})
    ordered = ordered.sort_values(["fill_ts", "_trace_order"], kind="stable")
    tolerance = max(1e-10, unit * 1e-7)
    records: list[dict[str, Any]] = []
    active: dict[str, Any] | None = None
    running_inventory = 0.0
    campaign_id = 0

    for row in ordered.to_dict("records"):
        side = str(row["side"]).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"invalid fill side: {side}")
        before = float(row["inventory_before_fill"])
        after = float(row["inventory_after_fill"])
        qty = float(row["fill_qty"])
        price = float(row["quote_px"])
        fee = float(row["fill_fee_usdc"])
        if not all(math.isfinite(value) for value in (before, after, qty, price, fee)):
            raise ValueError("fill trace contains non-finite campaign values")
        if qty <= 0.0 or price <= 0.0 or fee < 0.0:
            raise ValueError("fill trace contains invalid price, quantity, or fee")
        expected_after = before + (qty if side == "BUY" else -qty)
        if abs(expected_after - after) > tolerance:
            raise ValueError("fill trace inventory arithmetic does not close")
        if abs(before - running_inventory) > tolerance:
            raise ValueError("fill trace inventory path is discontinuous")
        if before * after < -(tolerance * tolerance):
            raise ValueError("a single fill crossed flat and changed campaign direction")

        if active is None:
            if abs(before) > tolerance or abs(after) <= tolerance:
                raise ValueError("campaign did not start on a flat-to-nonzero fill")
            campaign_id += 1
            active = {
                "day": str(day),
                "arm": str(arm),
                "campaign_id": int(campaign_id),
                "direction": "LONG" if after > 0.0 else "SHORT",
                "start_ts_ms": int(row["fill_ts"]),
                "end_ts_ms": 0,
                "closed": False,
                "censored": False,
                "fill_count": 0,
                "exposure_increasing_fill_count": 0,
                "reducing_fill_count": 0,
                "buy_fill_count": 0,
                "sell_fill_count": 0,
                "max_abs_inventory_btc": 0.0,
                "gross_cashflow_usdc": 0.0,
                "fee_usdc": 0.0,
            }

        if active is None:  # pragma: no cover - guarded above
            raise AssertionError("campaign state unexpectedly absent")
        active["fill_count"] += 1
        active["buy_fill_count" if side == "BUY" else "sell_fill_count"] += 1
        if abs(after) > abs(before) + tolerance:
            active["exposure_increasing_fill_count"] += 1
        elif abs(after) < abs(before) - tolerance:
            active["reducing_fill_count"] += 1
        active["max_abs_inventory_btc"] = max(
            float(active["max_abs_inventory_btc"]), abs(before), abs(after)
        )
        active["gross_cashflow_usdc"] += (
            -qty * price if side == "BUY" else qty * price
        )
        active["fee_usdc"] += fee
        running_inventory = after

        if abs(after) <= tolerance:
            active["end_ts_ms"] = int(row["fill_ts"])
            active["closed"] = True
            active["terminal_inventory_btc"] = 0.0
            active["terminal_mark_price"] = terminal_mark
            active["terminal_value_usdc"] = float(
                active["gross_cashflow_usdc"] - active["fee_usdc"]
            )
            records.append(active)
            active = None
            running_inventory = 0.0

    if active is not None:
        active["end_ts_ms"] = int(ordered["fill_ts"].max()) if len(ordered) else 0
        active["censored"] = True
        active["terminal_inventory_btc"] = float(running_inventory)
        active["terminal_mark_price"] = terminal_mark
        active["terminal_value_usdc"] = float(
            active["gross_cashflow_usdc"]
            - active["fee_usdc"]
            + running_inventory * terminal_mark
        )
        records.append(active)

    frame = pd.DataFrame(records)
    if frame.empty:
        if expected_campaign_count not in (None, 0):
            raise ValueError("replay reported campaigns but fill trace reconstructed none")
        return frame
    frame["duration_s"] = (
        pd.to_numeric(frame["end_ts_ms"])
        - pd.to_numeric(frame["start_ts_ms"])
    ).clip(lower=0) / 1000.0
    frame["max_abs_inventory_units"] = (
        pd.to_numeric(frame["max_abs_inventory_btc"]) / unit
    )
    units = frame["max_abs_inventory_units"].to_numpy(dtype=float)
    frame["inventory_level_bucket"] = np.where(
        units <= 1.0 + 1e-7,
        "single_level",
        np.where(units <= 3.0 + 1e-7, "levels_2_3", "levels_4_plus"),
    )
    if expected_campaign_count is not None and len(frame) != int(expected_campaign_count):
        raise ValueError(
            f"campaign count mismatch: replay={expected_campaign_count}, trace={len(frame)}"
        )
    closed_count = int(frame["closed"].sum())
    if expected_closed_count is not None and closed_count != int(expected_closed_count):
        raise ValueError(
            "closed campaign count mismatch: "
            f"replay={expected_closed_count}, trace={closed_count}"
        )
    return frame


def summarize_arm(
    result: Mapping[str, Any],
    fills: pd.DataFrame,
    campaigns: pd.DataFrame,
    *,
    day: str,
    arm: str,
    elapsed_hours: float,
) -> dict[str, Any]:
    if elapsed_hours <= 0.0:
        raise ValueError("replay elapsed hours must be positive")
    total_fills = int(result.get("fills_bid", 0) or 0) + int(
        result.get("fills_ask", 0) or 0
    )
    if len(fills) != total_fills:
        raise ValueError(
            f"fill trace is incomplete on {day}/{arm}: {len(fills)} != {total_fills}"
        )
    before = pd.to_numeric(fills["inventory_before_fill"], errors="raise")
    after = pd.to_numeric(fills["inventory_after_fill"], errors="raise")
    exposure = after.abs() > before.abs() + 1e-10
    reducing = after.abs() < before.abs() - 1e-10
    sides = fills["side"].astype(str).str.upper()
    closed = campaigns[campaigns["closed"].astype(bool)] if not campaigns.empty else campaigns
    short = closed[closed["direction"].eq("SHORT")] if not closed.empty else closed
    single_short = (
        short[short["inventory_level_bucket"].eq("single_level")]
        if not short.empty
        else short
    )
    multi_short = (
        short[~short["inventory_level_bucket"].eq("single_level")]
        if not short.empty
        else short
    )
    terminal_mtm = float(result.get("terminal_mtm_pnl", 0.0) or 0.0)
    reconstructed = float(
        pd.to_numeric(campaigns.get("terminal_value_usdc", pd.Series(dtype=float))).sum()
    )
    reconciliation_error = reconstructed - terminal_mtm
    if abs(reconciliation_error) > 1e-6:
        raise ValueError(
            f"campaign accounting failed on {day}/{arm}: {reconciliation_error}"
        )

    buy_exposure = int((exposure & sides.eq("BUY")).sum())
    sell_exposure = int((exposure & sides.eq("SELL")).sum())
    closed_count = int(len(closed))
    short_count = int(len(short))
    multi_short_count = int(len(multi_short))
    return {
        "day": str(day),
        "arm": str(arm),
        "elapsed_hours": float(elapsed_hours),
        "terminal_mtm_pnl_usdc": terminal_mtm,
        "final_pnl_usdc": float(result.get("pnl", 0.0) or 0.0),
        "campaign_accounting_reconciliation_error_usdc": float(reconciliation_error),
        "fills_total": total_fills,
        "buy_fill_count": int(sides.eq("BUY").sum()),
        "sell_fill_count": int(sides.eq("SELL").sum()),
        "buy_exposure_fill_count": buy_exposure,
        "sell_exposure_fill_count": sell_exposure,
        "buy_reducing_fill_count": int((reducing & sides.eq("BUY")).sum()),
        "sell_reducing_fill_count": int((reducing & sides.eq("SELL")).sum()),
        "buy_exposure_fills_per_hour": float(buy_exposure / elapsed_hours),
        "sell_exposure_fills_per_hour": float(sell_exposure / elapsed_hours),
        "exposure_side_imbalance_per_hour": float(
            (sell_exposure - buy_exposure) / elapsed_hours
        ),
        "campaign_count": int(len(campaigns)),
        "closed_campaign_count": closed_count,
        "open_campaign_count": int(len(campaigns) - closed_count),
        "closed_campaign_value_usdc": float(
            pd.to_numeric(closed.get("terminal_value_usdc", pd.Series(dtype=float))).sum()
        ),
        "short_campaign_count": short_count,
        "short_campaign_share": float(short_count / max(closed_count, 1)),
        "multi_level_short_count": multi_short_count,
        "multi_level_short_share_of_all": float(
            multi_short_count / max(closed_count, 1)
        ),
        "multi_level_short_rate_given_short": float(
            multi_short_count / max(short_count, 1)
        ),
        "single_level_short_value_usdc": float(
            pd.to_numeric(single_short.get("terminal_value_usdc", pd.Series(dtype=float))).sum()
        ),
        "multi_level_short_value_usdc": float(
            pd.to_numeric(multi_short.get("terminal_value_usdc", pd.Series(dtype=float))).sum()
        ),
        "abs_inventory_time_s": float(result.get("abs_inventory_time_s", 0.0) or 0.0),
        "sq_inventory_time_s": float(result.get("sq_inventory_time_s", 0.0) or 0.0),
        "q90_eval_count": int(result.get("dynamic_fill_hazard_eval_count", 0) or 0),
        "q90_cancel_request_count": int(
            result.get("dynamic_fill_hazard_cancel_request_count", 0) or 0
        ),
        "q90_cancel_ack_count": int(
            result.get("dynamic_fill_hazard_cancel_ack_count", 0) or 0
        ),
        "q90_pre_ack_fill_count": int(
            result.get("dynamic_fill_hazard_pre_ack_fill_count", 0) or 0
        ),
        "q90_recovery_count": int(
            result.get("dynamic_fill_hazard_recovery_count", 0) or 0
        ),
        "q90_reentry_count": int(
            result.get("dynamic_fill_hazard_reentry_count", 0) or 0
        ),
        "native_events": int(result.get("exchange_book_events_consumed", 0) or 0),
        "source_gap_events": int(result.get("exchange_book_source_gap_events", 0) or 0),
        "invalid_sequence_messages": int(
            result.get("exchange_book_invalid_sequence_messages", 0) or 0
        ),
    }


def paired_daily_differences(arm_daily: pd.DataFrame) -> pd.DataFrame:
    required = {"day", "arm", *PAIRED_METRICS}
    missing = sorted(required - set(arm_daily.columns))
    if missing:
        raise ValueError("arm daily table is missing: " + ", ".join(missing))
    if arm_daily.duplicated(["day", "arm"]).any():
        raise ValueError("arm daily table has duplicate day/arm rows")
    if set(arm_daily["arm"].astype(str)) != set(ARMS):
        raise ValueError("arm daily table does not contain exactly q90 OFF and ON")
    rows: list[dict[str, Any]] = []
    for day, frame in arm_daily.groupby("day", sort=True):
        by_arm = frame.set_index("arm")
        if set(by_arm.index.astype(str)) != set(ARMS):
            raise ValueError(f"paired q90 arm missing on {day}")
        row: dict[str, Any] = {"day": str(day)}
        if "quality_grade" in frame.columns:
            grades = sorted(set(frame["quality_grade"].astype(str)))
            if len(grades) != 1 or grades[0] not in {"A", "B"}:
                raise ValueError(f"q90 quality grade drifted on {day}: {grades}")
            row["quality_grade"] = grades[0]
        for metric in PAIRED_METRICS:
            off = float(by_arm.loc["q90_off", metric])
            on = float(by_arm.loc["q90_on", metric])
            row[f"q90_off_{metric}"] = off
            row[f"q90_on_{metric}"] = on
            row[f"diff_{metric}"] = on - off
        rows.append(row)
    return pd.DataFrame(rows)


def paired_bootstrap_inference(
    paired: pd.DataFrame,
    *,
    draws: int,
    seed: int,
) -> dict[str, dict[str, float]]:
    if paired.empty:
        raise ValueError("paired q90 daily table is empty")
    if draws < 1000:
        raise ValueError("paired bootstrap requires at least 1000 draws")
    rng = np.random.default_rng(int(seed))
    n_days = len(paired)
    samples = rng.integers(0, n_days, size=(int(draws), n_days))
    output: dict[str, dict[str, float]] = {}
    for metric in PAIRED_METRICS:
        values = pd.to_numeric(paired[f"diff_{metric}"], errors="raise").to_numpy(
            dtype=float
        )
        boot = values[samples].mean(axis=1)
        output[metric] = {
            "estimate": float(values.mean()),
            "lcb95": float(np.quantile(boot, 0.025)),
            "ucb95": float(np.quantile(boot, 0.975)),
            "daily_positive_rate": float(np.mean(values > 0.0)),
        }
    return output


def mechanism_decision(
    inference: Mapping[str, Mapping[str, float]],
) -> dict[str, Any]:
    buy_suppressed = float(inference["buy_exposure_fills_per_hour"]["ucb95"]) < 0.0
    imbalance_short = float(
        inference["exposure_side_imbalance_per_hour"]["lcb95"]
    ) > 0.0
    short_share_up = float(inference["short_campaign_share"]["lcb95"]) > 0.0
    multi_short_up = float(
        inference["multi_level_short_share_of_all"]["lcb95"]
    ) > 0.0
    terminal_harm = float(inference["terminal_mtm_pnl_usdc"]["ucb95"]) < 0.0
    portfolio_bias = bool(
        buy_suppressed and imbalance_short and short_share_up and multi_short_up
    )
    if portfolio_bias and terminal_harm:
        decision = "q90_short_multilevel_portfolio_harm_supported_development"
    elif portfolio_bias:
        decision = "q90_short_multilevel_portfolio_bias_supported_economic_harm_unresolved"
    else:
        decision = "q90_portfolio_bias_mechanism_not_supported_development"
    return {
        "decision": decision,
        "buy_exposure_suppression_supported": buy_suppressed,
        "sell_minus_buy_exposure_imbalance_increase_supported": imbalance_short,
        "short_campaign_share_increase_supported": short_share_up,
        "multi_level_short_share_increase_supported": multi_short_up,
        "terminal_mtm_harm_supported": terminal_harm,
        "portfolio_bias_supported": portfolio_bias,
        "portfolio_harm_supported": bool(portfolio_bias and terminal_harm),
    }


def _configure_arm_params(
    source: Mapping[str, Any],
    day: str,
    spec: Mapping[str, Any],
    *,
    q90_enabled: bool,
) -> dict[str, Any]:
    params = full_path._configure_params(source, day)
    replay = spec["replay_contract"]
    params.update(
        {
            "fill_cooldown_clock_mode": "wall_time",
            "variance_time_lineage_randomized_enabled": False,
            "post_cooldown_incremental_inventory_budget_enabled": False,
            "trace_variance_time_lineage_max": 0,
            "trace_post_cooldown_incremental_inventory_budget_max": 0,
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": int(replay["trace_fills_max_per_arm_day"]),
            "trace_first_add_decision_to_terminal_max": 0,
            "trace_first_opener_decision_to_terminal_max": 0,
            "trace_campaign_repair_max": 0,
            "collect_curves": False,
            "window_cache_write_enabled": False,
            "replay_purpose": "formal",
            "replay_initial_state_mode": "fresh_start",
            "replay_promotion_eligible": False,
            "sync_adjust_replay_mode": "disabled",
            "dynamic_fill_hazard_action_enabled": bool(q90_enabled),
            "dynamic_fill_hazard_cpp_parity_enabled": bool(q90_enabled),
            "dynamic_fill_hazard_cpp_parity_strict": True,
            "dynamic_fill_hazard_cpp_parity_trace_max": int(
                replay["q90_mismatch_trace_max"]
            ),
        }
    )
    if bool(params.get("ml_enabled", False)):
        raise ValueError("BUY q90 attribution requires the frozen ML-OFF baseline")
    if abs(float(params.get("maker_fill_prob", 1.0) or 0.0) - 1.0) > 1e-12:
        raise ValueError(
            "BUY q90 paired replay requires deterministic maker_fill_prob=1"
        )
    return params


def _assert_arm_parameter_whitelist(
    off: Mapping[str, Any],
    on: Mapping[str, Any],
) -> None:
    allowed = {
        "dynamic_fill_hazard_action_enabled",
        "dynamic_fill_hazard_cpp_parity_enabled",
    }

    def equal(left: Any, right: Any) -> bool:
        if isinstance(left, np.ndarray) or isinstance(right, np.ndarray):
            left_array = np.asarray(left)
            right_array = np.asarray(right)
            try:
                return bool(
                    np.array_equal(left_array, right_array, equal_nan=True)
                )
            except TypeError:
                return bool(np.array_equal(left_array, right_array))
        try:
            comparison = left == right
        except (TypeError, ValueError):
            return False
        return bool(comparison) if isinstance(comparison, (bool, np.bool_)) else False

    keys = set(off) | set(on)
    differing = {key for key in keys if not equal(off.get(key), on.get(key))}
    if differing != allowed:
        raise ValueError(
            "q90 ON/OFF parameter difference escaped the treatment whitelist: "
            f"{sorted(differing)}"
        )


def _simulate_arm(
    source: Mapping[str, Any],
    spec: Mapping[str, Any],
    day: str,
    window: Mapping[str, Any],
    *,
    arm: str,
    params: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    q90_enabled = arm == "q90_on"
    params = dict(params)
    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(source["source_identity"]["native_orderbook_root"]),
        day=str(day),
        symbol="BTCUSDC",
        tick_size=float(params.get("tick_size", bt.TICK)),
        warmup_hours=int(source["replay_contract"]["native_warmup_hours"]),
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
    if int(result.get("exchange_book_source_gap_events", 0) or 0) != 0:
        raise RuntimeError(f"native source gap on {day}/{arm}")
    if int(result.get("exchange_book_invalid_sequence_messages", 0) or 0) != 0:
        raise RuntimeError(f"native sequence failure on {day}/{arm}")
    if int(result.get("exchange_book_events_consumed", 0) or 0) <= 0:
        raise RuntimeError(f"native exchange-book tape was not consumed on {day}/{arm}")
    if q90_enabled:
        if not bool(result.get("dynamic_fill_hazard_cpp_parity_passed", False)):
            raise RuntimeError(f"BUY q90 Python/C++ lockstep failed on {day}")
        if int(result.get("dynamic_fill_hazard_cpp_mismatch_count", 0) or 0) != 0:
            raise RuntimeError(f"BUY q90 Python/C++ mismatch on {day}")
        expected_native = str(spec["native_module_identity"]["sha256"])
        actual_native = str(
            (result.get("dynamic_fill_hazard_cpp_identity") or {}).get(
                "native_module_sha256", ""
            )
        )
        if actual_native != expected_native:
            raise RuntimeError(
                f"BUY q90 native module drifted: {actual_native} != {expected_native}"
            )
    else:
        if bool(result.get("dynamic_fill_hazard_action_enabled", True)):
            raise RuntimeError(f"q90 OFF arm remained enabled on {day}")
        if int(result.get("dynamic_fill_hazard_cancel_request_count", 0) or 0) != 0:
            raise RuntimeError(f"q90 OFF arm emitted a cancel request on {day}")

    fills = pd.DataFrame(result.get("_fill_trace") or ())
    if fills.empty:
        raise RuntimeError(f"fill trace is empty on {day}/{arm}")
    fills.insert(0, "arm", arm)
    fills.insert(0, "day", str(day))
    unit = max(
        float(params.get("order_size", 0.0) or 0.0),
        float(params.get("lot_size", bt.LOT_SIZE) or bt.LOT_SIZE),
    )
    campaigns = reconstruct_campaigns(
        fills,
        day=str(day),
        arm=arm,
        inventory_unit_btc=unit,
        terminal_mark_price=float(result.get("terminal_mark_price", 0.0) or 0.0),
        expected_campaign_count=int(result.get("campaign_count", 0) or 0),
        expected_closed_count=int(result.get("campaign_closed_count", 0) or 0),
    )
    trades = window["trades"]
    timestamps = pd.to_numeric(trades["transact_time"], errors="raise")
    elapsed_hours = max(
        1e-9,
        float(timestamps.iloc[-1] - timestamps.iloc[0]) / 3_600_000.0,
    )
    summary = summarize_arm(
        result,
        fills,
        campaigns,
        day=str(day),
        arm=arm,
        elapsed_hours=elapsed_hours,
    )
    return result, fills, campaigns, summary


def run_day(
    *,
    day: str,
    spec_path: Path = DEFAULT_SPEC_PATH,
) -> dict[str, Any]:
    started = time.monotonic()
    resolved_spec = Path(spec_path).expanduser().resolve()
    spec = _load_json(resolved_spec)
    validate_spec(spec)
    source = _runtime_source_contract(spec)
    base_params = full_path._configure_params(source, str(day))
    window = full_path._load_window(source, str(day), base_params)

    params_by_arm = {
        arm: _configure_arm_params(
            source,
            str(day),
            spec,
            q90_enabled=arm == "q90_on",
        )
        for arm in ARMS
    }
    _assert_arm_parameter_whitelist(
        params_by_arm["q90_off"],
        params_by_arm["q90_on"],
    )

    arm_results: dict[str, Mapping[str, Any]] = {}
    fill_frames: list[pd.DataFrame] = []
    campaign_frames: list[pd.DataFrame] = []
    summaries: list[dict[str, Any]] = []
    for arm in ARMS:
        result, fills, campaigns, summary = _simulate_arm(
            source,
            spec,
            str(day),
            window,
            arm=arm,
            params=params_by_arm[arm],
        )
        arm_results[arm] = result
        fill_frames.append(fills)
        campaign_frames.append(campaigns)
        summaries.append(summary)
    off_events = int(arm_results["q90_off"].get("exchange_book_events_consumed", 0) or 0)
    on_events = int(arm_results["q90_on"].get("exchange_book_events_consumed", 0) or 0)
    if off_events != on_events:
        raise RuntimeError(f"q90 arms consumed different native event counts on {day}")
    return {
        "day": str(day),
        "runtime_s": float(time.monotonic() - started),
        "fills": pd.concat(fill_frames, ignore_index=True),
        "campaigns": pd.concat(campaign_frames, ignore_index=True),
        "arm_summaries": summaries,
        "native_event_count": off_events,
    }


def _run_day_task(task: tuple[str, str]) -> tuple[str, dict[str, Any]]:
    spec_path, day = task
    return day, run_day(day=day, spec_path=Path(spec_path))


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False)
        + "\n",
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
    *,
    spec_file_sha256: str,
) -> dict[str, Any]:
    day = str(result["day"])
    day_dir = output / "days"
    day_dir.mkdir(parents=True, exist_ok=True)
    fills_path = day_dir / f"{day}.fills.parquet"
    campaigns_path = day_dir / f"{day}.campaigns.parquet"
    summary_path = day_dir / f"{day}.json"
    _atomic_parquet(fills_path, result["fills"])
    _atomic_parquet(campaigns_path, result["campaigns"])
    payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "day": day,
        "runtime_s": float(result["runtime_s"]),
        "native_event_count": int(result["native_event_count"]),
        "arm_summaries": result["arm_summaries"],
        "fills": {
            "path": str(fills_path),
            "rows": int(len(result["fills"])),
            "sha256": sha256_file(fills_path),
        },
        "campaigns": {
            "path": str(campaigns_path),
            "rows": int(len(result["campaigns"])),
            "sha256": sha256_file(campaigns_path),
        },
        "spec_file_sha256": str(spec_file_sha256),
    }
    _atomic_json(summary_path, payload)
    return payload


def _load_checkpoint(output: Path, day: str, spec_sha256: str) -> dict[str, Any] | None:
    path = output / "days" / f"{day}.json"
    if not path.is_file():
        return None
    payload = _load_json(path)
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != IDENTITY
        or payload.get("spec_file_sha256") != spec_sha256
    ):
        return None
    for key in ("fills", "campaigns"):
        identity = payload[key]
        artifact = Path(str(identity["path"]))
        if not artifact.is_file() or sha256_file(artifact) != str(identity["sha256"]):
            return None
    return payload


def _aggregate_arm_summary(arm_daily: pd.DataFrame, arm: str) -> dict[str, Any]:
    frame = arm_daily[arm_daily["arm"].eq(arm)]
    closed = float(frame["closed_campaign_count"].sum())
    short = float(frame["short_campaign_count"].sum())
    multi_short = float(frame["multi_level_short_count"].sum())
    hours = float(frame["elapsed_hours"].sum())
    return {
        "arm": arm,
        "days": int(frame["day"].nunique()),
        "terminal_mtm_pnl_usdc": float(frame["terminal_mtm_pnl_usdc"].sum()),
        "closed_campaign_value_usdc": float(frame["closed_campaign_value_usdc"].sum()),
        "fills_total": int(frame["fills_total"].sum()),
        "buy_exposure_fills_per_hour": float(
            frame["buy_exposure_fill_count"].sum() / max(hours, 1e-12)
        ),
        "sell_exposure_fills_per_hour": float(
            frame["sell_exposure_fill_count"].sum() / max(hours, 1e-12)
        ),
        "short_campaign_share": float(short / max(closed, 1.0)),
        "multi_level_short_share_of_all": float(multi_short / max(closed, 1.0)),
        "multi_level_short_rate_given_short": float(multi_short / max(short, 1.0)),
        "single_level_short_value_usdc": float(
            frame["single_level_short_value_usdc"].sum()
        ),
        "multi_level_short_value_usdc": float(
            frame["multi_level_short_value_usdc"].sum()
        ),
        "abs_inventory_time_s": float(frame["abs_inventory_time_s"].sum()),
        "q90_cancel_request_count": int(frame["q90_cancel_request_count"].sum()),
        "q90_cancel_ack_count": int(frame["q90_cancel_ack_count"].sum()),
        "q90_pre_ack_fill_count": int(frame["q90_pre_ack_fill_count"].sum()),
        "q90_reentry_count": int(frame["q90_reentry_count"].sum()),
    }


def build_report(
    spec: Mapping[str, Any],
    checkpoints: Sequence[Mapping[str, Any]],
    *,
    diagnostic_subset: bool,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    arm_daily = pd.DataFrame(
        [summary for checkpoint in checkpoints for summary in checkpoint["arm_summaries"]]
    ).sort_values(["day", "arm"], ignore_index=True)
    grade_by_day = {
        **{
            str(day): "A"
            for day in spec["panels"]["development_primary_grade_a_days"]
        },
        **{
            str(day): "B"
            for day in spec["panels"]["development_sensitivity_grade_b_days"]
        },
    }
    arm_daily["quality_grade"] = arm_daily["day"].map(grade_by_day)
    if arm_daily["quality_grade"].isna().any():
        raise ValueError("q90 arm summary contains a day outside the quality identity")
    paired = paired_daily_differences(arm_daily)
    grade_a = paired[paired["quality_grade"].eq("A")]
    grade_b = paired[paired["quality_grade"].eq("B")]
    primary_inference = (
        paired_bootstrap_inference(
            grade_a,
            draws=int(spec["inference_contract"]["bootstrap_draws"]),
            seed=int(spec["inference_contract"]["bootstrap_seed"]),
        )
        if not grade_a.empty
        else {}
    )
    sensitivity_inference = (
        paired_bootstrap_inference(
            grade_b,
            draws=int(spec["inference_contract"]["bootstrap_draws"]),
            seed=int(spec["inference_contract"]["bootstrap_seed"]) + 1,
        )
        if not grade_b.empty
        else {}
    )
    all40_descriptive = paired_bootstrap_inference(
        paired,
        draws=int(spec["inference_contract"]["bootstrap_draws"]),
        seed=int(spec["inference_contract"]["bootstrap_seed"]) + 2,
    )
    if diagnostic_subset:
        decision_payload = {
            "decision": "diagnostic_subset_only_no_family_decision",
            "portfolio_bias_supported": False,
            "portfolio_harm_supported": False,
        }
    else:
        if len(grade_a) != 24 or len(grade_b) != 16:
            raise ValueError("formal q90 report requires all 24 Grade-A and 16 Grade-B days")
        decision_payload = mechanism_decision(primary_inference)
    report = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "development_days": list(map(str, spec["panels"]["development_days"])),
        "evaluated_days": paired["day"].astype(str).tolist(),
        "diagnostic_subset": bool(diagnostic_subset),
        **decision_payload,
        "arm_aggregates": {
            arm: _aggregate_arm_summary(arm_daily, arm) for arm in ARMS
        },
        "primary_grade_a_paired_day_inference": primary_inference,
        "grade_b_sensitivity_paired_day_inference": sensitivity_inference,
        "all40_descriptive_paired_day_inference": all40_descriptive,
        "interpretation": {
            "effect": "q90_on_minus_q90_off_under_authoritative_replay",
            "campaign_matching_after_path_divergence": False,
            "q90_model_oos_policy_validation": False,
            "reason": "the fixed q90 model overlaps part of this Development panel; this identity is mechanism attribution, not future policy confirmation",
        },
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
            "automatic_live_rollback_authorized": False,
        },
    }
    report["report_payload_sha256"] = canonical_sha256(report)
    return report, arm_daily, paired


def _report_markdown(report: Mapping[str, Any]) -> str:
    primary = report["primary_grade_a_paired_day_inference"]
    displayed = primary or report["all40_descriptive_paired_day_inference"]
    lines = [
        "# BUY q90 Portfolio Path Attribution v1",
        "",
        "Last materially modified: 2026-07-31",
        "",
        "Development-only paired full-path replay. q90 ON minus q90 OFF; no Validation, holdout, action, rollback, or live authority.",
        "",
        f"- decision: `{report['decision']}`",
        f"- evaluated days: `{len(report['evaluated_days'])}`",
        f"- portfolio bias supported: `{str(bool(report.get('portfolio_bias_supported', False))).lower()}`",
        f"- portfolio harm supported: `{str(bool(report.get('portfolio_harm_supported', False))).lower()}`",
        "",
        "| Grade-A primary metric (ON-OFF daily mean) | Estimate | 95% interval |",
        "|---|---:|---:|",
    ]
    for metric in PAIRED_METRICS:
        row = displayed[metric]
        lines.append(
            f"| {metric} | {row['estimate']:.8g} | [{row['lcb95']:.8g}, {row['ucb95']:.8g}] |"
        )
    lines.extend(
        [
            "",
            "Campaigns are reconstructed independently inside each arm. They are never matched after q90 changes the path.",
            "The 24 Grade-A days determine the mechanism decision; 16 Grade-B days are sensitivity only and are never pooled to rescue it.",
            "The q90 model overlaps part of Development, so this is causal replay attribution of a fixed policy, not out-of-sample policy validation.",
            "",
        ]
    )
    return "\n".join(lines)


def _check_storage(output: Path, spec: Mapping[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(output.parent).free
    estimate = int(spec["storage_gate"]["estimated_output_bytes"])
    reserve = int(spec["storage_gate"]["minimum_free_reserve_bytes"])
    if free < reserve + estimate:
        raise OSError(
            f"q90 evidence volume has {free} free bytes; requires {reserve + estimate}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--days", nargs="*", default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)

    spec_path = args.spec.expanduser().resolve()
    spec = _load_json(spec_path)
    validate_spec(spec)
    source = _runtime_source_contract(spec)
    output = args.output_dir.expanduser().resolve()
    _check_storage(output, spec)
    output.mkdir(parents=True, exist_ok=True)
    market_manifest, junit = validate_frozen_identities(spec_path, spec, source)
    _atomic_json(output / "market_source_manifest.json", {"rows": market_manifest})

    frozen_days = list(map(str, spec["panels"]["development_days"]))
    selected_days = frozen_days if args.days is None else list(map(str, args.days))
    if not selected_days or not set(selected_days).issubset(frozen_days):
        raise ValueError("requested q90 days are outside frozen Development")
    diagnostic_subset = selected_days != frozen_days
    spec_sha = sha256_file(spec_path)
    checkpoints: dict[str, dict[str, Any]] = {}
    pending: list[str] = []
    for day in selected_days:
        checkpoint = _load_checkpoint(output, day, spec_sha) if args.resume else None
        if checkpoint is None:
            pending.append(day)
        else:
            checkpoints[day] = checkpoint

    tasks = [(str(spec_path), day) for day in pending]
    workers = max(1, int(args.workers))
    if workers == 1:
        for task in tasks:
            day, result = _run_day_task(task)
            checkpoints[day] = _checkpoint_day(
                output, result, spec_file_sha256=spec_sha
            )
    elif tasks:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run_day_task, task): task[1] for task in tasks}
            for future in as_completed(futures):
                day, result = future.result()
                checkpoints[day] = _checkpoint_day(
                    output, result, spec_file_sha256=spec_sha
                )

    ordered = [checkpoints[day] for day in selected_days]
    report, arm_daily, paired = build_report(
        spec, ordered, diagnostic_subset=diagnostic_subset
    )
    _atomic_parquet(output / "arm_daily.parquet", arm_daily)
    _atomic_parquet(output / "paired_daily.parquet", paired)
    report["artifacts"] = {
        "arm_daily": str(output / "arm_daily.parquet"),
        "paired_daily": str(output / "paired_daily.parquet"),
        "market_source_manifest": str(output / "market_source_manifest.json"),
    }
    report["spec"] = {"path": str(spec_path), "sha256": spec_sha}
    report["test_identity"] = junit
    _atomic_json(output / "report.json", report)
    (output / "report.md").write_text(_report_markdown(report), encoding="utf-8")
    print(json.dumps({"decision": report["decision"], "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
