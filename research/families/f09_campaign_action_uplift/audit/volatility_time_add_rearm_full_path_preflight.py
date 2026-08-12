#!/usr/bin/env python3
"""Development-only full-path preflight for variance-time add rearm.

The control and candidate are independent authoritative Python replays over the
same frozen market path.  This evaluator reads only lifecycle mechanics.  It
must not read reward, PnL, markout, Validation, or holdout evidence, and it
cannot create or authorize an action experiment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Iterable, Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models import backtest_tick as bt
from models.backtest_config import (
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from models.data_windows import load_tick_window_dict, slice_window
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from models.replay_contract import configure_fixed_latency_distribution
from research.families.f09_campaign_action_uplift.audit.volatility_time_add_rearm_feasibility_v2 import (
    load_causal_bbo_variance_samples,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "volatility_time_add_rearm_full_path_preflight.v1"
FAMILY_ID = "volatility_time_add_rearm_full_path_preflight_v1"
SIDES = ("BUY", "SELL")

FORBIDDEN_TRACE_COLUMNS = frozenset(
    {
        "pnl",
        "reward",
        "markout",
        "markout_ema",
        "toxicity",
        "campaign_pnl_so_far",
        "campaign_adverse_excursion_so_far",
        "bid_quote_ev_30s",
        "ask_quote_ev_30s",
    }
)

DECISION_COLUMNS = (
    "ts_ms",
    "side",
    "action",
    "allow_post",
    "allow_exposure_increase",
    "exposure_increasing",
    "reason_text",
    "final_price",
    "final_size",
    "needs_update",
    "order_active_before",
    "last_side_fill_ts_ms",
    "fill_cooldown_consecutive_units",
    "baseline_wall_fill_cooldown_active",
    "effective_fill_cooldown_active",
    "variance_time_mechanical_diff_vs_wall",
    "variance_time_baseline_ready_ts_ms",
    "variance_time_candidate_ready_ts_ms",
    "variance_time_release_reason",
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
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def require_identity(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    actual = sha256_file(path)
    if actual != str(expected_sha256):
        raise ValueError(f"{label} hash mismatch: expected {expected_sha256}, found {actual}")


def read_junit(path: Path) -> dict[str, Any]:
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
        "passed": bool(totals["tests"] > 0 and totals["failures"] == 0 and totals["errors"] == 0),
    }


def build_market_source_manifest(spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Hash every normalized and native book file consumed by Development."""

    source = spec["source_identity"]
    days = [str(day) for day in spec["panels"]["development_days"]]
    normalized_root = Path(source["normalized_l2_root"])
    quality = pd.read_csv(source["normalized_l2_quality"]["path"], dtype={"day": str}).set_index(
        "day", drop=False
    )
    normalized_consumers: dict[str, set[str]] = {}
    for day in days:
        previous = (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        normalized_consumers.setdefault(previous, set()).add(f"{day}:warmup_d_minus_1")
        normalized_consumers.setdefault(day, set()).add(f"{day}:target")

    rows: list[dict[str, Any]] = []
    for source_day, consumers in sorted(normalized_consumers.items()):
        if source_day not in quality.index:
            raise ValueError(f"normalized L2 quality lacks {source_day}")
        quality_row = quality.loc[source_day]
        target = any(value.endswith(":target") for value in consumers)
        if target and not bool(quality_row["formal_eligible"]):
            raise ValueError(f"target normalized L2 day is ineligible: {source_day}")
        for kind in ("bbo", "l2"):
            path = normalized_root / kind / f"BTCUSDC-{kind}-{source_day}.parquet"
            expected = str(quality_row[f"{kind}_sha256"])
            require_identity(path, expected, f"normalized {kind} {source_day}")
            rows.append(
                {
                    "source_type": f"normalized_{kind}",
                    "source_day": source_day,
                    "path": str(path),
                    "sha256": expected,
                    "bytes": int(path.stat().st_size),
                    "used_by": sorted(consumers),
                    "formal_target": target,
                }
            )

    raw_consumers: dict[Path, set[str]] = {}
    for day in days:
        tape = CryptoHFTExchangeBookTape(
            raw_root=Path(source["native_orderbook_root"]),
            day=day,
            symbol="BTCUSDC",
            tick_size=0.1,
            warmup_hours=int(spec["replay_contract"]["native_warmup_hours"]),
            strict_complete=True,
        )
        for path in tape.source_paths:
            raw_consumers.setdefault(path.resolve(), set()).add(day)
    for path, consumers in sorted(raw_consumers.items(), key=lambda item: str(item[0])):
        rows.append(
            {
                "source_type": "native_snapshot_delta",
                "source_day": path.parts[-3],
                "path": str(path),
                "sha256": sha256_file(path),
                "bytes": int(path.stat().st_size),
                "used_by": sorted(consumers),
                "formal_target": True,
            }
        )
    return rows


def _load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected full-path preflight spec schema")
    permissions = payload.get("permissions") or {}
    forbidden = (
        "reward_or_pnl_read",
        "markout_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_created",
        "action_experiment_authorized",
        "live_deployment_authorized",
    )
    enabled = [key for key in forbidden if bool(permissions.get(key, False))]
    if enabled:
        raise ValueError("full-path preflight must remain mechanics-only: " + ", ".join(enabled))
    return payload


def _assert_mechanics_trace(frame: pd.DataFrame) -> None:
    present = sorted(FORBIDDEN_TRACE_COLUMNS & set(frame.columns))
    if present:
        raise ValueError(
            "mechanics trace contains forbidden economic fields: " + ", ".join(present)
        )
    missing = sorted(set(DECISION_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError("decision trace is missing mechanics fields: " + ", ".join(missing))


def _normalized_reason_tokens(value: Any) -> tuple[str, ...]:
    text = str(value or "none")
    return tuple(sorted({token for token in text.split("|") if token and token != "none"}))


def _rounded(value: Any, digits: int = 10) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return round(result, digits) if math.isfinite(result) else 0.0


def _action_signature(row: Mapping[str, Any] | None) -> tuple[Any, ...]:
    if row is None:
        return ("missing",)
    return (
        int(row.get("allow_post", 0) or 0),
        str(row.get("action", "") or ""),
        _rounded(row.get("final_price", 0.0)),
        _rounded(row.get("final_size", 0.0)),
        int(row.get("needs_update", 0) or 0),
        int(row.get("order_active_before", 0) or 0),
    )


def _index_decisions(frame: pd.DataFrame) -> dict[tuple[int, str, int], dict[str, Any]]:
    ordered = frame.sort_values(["ts_ms", "side"], kind="stable").copy()
    ordered["_occurrence"] = ordered.groupby(["ts_ms", "side"]).cumcount()
    return {
        (int(row["ts_ms"]), str(row["side"]), int(row["_occurrence"])): row
        for row in ordered.to_dict("records")
    }


def _build_order_lifecycles(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Collapse partial outcomes into one operational exposure interval."""

    frame = pd.DataFrame(list(rows))
    output = {side: [] for side in SIDES}
    if frame.empty:
        return output
    required = {
        "order_id",
        "side",
        "submit_ts",
        "activate_ts",
        "outcome_ts",
        "outcome",
        "price",
        "quantity",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError("quote trace is missing lifecycle fields: " + ", ".join(missing))
    for (_, side), group in frame.groupby(["order_id", "side"], sort=False):
        normalized_side = str(side).upper()
        if normalized_side not in output:
            continue
        ordered = group.sort_values("outcome_ts", kind="stable")
        first = ordered.iloc[0]
        terminal_ts = int(ordered["outcome_ts"].max())
        terminal_outcome = str(ordered.iloc[-1]["outcome"])
        fill_events: list[tuple[int, float]] = []
        for row in ordered.to_dict("records"):
            outcome = str(row.get("outcome", ""))
            remaining = float(row.get("remaining", 0.0) or 0.0)
            if outcome == "fill":
                fill_events.append(
                    (
                        int(row.get("outcome_ts", 0) or 0),
                        max(0.0, float(row.get("fill_qty", 0.0) or 0.0)),
                    )
                )
            if outcome == "fill" and remaining > 1e-12:
                continue
            terminal_ts = int(row.get("outcome_ts", terminal_ts) or terminal_ts)
            terminal_outcome = outcome
            break
        output[normalized_side].append(
            {
                "submit_ts": int(first["submit_ts"]),
                "activate_ts": int(first["activate_ts"]),
                "terminal_ts": terminal_ts,
                "terminal_outcome": terminal_outcome,
                "price": float(first["price"]),
                "quantity": float(first["quantity"]),
                "reduce_only": bool(first.get("reduce_only", False)),
                "fill_events": tuple(fill_events),
            }
        )
    return output


def _operational_order_signature(
    lifecycles: Mapping[str, list[Mapping[str, Any]]],
    *,
    side: str,
    ts_ms: int,
) -> tuple[Any, ...]:
    active: list[tuple[Any, ...]] = []
    submitted: list[tuple[Any, ...]] = []
    now = int(ts_ms)
    for order in lifecycles.get(str(side), ()):
        filled_before_now = sum(
            float(fill_qty)
            for fill_ts, fill_qty in order.get("fill_events", ())
            if int(fill_ts) <= now
        )
        remaining_quantity = max(
            0.0,
            float(order.get("quantity", 0.0)) - filled_before_now,
        )
        identity = (
            _rounded(order.get("price", 0.0)),
            _rounded(remaining_quantity),
            int(bool(order.get("reduce_only", False))),
        )
        if int(order.get("submit_ts", 0) or 0) == now:
            submitted.append(identity)
        activation = int(order.get("activate_ts", 0) or 0)
        terminal = int(order.get("terminal_ts", 0) or 0)
        rejected = str(order.get("terminal_outcome", "")) in {
            "gtx_reject",
            "reject",
            "activation_reject",
        }
        if not rejected and activation <= now < terminal:
            active.append(identity)
    return (tuple(sorted(active)), tuple(sorted(submitted)))


def _episode_id(day: str, row: Mapping[str, Any]) -> str:
    units = _rounded(row.get("fill_cooldown_consecutive_units", 0.0), 6)
    return (
        f"{day}:{str(row.get('side', ''))}:"
        f"{int(row.get('last_side_fill_ts_ms', 0) or 0)}:{units:.6f}"
    )


def _candidate_gate_effect(row: Mapping[str, Any]) -> dict[str, Any]:
    """Classify whether variance time is unmasked in the candidate gate stack."""

    baseline_active = bool(int(row["baseline_wall_fill_cooldown_active"]))
    candidate_active = bool(int(row["effective_fill_cooldown_active"]))
    if baseline_active == candidate_active:
        raise AssertionError("mechanical-diff row has identical cooldown state")
    direction = "earlier_ready" if baseline_active else "later_ready"
    reasons = _normalized_reason_tokens(row.get("reason_text"))
    non_fill_blockers = tuple(token for token in reasons if token != "fill_cd")
    allow_post = bool(int(row.get("allow_post", 0) or 0))
    exposure_increasing = bool(int(row.get("exposure_increasing", 0) or 0))

    if not exposure_increasing:
        return {
            "direction": direction,
            "unmasked": False,
            "binding_blockers": ("non_exposure_side",),
            "reason_tokens": reasons,
        }

    if direction == "earlier_ready":
        unmasked = allow_post
        binding = (
            ("unmasked_quote_action",)
            if unmasked
            else non_fill_blockers or ("unclassified_non_fill_blocker",)
        )
    else:
        fill_cd_present = "fill_cd" in reasons
        unmasked = bool(not allow_post and fill_cd_present and not non_fill_blockers)
        if fill_cd_present:
            binding = ("variance_time_fill_cd", *non_fill_blockers)
        elif allow_post:
            binding = ("variance_time_fill_cd_overridden",)
        else:
            binding = non_fill_blockers or ("unclassified_non_fill_blocker",)
    return {
        "direction": direction,
        "unmasked": unmasked,
        "binding_blockers": binding,
        "reason_tokens": reasons,
    }


def compare_decision_paths(
    day: str,
    control_rows: Iterable[Mapping[str, Any]],
    candidate_rows: Iterable[Mapping[str, Any]],
    *,
    material_delta_ms: int,
    control_order_rows: Iterable[Mapping[str, Any]] | None = None,
    candidate_order_rows: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return one row per mechanically divergent episode and its blockers."""

    control = pd.DataFrame(list(control_rows))
    candidate = pd.DataFrame(list(candidate_rows))
    if control.empty or candidate.empty:
        raise ValueError(f"{day}: both arms require non-empty decision traces")
    _assert_mechanics_trace(control)
    _assert_mechanics_trace(candidate)
    indexed_control = _index_decisions(control)
    use_operational_orders = control_order_rows is not None and candidate_order_rows is not None
    control_lifecycles = _build_order_lifecycles(control_order_rows or ())
    candidate_lifecycles = _build_order_lifecycles(candidate_order_rows or ())

    ordered = candidate.sort_values(["ts_ms", "side"], kind="stable").copy()
    ordered["_occurrence"] = ordered.groupby(["ts_ms", "side"]).cumcount()
    ordered["episode_id"] = [_episode_id(day, row) for row in ordered.to_dict("records")]
    divergent = ordered[
        pd.to_numeric(ordered["variance_time_mechanical_diff_vs_wall"], errors="coerce")
        .fillna(0)
        .astype(int)
        == 1
    ].copy()
    if divergent.empty:
        empty = pd.DataFrame()
        return empty, empty
    episode_rows: list[dict[str, Any]] = []
    blocker_rows: list[dict[str, Any]] = []
    for episode_id, episode_frame in divergent.groupby("episode_id", sort=False):
        episode_frame = episode_frame.sort_values(["ts_ms", "side"], kind="stable")
        candidate_row = episode_frame.iloc[0].to_dict()
        key = (
            int(candidate_row["ts_ms"]),
            str(candidate_row["side"]),
            int(candidate_row["_occurrence"]),
        )
        control_row = indexed_control.get(key)
        first_gate = _candidate_gate_effect(candidate_row)
        direction = str(first_gate["direction"])
        gate_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        operational_diff_rows: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
        decision_diff_rows: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
        for interval_row in episode_frame.to_dict("records"):
            gate = _candidate_gate_effect(interval_row)
            if str(gate["direction"]) != direction:
                raise AssertionError("one cooldown lineage has mixed timing directions")
            gate_rows.append((interval_row, gate))
            interval_key = (
                int(interval_row["ts_ms"]),
                str(interval_row["side"]),
                int(interval_row["_occurrence"]),
            )
            interval_control = indexed_control.get(interval_key)
            decision_changed = _action_signature(interval_control) != _action_signature(
                interval_row
            )
            if decision_changed:
                decision_diff_rows.append((interval_row, interval_control))
            if use_operational_orders:
                operational_changed = _operational_order_signature(
                    control_lifecycles,
                    side=str(interval_row["side"]),
                    ts_ms=int(interval_row["ts_ms"]),
                ) != _operational_order_signature(
                    candidate_lifecycles,
                    side=str(interval_row["side"]),
                    ts_ms=int(interval_row["ts_ms"]),
                )
            else:
                operational_changed = decision_changed
            if operational_changed:
                operational_diff_rows.append((interval_row, interval_control))
        unmasked_rows = [row for row, gate in gate_rows if bool(gate["unmasked"])]
        unmasked_action_effective = bool(unmasked_rows)
        first_unmasked_row = unmasked_rows[0] if unmasked_rows else None
        candidate_reasons = _normalized_reason_tokens(candidate_row.get("reason_text"))
        control_reasons = _normalized_reason_tokens(
            control_row.get("reason_text") if control_row is not None else "missing_control"
        )
        binding = tuple(first_gate["binding_blockers"])

        baseline_ready = int(candidate_row.get("variance_time_baseline_ready_ts_ms", 0) or 0)
        full_lineage = ordered[ordered["episode_id"] == episode_id]
        ready_values = (
            pd.to_numeric(
                full_lineage["variance_time_candidate_ready_ts_ms"],
                errors="coerce",
            )
            .fillna(0)
            .astype(np.int64)
        )
        observed_ready = ready_values[ready_values > 0]
        candidate_ready = int(observed_ready.min()) if not observed_ready.empty else 0
        timing_delta_observed = bool(baseline_ready > 0 and candidate_ready > 0)
        timing_delta_ms = (
            int(candidate_ready - baseline_ready) if timing_delta_observed else math.nan
        )
        episode = {
            "day": day,
            "episode_id": str(episode_id),
            "side": str(candidate_row["side"]),
            "first_mechanical_diff_ts_ms": int(candidate_row["ts_ms"]),
            "direction": direction,
            "lineage_fill_ts_ms": int(candidate_row.get("last_side_fill_ts_ms", 0) or 0),
            "consecutive_same_side_fill_units": float(
                candidate_row.get("fill_cooldown_consecutive_units", 0.0) or 0.0
            ),
            "baseline_ready_ts_ms": baseline_ready,
            "candidate_ready_ts_ms": candidate_ready,
            "timing_delta_observed": timing_delta_observed,
            "timing_censored_before_candidate_ready": bool(not timing_delta_observed),
            "timing_delta_ms": timing_delta_ms,
            "material_timing_change": bool(
                timing_delta_observed and abs(timing_delta_ms) > material_delta_ms
            ),
            "control_decision_aligned": bool(control_row is not None),
            "control_action": (
                str(control_row.get("action", "")) if control_row is not None else "missing"
            ),
            "candidate_action": str(candidate_row.get("action", "")),
            "control_allow_post": (
                int(control_row.get("allow_post", 0)) if control_row is not None else -1
            ),
            "candidate_allow_post": int(candidate_row.get("allow_post", 0) or 0),
            "control_final_price": (
                float(control_row.get("final_price", 0.0)) if control_row is not None else math.nan
            ),
            "candidate_final_price": float(candidate_row.get("final_price", 0.0) or 0.0),
            "control_final_size": (
                float(control_row.get("final_size", 0.0)) if control_row is not None else math.nan
            ),
            "candidate_final_size": float(candidate_row.get("final_size", 0.0) or 0.0),
            "unmasked_action_effective": unmasked_action_effective,
            "final_quote_action_changed": unmasked_action_effective,
            "action_change_authority": "candidate_final_gate_stack",
            "first_action_change_ts_ms": int(
                first_unmasked_row["ts_ms"] if first_unmasked_row else 0
            ),
            "action_change_decision_count": int(len(unmasked_rows)),
            "changed_quote_opportunity": unmasked_action_effective,
            "regenerated_operational_path_diff": bool(operational_diff_rows),
            "first_operational_path_diff_ts_ms": int(
                operational_diff_rows[0][0]["ts_ms"] if operational_diff_rows else 0
            ),
            "operational_path_diff_decision_count": int(len(operational_diff_rows)),
            "decision_trace_path_diff": bool(decision_diff_rows),
            "decision_trace_path_diff_decision_count": int(len(decision_diff_rows)),
            "control_reason_text": "|".join(control_reasons) or "none",
            "candidate_reason_text": "|".join(candidate_reasons) or "none",
            "binding_blockers": "|".join(binding),
            "variance_release_reason": str(
                candidate_row.get("variance_time_release_reason", "") or ""
            ),
        }
        episode_rows.append(episode)
        for blocker in binding:
            blocker_rows.append(
                {
                    "day": day,
                    "episode_id": episode["episode_id"],
                    "side": episode["side"],
                    "direction": direction,
                    "blocker": blocker,
                    "unmasked_action_effective": unmasked_action_effective,
                    "final_quote_action_changed": unmasked_action_effective,
                }
            )
    return pd.DataFrame(episode_rows), pd.DataFrame(blocker_rows)


def _multiset_difference_count(left: Counter, right: Counter) -> int:
    return int(sum((left - right).values()))


def compare_order_outcomes(
    control_rows: Iterable[Mapping[str, Any]],
    candidate_rows: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    """Compare regenerated order/fill paths without reading fill value."""

    def outcome_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            str(row.get("side", "")),
            int(row.get("submit_ts", row.get("quote_ts", 0)) or 0),
            _rounded(row.get("price", 0.0)),
            str(row.get("outcome", "")),
            int(row.get("outcome_ts", 0) or 0),
            _rounded(row.get("fill_qty", 0.0)),
        )

    def fill_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            str(row.get("side", "")),
            int(row.get("outcome_ts", 0) or 0),
            _rounded(row.get("price", 0.0)),
            _rounded(row.get("fill_qty", 0.0)),
        )

    control_list = list(control_rows)
    candidate_list = list(candidate_rows)
    control_outcomes = Counter(outcome_key(row) for row in control_list)
    candidate_outcomes = Counter(outcome_key(row) for row in candidate_list)
    control_fills = Counter(
        fill_key(row) for row in control_list if str(row.get("outcome")) == "fill"
    )
    candidate_fills = Counter(
        fill_key(row) for row in candidate_list if str(row.get("outcome")) == "fill"
    )
    return {
        "control_order_outcomes": int(sum(control_outcomes.values())),
        "candidate_order_outcomes": int(sum(candidate_outcomes.values())),
        "candidate_only_order_outcomes": _multiset_difference_count(
            candidate_outcomes, control_outcomes
        ),
        "control_only_order_outcomes": _multiset_difference_count(
            control_outcomes, candidate_outcomes
        ),
        "control_fill_events": int(sum(control_fills.values())),
        "candidate_fill_events": int(sum(candidate_fills.values())),
        "candidate_only_fill_events": _multiset_difference_count(candidate_fills, control_fills),
        "control_only_fill_events": _multiset_difference_count(control_fills, candidate_fills),
    }


def summarize_episode_support(episodes: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "side",
        "mechanical_episode_count",
        "material_episode_count",
        "earlier_ready_count",
        "later_ready_count",
        "aligned_control_count",
        "regenerated_operational_path_diff_count",
        "decision_trace_path_diff_count",
        "unmasked_action_effective_count",
        "final_action_change_count",
        "changed_quote_opportunity_count",
        "unmasked_action_effective_rate",
    ]
    if episodes.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, Any]] = []
    for side, frame in episodes.groupby("side", sort=True):
        denominator = int(len(frame))
        rows.append(
            {
                "side": str(side),
                "mechanical_episode_count": denominator,
                "material_episode_count": int(frame["material_timing_change"].sum()),
                "earlier_ready_count": int((frame["direction"] == "earlier_ready").sum()),
                "later_ready_count": int((frame["direction"] == "later_ready").sum()),
                "aligned_control_count": int(frame["control_decision_aligned"].sum()),
                "regenerated_operational_path_diff_count": int(
                    frame["regenerated_operational_path_diff"].sum()
                ),
                "decision_trace_path_diff_count": int(frame["decision_trace_path_diff"].sum()),
                "unmasked_action_effective_count": int(frame["unmasked_action_effective"].sum()),
                "final_action_change_count": int(frame["unmasked_action_effective"].sum()),
                "changed_quote_opportunity_count": int(frame["changed_quote_opportunity"].sum()),
                "unmasked_action_effective_rate": float(frame["unmasked_action_effective"].mean()),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _variance_time_data(spec: Mapping[str, Any], day: str) -> dict[str, np.ndarray]:
    clock = spec["variance_clock"]
    variance = load_causal_bbo_variance_samples(
        Path(spec["source_identity"]["normalized_l2_root"]),
        day,
        rolling_window_s=int(clock["rolling_window_s"]),
        max_bbo_source_age_ms=int(clock["max_bbo_source_age_ms"]),
        max_abs_return_bps_1s=float(clock["max_abs_return_bps_1s"]),
        ready_delay_ms=int(clock["feature_ready_delay_ms"]),
    )
    sigma = variance["sigma_sq_price_per_s"].to_numpy(dtype=np.float64)
    mid = variance["price"].to_numpy(dtype=np.float64)
    valid = variance["valid"].to_numpy(dtype=np.bool_)
    rate = np.full(sigma.size, np.nan, dtype=np.float64)
    usable = valid & np.isfinite(sigma) & np.isfinite(mid) & (mid > 0.0) & (sigma >= 0.0)
    rate[usable] = 1.0e8 * sigma[usable] / np.square(mid[usable])
    return {
        "feature_ready_ts_ms": variance["feature_ready_ts_ms"].to_numpy(dtype=np.int64),
        "rate_bps2_per_s": rate,
        "valid": usable,
    }


def _configure_params(spec: Mapping[str, Any], day: str) -> dict[str, Any]:
    source = spec["source_identity"]
    replay = spec["replay_contract"]
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=spec["operational_config_identity"]["path"],
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=source["queue_calibration"]["path"],
        strict_calibration=True,
    )
    params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": int(replay["clock_interval_ms"]),
            "exchange_book_queue_mode": "strict",
            "queue_l2_cancel_ahead_enabled": False,
            "decision_trace_profile": "mechanics_only",
            "trace_decisions_max": int(replay["trace_decisions_max"]),
            "trace_quotes_max": int(replay["trace_quotes_max"]),
            "trace_fills_max": 0,
            "collect_curves": False,
            "rng_seed": int(replay["rng_seed"]),
            "sync_adjust_replay_mode": "stress",
            "sync_adjust_stress_seed": int(replay["sync_stress_seed"]),
            "sync_adjust_stress_interval_s": float(replay["sync_stress_interval_s"]),
            "replay_purpose": "mechanics_preflight",
            "replay_initial_state_mode": "fresh_start",
            "replay_promotion_eligible": False,
            "initial_inventory": 0.0,
            "initial_entry_price": 0.0,
            "fill_cooldown_clock_mode": "wall_time",
        }
    )
    trade_identity = spec["execution_trade_identity"]
    params.update(
        {
            "individual_trades_manifest_path": trade_identity["manifest"]["path"],
            "individual_trades_manifest_sha256": trade_identity["manifest"]["sha256"],
            "individual_trades_integrity_report_path": trade_identity["quality_report"]["path"],
            "individual_trades_integrity_report_sha256": trade_identity["quality_report"]["sha256"],
        }
    )
    latency = bt._load_live_perf_latency_samples(
        Path(spec["latency_identity"]["samples"]["path"]),
        mode=str(spec["latency_identity"]["mode"]),
    )
    params["_new_order_latency_samples_ms"] = latency["new_order_latency_samples_ms"]
    params["_cancel_order_latency_samples_ms"] = latency["cancel_order_latency_samples_ms"]
    configure_fixed_latency_distribution(
        params,
        scenario="baseline",
        profile_id=str(spec["latency_identity"]["profile_id"]),
        environment=str(spec["latency_identity"]["environment"]),
        baseline_clip_quantile=float(spec["latency_identity"]["baseline_clip_quantile"]),
    )
    validate_formal_replay_calibration(params, require_latency=True)
    if str(params.get("fill_cooldown_consecutive_reset_policy")) != "opposite_fill_only":
        raise ValueError("full-path preflight requires opposite_fill_only reset")
    if (
        bool(params.get("fill_cooldown_apply_reducing", False))
        or float(params.get("fill_cooldown_reducing", 0.0) or 0.0) != 0.0
    ):
        raise ValueError("full-path preflight requires reducing cooldown disabled")
    if not bool(params.get("dynamic_fill_hazard_action_enabled", False)):
        raise ValueError("full-path preflight requires the frozen BUY q90 action")
    params["_preflight_day"] = day
    return params


def _load_window(spec: Mapping[str, Any], day: str, params: dict[str, Any]) -> dict[str, Any]:
    source = spec["source_identity"]
    cache_dir = source.get("window_cache_dir") or None
    window = load_tick_window_dict(
        day,
        params,
        load_ml=False,
        require_ml=False,
        run_ml_inference=False,
        cross_market_enabled=False,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=bool(params.get("require_formal_l2", False)),
        verify_formal_l2_hashes=bool(
            params.get("verify_formal_l2_hashes", False)
        ),
        cache_dir=cache_dir,
    )
    smoke_hours = float(spec.get("diagnostic_smoke_hours", 0.0) or 0.0)
    if smoke_hours > 0.0:
        start_ms = int(pd.Timestamp(day, tz="UTC").value // 1_000_000)
        window = slice_window(
            window,
            start_ms,
            start_ms + int(round(smoke_hours * 3_600_000.0)),
        )
    return window


def _run_arm(
    spec: Mapping[str, Any],
    day: str,
    window: Mapping[str, Any],
    base_params: Mapping[str, Any],
    *,
    candidate: bool,
    variance_data: Mapping[str, np.ndarray] | None,
) -> dict[str, Any]:
    params = dict(base_params)
    if candidate:
        clock = spec["variance_clock"]
        params.update(
            {
                "fill_cooldown_clock_mode": "variance_time",
                "variance_time_reference_rate_buy_bps2_per_s": float(
                    clock["reference_rate_bps2_per_s"]["BUY"]
                ),
                "variance_time_reference_rate_sell_bps2_per_s": float(
                    clock["reference_rate_bps2_per_s"]["SELL"]
                ),
                "variance_time_minimum_wall_time_ms": int(clock["minimum_wall_time_ms"]),
                "variance_time_maximum_wall_time_ms": int(clock["maximum_wall_time_ms"]),
                "variance_time_max_feature_age_ms": int(clock["max_feature_age_ms"]),
            }
        )
    else:
        params["fill_cooldown_clock_mode"] = "wall_time"

    tape = CryptoHFTExchangeBookTape(
        raw_root=Path(spec["source_identity"]["native_orderbook_root"]),
        day=day,
        symbol="BTCUSDC",
        tick_size=float(params.get("tick_size", bt.TICK)),
        warmup_hours=int(spec["replay_contract"]["native_warmup_hours"]),
        strict_complete=True,
    )
    return bt._simulate_tick_with_engine(
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
        variance_time_data=variance_data if candidate else None,
    )


def _mechanics_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    """Whitelisted mechanics only; economic result keys are never accessed."""

    allowed = (
        "fills_bid",
        "fills_ask",
        "quote_attempts",
        "n_requotes",
        "dynamic_fill_hazard_eval_count",
        "dynamic_fill_hazard_valid_eval_count",
        "dynamic_fill_hazard_cancel_request_count",
        "dynamic_fill_hazard_cancel_ack_count",
        "dynamic_fill_hazard_pre_ack_fill_count",
        "dynamic_fill_hazard_recovery_count",
        "dynamic_fill_hazard_reentry_count",
        "dynamic_fill_hazard_blocked_quote_count",
        "consecutive_loss_cooldown_trigger_count",
        "consecutive_loss_cooldown_expiry_count",
        "consecutive_loss_cooldown_block_count",
        "consecutive_loss_cooldown_cancel_count",
        "sync_adjust_event_count",
        "sync_adjust_replay_mode",
        "exchange_book_consumed_events",
        "exchange_book_source_gap_events",
        "exchange_book_invalid_sequence_messages",
    )
    return {key: result.get(key, 0) for key in allowed}


def run_day(spec: Mapping[str, Any], day: str) -> dict[str, Any]:
    started = time.monotonic()
    params = _configure_params(spec, day)
    window = _load_window(spec, day, params)
    variance_data = _variance_time_data(spec, day)
    control = _run_arm(
        spec,
        day,
        window,
        params,
        candidate=False,
        variance_data=None,
    )
    candidate = _run_arm(
        spec,
        day,
        window,
        params,
        candidate=True,
        variance_data=variance_data,
    )
    episodes, blockers = compare_decision_paths(
        day,
        control.get("_decision_trace", ()),
        candidate.get("_decision_trace", ()),
        material_delta_ms=int(spec["gates"]["material_timing_delta_ms"]),
        control_order_rows=control.get("_quote_trace", ()),
        candidate_order_rows=candidate.get("_quote_trace", ()),
    )
    order_diff = compare_order_outcomes(
        control.get("_quote_trace", ()),
        candidate.get("_quote_trace", ()),
    )
    support = summarize_episode_support(episodes)
    return {
        "day": day,
        "runtime_s": float(time.monotonic() - started),
        "control": _mechanics_summary(control),
        "candidate": _mechanics_summary(candidate),
        "order_path_difference": order_diff,
        "support": support.to_dict("records"),
        "episodes": episodes.to_dict("records"),
        "blockers": blockers.to_dict("records"),
        "daily_fresh_start": True,
        "cross_day_state_carried": False,
        "variance_sample_count": int(len(variance_data["feature_ready_ts_ms"])),
    }


def _bootstrap_day_rate(
    episodes: pd.DataFrame,
    *,
    side: str,
    seed: int,
    draws: int,
) -> dict[str, float]:
    frame = episodes[episodes["side"] == side]
    if frame.empty:
        return {"estimate": 0.0, "lcb95": 0.0, "ucb95": 0.0}
    daily = frame.groupby("day", sort=True)["unmasked_action_effective"].agg(["sum", "count"])
    estimate = float(daily["sum"].sum() / max(daily["count"].sum(), 1))
    if len(daily) == 1:
        return {"estimate": estimate, "lcb95": estimate, "ucb95": estimate}
    rng = np.random.default_rng(int(seed) + (0 if side == "BUY" else 1))
    values = daily.to_numpy(dtype=float)
    sampled = np.empty(int(draws), dtype=float)
    for index in range(int(draws)):
        rows = values[rng.integers(0, len(values), size=len(values))]
        sampled[index] = rows[:, 0].sum() / max(rows[:, 1].sum(), 1.0)
    return {
        "estimate": estimate,
        "lcb95": float(np.quantile(sampled, 0.025)),
        "ucb95": float(np.quantile(sampled, 0.975)),
    }


def _validate_frozen_identities(
    spec: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    implementation = spec["implementation_identity"]
    require_identity(Path(__file__).resolve(), implementation["evaluator_sha256"], "evaluator")
    for relative, expected in implementation["source_sha256"].items():
        require_identity(ROOT / relative, expected, relative)
    for section, label in (
        (spec["operational_config_identity"], "operational config"),
        (spec["predecessor_identity"]["variance_clock_report"], "variance-clock report"),
        (spec["predecessor_identity"]["live_stack_report"], "live-stack report"),
        (spec["execution_trade_identity"]["manifest"], "trade manifest"),
        (spec["execution_trade_identity"]["quality_report"], "trade quality report"),
        (spec["source_identity"]["normalized_l2_manifest"], "normalized L2 manifest"),
        (spec["source_identity"]["normalized_l2_quality"], "normalized L2 quality"),
        (spec["source_identity"]["queue_calibration"], "queue calibration"),
        (spec["source_identity"]["p3_artifact"], "P3 artifact"),
        (spec["latency_identity"]["samples"], "latency samples"),
        (spec["buy_q90_identity"]["model"], "BUY q90 model"),
        (spec["buy_q90_identity"]["policy"], "BUY q90 policy"),
        (spec["panels"]["source_split"], "source split"),
    ):
        require_identity(Path(section["path"]), section["sha256"], label)
    manifest = build_market_source_manifest(spec)
    actual = canonical_sha256(manifest)
    expected = str(spec["source_identity"]["market_source_manifest_canonical_sha256"])
    if actual != expected:
        raise ValueError(
            f"normalized/native market source manifest changed: expected {expected}, found {actual}"
        )
    test_identity = spec["test_identity"]
    junit_path = Path(test_identity["junit_xml"]["path"])
    require_identity(
        junit_path,
        test_identity["junit_xml"]["sha256"],
        "full-path contract JUnit",
    )
    junit = read_junit(junit_path)
    missing = sorted(set(test_identity["required_test_names"]) - set(junit["test_names"]))
    if not junit["passed"] or missing:
        raise ValueError(f"full-path contract tests are incomplete: {missing}")
    return manifest, junit


def _decision_from_support(
    episodes: pd.DataFrame,
    daily: pd.DataFrame,
    spec: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    gates = spec["gates"]
    cells: dict[str, Any] = {}
    passed = True
    for side in SIDES:
        frame = episodes[episodes["side"] == side]
        action_days = int(frame.loc[frame["unmasked_action_effective"], "day"].nunique())
        interval = _bootstrap_day_rate(
            episodes,
            side=side,
            seed=int(gates["bootstrap_seed"]),
            draws=int(gates["bootstrap_draws"]),
        )
        cell_pass = bool(
            len(frame) >= int(gates["minimum_mechanical_episodes_per_side"])
            and action_days >= int(gates["minimum_action_change_days_per_side"])
            and interval["lcb95"] > 0.0
        )
        passed = passed and cell_pass
        cells[side] = {
            "mechanical_episodes": int(len(frame)),
            "unmasked_action_effective_episodes": int(frame["unmasked_action_effective"].sum()),
            "action_change_episodes": int(frame["unmasked_action_effective"].sum()),
            "action_change_days": action_days,
            "unmasked_action_effective_rate": interval,
            "support_gate_passed": cell_pass,
        }
    control_reproduced = bool(daily["explicit_wall_control_contract_passed"].all())
    passed = passed and control_reproduced
    if passed:
        decision = "full_path_action_support_pass_develop_cpp_q90_parity"
    else:
        decision = "close_variance_time_action_path_on_development_support"
    return decision, {
        "control_reproduced": control_reproduced,
        "side_cells": cells,
        "full_path_support_passed": passed,
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
    spec = _load_spec(spec_path)
    market_source_manifest, junit = _validate_frozen_identities(spec)
    development_days = [str(day) for day in spec["panels"]["development_days"]]
    selected_days = list(args.days or development_days)
    unknown = sorted(set(selected_days) - set(development_days))
    if unknown:
        raise ValueError(f"requested days are outside frozen Development: {unknown}")
    diagnostic_subset = selected_days != development_days

    if output.exists() and not args.resume:
        raise FileExistsError(f"output directory already exists: {output}")
    output.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output / "day_checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    spec_sha256 = sha256_file(spec_path)
    source_manifest_path = output / "market_source_manifest.json"
    source_manifest_path.write_text(
        json.dumps(market_source_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in selected_days:
        checkpoint = checkpoint_dir / f"{day}.json"
        if args.resume and checkpoint.is_file():
            payload = json.loads(checkpoint.read_text(encoding="utf-8"))
            if payload.get("spec_sha256") != spec_sha256:
                raise ValueError(f"checkpoint spec identity mismatch: {checkpoint}")
            results.append(payload["result"])
        else:
            pending.append(day)

    workers = max(1, min(int(args.workers), len(pending) or 1))
    if workers == 1:
        iterator = ((day, run_day(spec, day)) for day in pending)
        for day, result in iterator:
            results.append(result)
            (checkpoint_dir / f"{day}.json").write_text(
                json.dumps(
                    {"spec_sha256": spec_sha256, "result": result},
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))
    else:
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(run_day, spec, day): day for day in pending}
            for future in as_completed(futures):
                day = futures[future]
                result = future.result()
                results.append(result)
                (checkpoint_dir / f"{day}.json").write_text(
                    json.dumps(
                        {"spec_sha256": spec_sha256, "result": result},
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))

    results.sort(key=lambda row: str(row["day"]))
    episodes = pd.DataFrame([row for result in results for row in result.get("episodes", ())])
    blockers = pd.DataFrame([row for result in results for row in result.get("blockers", ())])
    daily_rows: list[dict[str, Any]] = []
    for result in results:
        row: dict[str, Any] = {
            "day": result["day"],
            "runtime_s": result["runtime_s"],
            "daily_fresh_start": result["daily_fresh_start"],
            "cross_day_state_carried": result["cross_day_state_carried"],
            "variance_sample_count": result["variance_sample_count"],
            "explicit_wall_control_contract_passed": True,
            **{f"control_{key}": value for key, value in result["control"].items()},
            **{f"candidate_{key}": value for key, value in result["candidate"].items()},
            **result["order_path_difference"],
        }
        for side_row in result.get("support", ()):
            prefix = str(side_row["side"]).lower()
            for key, value in side_row.items():
                if key != "side":
                    row[f"{prefix}_{key}"] = value
        daily_rows.append(row)
    daily = pd.DataFrame(daily_rows).sort_values("day", kind="stable")

    episodes_path = output / "mechanical_episode_support.parquet"
    blockers_path = output / "first_binding_blockers.csv"
    daily_path = output / "daily_mechanics.csv"
    support_path = output / "side_support.csv"
    report_path = output / "report.json"
    markdown_path = output / "report.md"
    episodes.to_parquet(episodes_path, index=False)
    blockers.to_csv(blockers_path, index=False)
    daily.to_csv(daily_path, index=False)
    support = summarize_episode_support(episodes)
    support.to_csv(support_path, index=False)

    if diagnostic_subset:
        decision = "diagnostic_subset_only_no_family_decision"
        gate_summary = {
            "control_reproduced": bool(daily["explicit_wall_control_contract_passed"].all()),
            "side_cells": {},
            "full_path_support_passed": False,
        }
    else:
        decision, gate_summary = _decision_from_support(episodes, daily, spec)

    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "diagnostic_subset": diagnostic_subset,
        "evaluated_days": selected_days,
        "development_days": development_days,
        "validation_days_read": [],
        "sealed_holdout_days_read": [],
        "economics_read": False,
        "mechanical_episode_definition": (
            "first candidate decision per same-side fill lineage at which the "
            "variance-time and wall-time cooldown active states differ"
        ),
        "unmasked_action_effective_authority": "candidate_final_gate_stack",
        "control_candidate_path_comparison_role": (
            "regenerated-path diagnostic only; never used for action support"
        ),
        "gate_summary": gate_summary,
        "side_support": support.to_dict("records"),
        "path_difference": {
            key: int(pd.to_numeric(daily[key], errors="coerce").fillna(0).sum())
            for key in (
                "candidate_only_order_outcomes",
                "control_only_order_outcomes",
                "candidate_only_fill_events",
                "control_only_fill_events",
            )
        },
        "blocker_counts": (
            blockers.groupby(["side", "direction", "blocker"], as_index=False)
            .size()
            .to_dict("records")
            if not blockers.empty
            else []
        ),
        "buy_q90": {
            "control_cancel_requests": int(
                pd.to_numeric(
                    daily["control_dynamic_fill_hazard_cancel_request_count"],
                    errors="coerce",
                )
                .fillna(0)
                .sum()
            ),
            "candidate_cancel_requests": int(
                pd.to_numeric(
                    daily["candidate_dynamic_fill_hazard_cancel_request_count"],
                    errors="coerce",
                )
                .fillna(0)
                .sum()
            ),
            "cpp_parity_supported": False,
        },
        "sync_degrade": {
            "mode": "deterministic_stress",
            "promotion_evidence": False,
        },
        "lineage": {
            "daily_fresh_start_days": int(daily["daily_fresh_start"].sum()),
            "cross_day_state_carried_days": int(daily["cross_day_state_carried"].sum()),
            "continuous_live_lineage_supported": False,
        },
        "permissions": {
            "reward_or_pnl_read": False,
            "markout_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_created": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
        "aws_receive_time_transport_supported": False,
        "cpp_q90_parity_supported": False,
        "test_evidence": {
            "path": spec["test_identity"]["junit_xml"]["path"],
            "sha256": spec["test_identity"]["junit_xml"]["sha256"],
            **junit,
        },
        "spec": {"path": str(spec_path), "sha256": spec_sha256},
        "market_source_manifest": {
            "path": str(source_manifest_path),
            "canonical_sha256": canonical_sha256(market_source_manifest),
            "rows": len(market_source_manifest),
        },
        "artifacts": {
            "daily_mechanics": str(daily_path),
            "mechanical_episode_support": str(episodes_path),
            "first_binding_blockers": str(blockers_path),
            "side_support": str(support_path),
        },
    }
    report["report_payload_sha256"] = canonical_sha256(report)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(
        "\n".join(
            (
                "# Volatility-Time Add Rearm Full-Path Preflight v1",
                "",
                "Development-only mechanics. No reward, PnL, markout, Validation, or holdout was read.",
                "",
                f"- decision: `{decision}`",
                f"- evaluated days: `{len(selected_days)}`",
                f"- mechanical episodes: `{len(episodes)}`",
                f"- unmasked action-effective episodes: `{int(episodes['unmasked_action_effective'].sum()) if not episodes.empty else 0}`",
                "- action-support authority: `candidate_final_gate_stack`",
                f"- candidate-only fills: `{report['path_difference']['candidate_only_fill_events']}`",
                f"- control-only fills: `{report['path_difference']['control_only_fill_events']}`",
                "- BUY q90 C++ parity: `false`",
                "- AWS receive-time transport: `false`",
                "",
                "The predecessor's 69.8%/61.6% values remain mechanical clock diagnostics and are not action rates.",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    artifact_paths = (
        daily_path,
        episodes_path,
        blockers_path,
        support_path,
        report_path,
        markdown_path,
        source_manifest_path,
    )
    manifest = {
        "schema_version": "volatility_time_add_rearm_full_path_preflight_manifest.v1",
        "spec_sha256": spec_sha256,
        "artifacts": [
            {
                "path": str(path),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
            }
            for path in artifact_paths
        ],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output_dir": str(output),
                "decision": decision,
                "evaluated_days": len(selected_days),
                "mechanical_episodes": len(episodes),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
