"""Validate the economic-outcome-blind exact opener opportunity journal."""

from __future__ import annotations

import argparse
import json
import math
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from execution.exact_opportunity_tape import (
    DECISION_EVENT,
    EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION,
    ORDER_EVENTS,
    ExactQuoteOpportunityTapeRow,
    exact_quote_role,
)

SCHEMA_VERSION = "external_adverse_quote_edge_guard_exact_opener_mechanics.v2"
DEFAULT_MIN_CANDIDATE_RATE = 0.05
VALIDATOR_CONTRACT_VERSION = (
    "external_adverse_quote_edge_guard_exact_opener_validator.v2.1"
)
_INTERNAL_COLUMNS = frozenset({"input_path"})
_BASELINE_ELIGIBLE_ACTIONS = frozenset({"place", "replace", "keep"})
_TERMINAL_ORDER_EVENTS = frozenset(
    {
        "full_fill",
        "cancel_ack",
        "cancel_ack_reconciled",
        "expired",
        "rejected",
        "local_shutdown_cancel",
    }
)
_NUMERIC_COLUMNS = (
    "event_ts_ns",
    "exchange_ts_ns",
    "visibility_ts_ns",
    "decision_start_ts_ns",
    "feature_ready_ts_ns",
    "signed_inventory_before",
    "exposure_increasing",
    "baseline_eligible",
    "baseline_quote_price",
    "candidate_quote_price",
    "guard_valid",
    "requested_outward_ticks",
    "effective_outward_ticks",
    "queue_reset",
    "lifecycle_sequence",
    "order_quantity",
    "remaining_quantity",
    "fill_quantity",
    "fill_price",
)


def load_exact_opportunity_tape(paths: Sequence[Path]) -> pd.DataFrame:
    """Load journals without reading any external outcome table."""

    if not paths:
        raise ValueError("at least one exact opportunity tape is required")
    frames: list[pd.DataFrame] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        frame = pd.read_csv(path)
        frame["input_path"] = str(path)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _present_identifier(series: pd.Series) -> pd.Series:
    """Return true only for explicit, non-null stable identifiers."""

    text = series.astype("string").str.strip()
    return (
        series.notna()
        & text.notna()
        & text.ne("")
        & ~text.str.lower().isin({"nan", "none", "null", "<na>"})
    )


def exact_opener_validator_contract() -> dict[str, Any]:
    """Return the frozen collection-time validator semantics."""

    return {
        "validator_contract_version": VALIDATOR_CONTRACT_VERSION,
        "tape_schema_version": EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION,
        "exact_schema_columns": sorted(
            ExactQuoteOpportunityTapeRow.__dataclass_fields__
        ),
        "allowed_internal_columns": sorted(_INTERNAL_COLUMNS),
        "baseline_eligible_final_actions": sorted(
            _BASELINE_ELIGIBLE_ACTIONS
        ),
        "candidate_change_contract": {
            "guard_valid": 1,
            "guard_adverse_side_equals_quote_side": True,
            "effective_outward_ticks_strictly_positive": True,
            "effective_outward_ticks_at_most_requested": True,
            "candidate_coordinate_differs_from_baseline": True,
        },
        "candidate_rate_gate": {
            "minimum": DEFAULT_MIN_CANDIDATE_RATE,
            "scope": "BUY_and_SELL_each_independently",
            "pooled_rate": "diagnostic_only",
        },
        "identifier_contract": "nonempty_non_null_non_nan",
        "lifecycle_sequence_contract": "positive_unique_strictly_increasing_per_order",
        "terminal_order_events": sorted(_TERMINAL_ORDER_EVENTS),
        "maximum_terminal_outcomes_per_order": 1,
        "economic_outcomes_read": False,
        "operational_lifecycle_outcomes_read": True,
    }


def _side_summary(
    opener: pd.DataFrame,
    side: str,
    *,
    minimum_candidate_rate: float,
) -> dict[str, Any]:
    subset = opener.loc[opener["side"] == side]
    denominator = int(len(subset))
    candidates = int(subset["candidate_changed"].sum())
    rate = candidates / denominator if denominator else 0.0
    return {
        "side": side,
        "exact_eligible_opener_opportunities": denominator,
        "candidate_quote_changes": candidates,
        "candidate_rate": rate,
        "minimum_candidate_rate": float(minimum_candidate_rate),
        "candidate_rate_supported": bool(
            denominator > 0 and rate >= float(minimum_candidate_rate)
        ),
    }


def validate_exact_opportunity_tape(
    frame: pd.DataFrame,
    *,
    minimum_candidate_rate: float = DEFAULT_MIN_CANDIDATE_RATE,
) -> dict[str, Any]:
    """Fail closed on clocks, role identity, or lifecycle linkage drift."""

    if frame.empty:
        raise ValueError("exact opportunity tape is empty")
    threshold = float(minimum_candidate_rate)
    _require(
        math.isfinite(threshold) and 0.0 < threshold <= 1.0,
        "minimum candidate rate must be in (0, 1]",
    )
    expected = set(ExactQuoteOpportunityTapeRow.__dataclass_fields__)
    actual = set(frame.columns)
    missing = sorted(expected - actual)
    _require(not missing, f"exact opportunity tape missing columns: {missing}")
    unexpected = sorted(actual - expected - _INTERNAL_COLUMNS)
    _require(
        not unexpected,
        f"exact opportunity tape has unexpected columns: {unexpected}",
    )

    work = frame.copy()
    for column in _NUMERIC_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="raise")
        _require(
            work[column].map(math.isfinite).all(),
            f"numeric tape column contains non-finite values: {column}",
        )
    _require(
        work["schema_version"]
        .astype(str)
        .eq(EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION)
        .all(),
        "exact opportunity tape schema version mismatch",
    )
    valid_events = {DECISION_EVENT, *ORDER_EVENTS}
    unknown_events = sorted(
        set(work["event_type"].astype(str)) - valid_events
    )
    _require(not unknown_events, f"unsupported event types: {unknown_events}")

    decisions = work.loc[work["event_type"] == DECISION_EVENT].copy()
    _require(not decisions.empty, "exact opportunity tape has no decision rows")
    _require(
        _present_identifier(decisions["decision_id"]).all(),
        "decision rows require stable decision_id",
    )
    _require(
        _present_identifier(decisions["decision_group_id"]).all(),
        "decision rows require stable decision_group_id",
    )
    _require(
        not decisions["decision_id"].duplicated().any(),
        "decision_id must be unique",
    )
    _require(
        decisions["side"].astype(str).isin({"BUY", "SELL"}).all(),
        "decision side must be BUY or SELL",
    )
    _require(
        decisions["decision_start_ts_ns"].gt(0).all()
        and decisions["feature_ready_ts_ns"].gt(0).all()
        and decisions["event_ts_ns"].gt(0).all(),
        "decision clocks must be positive",
    )
    _require(
        decisions["decision_start_ts_ns"]
        .le(decisions["feature_ready_ts_ns"])
        .all(),
        "feature ready time precedes decision start",
    )
    _require(
        decisions["feature_ready_ts_ns"].le(decisions["event_ts_ns"]).all(),
        "decision used a feature after its journaled action time",
    )

    expected_roles = [
        exact_quote_role(side, inventory)
        for side, inventory in zip(
            decisions["side"],
            decisions["signed_inventory_before"],
            strict=True,
        )
    ]
    _require(
        decisions["role"].astype(str).tolist() == expected_roles,
        "decision role does not match signed decision-visible inventory",
    )
    expected_exposure = decisions["role"].isin({"opener", "add"}).astype(int)
    _require(
        decisions["exposure_increasing"].astype(int).eq(expected_exposure).all(),
        "exposure_increasing does not match side and signed inventory",
    )
    _require(
        decisions["baseline_eligible"]
        .astype(int)
        .le(decisions["exposure_increasing"].astype(int))
        .all(),
        "reducing decisions cannot enter the guard denominator",
    )
    for column in ("exposure_increasing", "baseline_eligible", "guard_valid"):
        _require(
            decisions[column].astype(int).isin({0, 1}).all(),
            f"{column} must be binary",
        )
    baseline_eligible = decisions["baseline_eligible"].astype(int).eq(1)
    _require(
        decisions.loc[baseline_eligible, "final_executed_action"]
        .astype(str)
        .isin(_BASELINE_ELIGIBLE_ACTIONS)
        .all(),
        "baseline_eligible requires final place, replace, or keep",
    )
    _require(
        decisions["baseline_quote_price"].gt(0.0).all()
        and decisions["candidate_quote_price"].gt(0.0).all(),
        "decision quote coordinates must be positive",
    )
    buy_decisions = decisions["side"].eq("BUY")
    sell_decisions = decisions["side"].eq("SELL")
    _require(
        decisions.loc[buy_decisions, "candidate_quote_price"]
        .le(decisions.loc[buy_decisions, "baseline_quote_price"])
        .all()
        and decisions.loc[sell_decisions, "candidate_quote_price"]
        .ge(decisions.loc[sell_decisions, "baseline_quote_price"])
        .all(),
        "outward-only quote invariant failed",
    )
    reducing = decisions["role"].eq("reducing")
    _require(
        decisions.loc[reducing, "candidate_quote_price"]
        .eq(decisions.loc[reducing, "baseline_quote_price"])
        .all(),
        "reducing quote changed under an exposure-only guard",
    )
    coordinate_changed = decisions["candidate_quote_price"].ne(
        decisions["baseline_quote_price"]
    )
    positive_effective_ticks = decisions["effective_outward_ticks"].gt(0)
    _require(
        coordinate_changed.eq(positive_effective_ticks).all(),
        "candidate coordinate change and effective outward ticks disagree",
    )
    candidate_changed = coordinate_changed & positive_effective_ticks
    _require(
        decisions.loc[candidate_changed, "guard_valid"].astype(int).eq(1).all(),
        "candidate change requires guard_valid=1",
    )
    _require(
        decisions.loc[candidate_changed, "guard_adverse_side"]
        .astype(str)
        .eq(decisions.loc[candidate_changed, "side"].astype(str))
        .all(),
        "candidate change requires guard_adverse_side to match side",
    )
    _require(
        decisions.loc[candidate_changed, "effective_outward_ticks"]
        .le(decisions.loc[candidate_changed, "requested_outward_ticks"])
        .all(),
        "candidate effective outward ticks exceed requested ticks",
    )

    grouped = decisions.groupby("decision_group_id", sort=False)
    _require(
        grouped["side"].nunique().eq(2).all()
        and grouped.size().eq(2).all(),
        "each exact decision group requires one BUY and one SELL row",
    )
    _require(
        grouped["decision_start_ts_ns"].nunique().eq(1).all()
        and grouped["signed_inventory_before"].nunique().eq(1).all(),
        "paired sides must share decision start and signed inventory",
    )

    order_events = work.loc[work["event_type"].isin(ORDER_EVENTS)].copy()
    decision_ids = set(decisions["decision_id"].astype(str))
    if not order_events.empty:
        _require(
            _present_identifier(order_events["origin_decision_id"]).all(),
            "order events require stable origin_decision_id",
        )
        _require(
            _present_identifier(order_events["client_order_id"]).all(),
            "order events require stable client_order_id",
        )
        _require(
            order_events["origin_decision_id"].astype(str).isin(decision_ids).all(),
            "order event references an unknown origin decision",
        )
        _require(
            order_events["feature_ready_ts_ns"]
            .le(order_events["event_ts_ns"])
            .all(),
            "order event predates its origin feature-ready clock",
        )
        _require(
            order_events["decision_start_ts_ns"]
            .le(order_events["feature_ready_ts_ns"])
            .all(),
            "order event has an invalid origin decision clock",
        )
        _require(
            order_events["visibility_ts_ns"].eq(order_events["event_ts_ns"]).all(),
            "order event visibility and journal clocks disagree",
        )
        exchange_clock_present = order_events["exchange_ts_ns"].gt(0)
        _require(
            order_events.loc[exchange_clock_present, "exchange_ts_ns"]
            .le(order_events.loc[exchange_clock_present, "visibility_ts_ns"])
            .all(),
            "exchange event time is after strategy visibility time",
        )
        trigger_present = _present_identifier(order_events["trigger_decision_id"])
        trigger_ids = set(
            order_events.loc[trigger_present, "trigger_decision_id"].astype(str)
        )
        _require(
            trigger_ids.issubset(decision_ids),
            "cancel trigger references an unknown decision",
        )
        for client_order_id, events in order_events.groupby(
            "client_order_id", sort=False
        ):
            ordered = events.sort_values(
                ["event_ts_ns", "lifecycle_sequence"], kind="stable"
            )
            _require(
                ordered["event_ts_ns"].is_monotonic_increasing,
                f"visibility clock regressed for {client_order_id}",
            )
            sequences = ordered["lifecycle_sequence"]
            _require(
                sequences.gt(0).all(),
                f"lifecycle sequence must be positive for {client_order_id}",
            )
            _require(
                sequences.is_unique and sequences.diff().dropna().gt(0).all(),
                f"lifecycle sequence must be unique and strictly increasing for {client_order_id}",
            )
            terminal_count = int(
                ordered["event_type"].isin(_TERMINAL_ORDER_EVENTS).sum()
            )
            _require(
                terminal_count <= 1,
                f"order has multiple terminal outcomes: {client_order_id}",
            )

    submit_origins = set(
        order_events.loc[order_events["event_type"] == "submit", "origin_decision_id"]
        .astype(str)
        .tolist()
    )
    submitted_decisions = decisions.loc[
        decisions["final_executed_action"]
        .astype(str)
        .isin({"place", "replace", "place_rejected", "replace_rejected"})
    ]
    missing_submit = sorted(
        set(submitted_decisions["decision_id"].astype(str)) - submit_origins
    )
    _require(
        not missing_submit,
        f"submitted decisions lack native submit events: {missing_submit[:5]}",
    )

    opener = decisions.loc[
        decisions["role"].eq("opener")
        & decisions["baseline_eligible"].astype(bool)
    ].copy()
    opener["utc_day"] = pd.to_datetime(
        opener["decision_start_ts_ns"], unit="ns", utc=True
    ).dt.strftime("%Y-%m-%d")
    opener["candidate_changed"] = candidate_changed.loc[opener.index]
    side_summaries = [
        _side_summary(
            opener,
            side,
            minimum_candidate_rate=threshold,
        )
        for side in ("BUY", "SELL")
    ]
    denominator = int(len(opener))
    candidates = int(opener["candidate_changed"].sum())
    rate = candidates / denominator if denominator else 0.0
    daily_support = []
    for (utc_day, side), group in opener.groupby(
        ["utc_day", "side"], sort=True
    ):
        daily_support.append(
            {
                "utc_day": str(utc_day),
                "side": str(side),
                "eligible_opportunities": int(len(group)),
                "candidate_quote_changes": int(
                    group["candidate_changed"].sum()
                ),
                "candidate_rate": float(group["candidate_changed"].mean()),
            }
        )
    side_gate_passed = bool(
        side_summaries
        and all(summary["candidate_rate_supported"] for summary in side_summaries)
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "validator_contract_version": VALIDATOR_CONTRACT_VERSION,
        "validator_contract": exact_opener_validator_contract(),
        "tape_schema_version": EXACT_OPPORTUNITY_TAPE_SCHEMA_VERSION,
        "economic_outcomes_read": False,
        "external_outcome_tables_read": False,
        "operational_lifecycle_outcomes_read": True,
        "decision_rows": int(len(decisions)),
        "decision_groups": int(decisions["decision_group_id"].nunique()),
        "order_event_rows": int(len(order_events)),
        "exact_eligible_opener_opportunities": denominator,
        "candidate_quote_changes": candidates,
        "candidate_rate": rate,
        "minimum_candidate_rate": threshold,
        "candidate_rate_gate_scope": "BUY_and_SELL_each_independently",
        "candidate_rate_supported": side_gate_passed,
        "pooled_candidate_rate_supported_diagnostic": bool(
            denominator > 0 and rate >= threshold
        ),
        "side_summaries": side_summaries,
        "daily_support": daily_support,
        "role_counts": {
            str(key): int(value)
            for key, value in decisions["role"].value_counts().items()
        },
        "final_action_counts": {
            str(key): int(value)
            for key, value in decisions["final_executed_action"]
            .value_counts()
            .items()
        },
        "order_event_counts": {
            str(key): int(value)
            for key, value in order_events["event_type"].value_counts().items()
        },
        "transport_supported": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tape", action="append", required=True, type=Path)
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument(
        "--minimum-candidate-rate",
        type=float,
        default=DEFAULT_MIN_CANDIDATE_RATE,
    )
    args = parser.parse_args()
    report = validate_exact_opportunity_tape(
        load_exact_opportunity_tape(args.tape),
        minimum_candidate_rate=args.minimum_candidate_rate,
    )
    _atomic_json(args.output_json, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
