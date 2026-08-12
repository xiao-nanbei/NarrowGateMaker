#!/usr/bin/env python3
"""Validate complete order lifecycle identity before dynamic fill modeling."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from models.audit.order_lifecycle import build_order_risk_intervals

SCHEMA_VERSION = "event_identity_and_riskset.v2"
EXPERIMENT_ID = "event_identity_and_riskset_v2"
LABEL_IDENTITY = "dynamic_order_lifecycle_start_stop.v2"


def _numeric(frame: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    if name not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce").fillna(default)


def audit_lifecycle_events(
    events: pd.DataFrame,
    *,
    require_native_book: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if events.empty:
        raise ValueError("lifecycle event panel is empty")
    required = {
        "order_id",
        "campaign_id",
        "side",
        "event_type",
        "event_ts_ns",
        "event_seq",
        "state_before",
        "state_after",
        "remaining_qty",
        "order_activation_ts_ns",
        "cancel_request_ts_ns",
        "cancel_ack_ts_ns",
        "fill_while_cancel_pending_qty",
        "future_mid_first_hit_ts_ns",
        "future_mid_first_hit_source",
        "same_ms_ordering_resolved",
        "repair_risk_entry_ts_ns",
        "repair_risk_exit_ts_ns",
        "repair_at_risk",
        "campaign_active",
        "reducing_quote_active",
        "reducing_quote_eligible",
        "inventory",
        "repair_ts_ns",
    }
    missing = sorted(required - set(events.columns))
    if missing:
        raise ValueError(f"lifecycle panel missing required columns: {missing}")

    identity_columns = (
        ["day", "order_id"] if "day" in events.columns else ["order_id"]
    )
    ordered = events.sort_values(
        [*identity_columns, "event_ts_ns", "event_seq"], kind="mergesort"
    ).reset_index(drop=True)
    sequence_diff = ordered.groupby(identity_columns)["event_seq"].diff()
    time_diff = ordered.groupby(identity_columns)["event_ts_ns"].diff()
    event_type = ordered["event_type"].astype(str)
    fill_mask = event_type.isin({"partial_fill", "full_fill"})
    cancel_ack = event_type.eq("cancel_ack")
    jump = event_type.eq("native_price_jump")
    repair_enter = event_type.eq("repair_risk_enter")
    repair = event_type.eq("campaign_repair")
    risk_snapshot = event_type.eq("risk_snapshot")

    fill_qty = _numeric(ordered, "fill_qty")
    remaining_start = _numeric(ordered, "remaining_qty_start")
    remaining_end = _numeric(ordered, "remaining_qty_end")
    fill_identity_error = (
        remaining_start - fill_qty - remaining_end
    ).abs()
    ack_without_request = cancel_ack & (
        _numeric(ordered, "cancel_request_ts_ns") <= 0
    )
    ack_before_request = cancel_ack & (
        _numeric(ordered, "cancel_ack_ts_ns")
        < _numeric(ordered, "cancel_request_ts_ns")
    )
    invalid_repair_entry = repair_enter & ~(
        ordered["campaign_active"].astype(bool)
        & (_numeric(ordered, "inventory").abs() > 1e-12)
        & ordered["reducing_quote_active"].astype(bool)
        & ordered["reducing_quote_eligible"].astype(bool)
        & ordered["repair_at_risk"].astype(bool)
    )
    jump_absorbing = jump & ordered["state_after"].astype(str).isin(
        {"filled", "cancelled", "rejected", "censored"}
    )
    native_source_invalid = jump & ~ordered[
        "future_mid_first_hit_source"
    ].astype(str).eq("native_exchange_book_mid")
    unresolved_same_ms = jump & ~ordered["same_ms_ordering_resolved"].astype(bool)
    snapshot_source_ts = _numeric(ordered, "feature_source_ts_ns")
    snapshot_ready_ts = _numeric(ordered, "feature_ready_ts_ns")
    snapshot_event_ts = _numeric(ordered, "event_ts_ns")
    snapshot_future = risk_snapshot & (
        (snapshot_source_ts > snapshot_ready_ts)
        | (snapshot_ready_ts > snapshot_event_ts)
    )
    snapshot_queue_unsupported = risk_snapshot & (
        ~_numeric(ordered, "exact_queue_path_valid").astype(bool)
        | _numeric(ordered, "queue_path_ambiguous").astype(bool)
    )

    intervals = build_order_risk_intervals(ordered)
    if not intervals.empty:
        activation = _numeric(intervals, "order_activation_ts_ns")
        interval_start = _numeric(intervals, "risk_interval_start_ts_ns")
        intervals["formal_fill_hazard_eligible"] = (
            intervals["fill_at_risk"].astype(bool)
            & (activation > 0)
            & (interval_start >= activation)
            & intervals["same_ms_ordering_resolved"].astype(bool)
        ).astype(int)
        intervals["formal_repair_hazard_eligible"] = (
            intervals["repair_at_risk"].astype(bool)
            & intervals["campaign_active"].astype(bool)
            & intervals["reducing_quote_active"].astype(bool)
            & intervals["reducing_quote_eligible"].astype(bool)
            & (_numeric(intervals, "inventory").abs() > 1e-12)
        ).astype(int)
        intervals["label_identity"] = LABEL_IDENTITY

    checks = {
        "event_sequence_strictly_increases": int((sequence_diff <= 0).sum()) == 0,
        "event_time_never_regresses": int((time_diff < 0).sum()) == 0,
        "remaining_quantity_nonnegative": int(
            (_numeric(ordered, "remaining_qty") < -1e-12).sum()
        )
        == 0,
        "fill_quantity_identity": int(
            (fill_mask & (fill_identity_error > 1e-12)).sum()
        )
        == 0,
        "fill_quantity_positive": int((fill_mask & (fill_qty <= 0)).sum()) == 0,
        "cancel_ack_has_request": int(ack_without_request.sum()) == 0,
        "cancel_ack_not_before_request": int(ack_before_request.sum()) == 0,
        "pending_cancel_fill_quantity_nonnegative": int(
            (_numeric(ordered, "fill_while_cancel_pending_qty") < 0).sum()
        )
        == 0,
        "native_jump_is_nonabsorbing": int(jump_absorbing.sum()) == 0,
        "native_jump_source_identity": (
            int(native_source_invalid.sum()) == 0
            and (not require_native_book or int(jump.sum()) > 0)
        ),
        "repair_uses_delayed_entry_risk_set": int(invalid_repair_entry.sum()) == 0,
        "repair_has_campaign_identity": int(
            (repair & (_numeric(ordered, "campaign_id") <= 0)).sum()
        )
        == 0,
        "risk_intervals_nonempty": not intervals.empty,
        "formal_fill_risk_support": (
            not intervals.empty
            and int(intervals["formal_fill_hazard_eligible"].sum()) > 0
        ),
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    strict_snapshot_rows = int(
        (
            risk_snapshot
            & ~snapshot_future
            & ~snapshot_queue_unsupported
            & ordered["same_ms_ordering_resolved"].astype(bool)
        ).sum()
    )
    dynamic_snapshot_identity_passed = bool(
        int(risk_snapshot.sum()) > 0
        and int(snapshot_future.sum()) == 0
        and strict_snapshot_rows > 0
    )
    counts = {str(key): int(value) for key, value in event_type.value_counts().items()}
    summary = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "label_identity": LABEL_IDENTITY,
        "status": "passed" if not failed else "blocked",
        "rows": int(len(ordered)),
        "orders": int(
            ordered[identity_columns].drop_duplicates().shape[0]
        ),
        "campaigns": int(
            ordered.loc[
                _numeric(ordered, "campaign_id") > 0,
                (["day", "campaign_id"] if "day" in ordered.columns else ["campaign_id"]),
            ].drop_duplicates().shape[0]
        ),
        "event_counts": counts,
        "risk_interval_rows": int(len(intervals)),
        "formal_fill_hazard_interval_rows": (
            int(intervals["formal_fill_hazard_eligible"].sum())
            if not intervals.empty
            else 0
        ),
        "formal_repair_hazard_interval_rows": (
            int(intervals["formal_repair_hazard_eligible"].sum())
            if not intervals.empty
            else 0
        ),
        "unresolved_native_same_ms_jump_rows": int(unresolved_same_ms.sum()),
        "dynamic_risk_snapshot_rows": int(risk_snapshot.sum()),
        "strict_dynamic_risk_snapshot_rows": strict_snapshot_rows,
        "future_dynamic_risk_snapshot_rows": int(snapshot_future.sum()),
        "queue_unsupported_dynamic_risk_snapshot_rows": int(
            snapshot_queue_unsupported.sum()
        ),
        "dynamic_risk_snapshot_identity_passed": dynamic_snapshot_identity_passed,
        "unresolved_same_ms_policy": (
            "exclude from formal fill-hazard rows; retain as diagnostic"
        ),
        "checks": checks,
        "failed_checks": failed,
        "event_identity_and_riskset_passed": not failed,
        "action_family_allowed": False,
        "dynamic_fill_hazard_allowed": bool(
            not failed and dynamic_snapshot_identity_passed
        ),
        "next_gate": (
            "attach strict Binance individual↔aggTrade mapping and prove "
            "historical/live aggregate feature parity"
        ),
    }
    return intervals, summary


def aggregate_lifecycle_audits(
    summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    """Combine disjoint daily audits without joining recycled order IDs."""

    if not summaries:
        raise ValueError("at least one lifecycle audit is required")
    check_names = sorted(
        {
            str(name)
            for summary in summaries
            for name in dict(summary.get("checks") or {})
        }
    )
    checks = {
        name: all(bool(summary.get("checks", {}).get(name, False)) for summary in summaries)
        for name in check_names
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    event_counts: dict[str, int] = {}
    for summary in summaries:
        for name, value in dict(summary.get("event_counts") or {}).items():
            event_counts[str(name)] = event_counts.get(str(name), 0) + int(value)
    numeric_fields = (
        "rows",
        "orders",
        "campaigns",
        "risk_interval_rows",
        "formal_fill_hazard_interval_rows",
        "formal_repair_hazard_interval_rows",
        "unresolved_native_same_ms_jump_rows",
        "dynamic_risk_snapshot_rows",
        "strict_dynamic_risk_snapshot_rows",
        "future_dynamic_risk_snapshot_rows",
        "queue_unsupported_dynamic_risk_snapshot_rows",
    )
    dynamic_identity = all(
        bool(summary.get("dynamic_risk_snapshot_identity_passed", False))
        for summary in summaries
    )
    output = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "label_identity": LABEL_IDENTITY,
        **{
            name: int(sum(int(summary.get(name, 0) or 0) for summary in summaries))
            for name in numeric_fields
        },
        "partition_count": len(summaries),
        "partition_days": [str(summary.get("partition_day", "")) for summary in summaries],
        "event_counts": event_counts,
        "checks": checks,
        "failed_checks": failed,
        "dynamic_risk_snapshot_identity_passed": dynamic_identity,
        "unresolved_same_ms_policy": (
            "exclude from formal fill-hazard rows; retain as diagnostic"
        ),
        "event_identity_and_riskset_passed": not failed,
        "action_family_allowed": False,
        "dynamic_fill_hazard_allowed": bool(not failed and dynamic_identity),
        "next_gate": summaries[0].get("next_gate", ""),
    }
    output["status"] = "passed" if not failed else "blocked"
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lifecycle", type=Path, required=True)
    parser.add_argument("--output-intervals", type=Path, required=True)
    parser.add_argument("--output-summary", type=Path, required=True)
    parser.add_argument("--allow-zero-native-jumps", action="store_true")
    args = parser.parse_args()

    lifecycle = pd.read_parquet(args.lifecycle.expanduser().resolve())
    intervals, summary = audit_lifecycle_events(
        lifecycle,
        require_native_book=not args.allow_zero_native_jumps,
    )
    interval_path = args.output_intervals.expanduser().resolve()
    interval_path.parent.mkdir(parents=True, exist_ok=True)
    intervals.to_parquet(interval_path, index=False)
    summary_path = args.output_summary.expanduser().resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
