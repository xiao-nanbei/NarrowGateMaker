#!/usr/bin/env python3
"""Audit q90 terminal holds that remain in an active-order risk set.

This identity is mechanics-only. It reads lifecycle timestamps and q90 shadow
features, but it deliberately excludes fills, PnL, markout, campaign outcomes,
and any action-policy promotion decision.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SCHEMA_VERSION = "buy_q90_terminal_hold_riskset_audit.v1"
IDENTITY = "buy_q90_terminal_hold_riskset_audit_v1"
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "buy_q90_terminal_hold_riskset_audit_v1_spec_20260801.json"
)

SHADOW_COLUMNS = (
    "timestamp",
    "client_order_id",
    "inventory_role",
    "valid",
    "reason",
    "edge_ms",
    "elapsed_ms",
    "feature_source_ts_ns",
    "feature_ready_ts_ns",
    "deep_generation",
    "deep_age_ms",
    "order_price",
    "mid",
    "microprice",
    "favorable_probability",
    "adverse_probability",
    "executed_action",
)
ACTION_COLUMNS = (
    "timestamp",
    "client_order_id",
    "inventory_role",
    "event",
    "adverse_value",
    "entry_threshold",
    "order_state",
    "cancel_succeeded",
    "hold_age_ms",
)
OUTCOME_COLUMNS = (
    "timestamp",
    "event_type",
    "client_order_id",
    "side",
    "price",
    "quantity",
    "age_ms",
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


def _require_file(identity: Mapping[str, Any], label: str) -> Path:
    path = Path(str(identity.get("path", ""))).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    expected = str(identity.get("sha256", "")).strip().lower()
    actual = sha256_file(path)
    if len(expected) != 64 or actual != expected:
        raise ValueError(
            f"{label} hash mismatch: expected={expected} actual={actual}"
        )
    return path


def load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("terminal-hold risk-set spec must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected terminal-hold risk-set schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected terminal-hold risk-set identity")
    if payload.get("status") != "frozen_before_journal_generation":
        raise ValueError("terminal-hold risk-set status drifted")
    frozen = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen) != 64 or canonical_spec_sha256(payload) != frozen:
        raise ValueError("terminal-hold risk-set canonical spec hash mismatch")
    if not bool(payload.get("economic_outputs_prohibited", False)):
        raise ValueError("terminal-hold audit must prohibit economic outputs")
    permissions = payload.get("permissions") or {}
    if not permissions or any(bool(value) for value in permissions.values()):
        raise ValueError("terminal-hold audit cannot grant permissions")
    return payload


def _utc_bounds(day: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(day, tz="UTC")
    return start, start + pd.Timedelta(days=1)


def _day_mask(values: pd.Series, day: str) -> pd.Series:
    start, end = _utc_bounds(day)
    timestamps = pd.to_datetime(values, unit="s", utc=True, errors="coerce")
    return timestamps.ge(start) & timestamps.lt(end)


def _read_shadow_day(path: Path, day: str) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for frame in pd.read_csv(
        path,
        usecols=list(SHADOW_COLUMNS),
        chunksize=100_000,
    ):
        selected = frame.loc[_day_mask(frame["timestamp"], day)]
        if not selected.empty:
            parts.append(selected.copy())
    if not parts:
        raise ValueError(f"q90 shadow tape has no rows for {day}")
    return pd.concat(parts, ignore_index=True)


def _read_action_day(path: Path, day: str) -> pd.DataFrame:
    frame = pd.read_csv(path, usecols=list(ACTION_COLUMNS))
    return frame.loc[_day_mask(frame["timestamp"], day)].copy()


def _read_order_rows(path: Path, client_order_ids: set[str]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for frame in pd.read_csv(
        path,
        usecols=list(OUTCOME_COLUMNS),
        chunksize=250_000,
    ):
        selected = frame.loc[frame["client_order_id"].isin(client_order_ids)]
        if not selected.empty:
            parts.append(selected.copy())
    if not parts:
        raise ValueError("no order lifecycle rows matched terminal-hold orders")
    return pd.concat(parts, ignore_index=True)


def _unique_timestamp_by_event(
    frame: pd.DataFrame,
    *,
    event_column: str,
    event: str,
) -> dict[str, float]:
    selected = frame.loc[frame[event_column].eq(event)]
    counts = selected.groupby("client_order_id").size()
    if not counts.empty and int(counts.max()) != 1:
        raise ValueError(f"expected one {event} row per order")
    return {
        str(row.client_order_id): float(row.timestamp)
        for row in selected.itertuples(index=False)
    }


def _classify_invalid_rows(
    invalid: pd.DataFrame,
    cancel_ack_ts: Mapping[str, float],
) -> pd.DataFrame:
    output = invalid.copy()
    output["cancel_ack_ts"] = output["client_order_id"].map(cancel_ack_ts)
    output["exchange_order_terminal"] = (
        output["cancel_ack_ts"].notna()
        & output["timestamp"].ge(output["cancel_ack_ts"])
    )
    above_mid = output["order_price"].gt(output["mid"])
    output["invalid_class"] = "unclassified_invalid"
    output.loc[
        output["exchange_order_terminal"] & above_mid,
        "invalid_class",
    ] = "terminal_price_above_best_bid"
    output.loc[
        output["exchange_order_terminal"] & ~above_mid,
        "invalid_class",
    ] = "terminal_price_outside_book_unresolved_without_best_bid"
    output.loc[
        ~output["exchange_order_terminal"],
        "invalid_class",
    ] = "active_price_outside_snapshot_range_unresolved_without_floor"
    output["active_fill_riskset_allowed"] = ~output[
        "exchange_order_terminal"
    ]
    output["active_fill_riskset_violation"] = output[
        "exchange_order_terminal"
    ]
    return output


def _event_row(
    *,
    client_order_id: str,
    timestamp: float,
    event: str,
    source: str,
    exchange_order_state: str,
    q90_hold_state: str,
    recovery_state: str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "client_order_id": str(client_order_id),
        "timestamp": float(timestamp),
        "event": str(event),
        "source": str(source),
        "exchange_order_state": str(exchange_order_state),
        "q90_hold_state": str(q90_hold_state),
        "post_cancel_recovery_state": str(recovery_state),
        "details_json": json.dumps(
            dict(details or {}),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ),
    }


def _build_journal(
    shadow: pd.DataFrame,
    actions: pd.DataFrame,
    outcomes: pd.DataFrame,
    terminal_ids: set[str],
    day_end_epoch_s: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for client_order_id in sorted(terminal_ids):
        action_rows = actions.loc[
            actions["client_order_id"].eq(client_order_id)
        ].sort_values("timestamp")
        outcome_rows = outcomes.loc[
            outcomes["client_order_id"].eq(client_order_id)
        ].sort_values("timestamp")
        shadow_rows = shadow.loc[
            shadow["client_order_id"].eq(client_order_id)
        ].sort_values("timestamp")
        cancel = action_rows.loc[action_rows["event"].eq("cancel_request")]
        ack = outcome_rows.loc[outcome_rows["event_type"].eq("canceled")]
        if len(cancel) != 1 or len(ack) != 1:
            raise ValueError(
                f"{client_order_id} lacks a unique cancel request/ACK"
            )
        cancel_row = cancel.iloc[0]
        ack_row = ack.iloc[0]
        rows.append(
            _event_row(
                client_order_id=client_order_id,
                timestamp=float(cancel_row["timestamp"]),
                event="cancel_request",
                source="dynamic_fill_hazard_action",
                exchange_order_state=str(cancel_row["order_state"]),
                q90_hold_state="active_waiting_cancel_ack",
                recovery_state="not_entered",
                details={
                    "entry_score": float(cancel_row["adverse_value"]),
                    "entry_threshold": float(cancel_row["entry_threshold"]),
                    "cancel_succeeded": int(cancel_row["cancel_succeeded"]),
                    "inventory_role": str(cancel_row["inventory_role"]),
                },
            )
        )
        ack_ts = float(ack_row["timestamp"])
        rows.append(
            _event_row(
                client_order_id=client_order_id,
                timestamp=ack_ts,
                event="cancel_ack_exchange_terminal",
                source="order_outcomes",
                exchange_order_state="terminal_canceled",
                q90_hold_state="terminal_hold",
                recovery_state="post_cancel_recovery_required",
                details={
                    "order_price": float(ack_row["price"]),
                    "order_age_ms": float(ack_row["age_ms"]),
                },
            )
        )
        post_ack = shadow_rows.loc[shadow_rows["timestamp"].ge(ack_ts)]
        invalid = post_ack.loc[post_ack["valid"].eq(0)]
        if not invalid.empty:
            first = invalid.iloc[0]
            last = invalid.iloc[-1]
            rows.append(
                _event_row(
                    client_order_id=client_order_id,
                    timestamp=float(first["timestamp"]),
                    event="first_post_ack_active_riskset_invalid",
                    source="dynamic_fill_hazard_shadow",
                    exchange_order_state="terminal_canceled",
                    q90_hold_state="terminal_hold",
                    recovery_state="unsupported_active_order_observation",
                    details={
                        "reason": str(first["reason"]),
                        "order_price": float(first["order_price"]),
                        "mid": float(first["mid"]),
                    },
                )
            )
            rows.append(
                _event_row(
                    client_order_id=client_order_id,
                    timestamp=float(last["timestamp"]),
                    event="last_post_ack_active_riskset_invalid",
                    source="dynamic_fill_hazard_shadow",
                    exchange_order_state="terminal_canceled",
                    q90_hold_state="terminal_hold",
                    recovery_state="unsupported_active_order_observation",
                    details={
                        "reason": str(last["reason"]),
                        "invalid_rows": int(len(invalid)),
                    },
                )
            )
        recovered = action_rows.loc[action_rows["event"].eq("score_recovered")]
        reentered = shadow_rows.loc[
            shadow_rows["executed_action"].eq("baseline_reenter")
        ]
        if len(recovered) > 1 or len(reentered) > 1:
            raise ValueError(f"{client_order_id} has duplicate recovery/re-entry")
        if len(recovered) == 1:
            recovery = recovered.iloc[0]
            rows.append(
                _event_row(
                    client_order_id=client_order_id,
                    timestamp=float(recovery["timestamp"]),
                    event="score_recovered_from_terminal_active_riskset",
                    source="dynamic_fill_hazard_action",
                    exchange_order_state="terminal_canceled",
                    q90_hold_state="terminal_hold",
                    recovery_state="contract_invalid_recovery_evidence",
                    details={
                        "score": float(recovery["adverse_value"]),
                        "entry_threshold": float(recovery["entry_threshold"]),
                    },
                )
            )
        if len(reentered) == 1:
            reentry = reentered.iloc[0]
            rows.append(
                _event_row(
                    client_order_id=client_order_id,
                    timestamp=float(reentry["timestamp"]),
                    event="release_and_baseline_reentry",
                    source="dynamic_fill_hazard_shadow",
                    exchange_order_state="terminal_canceled",
                    q90_hold_state="released",
                    recovery_state="contract_invalid_recovery_consumed",
                    details={
                        "valid": int(reentry["valid"]),
                        "reason": str(reentry["reason"]),
                    },
                )
            )
        else:
            rows.append(
                _event_row(
                    client_order_id=client_order_id,
                    timestamp=float(day_end_epoch_s),
                    event="day_end_censor_terminal_hold",
                    source="audit_clock",
                    exchange_order_state="terminal_canceled",
                    q90_hold_state="terminal_hold",
                    recovery_state="unsupported_unresolved",
                    details={},
                )
            )
    journal = pd.DataFrame(rows).sort_values(
        ["client_order_id", "timestamp", "event"],
        kind="mergesort",
    )
    return journal.reset_index(drop=True)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    return value


def run_audit(spec_path: Path) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    spec = load_spec(spec_path)
    source = spec.get("source_identity") or {}
    shadow_path = _require_file(source["live_q90_shadow_tape"], "q90 shadow tape")
    action_path = _require_file(source["live_q90_action_log"], "q90 action log")
    outcome_path = _require_file(source["live_order_outcomes"], "order outcomes")
    _require_file(spec["parent_spec_identity"], "parent q90 parity spec")
    _require_file(spec["parent_report_identity"], "parent q90 parity report")

    day = str(spec["day"])
    shadow = _read_shadow_day(shadow_path, day)
    outside = shadow.loc[
        shadow["reason"].eq("order_price_outside_deep_book")
    ].copy()
    terminal_ids = set(outside["client_order_id"].astype(str))
    actions = _read_action_day(action_path, day)
    actions = actions.loc[actions["client_order_id"].isin(terminal_ids)].copy()
    outcomes = _read_order_rows(outcome_path, terminal_ids)
    cancel_ack_ts = _unique_timestamp_by_event(
        outcomes,
        event_column="event_type",
        event="canceled",
    )
    invalid_observations = _classify_invalid_rows(outside, cancel_ack_ts)

    all_terminal_rows = shadow.loc[
        shadow["client_order_id"].isin(terminal_ids)
    ].copy()
    all_terminal_rows["cancel_ack_ts"] = all_terminal_rows[
        "client_order_id"
    ].map(cancel_ack_ts)
    post_ack = all_terminal_rows.loc[
        all_terminal_rows["timestamp"].ge(all_terminal_rows["cancel_ack_ts"])
    ].copy()
    post_ack["active_fill_riskset_violation"] = True

    start, end = _utc_bounds(day)
    journal = _build_journal(
        shadow,
        actions,
        outcomes,
        terminal_ids,
        float(end.timestamp()),
    )
    order_summary = (
        post_ack.groupby("client_order_id", as_index=False)
        .agg(
            post_ack_riskset_rows=("valid", "size"),
            post_ack_valid_rows=("valid", "sum"),
            first_post_ack_ts=("timestamp", "min"),
            last_post_ack_ts=("timestamp", "max"),
            order_price=("order_price", "first"),
            median_mid=("mid", "median"),
            inventory_role=("inventory_role", "first"),
        )
    )
    invalid_counts = invalid_observations["invalid_class"].value_counts()
    reentry_ids = set(
        shadow.loc[
            shadow["executed_action"].eq("baseline_reenter"),
            "client_order_id",
        ].astype(str)
    ).intersection(terminal_ids)
    recovered_ids = set(
        actions.loc[
            actions["event"].eq("score_recovered"),
            "client_order_id",
        ].astype(str)
    )
    if recovered_ids != reentry_ids:
        raise ValueError("terminal-hold recovery and re-entry ids differ")

    expected = spec.get("known_before_freeze") or {}
    observed_contract = {
        "outside_deep_book_rows": int(len(outside)),
        "terminal_hold_orders": int(len(terminal_ids)),
        "price_above_mid_rows": int(outside["order_price"].gt(outside["mid"]).sum()),
        "recovered_and_reentered_orders": int(len(reentry_ids)),
        "hold_invalid_end_orders": int(len(terminal_ids - reentry_ids)),
    }
    for key, value in expected.items():
        if key in observed_contract and int(value) != observed_contract[key]:
            raise ValueError(
                f"known mechanics drift for {key}: "
                f"expected={value} observed={observed_contract[key]}"
            )

    report = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "day": day,
        "panel": "Development mechanics-only observed live slice",
        "source_window_start_utc": start.isoformat(),
        "source_window_end_utc": end.isoformat(),
        "observed_contract": observed_contract,
        "riskset_audit": {
            "post_ack_active_order_evaluation_rows": int(len(post_ack)),
            "post_ack_valid_active_order_evaluation_rows": int(post_ack["valid"].sum()),
            "post_ack_invalid_active_order_evaluation_rows": int((post_ack["valid"] == 0).sum()),
            "all_post_ack_rows_violate_active_fill_riskset": bool(len(post_ack) > 0),
            "active_order_fill_hazard_after_terminal_allowed": False,
            "invariant_passed": bool(len(post_ack) == 0),
        },
        "outside_deep_book_classification": {
            str(key): int(value) for key, value in invalid_counts.items()
        },
        "invalid_reason_counts_all_day": {
            str(key): int(value)
            for key, value in shadow.loc[shadow["valid"].eq(0), "reason"]
            .value_counts()
            .items()
        },
        "state_contract": {
            "exchange_order_terminal": "cancel ACK removes exchange fill risk",
            "q90_hold_terminal": "policy permission hold may remain after ACK",
            "post_cancel_recovery_state": "must use a separately frozen recovery estimand",
            "current_recovery_evidence": "invalid because it reuses the terminal active-order hazard state",
        },
        "inherited_parity_gates": dict(spec["inherited_parity_gates"]),
        "inherited_parity_gates_evaluated": False,
        "decision": "terminal_active_riskset_contract_failed_post_cancel_recovery_undefined",
        "interpretation": {
            "depth_expansion_is_a_fix": False,
            "hold_timeout_authorized": False,
            "q90_threshold_change_authorized": False,
            "f07_economic_result_read": False,
            "f07_v2_registration_unblocked": False,
            "exact_aws_deep_transport_is_final_gate_not_audit_prerequisite": True,
        },
        "economic_outputs_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "permissions": dict(spec["permissions"]),
    }
    return _json_safe(report), {
        "order_summary": order_summary,
        "lifecycle_journal": journal,
        "invalid_observations": invalid_observations,
        "post_ack_riskset_rows": post_ack,
        "action_extract": actions,
        "order_outcome_extract": outcomes,
    }


def _write_outputs(
    output_dir: Path,
    report: Mapping[str, Any],
    frames: Mapping[str, pd.DataFrame],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name in ("order_summary", "lifecycle_journal"):
        path = output_dir / f"{name}.csv"
        frames[name].to_csv(path, index=False)
        paths[name] = path
    for name in (
        "invalid_observations",
        "post_ack_riskset_rows",
        "action_extract",
        "order_outcome_extract",
    ):
        path = output_dir / f"{name}.parquet"
        frames[name].to_parquet(path, index=False, compression="zstd")
        paths[name] = path
    report_path = output_dir / "report.json"
    report_path.write_text(
        json.dumps(
            report,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    paths["report"] = report_path
    manifest = {
        "schema_version": "buy_q90_terminal_hold_riskset_artifacts.v1",
        "identity": IDENTITY,
        "files": {
            name: {
                "path": str(path),
                "bytes": int(path.stat().st_size),
                "sha256": sha256_file(path),
            }
            for name, path in sorted(paths.items())
        },
        "permissions": dict(report["permissions"]),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    report, frames = run_audit(args.spec)
    manifest = _write_outputs(args.output_dir.expanduser().resolve(), report, frames)
    print(
        json.dumps(
            {
                "decision": report["decision"],
                "riskset_audit": report["riskset_audit"],
                "manifest": manifest,
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
