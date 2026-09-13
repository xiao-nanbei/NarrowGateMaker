#!/usr/bin/env python3
"""Audit event identity and risk-set eligibility before hazard modeling.

This module deliberately does not fit a hazard model.  It verifies that fill,
cancel, price-jump, and campaign-repair events have distinct clocks and valid
risk sets.  A failed audit is a completed infrastructure result and blocks
``dynamic_fill_hazard_m0_v2``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "event_identity_and_riskset.v1"
EXPERIMENT_ID = "event_identity_and_riskset_v1"


@dataclass(frozen=True)
class RiskSetContract:
    cause: str
    event_role: str
    estimand: str
    delayed_entry: bool
    absorbing_for_dynamic_fill_estimand: bool
    required_columns: tuple[str, ...]


COMMON_REQUIRED_COLUMNS = (
    "day",
    "decision_id",
    "order_id",
    "campaign_id",
    "side",
    "decision_ts_ns",
    "feature_ready_ts_ns",
    "censor_ts_ns",
    "risk_interval_start_ts_ns",
    "risk_interval_end_ts_ns",
)

RISK_SET_CONTRACTS: tuple[RiskSetContract, ...] = (
    RiskSetContract(
        cause="fill",
        event_role="market_lifecycle_transition",
        estimand=(
            "dynamic favorable/adverse fill hazard while the order is active "
            "and remaining quantity is positive"
        ),
        delayed_entry=True,
        absorbing_for_dynamic_fill_estimand=True,
        required_columns=(
            "order_activation_ts_ns",
            "remaining_qty_start",
            "remaining_qty_end",
            "fill_ts_ns",
            "fill_event_seq",
            "fill_qty",
            "fill_is_partial",
            "remaining_qty_after_fill",
        ),
    ),
    RiskSetContract(
        cause="cancel",
        event_role="policy_stopping_time_then_exchange_transition",
        estimand=(
            "cancel request under the behavior policy followed by ACK, partial "
            "or full fill while pending, and cancellation of remaining quantity"
        ),
        delayed_entry=False,
        absorbing_for_dynamic_fill_estimand=False,
        required_columns=(
            "cancel_request_ts_ns",
            "cancel_request_event_seq",
            "cancel_ack_ts_ns",
            "cancel_event_seq",
            "remaining_qty_at_cancel_request",
            "remaining_qty_at_cancel_ack",
            "fill_while_cancel_pending_qty",
        ),
    ),
    RiskSetContract(
        cause="jump",
        event_role="market_state_transition",
        estimand=(
            "native future-mid first hit with explicit same-timestamp ordering; "
            "the order remains at risk after the state transition"
        ),
        delayed_entry=False,
        absorbing_for_dynamic_fill_estimand=False,
        required_columns=(
            "future_mid_first_hit_ts_ns",
            "future_mid_first_hit_direction",
            "future_mid_first_hit_source",
            "future_mid_first_hit_event_seq",
            "same_ms_ordering_resolved",
        ),
    ),
    RiskSetContract(
        cause="repair",
        event_role="campaign_multistate_transition",
        estimand=(
            "campaign repair after delayed entry into a nonzero-inventory, "
            "active-campaign, reducing-path risk set"
        ),
        delayed_entry=True,
        absorbing_for_dynamic_fill_estimand=False,
        required_columns=(
            "repair_risk_entry_ts_ns",
            "repair_risk_exit_ts_ns",
            "repair_at_risk",
            "campaign_active",
            "reducing_quote_active",
            "reducing_quote_eligible",
            "inventory",
            "repair_ts_ns",
            "campaign_repair_event_seq",
        ),
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _numeric(frame: pd.DataFrame, name: str, default: float = math.nan) -> pd.Series:
    if name not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def _nonempty(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(False, index=frame.index, dtype=bool)
    return frame[name].notna() & frame[name].astype(str).str.strip().ne("")


def _event_count(frame: pd.DataFrame, event: str) -> int:
    if "first_event" not in frame:
        return 0
    return int(frame["first_event"].astype(str).eq(event).sum())


def _same_timestamp_ambiguity(frame: pd.DataFrame) -> int:
    timestamp_columns = [
        name
        for name in (
            "fill_ts_ns",
            "cancel_ack_ts_ns",
            "future_mid_first_hit_ts_ns",
            "repair_ts_ns",
        )
        if name in frame
    ]
    if len(timestamp_columns) < 2:
        return 0
    values = np.column_stack(
        [_numeric(frame, name, 0.0).fillna(0.0).to_numpy() for name in timestamp_columns]
    )
    values.sort(axis=1)
    positive_pairs = (values[:, 1:] == values[:, :-1]) & (values[:, 1:] > 0)
    return int(positive_pairs.any(axis=1).sum())


def _cause_audit(frame: pd.DataFrame, contract: RiskSetContract) -> dict[str, Any]:
    missing = sorted(set(contract.required_columns) - set(frame.columns))
    failures: list[str] = []
    if missing:
        failures.append("missing_required_columns")

    violations: dict[str, int] = {}
    if contract.cause == "fill" and not missing:
        activation = _numeric(frame, "order_activation_ts_ns")
        start = _numeric(frame, "risk_interval_start_ts_ns")
        remaining = _numeric(frame, "remaining_qty_start")
        violations["risk_before_activation"] = int((start < activation).sum())
        violations["nonpositive_remaining_qty_at_risk"] = int((remaining <= 0).sum())
        fill = _numeric(frame, "fill_ts_ns", 0.0).fillna(0.0)
        fill_qty = _numeric(frame, "fill_qty", 0.0).fillna(0.0)
        violations["fill_event_without_positive_qty"] = int(
            ((fill > 0) & (fill_qty <= 0)).sum()
        )
    elif contract.cause == "cancel" and not missing:
        request = _numeric(frame, "cancel_request_ts_ns", 0.0).fillna(0.0)
        ack = _numeric(frame, "cancel_ack_ts_ns", 0.0).fillna(0.0)
        violations["ack_without_request"] = int(((ack > 0) & (request <= 0)).sum())
        violations["ack_before_request"] = int(
            ((ack > 0) & (request > 0) & (ack < request)).sum()
        )
        pending_fill = _numeric(frame, "fill_while_cancel_pending_qty", 0.0)
        violations["negative_pending_fill_qty"] = int((pending_fill < 0).sum())
    elif contract.cause == "jump" and not missing:
        jump = _numeric(frame, "future_mid_first_hit_ts_ns", 0.0).fillna(0.0)
        resolved = _numeric(frame, "same_ms_ordering_resolved", 0.0).fillna(0.0)
        violations["native_hit_without_source"] = int(
            ((jump > 0) & ~_nonempty(frame, "future_mid_first_hit_source")).sum()
        )
        violations["native_hit_with_unresolved_ordering"] = int(
            ((jump > 0) & (resolved <= 0)).sum()
        )
        if "first_event" in frame and "first_event_ts_ns" in frame:
            legacy_jump = frame["first_event"].astype(str).eq("adverse_price_jump")
            first_ts = _numeric(frame, "first_event_ts_ns", 0.0).fillna(0.0)
            violations["jump_label_not_native_first_hit"] = int(
                (legacy_jump & ((jump <= 0) | (first_ts != jump))).sum()
            )
    elif contract.cause == "repair" and not missing:
        entry = _numeric(frame, "repair_risk_entry_ts_ns", 0.0).fillna(0.0)
        repair = _numeric(frame, "repair_ts_ns", 0.0).fillna(0.0)
        at_risk = _numeric(frame, "repair_at_risk", 0.0).fillna(0.0) > 0
        interval_start = _numeric(frame, "risk_interval_start_ts_ns", 0.0).fillna(0.0)
        violations["repair_without_delayed_entry"] = int(
            ((repair > 0) & ((entry <= 0) | (repair < entry))).sum()
        )
        violations["repair_event_outside_risk_set"] = int(
            ((repair > 0) & ~at_risk).sum()
        )
        violations["repair_exposure_before_entry"] = int(
            (at_risk & (interval_start < entry)).sum()
        )
        inventory = _numeric(frame, "inventory", 0.0).fillna(0.0)
        campaign_active = _numeric(frame, "campaign_active", 0.0).fillna(0.0) > 0
        reducing = _numeric(frame, "reducing_quote_active", 0.0).fillna(0.0) > 0
        violations["invalid_repair_risk_indicator"] = int(
            (at_risk & ((inventory == 0) | ~campaign_active | ~reducing)).sum()
        )

    positive_violations = {key: value for key, value in violations.items() if value}
    if positive_violations:
        failures.append("risk_set_or_identity_violation")
    return {
        **asdict(contract),
        "missing_columns": missing,
        "event_rows": _event_count(
            frame,
            {
                "fill": "favorable_fill",
                "cancel": "cancel",
                "jump": "adverse_price_jump",
                "repair": "campaign_repair",
            }[contract.cause],
        )
        + (_event_count(frame, "adverse_fill") if contract.cause == "fill" else 0),
        "violations": violations,
        "failures": failures,
        "passed": not failures,
    }


def audit_event_identity_and_risksets(
    frame: pd.DataFrame,
    *,
    input_path: str = "",
    input_sha256: str = "",
) -> dict[str, Any]:
    missing_common = sorted(set(COMMON_REQUIRED_COLUMNS) - set(frame.columns))
    common_failures: list[str] = []
    if missing_common:
        common_failures.append("missing_common_columns")

    feature_time_violations = 0
    interval_order_violations = 0
    if {"feature_ready_ts_ns", "decision_ts_ns"}.issubset(frame.columns):
        feature_time_violations = int(
            (
                _numeric(frame, "feature_ready_ts_ns")
                > _numeric(frame, "decision_ts_ns")
            ).sum()
        )
        if feature_time_violations:
            common_failures.append("future_feature_visibility")
    if {"risk_interval_start_ts_ns", "risk_interval_end_ts_ns"}.issubset(
        frame.columns
    ):
        interval_order_violations = int(
            (
                _numeric(frame, "risk_interval_end_ts_ns")
                <= _numeric(frame, "risk_interval_start_ts_ns")
            ).sum()
        )
        if interval_order_violations:
            common_failures.append("invalid_risk_interval")

    cause_audits = [_cause_audit(frame, contract) for contract in RISK_SET_CONTRACTS]
    cause_by_name = {row["cause"]: row for row in cause_audits}
    event_gate = (
        not common_failures
        and bool(cause_by_name["fill"]["passed"])
        and bool(cause_by_name["jump"]["passed"])
    )
    all_passed = not common_failures and all(row["passed"] for row in cause_audits)

    label_counts = (
        frame["label_identity"].fillna("").astype(str).value_counts().to_dict()
        if "label_identity" in frame
        else {}
    )
    native_available = _numeric(
        frame, "future_mid_first_hit_ts_ns", 0.0
    ).fillna(0.0) > 0
    legacy_jump = _numeric(frame, "adverse_price_jump_ts_ns", 0.0).fillna(0.0)
    native_jump = _numeric(frame, "future_mid_first_hit_ts_ns", 0.0).fillna(0.0)
    block_reasons = list(common_failures)
    for cause in cause_audits:
        block_reasons.extend(f"{cause['cause']}:{reason}" for reason in cause["failures"])

    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "status": "passed" if all_passed else "blocked",
        "rows": int(len(frame)),
        "days": int(frame["day"].astype(str).nunique()) if "day" in frame else 0,
        "input_path": input_path,
        "input_sha256": input_sha256,
        "common_contract": {
            "required_columns": list(COMMON_REQUIRED_COLUMNS),
            "missing_columns": missing_common,
            "feature_time_violations": feature_time_violations,
            "interval_order_violations": interval_order_violations,
            "failures": common_failures,
        },
        "causes": cause_audits,
        "label_identity_counts": {str(key): int(value) for key, value in label_counts.items()},
        "legacy_native_jump_audit": {
            "legacy_jump_rows": int((legacy_jump > 0).sum()),
            "native_hit_available_rows": int(native_available.sum()),
            "legacy_native_exact_match_rows": int(
                ((legacy_jump > 0) & (legacy_jump == native_jump)).sum()
            ),
            "legacy_native_mismatch_rows": int(
                ((legacy_jump > 0) & (legacy_jump != native_jump)).sum()
            ),
            "legacy_zero_elapsed_jump_rows": int(
                (
                    (legacy_jump > 0)
                    & (legacy_jump == _numeric(frame, "decision_ts_ns", 0.0).fillna(0.0))
                ).sum()
            ),
            "same_timestamp_ambiguous_rows": _same_timestamp_ambiguity(frame),
        },
        "dynamic_fill_hazard_event_gate_passed": event_gate,
        "dynamic_fill_hazard_allowed": False,
        "dynamic_fill_hazard_block_reason": (
            "requires_event_identity_gate_and_live_taker_flow_parity_v1"
        ),
        "action_family_allowed": False,
        "block_reasons": sorted(set(block_reasons)),
    }


def _load_table(path: Path) -> pd.DataFrame:
    suffixes = "".join(path.suffixes).lower()
    if suffixes.endswith(".parquet"):
        return pd.read_parquet(path)
    if suffixes.endswith(".csv") or suffixes.endswith(".csv.gz"):
        return pd.read_csv(path)
    raise ValueError(f"unsupported panel format: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-panel", type=Path, required=True)
    parser.add_argument("--output-summary", type=Path, required=True)
    args = parser.parse_args()

    input_path = args.input_panel.expanduser().resolve()
    frame = _load_table(input_path)
    summary = audit_event_identity_and_risksets(
        frame,
        input_path=str(input_path),
        input_sha256=_sha256(input_path),
    )
    output = args.output_summary.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
