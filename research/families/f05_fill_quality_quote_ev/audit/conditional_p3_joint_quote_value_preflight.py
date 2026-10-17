#!/usr/bin/env python3
"""Mechanics-only preflight for exact-distance conditional P3 quote value.

This module deliberately stops before fitting a value model.  It verifies that
BUY and SELL placement cohorts can be joined at one canonical decision clock,
that the v4.1 P3 surface supports every frozen executable candidate, and that
the resulting panel has enough independent day and fill support to justify a
nested direct-value identity.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

IDENTITY = "conditional_p3_joint_quote_value_preflight_v1"
SCHEMA_VERSION = "conditional_p3_joint_quote_value_preflight.v1"
SIDES = ("BUY", "SELL")
ROLES = ("opener", "add", "reducing")
GRID_ACTIONS = (
    "closer_4tick",
    "closer_2tick",
    "closer_1tick",
    "current",
    "farther_1tick",
    "farther_2tick",
    "farther_4tick",
)
FORMAL_GAPS = (2, 4)
NEGATIVE_CONTROL_GAP = 1

SIDE_ROW_COLUMNS = (
    "day",
    "decision_ts_ns",
    "cohort_id",
    "side",
    "inventory_role",
    "feature_ready_ts_ns",
    "best_bid_ticks",
    "best_ask_ticks",
    "p3_fold_id",
    "p3_context_sha256",
    "p3_supported",
    *tuple(f"{action}__price_ticks" for action in GRID_ACTIONS),
    *tuple(f"{action}__p_touch" for action in GRID_ACTIONS),
    *tuple(f"{action}__activated" for action in GRID_ACTIONS),
    *tuple(f"{action}__filled" for action in GRID_ACTIONS),
)

PERMISSIONS = {
    "development_only": True,
    "economic_outcomes_read": False,
    "value_model_fit_authorized": False,
    "action_experiment_authorized": False,
    "live_authority": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}


@dataclass(frozen=True)
class PreflightGates:
    """Outcome-blind support gates for opening the direct-value stage."""

    minimum_supported_days: int = 30
    required_oof_fold_count: int = 4
    minimum_days_per_oof_fold: int = 5
    minimum_filled_rows_per_side_role_action: int = 30
    require_all_grid_activated: bool = True
    require_exact_bbo_clock: bool = True


def joint_action_names() -> tuple[str, ...]:
    """Return the finite joint tuple set without assuming cross-side additivity."""

    names = ["baseline__BUY_current__SELL_current"]
    for side in SIDES:
        other = "SELL" if side == "BUY" else "BUY"
        for gap in (NEGATIVE_CONTROL_GAP, *FORMAL_GAPS):
            for direction in ("closer", "farther"):
                names.append(
                    f"{side}_{direction}_{gap}tick__{other}_current"
                )
    return tuple(names)


def _require_exact_schema(frame: pd.DataFrame) -> None:
    actual = set(frame.columns)
    expected = set(SIDE_ROW_COLUMNS)
    if actual != expected:
        raise ValueError(
            "side support schema mismatch; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )


def _as_int(frame: pd.DataFrame, column: str) -> np.ndarray:
    values = pd.to_numeric(frame[column], errors="raise").to_numpy(dtype=np.int64)
    return values


def validate_side_support_rows(side_rows: pd.DataFrame) -> pd.DataFrame:
    """Validate one row per side-specific canonical placement opportunity."""

    if not isinstance(side_rows, pd.DataFrame) or side_rows.empty:
        raise ValueError("side support rows must be a non-empty DataFrame")
    _require_exact_schema(side_rows)
    frame = side_rows.copy(deep=True)

    frame["day"] = frame["day"].astype(str)
    frame["cohort_id"] = frame["cohort_id"].astype("string")
    if frame["cohort_id"].isna().any() or frame["cohort_id"].str.strip().eq("").any():
        raise ValueError("cohort_id must be non-empty")
    if frame["cohort_id"].duplicated().any():
        raise ValueError("cohort_id must be unique")

    frame["side"] = frame["side"].astype(str).str.upper()
    if not frame["side"].isin(SIDES).all():
        raise ValueError("side must be BUY or SELL")
    frame["inventory_role"] = frame["inventory_role"].astype(str)
    if not frame["inventory_role"].isin(ROLES).all():
        raise ValueError("inventory_role is outside the frozen role set")

    decision = _as_int(frame, "decision_ts_ns")
    ready = _as_int(frame, "feature_ready_ts_ns")
    if np.any(decision % 10_000_000_000 != 0):
        raise ValueError("decision_ts_ns must be a canonical 10-second boundary")
    if np.any(ready > decision):
        raise ValueError("future feature_ready_ts_ns exceeds decision time")
    days = pd.to_datetime(decision, unit="ns", utc=True).strftime("%Y-%m-%d")
    if not np.array_equal(days, frame["day"].to_numpy(dtype=str)):
        raise ValueError("day does not match decision_ts_ns in UTC")

    bid = _as_int(frame, "best_bid_ticks")
    ask = _as_int(frame, "best_ask_ticks")
    if np.any(bid <= 0) or np.any(ask <= bid):
        raise ValueError("integer-tick BBO is invalid")
    if frame.duplicated(["day", "decision_ts_ns", "side"]).any():
        raise ValueError("side decision keys must be unique")

    if not frame["p3_supported"].isin((0, 1, False, True)).all():
        raise ValueError("p3_supported must be binary")
    if not frame["p3_supported"].astype(bool).all():
        raise ValueError("unsupported P3 rows cannot enter the support panel")
    fold = frame["p3_fold_id"].astype("string")
    context_hash = frame["p3_context_sha256"].astype("string")
    if fold.isna().any() or fold.str.strip().eq("").any():
        raise ValueError("p3_fold_id must be non-empty")
    if (
        context_hash.isna().any()
        or ~context_hash.str.fullmatch(r"[0-9a-f]{64}").all()
    ):
        raise ValueError("p3_context_sha256 must be a lowercase SHA256")

    for action in GRID_ACTIONS:
        price = _as_int(frame, f"{action}__price_ticks")
        probability = pd.to_numeric(
            frame[f"{action}__p_touch"], errors="raise"
        ).to_numpy(dtype=float)
        activated = pd.to_numeric(
            frame[f"{action}__activated"], errors="raise"
        ).to_numpy(dtype=np.int8)
        filled = pd.to_numeric(
            frame[f"{action}__filled"], errors="raise"
        ).to_numpy(dtype=np.int8)
        if np.any(price <= 0):
            raise ValueError(f"{action} executable price tick is invalid")
        if (
            not np.isfinite(probability).all()
            or np.any(probability < 0.0)
            or np.any(probability > 1.0)
        ):
            raise ValueError(f"{action} P3 output is not a probability")
        if not np.isin(activated, (0, 1)).all() or not np.isin(filled, (0, 1)).all():
            raise ValueError(f"{action} lifecycle flags must be binary")
        if np.any(filled > activated):
            raise ValueError(f"{action} fill cannot precede successful activation")
        buy = frame["side"].eq("BUY").to_numpy()
        if np.any(buy & (price >= ask)) or np.any(~buy & (price <= bid)):
            raise ValueError(f"{action} violates GTX at the decision BBO")

    return frame.sort_values(
        ["day", "decision_ts_ns", "side"], kind="stable"
    ).reset_index(drop=True)


def evaluate_preflight(
    side_rows: pd.DataFrame,
    *,
    gates: PreflightGates | None = None,
) -> dict[str, Any]:
    """Evaluate mechanics support without consuming any economic outcome."""

    gates = PreflightGates() if gates is None else gates
    frame = validate_side_support_rows(side_rows)
    paired_counts = frame.groupby(["day", "decision_ts_ns"], observed=True)[
        "side"
    ].nunique()
    paired_keys = paired_counts.loc[paired_counts.eq(2)].index
    paired = (
        frame.set_index(["day", "decision_ts_ns"])
        .loc[paired_keys]
        .reset_index()
    )
    if paired.duplicated(["day", "decision_ts_ns", "side"]).any():
        raise RuntimeError("paired side opportunity identity changed")

    supported_days = tuple(sorted(paired["day"].unique()))
    folds = (
        paired.loc[:, ["day", "p3_fold_id"]]
        .drop_duplicates()
        .groupby("p3_fold_id", observed=True)["day"]
        .nunique()
        .sort_index()
    )
    lifecycle_rows: list[dict[str, Any]] = []
    for (side, role), group in paired.groupby(
        ["side", "inventory_role"], observed=True
    ):
        for action in GRID_ACTIONS:
            lifecycle_rows.append(
                {
                    "side": str(side),
                    "inventory_role": str(role),
                    "action": action,
                    "opportunity_rows": int(len(group)),
                    "activated_rows": int(group[f"{action}__activated"].sum()),
                    "filled_rows": int(group[f"{action}__filled"].sum()),
                }
            )
    lifecycle = pd.DataFrame(lifecycle_rows)
    formal = lifecycle.loc[
        lifecycle["action"].isin(
            tuple(
                f"{direction}_{gap}tick"
                for gap in FORMAL_GAPS
                for direction in ("closer", "farther")
            )
            + ("current",)
        )
    ]

    all_grid_activated = bool(
        all(paired[f"{action}__activated"].eq(1).all() for action in GRID_ACTIONS)
    )
    minimum_fill_support = (
        int(formal["filled_rows"].min()) if not formal.empty else 0
    )
    gate_results = {
        "minimum_supported_days": len(supported_days)
        >= int(gates.minimum_supported_days),
        "required_oof_fold_count": len(folds) == int(gates.required_oof_fold_count),
        "minimum_days_per_oof_fold": bool(
            len(folds) > 0
            and folds.ge(int(gates.minimum_days_per_oof_fold)).all()
        ),
        "minimum_filled_rows_per_side_role_action": minimum_fill_support
        >= int(gates.minimum_filled_rows_per_side_role_action),
        "all_grid_activated": (not gates.require_all_grid_activated)
        or all_grid_activated,
        "exact_bbo_clock": bool(gates.require_exact_bbo_clock),
    }
    passed = bool(all(gate_results.values()))

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "question": "is canonical exact-distance support sufficient to fit direct joint quote value",
        "estimand_boundary": {
            "p3_event": "side_specific_aggressive_reach",
            "p3_horizon_s": 10.0,
            "p3_role": "ordinary_direct_value_input",
            "p3_probability_multiplied_outside_value_model": False,
            "economic_outcomes_read": False,
            "f06_lifecycle_role": "mechanics_support_only",
            "joint_action_names": list(joint_action_names()),
            "cross_side_additivity_assumed": False,
        },
        "gates": asdict(gates),
        "gate_results": gate_results,
        "supported": passed,
        "decision": (
            "register_nested_direct_value_identity"
            if passed
            else "stop_before_direct_value_fit_support_insufficient"
        ),
        "support": {
            "input_side_rows": int(len(frame)),
            "input_unique_decision_buckets": int(
                frame[["day", "decision_ts_ns"]].drop_duplicates().shape[0]
            ),
            "paired_joint_quote_buckets": int(len(paired_keys)),
            "paired_side_rows": int(len(paired)),
            "supported_days": list(supported_days),
            "supported_day_count": int(len(supported_days)),
            "days_per_oof_fold": {str(key): int(value) for key, value in folds.items()},
            "minimum_formal_cell_filled_rows": minimum_fill_support,
            "all_grid_activated": all_grid_activated,
            "side_role_rows": [
                {
                    "side": str(side),
                    "inventory_role": str(role),
                    "rows": int(len(group)),
                }
                for (side, role), group in paired.groupby(
                    ["side", "inventory_role"], observed=True
                )
            ],
            "lifecycle_support": lifecycle.to_dict("records"),
        },
        "permissions": {
            **PERMISSIONS,
            "direct_value_registration_eligible": passed,
            "value_model_fit_authorized": False,
        },
    }


__all__ = [
    "FORMAL_GAPS",
    "GRID_ACTIONS",
    "IDENTITY",
    "PERMISSIONS",
    "PreflightGates",
    "SIDE_ROW_COLUMNS",
    "evaluate_preflight",
    "joint_action_names",
    "validate_side_support_rows",
]
