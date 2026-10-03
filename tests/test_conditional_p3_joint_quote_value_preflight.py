from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit.conditional_p3_joint_quote_value_preflight import (
    GRID_ACTIONS,
    PreflightGates,
    evaluate_preflight,
    joint_action_names,
    validate_side_support_rows,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _rows(*, days: int = 6, fills: int = 1) -> pd.DataFrame:
    rows = []
    for day_offset in range(days):
        day = f"2026-06-{day_offset + 1:02d}"
        ts = int(pd.Timestamp(day, tz="UTC").value)
        for side in ("BUY", "SELL"):
            bid, ask = 650_000, 650_001
            current = bid - 10 if side == "BUY" else ask + 10
            row = {
                "day": day,
                "decision_ts_ns": ts,
                "cohort_id": f"{day}:{side}",
                "side": side,
                "inventory_role": "opener",
                "feature_ready_ts_ns": ts,
                "best_bid_ticks": bid,
                "best_ask_ticks": ask,
                "p3_fold_id": "fold_01",
                "p3_context_sha256": _sha(day),
                "p3_supported": 1,
            }
            for action in GRID_ACTIONS:
                direction, raw_gap = action.split("_", 1) if action != "current" else ("current", "0tick")
                gap = int(raw_gap.removesuffix("tick")) if action != "current" else 0
                if side == "BUY":
                    price = current + gap if direction == "closer" else current - gap
                else:
                    price = current - gap if direction == "closer" else current + gap
                row[f"{action}__price_ticks"] = price
                row[f"{action}__p_touch"] = 0.25
                row[f"{action}__activated"] = 1
                row[f"{action}__filled"] = int(day_offset < fills)
            rows.append(row)
    return pd.DataFrame(rows)


def test_joint_action_set_changes_at_most_one_side() -> None:
    names = joint_action_names()
    assert len(names) == 13
    assert names[0] == "baseline__BUY_current__SELL_current"
    assert "BUY_closer_2tick__SELL_current" in names
    assert "SELL_farther_4tick__BUY_current" in names


def test_preflight_passes_only_when_all_frozen_support_gates_pass() -> None:
    report = evaluate_preflight(
        _rows(days=6, fills=2),
        gates=PreflightGates(
            minimum_supported_days=6,
            required_oof_fold_count=1,
            minimum_days_per_oof_fold=6,
            minimum_filled_rows_per_side_role_action=2,
        ),
    )
    assert report["supported"] is True
    assert report["support"]["paired_joint_quote_buckets"] == 6
    assert report["permissions"]["direct_value_registration_eligible"] is True
    assert report["permissions"]["value_model_fit_authorized"] is False
    assert report["permissions"]["action_experiment_authorized"] is False


def test_preflight_fails_closed_before_value_fit() -> None:
    report = evaluate_preflight(_rows(days=6, fills=1))
    assert report["supported"] is False
    assert report["decision"] == "stop_before_direct_value_fit_support_insufficient"
    assert report["permissions"]["value_model_fit_authorized"] is False
    assert report["estimand_boundary"]["economic_outcomes_read"] is False


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("feature_ready_ts_ns", 1, "future feature_ready"),
        ("p3_supported", 0, "unsupported P3"),
        ("best_ask_ticks", 649_999, "integer-tick BBO"),
    ],
)
def test_validation_rejects_causal_or_support_drift(
    column: str,
    value: int,
    message: str,
) -> None:
    frame = _rows()
    if column == "feature_ready_ts_ns":
        frame.loc[0, column] = frame.loc[0, "decision_ts_ns"] + value
    else:
        frame.loc[0, column] = value
    with pytest.raises(ValueError, match=message):
        validate_side_support_rows(frame)


def test_validation_rejects_hidden_outcome_column() -> None:
    frame = _rows()
    frame["terminal_pnl_usdc"] = 0.0
    with pytest.raises(ValueError, match="schema mismatch"):
        validate_side_support_rows(frame)


def test_validation_rejects_duplicate_side_decision() -> None:
    frame = _rows()
    duplicate = frame.iloc[[0]].copy()
    duplicate["cohort_id"] = "different-cohort"
    with pytest.raises(ValueError, match="side decision keys"):
        validate_side_support_rows(pd.concat([frame, duplicate], ignore_index=True))
