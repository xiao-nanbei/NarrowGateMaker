from __future__ import annotations

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    conditional_p3_joint_quote_sparse_value_diagnostic as diagnostic,
)


def test_joint_action_definition_preserves_side_specific_distance() -> None:
    assert diagnostic._action_definition(diagnostic.BASELINE_ACTION) == (
        None,
        "current",
        0,
        0,
    )
    assert diagnostic._action_definition("BUY_closer_4tick__SELL_current") == (
        "BUY",
        "closer_4tick",
        -4,
        0,
    )
    assert diagnostic._action_definition("SELL_farther_2tick__BUY_current") == (
        "SELL",
        "farther_2tick",
        0,
        2,
    )


def test_owner_denominator_produces_three_chronological_folds() -> None:
    days = tuple(
        (pd.Timestamp("2026-01-01") + pd.Timedelta(days=index)).strftime("%Y-%m-%d")
        for index in range(28)
    )
    folds = diagnostic._folds(
        days,
        {
            "evaluation": {
                "min_train_days": 12,
                "embargo_days": 1,
                "test_days": 5,
            }
        },
    )
    assert len(folds) == 3
    assert all(max(fold["train_days"]) < min(fold["test_days"]) for fold in folds)
    assert sum(len(fold["test_days"]) for fold in folds) == 13


def test_paired_support_rejects_noncanonical_denominator() -> None:
    frame = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "decision_ts_ns": [1, 1],
            "side": ["BUY", "SELL"],
        }
    )
    try:
        diagnostic._paired_support(frame)
    except ValueError as exc:
        assert "denominator changed" in str(exc)
    else:
        raise AssertionError("noncanonical denominator was accepted")
