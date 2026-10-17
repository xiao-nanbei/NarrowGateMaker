from __future__ import annotations

import pandas as pd
import pytest

from scripts.freeze_p3_reach_time_hazard_spec import chronological_oof_folds


def _days(count: int) -> list[str]:
    start = pd.Timestamp("2025-01-01")
    return [
        (start + pd.Timedelta(days=index)).date().isoformat()
        for index in range(count)
    ]


def test_chronological_oof_folds_are_expanding_and_disjoint_at_score_time() -> None:
    days = _days(156)
    folds = chronological_oof_folds(days)
    assert len(folds) == 4
    assert [fold["train_count"] for fold in folds] == [60, 81, 102, 123]
    assert all(fold["calibration_count"] == 12 for fold in folds)
    assert all(fold["test_count"] == 21 for fold in folds)
    for fold in folds:
        assert max(fold["train_days"]) < min(fold["calibration_days"])
        assert max(fold["calibration_days"]) < min(fold["test_days"])
        assert not set(fold["train_days"]).intersection(fold["test_days"])


def test_formal_panel_size_and_order_fail_closed() -> None:
    with pytest.raises(ValueError, match="156"):
        chronological_oof_folds(_days(155))
    days = _days(156)
    days[10], days[11] = days[11], days[10]
    with pytest.raises(ValueError, match="156"):
        chronological_oof_folds(days)
