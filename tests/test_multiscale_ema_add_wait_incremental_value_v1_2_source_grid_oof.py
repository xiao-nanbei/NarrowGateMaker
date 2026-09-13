from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_2_source_grid_oof_study as study,
)


def _frozen_folds() -> list[dict[str, object]]:
    path = Path(
        "research/families/f05_fill_quality_quote_ev/docs/"
        "multiscale_ema_add_wait_incremental_value_v1_1_spec_20260809.json"
    )
    return json.loads(path.read_text())["chronological_oof"]["folds"]


def _panel() -> pd.DataFrame:
    folds = _frozen_folds()
    test_days = [day for fold in folds for day in fold["test_days"]]
    history_days = [
        "2026-04-17",
        "2026-04-18",
        "2026-04-19",
        "2026-04-20",
        "2026-04-22",
        "2026-04-23",
        "2026-05-01",
        "2026-05-02",
        "2026-05-03",
        "2026-05-04",
        "2026-05-05",
        "2026-05-06",
        "2026-05-13",
        "2026-05-29",
        "2026-05-30",
        "2026-05-31",
    ]
    rows = []
    for day in history_days + test_days:
        for side in ("BUY", "SELL"):
            for ordinal in range(4):
                rows.append(
                    {
                        "utc_day": day,
                        "side": side,
                        "opportunity_id": f"{day}:{side}:{ordinal}",
                    }
                )
    return pd.DataFrame(rows)


def test_expected_oof_membership_excludes_training_history() -> None:
    expected_ids, day_to_fold, test_days = study._expected_oof_membership(
        _panel(), _frozen_folds()
    )

    assert len(test_days) == 24
    assert len(day_to_fold) == 24
    assert len(expected_ids) == 192
    assert not any(identifier.startswith("2026-04-17") for identifier in expected_ids)


def test_expected_oof_membership_rejects_overlapping_test_days() -> None:
    folds = _frozen_folds()
    folds[1]["test_days"] = list(folds[1]["test_days"]) + [
        folds[0]["test_days"][0]
    ]

    with pytest.raises(study.SourceGridOofStudyError, match="multiple folds"):
        study._expected_oof_membership(_panel(), folds)
