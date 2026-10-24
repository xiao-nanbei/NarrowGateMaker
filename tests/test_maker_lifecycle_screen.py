from __future__ import annotations

from pathlib import Path

import numpy as np

from research.families.f10_live_replay_attribution.audit.maker_lifecycle_screen import (
    TARGETS,
    _artifact_path,
    _daily_improvement,
    make_development_folds,
)


def test_development_folds_have_no_claimed_late_holdout() -> None:
    days = [f"2026-01-{value:02d}" for value in range(1, 29)]
    folds = make_development_folds(
        days,
        min_train_days=10,
        test_days=5,
        embargo_days=1,
        blocked_folds=4,
    )
    assert folds
    assert {fold.panel for fold in folds} == {"chronological", "blocked_day_crossfit"}
    assert all(fold.panel != "late_holdout" for fold in folds)


def test_screen_targets_do_not_pool_side_or_claim_action_value() -> None:
    assert ("target_decision_to_terminal_mtm", "regression", "primary") in TARGETS
    assert ("target_tail", "classification", "primary") in TARGETS


def test_daily_improvement_is_positive_when_m1_prediction_is_better() -> None:
    y = np.array([0.0, 1.0, 0.0, 1.0])
    m0 = np.array([0.5, 0.5, 0.5, 0.5])
    m1 = np.array([0.1, 0.9, 0.1, 0.9])
    days = np.array(["a", "a", "b", "b"])
    rows = _daily_improvement(y, m0, m1, days, "classification")
    assert all(row["loss_improvement_m1_vs_m0"] > 0.0 for row in rows)


def test_artifact_path_preserves_dotted_prefix() -> None:
    assert str(_artifact_path(Path("run.m0_m1"), ".json")) == "run.m0_m1.json"
