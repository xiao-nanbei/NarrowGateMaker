from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.state_conditioned_policy_artifact import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
    FEATURES,
    _fit_action_models,
    _paired_contrast,
    _predict_actions,
)


def _panel() -> pd.DataFrame:
    rows = []
    for idx in range(240):
        action = CONTROL_ACTION if idx % 2 == 0 else CANDIDATE_ACTION
        signal = -1.0 if idx % 4 < 2 else 1.0
        row = {name: 0.0 for name in FEATURES}
        row.update(
            {
                "action": action,
                "microprice_shift_bps": signal,
                "reward": signal if action == CANDIDATE_ACTION else 0.0,
                f"behavior_prob_{CONTROL_ACTION}": 0.5,
                f"behavior_prob_{CANDIDATE_ACTION}": 0.5,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def test_fitted_artifact_model_selects_state_specific_widen() -> None:
    panel = _panel()
    fitted = _fit_action_models(panel, alpha=1.0, min_action_rows=100)
    selected = _predict_actions(panel, fitted)

    positive = panel["microprice_shift_bps"].to_numpy() > 0.0
    assert selected[positive].eq(CANDIDATE_ACTION).all()
    assert selected[~positive].eq(CONTROL_ACTION).all()
    assert np.isfinite(fitted["mean"]).all()
    assert np.isfinite(fitted["scale"]).all()


def test_paired_contrast_uses_baseline_policy_not_behavior_value() -> None:
    candidate = pd.DataFrame(
        {
            "decision_id": ["a", "b"],
            "day": ["2026-01-01", "2026-01-02"],
            "ope_dr_value": [1.0, 2.0],
        }
    )
    baseline = pd.DataFrame({"decision_id": ["a", "b"], "ope_dr_value": [0.25, 1.25]})
    ope_summary = {
        "numerical_ope_gate_passed": True,
        "overlap": {"effective_sample_size": 100.0},
    }

    contrast = _paired_contrast(
        candidate,
        baseline,
        candidate_summary=ope_summary,
        baseline_summary=ope_summary,
        trials=100,
        seed=7,
    )

    assert contrast["estimators"]["candidate_minus_baseline_dr_uplift"] == 0.75
    assert contrast["daily_uplift"]["positive_days"] == 2
