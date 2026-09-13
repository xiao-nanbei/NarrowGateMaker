from __future__ import annotations

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_state_policy import (
    CONTROL_ACTION,
    STATE_FEATURES,
    _candidate_actions_from_reward_rows,
    _paired_contrast,
    _policy_family,
)
from research.families.f09_campaign_action_uplift.causal_path_features import CAUSAL_PATH_POLICY_FEATURES


def test_candidate_mapping_defaults_unscored_rows_to_r0() -> None:
    panel = pd.DataFrame(
        {
            "decision_id": ["a", "b", "c"],
            "action": [CONTROL_ACTION, "r1_rearm", "r2_rearm_widen_1tick"],
        }
    )
    reward_rows = pd.DataFrame(
        {
            "decision_id": ["b", "c"],
            "ope_candidate_action": ["r1_rearm", CONTROL_ACTION],
        }
    )

    output = _candidate_actions_from_reward_rows(panel, reward_rows)

    assert output.set_index("decision_id")["candidate_action"].to_dict() == {
        "a": CONTROL_ACTION,
        "b": "r1_rearm",
        "c": CONTROL_ACTION,
    }


def test_candidate_mapping_rejects_duplicate_decisions() -> None:
    panel = pd.DataFrame({"decision_id": ["a"], "action": [CONTROL_ACTION]})
    reward_rows = pd.DataFrame(
        {
            "decision_id": ["a", "a"],
            "ope_candidate_action": [CONTROL_ACTION, "r1_rearm"],
        }
    )

    with pytest.raises(ValueError, match="invalid candidate action mapping"):
        _candidate_actions_from_reward_rows(panel, reward_rows)


def test_paired_contrast_is_candidate_minus_r0() -> None:
    candidate = pd.DataFrame(
        {
            "decision_id": ["a", "b"],
            "day": ["2026-01-01", "2026-01-02"],
            "side": ["BUY", "BUY"],
            "ope_fold": [0, 0],
            "ope_candidate_action": ["r1_rearm", CONTROL_ACTION],
            "ope_dr_value": [2.0, 1.0],
            "ope_prediction_valid": [1, 1],
            "ope_clipped_importance_weight": [3.0, 3.0],
            "ope_unsupported_candidate_mass": [0.0, 0.0],
        }
    )
    control = pd.DataFrame(
        {
            "decision_id": ["a", "b"],
            "ope_dr_value": [1.0, 1.5],
            "ope_prediction_valid": [1, 1],
            "ope_clipped_importance_weight": [3.0, 3.0],
            "ope_unsupported_candidate_mass": [0.0, 0.0],
        }
    )

    row, paired = _paired_contrast(
        candidate,
        control,
        scope="buy",
        target="reward",
        trials=0,
        seed=1,
    )

    assert paired["dr_contrast"].tolist() == pytest.approx([1.0, -0.5])
    assert row["dr_contrast"] == pytest.approx(0.25)


def test_causal_path_family_extends_snapshot_without_external_features() -> None:
    family = _policy_family("causal_path_v2")

    assert tuple(family["features"][: len(STATE_FEATURES)]) == STATE_FEATURES
    assert tuple(family["features"][len(STATE_FEATURES) :]) == (
        CAUSAL_PATH_POLICY_FEATURES
    )
    assert not any("external" in name for name in family["features"])
