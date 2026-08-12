from __future__ import annotations

import pandas as pd

from research.families.f04_external_market_alpha.audit.fast3_direction_policy_audit import (
    _candidate_actions,
)


def test_candidate_actions_are_side_aware_and_bounded() -> None:
    frame = pd.DataFrame(
        {
            "fast3_adverse_probability_m1": [0.60, 0.40, 0.60],
            "fast3_external_adverse_delta": [0.02, -0.02, -0.01],
        }
    )
    assert _candidate_actions(frame, "adverse_widen").tolist() == [
        "widen_1tick",
        "baseline",
        "baseline",
    ]
    assert _candidate_actions(frame, "favorable_keep").tolist() == [
        "baseline",
        "prevent_over_widen",
        "baseline",
    ]
