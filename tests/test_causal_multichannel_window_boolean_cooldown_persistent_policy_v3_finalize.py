from __future__ import annotations

import math

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as modeled,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_persistent_policy_v3_finalize as finalize,
)


def _panel() -> modeled.PreparedPanel:
    opportunities = [f"o{index}" for index in range(1, 9)]
    metadata = pd.DataFrame(
        {
            "opportunity_id": opportunities,
            "utc_day": [f"2026-01-{index:02d}" for index in range(1, 9)],
            "side": ["BUY"] * 4 + ["SELL"] * 4,
            "role_at_fill": ["opener", "add"] * 4,
            "campaign_cluster_id": [f"c{index}" for index in range(1, 9)],
        }
    ).set_index("opportunity_id")
    outcomes = pd.DataFrame(
        {
            "CONTROL_85N": np.zeros(8),
            "D1": np.ones(8),
            "D2": np.full(8, 2.0),
        },
        index=metadata.index,
    )
    supported = pd.DataFrame(True, index=metadata.index, columns=outcomes.columns)
    return modeled.PreparedPanel(
        metadata=metadata,
        outcomes=outcomes,
        supported=supported,
        features=pd.DataFrame(index=metadata.index),
        observation_end_ts_ns=pd.Series(np.arange(8), index=metadata.index),
        unsupported_reasons=pd.DataFrame(index=metadata.index),
        redacted_finite_outcomes=0,
    )


def _outer_oof() -> pd.DataFrame:
    rows = []
    for scope, blocks in (
        ("prefix40_modeled_label_development", ("R0", "M0", "M1")),
        ("prefix33_raw_m2_common_support", ("R0", "M0", "M1", "M2")),
    ):
        for side, opportunities in (("BUY", range(1, 5)), ("SELL", range(5, 9))):
            for block in blocks:
                action = "D1" if block in {"R0", "M0"} else "D2"
                for index in opportunities:
                    rows.append(
                        {
                            "opportunity_id": f"o{index}",
                            "utc_day": f"2026-01-{index:02d}",
                            "side": side,
                            "role_at_fill": "opener" if index % 2 else "add",
                            "campaign_cluster_id": f"c{index}",
                            "selected_action": action,
                            "control_action": "CONTROL_85N",
                            "selected_nonbaseline": True,
                            "point_identified": True,
                            "fold_id": "outer1",
                            "method": finalize.METHOD,
                            "feature_block": block,
                            "panel_scope": scope,
                        }
                    )
    return pd.DataFrame(rows)


def test_run_inference_builds_paired_hierarchy_and_blocks_no_permissions(monkeypatch) -> None:
    monkeypatch.setattr(finalize, "BOOTSTRAP_DRAWS", 499)
    contrasts, report = finalize.run_inference(
        outer_oof=_outer_oof(),
        panel=_panel(),
    )

    assert contrasts["hypothesis"].nunique() == 20
    assert report["hypotheses"]["prefix40:BUY:M0-CONTROL"][
        "simultaneous_band"
    ]["mean_usdc"] == 1.0
    assert report["hypotheses"]["prefix40:BUY:M1-M0"][
        "simultaneous_band"
    ]["mean_usdc"] == 1.0
    assert report["hierarchies"]["prefix40"]["supported_sides"] == (
        "BUY",
        "SELL",
    )
    assert report["permissions"]["unified_policy_frozen"] is False
    assert report["permissions"]["live_authorized"] is False
    assert math.isfinite(report["inference"]["critical_value"])


def test_adjacent_jaccard_compares_only_neighboring_folds() -> None:
    assert finalize._adjacent_jaccard([{"a", "b"}, {"b", "c"}, {"c"}]) == [
        1.0 / 3.0,
        0.5,
    ]


def test_continuous_comparator_is_paired_to_each_boolean_cell(monkeypatch) -> None:
    monkeypatch.setattr(finalize, "BOOTSTRAP_DRAWS", 499)
    continuous = _outer_oof().copy()
    continuous["method"] = finalize.CONTINUOUS_METHOD
    continuous["selected_action"] = "D2"
    contrasts, report = finalize.run_inference(
        outer_oof=_outer_oof(),
        panel=_panel(),
        continuous_oof=continuous,
    )

    assert contrasts["hypothesis"].nunique() == 34
    assert len(report["continuous_minus_boolean_hypotheses"]) == 14
    hypothesis = "prefix40:BUY:M0:CONTINUOUS-BOOLEAN"
    assert report["hypotheses"][hypothesis]["simultaneous_band"]["mean_usdc"] == 1.0
