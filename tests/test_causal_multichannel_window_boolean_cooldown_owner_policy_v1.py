from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as modeled,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_policy_v1 as owner,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_persistent_policy_v3_oof as v3_oof,
)


def test_owner_policy_refit_is_sell_m2_small_and_buy_remains_control(monkeypatch) -> None:
    days = tuple(f"2026-01-{index:02d}" for index in range(1, 34))
    metadata = pd.DataFrame(
        {
            "opportunity_id": [f"o{index}" for index in range(66)],
            "utc_day": [day for day in days for _ in range(2)],
            "side": ["SELL"] * 66,
            "campaign_cluster_id": [f"c{index}" for index in range(66)],
        }
    ).set_index("opportunity_id")
    panel = modeled.PreparedPanel(
        metadata=metadata,
        outcomes=pd.DataFrame(index=metadata.index),
        supported=pd.DataFrame(index=metadata.index),
        features=pd.DataFrame({"p": np.ones(66, dtype=np.int8)}, index=metadata.index),
        observation_end_ts_ns=pd.Series(np.arange(66), index=metadata.index),
        unsupported_reasons=pd.DataFrame(index=metadata.index),
        redacted_finite_outcomes=0,
    )

    class Config:
        panel_days = {owner.SOURCE_SCOPE: days}

    class Policy:
        predicate_columns = ("p",)

        @staticmethod
        def choose(frame: pd.DataFrame) -> np.ndarray:
            return np.full(len(frame), "FIXED_1748S", dtype=object)

        @staticmethod
        def payload() -> dict:
            return {
                "side": "SELL",
                "default_action": "CONTROL_85N",
                "ordered_first_match_rules": [],
                "permissions": {},
            }

    audit = v3_oof.TreeFitAudit(
        side="SELL",
        feature_block="M2",
        profile="small",
        training_rows=66,
        training_days=33,
        training_campaigns=66,
        selected_feature_count=1,
        nonbaseline_leaf_count=1,
        compiled_rule_count=1,
        compiled_clause_count=1,
        compiled_literal_count=1,
        neutral_training_targets=0,
        training_action_rate=1.0,
        candidate_id="a" * 64,
    )
    captured = {}

    def fake_fit(*args, **kwargs):
        captured.update(kwargs)
        return Policy(), audit

    monkeypatch.setattr(v3_oof, "_fit_tree_policy", fake_fit)
    policy, refit = owner.fit_owner_policy(panel, config=Config())

    assert captured["side"] == "SELL"
    assert captured["feature_block"] == "M2"
    assert captured["profile"].name == "small"
    assert policy["permissions"]["research_supported"] is False
    assert refit["source_day_count"] == 33
    assert refit["refit_nonbaseline_action_rate"] == 1.0
