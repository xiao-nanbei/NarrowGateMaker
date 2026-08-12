from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeRegressor

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_oof as modeled,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_persistent_policy_v3_oof as v3,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    ChronologicalFold,
    duration_vocabulary,
)


def test_tree_compilation_uses_observed_state_guard() -> None:
    matrix = np.asarray([[-1], [0], [0], [1], [1], [1]], dtype=np.float32)
    targets = np.column_stack(
        [
            np.asarray([0.0, -1.0, -1.0, 2.0, 2.0, 2.0]),
            np.zeros(6),
            np.zeros(6),
            np.zeros(6),
            np.zeros(6),
            np.zeros(6),
            np.zeros(6),
        ]
    )
    model = DecisionTreeRegressor(max_depth=2, min_samples_leaf=1, random_state=7)
    model.fit(matrix, targets)

    policy, _ = v3._compile_tree_policy(
        model,
        side="BUY",
        feature_names=("signal",),
        action_scales=np.ones(7),
    )
    chosen = policy.choose(pd.DataFrame({"signal": [-1, 0, 1]}))

    assert chosen[0] == "CONTROL_85N"
    assert chosen[2] != "CONTROL_85N"


def _synthetic_panel() -> modeled.PreparedPanel:
    actions = duration_vocabulary("BUY")
    rows = []
    feature_rows = []
    outcomes = []
    support = []
    reasons = []
    ends = []
    for day_index in range(10):
        day = f"2026-01-{day_index + 1:02d}"
        for opportunity_index in range(4):
            opportunity = f"{day}-{opportunity_index}"
            signal = int((day_index + opportunity_index) % 2 == 0)
            rows.append(
                {
                    "opportunity_id": opportunity,
                    "utc_day": day,
                    "panel_role": "prefix40_modeled_label_development",
                    "side": "BUY",
                    "role_at_fill": "opener" if opportunity_index % 2 == 0 else "add",
                    "campaign_cluster_id": f"{day}::BUY::{opportunity_index // 2}",
                    "source_campaign_id": str(opportunity_index // 2),
                    "assignment_ts_ns": day_index * 10_000 + opportunity_index,
                }
            )
            feature_rows.append(
                {
                    "opportunity_id": opportunity,
                    "m0_signal": signal,
                    "m1_signal": signal,
                    "m2_signal": signal,
                    "r0_signal": signal,
                }
            )
            values = {action: 0.0 for action in actions}
            values[actions[1]] = 1.0 if signal else -0.5
            outcomes.append(values)
            support.append({action: True for action in actions})
            reasons.append({action: "supported" for action in actions})
            ends.append(day_index * 10_000 + opportunity_index + 1)
    metadata = pd.DataFrame(rows).set_index("opportunity_id")
    features = pd.DataFrame(feature_rows).set_index("opportunity_id")
    return modeled.PreparedPanel(
        metadata=metadata,
        outcomes=pd.DataFrame(outcomes, index=metadata.index),
        supported=pd.DataFrame(support, index=metadata.index),
        features=features,
        observation_end_ts_ns=pd.Series(ends, index=metadata.index),
        unsupported_reasons=pd.DataFrame(reasons, index=metadata.index),
        redacted_finite_outcomes=0,
    )


def test_cell_executes_nonbaseline_policy_on_untouched_outer_days() -> None:
    panel = _synthetic_panel()
    block = modeled.FeatureBlockSpec
    feature_blocks = {
        "M0": block(("m0_signal",), ("m0_signal",)),
        "M1": block(("m0_signal", "m1_signal"), ("m0_signal", "m1_signal")),
        "M2": block(
            ("m0_signal", "m1_signal", "m2_signal"),
            ("m0_signal", "m1_signal", "m2_signal"),
        ),
        "R0": block(("r0_signal",), ("r0_signal",)),
    }
    outer = ChronologicalFold(
        fold_id="outer0",
        train_days=tuple(f"2026-01-{value:02d}" for value in range(1, 9)),
        test_days=("2026-01-09", "2026-01-10"),
    )
    config = SimpleNamespace(
        feature_blocks=feature_blocks,
        outer_folds={"prefix40_modeled_label_development": (outer,)},
        search=SimpleNamespace(inner_folds=2, inner_minimum_train_days=2),
    )
    result = v3._run_cell(
        panel,
        config=config,
        panel_scope="prefix40_modeled_label_development",
        side="BUY",
        feature_block="M0",
        profiles=(v3.ComplexityProfile("test", 4, 2, 4, 1),),
        random_seed=11,
    )

    assert set(result.oof_rows["utc_day"]) == {"2026-01-09", "2026-01-10"}
    assert result.oof_rows["selected_nonbaseline"].any()
    assert not result.fold_reports[0]["outer_outcomes_used_for_fit"]
    assert result.fold_reports[0]["outer_fit_audit"]["compiled_rule_count"] >= 1
