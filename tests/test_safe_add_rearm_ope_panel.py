from __future__ import annotations

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.causal_path_features import empty_causal_path_features
from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_ope_panel import (
    build_randomized_panel,
    support_only_panel,
    validate_randomized_panel,
    validate_support_panel,
)
from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_support_preflight import (
    select_elapsed,
    summarize_support_panel,
)
from models.replay_policies import (
    SAFE_ADD_REARM_ACTIONS,
    apply_safe_add_rearm_action,
    normalize_safe_add_rearm_probabilities,
)


def _panel() -> pd.DataFrame:
    probabilities = {
        "r0_block": 0.80,
        "r1_rearm": 0.10,
        "r2_rearm_widen_1tick": 0.10,
    }
    rows = []
    for idx, action in enumerate(SAFE_ADD_REARM_ACTIONS):
        allow_post = int(action != "r0_block")
        delta_ticks = 0.0 if action != "r2_rearm_widen_1tick" else -1.0
        reward = float(idx - 1)
        fill_value = float(idx) if allow_post else 0.0
        campaign_cost = fill_value - reward
        rows.append(
            {
                "day": f"2026-01-{idx + 1:02d}",
                "decision_id": f"safe-{idx}",
                "decision_ts_ms": 1_000 + idx,
                "campaign_id": 1,
                "side": "BUY",
                "inventory_role": "add",
                "action": action,
                "behavior_propensity": probabilities[action],
                **{
                    f"behavior_prob_{name}": probability
                    for name, probability in probabilities.items()
                },
                "action_allow_post": allow_post,
                "action_delta_ticks": delta_ticks,
                "intervention_order_submit_count": allow_post,
                "intervention_fill_count": allow_post,
                "action_effective": allow_post,
                "action_clamp_reason": "",
                "reward": reward,
                "fill_value": fill_value,
                "campaign_cost": campaign_cost,
                "queue_cost": 0.0,
                "reward_identity_error": 0.0,
            }
        )
    return pd.DataFrame(rows)


def test_randomized_panel_has_one_intervention_and_exact_propensity() -> None:
    panel, metadata = build_randomized_panel(_panel())

    assert panel.groupby(["day", "campaign_id"]).size().max() == 1
    assert panel["decision_id"].is_unique
    assert set(panel["action"]) == set(SAFE_ADD_REARM_ACTIONS)
    assert metadata["action_bearing_evidence"] is True
    assert metadata["strategy_evidence"] is False
    assert metadata["propensity_source"] == (
        "logged_behavior_policy_at_replay_assignment"
    )


def test_panel_rejects_duplicate_campaign_intervention() -> None:
    frame = pd.concat(
        [_panel(), _panel().iloc[[0]].assign(decision_id="extra")],
        ignore_index=True,
    )
    frame.loc[len(frame) - 1, "day"] = frame.loc[0, "day"]
    with pytest.raises(ValueError, match="at most one"):
        validate_randomized_panel(frame)


def test_panel_rejects_propensity_mismatch() -> None:
    frame = _panel()
    frame.loc[0, "behavior_propensity"] = 0.50
    with pytest.raises(ValueError, match="exact probability"):
        validate_randomized_panel(frame)


def test_panel_rejects_r0_fill() -> None:
    frame = _panel()
    frame.loc[0, "intervention_fill_count"] = 1
    with pytest.raises(ValueError, match="R0"):
        validate_randomized_panel(frame)


def test_panel_rejects_partial_causal_path_mapping() -> None:
    frame = _panel()
    frame["path_feature_valid"] = 1.0
    with pytest.raises(ValueError, match="partial causal path"):
        validate_randomized_panel(frame)


def test_panel_accepts_complete_causal_path_mapping() -> None:
    frame = _panel()
    mapping = empty_causal_path_features(start_ts_ms=1_000, decision_ts_ms=1_300)
    for name, value in mapping.items():
        frame[name] = value
    validate_randomized_panel(frame)


def test_probability_vector_requires_overlap() -> None:
    with pytest.raises(ValueError, match="positive support"):
        normalize_safe_add_rearm_probabilities(
            {
                "r0_block": 1.0,
                "r1_rearm": 0.0,
                "r2_rearm_widen_1tick": 0.0,
            }
        )


def test_r2_widens_only_the_add_side_by_one_tick() -> None:
    buy = apply_safe_add_rearm_action(
        side="BUY",
        action="r2_rearm_widen_1tick",
        baseline_price=100.0,
        other_side_price=102.0,
        tick=0.1,
        max_pair_spread=5.0,
    )
    sell = apply_safe_add_rearm_action(
        side="SELL",
        action="r2_rearm_widen_1tick",
        baseline_price=102.0,
        other_side_price=100.0,
        tick=0.1,
        max_pair_spread=5.0,
    )

    assert buy.allow_post and buy.delta_ticks == pytest.approx(-1.0)
    assert sell.allow_post and sell.delta_ticks == pytest.approx(1.0)


def test_support_only_panel_strips_outcomes_and_remains_valid() -> None:
    support = support_only_panel(_panel())

    validate_support_panel(support)
    assert "reward" not in support
    assert "campaign_cost" not in support
    assert "fill_value" not in support
    assert set(support["action"]) == set(SAFE_ADD_REARM_ACTIONS)


def test_support_preflight_selects_by_worst_cell_without_outcomes() -> None:
    support = support_only_panel(_panel())
    support = pd.concat(
        [
            support.assign(
                side=side,
                decision_id=lambda x, side=side: x["decision_id"] + side,
                action_delta_ticks=lambda x, side=side: (
                    -x["action_delta_ticks"]
                    if side == "SELL"
                    else x["action_delta_ticks"]
                ),
            )
            for side in ("BUY", "SELL")
        ],
        ignore_index=True,
    )
    support["campaign_id"] = range(1, len(support) + 1)
    cells, summary = summarize_support_panel(
        support,
        elapsed_s=5.0,
        min_cell_assignments=1,
        min_cell_filled_orders=1,
    )
    summaries = pd.DataFrame(
        [
            summary,
            {
                **summary,
                "elapsed_s": 20.0,
                "min_candidate_cell_filled_orders": 2,
            },
        ]
    ).drop(columns=["behavior_probabilities"])

    selected = select_elapsed(summaries)

    assert len(cells) == 6
    assert summary["support_preflight_pass"] is True
    assert selected is not None
    assert selected["elapsed_s"] == pytest.approx(20.0)
