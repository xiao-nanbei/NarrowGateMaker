from __future__ import annotations

import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference as inference,
)


def _panel_and_oof() -> tuple[SimpleNamespace, pd.DataFrame]:
    opportunities = ["o1", "o2", "o3", "o4", "o5", "o6"]
    metadata = pd.DataFrame(
        {
            "opportunity_id": opportunities,
            "utc_day": ["2026-01-01"] * 4 + ["2026-01-02"] * 2,
            "side": ["BUY"] * 6,
            "role_at_fill": ["opener", "add", "add", "opener", "add", "add"],
            "campaign_cluster_id": ["c1", "c1", "c1", "c2", "c3", "c4"],
        }
    ).set_index("opportunity_id")
    outcomes = pd.DataFrame(
        {
            "CONTROL_85N": [0.0, 2.0, math.nan, 4.0, 0.0, 0.0],
            "D1": [1.0, 3.0, math.nan, 1.0, 2.0, -1.0],
            "D2": [2.0, 1.0, 5.0, 3.0, 1.0, 1.0],
        },
        index=metadata.index,
    )
    supported = pd.DataFrame(
        {
            "CONTROL_85N": [True, True, False, True, True, True],
            "D1": [True, True, False, True, True, False],
            "D2": [True] * 6,
        },
        index=metadata.index,
    )
    panel = SimpleNamespace(
        metadata=metadata,
        outcomes=outcomes,
        supported=supported,
    )
    policy_actions = {
        "M0": ["CONTROL_85N", "D1", "CONTROL_85N", "D1", "D1", "D1"],
        "M1": ["CONTROL_85N", "D1", "CONTROL_85N", "D2", "D2", "D1"],
        "M2": ["D2", "D1", "CONTROL_85N", "D2", "D2", "D2"],
    }
    rows = []
    for block, actions in policy_actions.items():
        for opportunity, action in zip(opportunities, actions, strict=True):
            row = metadata.loc[opportunity]
            rows.append(
                {
                    "opportunity_id": opportunity,
                    "panel_scope": "prefix40",
                    "side": "BUY",
                    "feature_block": block,
                    "method": "boolean",
                    "fold_id": "outer1" if row["utc_day"] == "2026-01-01" else "outer2",
                    "utc_day": row["utc_day"],
                    "campaign_cluster_id": row["campaign_cluster_id"],
                    "role_at_fill": row["role_at_fill"],
                    "selected_action": action,
                    "control_action": "CONTROL_85N",
                }
            )
    return panel, pd.DataFrame(rows)


def _ref(block: str) -> inference.PolicyRef:
    return inference.PolicyRef("prefix40", "BUY", block, "boolean")


def test_paired_contrast_structural_zero_and_dual_arm_support() -> None:
    panel, outer_oof = _panel_and_oof()
    rows = inference.build_paired_policy_contrast(
        outer_oof,
        panel,
        lhs=_ref("M0"),
        rhs=_ref("CONTROL"),
    ).set_index("opportunity_id")

    assert rows.loc["o1", "contrast_usdc"] == 0.0
    assert rows.loc["o3", "contrast_usdc"] == 0.0
    assert bool(rows.loc["o3", "point_identified"])
    assert rows.loc["o3", "identification_reason"] == "same_action_structural_zero"
    assert rows.loc["o2", "contrast_usdc"] == 1.0
    assert bool(rows.loc["o2", "point_identified"])
    assert not bool(rows.loc["o6", "point_identified"])
    assert math.isnan(rows.loc["o6", "contrast_usdc"])
    assert rows.loc["o6", "identification_reason"] == "different_action_unsupported"


def test_policy_vs_policy_uses_same_outer_oof_and_bound_panel() -> None:
    panel, outer_oof = _panel_and_oof()
    rows = inference.build_paired_policy_contrast(
        outer_oof,
        panel,
        lhs=_ref("M1"),
        rhs=_ref("M0"),
    ).set_index("opportunity_id")

    assert rows.loc["o1", "contrast_usdc"] == 0.0
    assert rows.loc["o4", "contrast_usdc"] == 2.0
    assert rows.loc["o5", "contrast_usdc"] == -1.0
    assert rows.loc["o6", "contrast_usdc"] == 0.0

    incomplete = outer_oof.loc[
        ~(
            (outer_oof["feature_block"] == "M1")
            & (outer_oof["opportunity_id"] == "o6")
        )
    ]
    with pytest.raises(inference.PersistentPolicyV3InferenceError, match="same outer-OOF"):
        inference.build_paired_policy_contrast(
            incomplete,
            panel,
            lhs=_ref("M1"),
            rhs=_ref("M0"),
        )


def test_hierarchical_weights_equalize_opportunities_campaigns_and_days() -> None:
    panel, outer_oof = _panel_and_oof()
    rows = inference.build_paired_policy_contrast(
        outer_oof,
        panel,
        lhs=_ref("M0"),
        rhs=_ref("CONTROL"),
    )

    campaign_day = rows.groupby(["utc_day", "campaign_cluster_id"])[
        "campaign_day_opportunity_weight"
    ].sum()
    day = rows.groupby("utc_day")["day_population_weight"].sum()
    assert np.allclose(campaign_day.to_numpy(), 1.0)
    assert np.allclose(day.to_numpy(), 1.0)
    assert rows["equal_day_population_weight"].sum() == pytest.approx(1.0)
    assert rows["campaign_population_weight"].sum() == pytest.approx(1.0)

    contributions = inference.equal_day_contributions(rows).set_index("utc_day")
    assert contributions.loc["2026-01-01", "identified_weight_fraction"] == pytest.approx(1.0)
    assert contributions.loc["2026-01-01", "identified_mean_usdc"] == pytest.approx(-4.0 / 3.0)
    assert contributions.loc["2026-01-02", "identified_weight_fraction"] == pytest.approx(0.5)
    assert contributions.loc["2026-01-02", "identified_mean_usdc"] == pytest.approx(2.0)


def test_campaign_weighted_sensitivity_does_not_equalize_days() -> None:
    panel, outer_oof = _panel_and_oof()
    rows = inference.build_paired_policy_contrast(
        outer_oof,
        panel,
        lhs=_ref("M1"),
        rhs=_ref("M0"),
    )
    estimate = inference.campaign_weighted_sensitivity(rows)

    # Four campaigns receive equal total weight; day one has two and day two has two.
    assert estimate.total_units == 4
    assert estimate.point_identified
    assert estimate.mean_usdc == pytest.approx(0.25)


def test_webb_wild_day_max_t_is_shared_and_deterministic() -> None:
    days = pd.Index([f"2026-01-{day:02d}" for day in range(1, 9)])
    first = pd.Series([1.0, 1.2, 0.8, 1.1, 0.9, 1.3, 0.7, 1.0], index=days)
    second = first.copy()
    family_a = inference.webb_wild_day_max_t(
        {"h1": first, "h2": second}, draws=999, seed=71
    )
    family_b = inference.webb_wild_day_max_t(
        {"h1": first, "h2": second}, draws=999, seed=71
    )

    assert family_a.critical_value == family_b.critical_value
    assert family_a["h1"].lcb_usdc == family_a["h2"].lcb_usdc
    assert family_a["h1"].lcb_usdc > 0.0
    assert len(family_a.multiplier_support) == 6
    assert family_a.shared_days == tuple(days)


def test_webb_max_t_handles_structural_zero_hypothesis() -> None:
    days = [f"2026-01-{day:02d}" for day in range(1, 7)]
    zeros = pd.Series(0.0, index=days)
    signal = pd.Series([0.1, 0.2, 0.0, 0.3, 0.1, 0.2], index=days)
    family = inference.webb_wild_day_max_t(
        {"zero": zeros, "signal": signal}, draws=499, seed=3
    )

    assert family["zero"].standard_error_usdc == 0.0
    assert family["zero"].lcb_usdc == 0.0
    assert family["zero"].ucb_usdc == 0.0


def _band(name: str, lcb: float) -> inference.SimultaneousBand:
    return inference.SimultaneousBand(
        hypothesis=name,
        mean_usdc=lcb + 1.0,
        standard_error_usdc=0.1,
        lcb_usdc=lcb,
        ucb_usdc=lcb + 2.0,
        day_count=20,
    )


def test_feature_hierarchy_stops_after_failed_parent() -> None:
    hypotheses = {
        "BUY:M0-CONTROL": _band("BUY:M0-CONTROL", -0.1),
        "BUY:M1-M0": _band("BUY:M1-M0", 0.5),
        "BUY:M2-M1": _band("BUY:M2-M1", 0.5),
    }
    decision = inference.apply_feature_hierarchy(
        hypotheses,
        {"BUY": ("BUY:M0-CONTROL", "BUY:M1-M0", "BUY:M2-M1")},
    )

    assert decision.supported_sides == ()
    assert decision.steps["BUY"][0].tested
    assert not decision.steps["BUY"][0].passed
    assert not decision.steps["BUY"][1].tested
    assert decision.steps["BUY"][1].reason == "parent_feature_block_not_supported"


def test_feature_hierarchy_requires_each_incremental_gate() -> None:
    names = ("BUY:M0-CONTROL", "BUY:M1-M0", "BUY:M2-M1")
    hypotheses = {name: _band(name, 0.2) for name in names}
    decision = inference.apply_feature_hierarchy(
        hypotheses,
        {"BUY": names},
        economic_epsilon_usdc=0.1,
    )

    assert decision.supported_sides == ("BUY",)
    assert all(step.passed for step in decision.steps["BUY"])


def test_missing_bound_reports_tipping_value_and_blocks_promotion() -> None:
    panel, outer_oof = _panel_and_oof()
    rows = inference.build_paired_policy_contrast(
        outer_oof,
        panel,
        lhs=_ref("M0"),
        rhs=_ref("CONTROL"),
    )
    sensitivity = inference.censoring_tipping_bound(rows)

    assert not sensitivity.point_identified
    assert not sensitivity.bounds_available
    assert sensitivity.tipping_unidentified_mean_usdc is not None
    assert not sensitivity.promotion_allowed_by_identification
    assert sensitivity.promotion_block_reason == "unidentified_weight_without_legal_bounds"

    hypothesis = "BUY:M0-CONTROL"
    hierarchy = inference.apply_feature_hierarchy(
        {hypothesis: _band(hypothesis, 1.0)},
        {"BUY": (hypothesis,)},
        censoring={hypothesis: sensitivity},
    )
    assert hierarchy.supported_sides == ()
    assert hierarchy.steps["BUY"][0].reason == "unidentified_weight_without_legal_bounds"


def test_legal_bounds_can_identify_population_lower_bound() -> None:
    panel, outer_oof = _panel_and_oof()
    rows = inference.build_paired_policy_contrast(
        outer_oof,
        panel,
        lhs=_ref("M0"),
        rhs=_ref("CONTROL"),
    )
    sensitivity = inference.censoring_tipping_bound(
        rows,
        uplift_bounds_usdc=(2.0, 3.0),
    )

    assert sensitivity.bounds_available
    assert sensitivity.population_lower_bound_usdc is not None
    assert sensitivity.population_lower_bound_usdc > 0.0
    assert sensitivity.promotion_allowed_by_identification


def test_distribution_utilities() -> None:
    assert inference.weighted_smd([0.0, 1.0], [1.0, 2.0]) == pytest.approx(2.0)
    assert inference.weighted_smd([1.0, 1.0], [1.0, 1.0]) == 0.0
    assert inference.weighted_psi([0.0, 1.0, 2.0, 3.0], [0.0, 1.0, 2.0, 3.0]) == pytest.approx(0.0)
    assert inference.weighted_psi([0.0, 0.0, 0.0, 1.0], [0.0, 1.0, 1.0, 1.0]) > 0.0

    prevalence = inference.tri_state_prevalence(
        [-1.0, 0.0, 1.0, math.nan],
        [1.0, 1.0, 2.0, 2.0],
    )
    assert prevalence.true_rate == pytest.approx(2.0 / 6.0)
    assert prevalence.false_rate == pytest.approx(1.0 / 6.0)
    assert prevalence.unobserved_rate == pytest.approx(3.0 / 6.0)
    assert prevalence.observed_rate == pytest.approx(3.0 / 6.0)


def test_tri_state_rejects_invalid_values() -> None:
    with pytest.raises(inference.PersistentPolicyV3InferenceError, match="-1, 0, 1"):
        inference.tri_state_prevalence([0, 1, 2])
