from __future__ import annotations

import hashlib
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    joint_quote_direct_value_surface as surface,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _spec(*, epsilon: float = 0.05) -> surface.JointQuoteDirectValueSpec:
    days = tuple(
        (pd.Timestamp("2026-01-01") + pd.Timedelta(days=index)).strftime("%Y-%m-%d")
        for index in range(14)
    )
    return surface.JointQuoteDirectValueSpec(
        identity="joint_quote_direct_value_surface_test_v1",
        development_identity="synthetic_development_only_v1",
        dataset_identity_sha256=_sha("dataset"),
        feature_dag_id="synthetic_feature_dag_v1",
        feature_dag_sha256=_sha("feature-dag"),
        development_days=days,
        baseline_action="baseline",
        actions=(
            surface.JointQuoteActionSpec(
                name="baseline",
                behavior_propensity=1.0 / 3.0,
                candidate_features=(
                    ("bid_offset_ticks", 0.0),
                    ("ask_offset_ticks", 0.0),
                ),
            ),
            surface.JointQuoteActionSpec(
                name="good_joint_quote",
                behavior_propensity=1.0 / 3.0,
                candidate_features=(
                    ("bid_offset_ticks", -2.0),
                    ("ask_offset_ticks", 2.0),
                ),
            ),
            surface.JointQuoteActionSpec(
                name="bad_joint_quote",
                behavior_propensity=1.0 / 3.0,
                candidate_features=(
                    ("bid_offset_ticks", 2.0),
                    ("ask_offset_ticks", -2.0),
                ),
            ),
        ),
        context_features=("market_state", "p3_touch_probability"),
        p3_features=("p3_touch_probability",),
        ridge_alphas=(0.1, 1.0),
        min_outer_train_days=8,
        outer_embargo_days=1,
        outer_test_days=2,
        min_inner_train_days=3,
        inner_embargo_days=1,
        inner_test_days=2,
        economic_epsilon_usdc=epsilon,
        confidence=0.95,
        bootstrap_samples=200,
        random_seed=20260803,
    )


def _panel(
    spec: surface.JointQuoteDirectValueSpec,
    *,
    effects: dict[str, float] | None = None,
) -> pd.DataFrame:
    action_effects = effects or {
        "baseline": 0.0,
        "good_joint_quote": 0.60,
        "bad_joint_quote": -0.25,
    }
    rows: list[dict[str, object]] = []
    for day_index, day in enumerate(spec.development_days):
        day_start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000_000_000)
        day_effect = 0.005 * np.sin(day_index)
        for action_index, action in enumerate(spec.actions):
            for row_index in range(12):
                market_state = (row_index - 5.5) / 5.5
                p3_touch = 0.1 + 0.05 * (row_index % 5)
                assignment_ts = day_start + (60 + action_index * 20 + row_index) * 1_000_000_000
                outcome = (
                    action_effects[action.name] + 0.15 * market_state + 0.10 * p3_touch + day_effect
                )
                rows.append(
                    {
                        "panel_schema_version": surface.PANEL_SCHEMA_VERSION,
                        "surface_spec_sha256": spec.canonical_sha256,
                        "development_identity": spec.development_identity,
                        "dataset_identity_sha256": spec.dataset_identity_sha256,
                        "feature_dag_id": spec.feature_dag_id,
                        "feature_dag_sha256": spec.feature_dag_sha256,
                        "panel_role": surface.PANEL_ROLE,
                        "target_semantics": surface.TARGET_SEMANTICS,
                        "target_unit": surface.TARGET_UNIT,
                        "randomization_unit": surface.RANDOMIZATION_UNIT,
                        "propensity_semantics": surface.PROPENSITY_SEMANTICS,
                        "day": day,
                        "assignment_episode_id": (f"{day}-{action.name}-{row_index}"),
                        "assignment_ts_ns": assignment_ts,
                        "feature_ready_ts_ns": assignment_ts - 1_000_000,
                        "washout_ts_ns": assignment_ts + 10_000_000_000,
                        "action": action.name,
                        "behavior_propensity": action.behavior_propensity,
                        surface.TARGET_COLUMN: outcome,
                        "market_state": market_state,
                        "p3_touch_probability": p3_touch,
                    }
                )
    return pd.DataFrame(rows)


def test_nested_chronology_prevents_future_training_leakage() -> None:
    spec = _spec()
    panel = _panel(spec)
    original = surface.evaluate_joint_quote_direct_value_surface(panel, spec)

    first_test_day = original.chronology_audit.loc[
        original.chronology_audit["level"].eq("outer"), "test_min_day"
    ].min()
    changed = panel.copy()
    changed.loc[changed["day"].ge(first_test_day), surface.TARGET_COLUMN] += 10_000.0
    rerun = surface.evaluate_joint_quote_direct_value_surface(changed, spec)

    original_fold = original.action_predictions.loc[
        original.action_predictions["outer_fold"].eq(0)
    ].sort_values(["assignment_episode_id", "predicted_action"])
    rerun_fold = rerun.action_predictions.loc[
        rerun.action_predictions["outer_fold"].eq(0)
    ].sort_values(["assignment_episode_id", "predicted_action"])
    np.testing.assert_allclose(
        original_fold["q_hat_usdc"], rerun_fold["q_hat_usdc"], atol=0.0, rtol=0.0
    )
    assert (
        original.chronology_audit["train_max_day"] < original.chronology_audit["test_min_day"]
    ).all()
    assert not original.chronology_audit["future_training_leakage"].any()


@pytest.mark.parametrize("failure", ["action", "propensity"])
def test_unknown_action_and_propensity_fail_closed(failure: str) -> None:
    spec = _spec()
    panel = _panel(spec)
    if failure == "action":
        panel.loc[0, "action"] = "unknown_quote"
        match = "unknown action"
    else:
        panel.loc[0, "behavior_propensity"] = 0.99
        match = "behavior_propensity"

    with pytest.raises(ValueError, match=match):
        surface.evaluate_joint_quote_direct_value_surface(panel, spec)


def test_strict_schema_and_causal_feature_clock_fail_closed() -> None:
    spec = _spec()
    panel = _panel(spec)
    with_extra = panel.assign(hidden_future_label=1.0)
    with pytest.raises(ValueError, match="schema mismatch"):
        surface.evaluate_joint_quote_direct_value_surface(with_extra, spec)

    future_feature = panel.copy()
    future_feature.loc[0, "feature_ready_ts_ns"] = future_feature.loc[0, "assignment_ts_ns"] + 1
    with pytest.raises(ValueError, match="future feature_ready"):
        surface.evaluate_joint_quote_direct_value_surface(future_feature, spec)


def test_candidate_selection_degrades_to_baseline_below_economic_epsilon() -> None:
    spec = _spec(epsilon=2.0)
    result = surface.evaluate_joint_quote_direct_value_surface(_panel(spec), spec)

    assert not result.selection_evidence["supported"].any()
    assert result.oof_policy["policy_is_baseline"].all()
    assert np.array_equal(
        result.oof_policy["dr_policy_minus_baseline_usdc"].to_numpy(),
        np.zeros(len(result.oof_policy)),
    )
    assert result.report["outer_oof_policy_vs_baseline"]["point_usdc"] == 0.0


def test_training_day_max_stat_lcb_selects_only_supported_candidate() -> None:
    spec = _spec(epsilon=0.05)
    result = surface.evaluate_joint_quote_direct_value_surface(_panel(spec), spec)

    good = result.selection_evidence.loc[result.selection_evidence["action"].eq("good_joint_quote")]
    bad = result.selection_evidence.loc[result.selection_evidence["action"].eq("bad_joint_quote")]
    assert good["supported"].all()
    assert good["simultaneous_lcb_usdc"].gt(spec.economic_epsilon_usdc).all()
    assert not bad["supported"].any()
    assert set(result.selection_evidence["method"]) == {"inner_oof_day_cluster_centered_max_stat"}
    for _, fold in result.selection_evidence.groupby("outer_fold"):
        assert fold["simultaneous_critical_usdc"].nunique() == 1
    assert result.report["target"]["touch_probability_multiplier_applied"] is False
    assert not result.report["permissions"]["validation_read"]
    assert not result.report["permissions"]["sealed_holdout_read"]
    assert not result.report["permissions"]["action_authorized"]
    assert not result.report["permissions"]["live_authorized"]


def test_doubly_robust_policy_contrast_identity() -> None:
    observed = np.asarray(["baseline", "candidate", "baseline", "candidate"])
    policy = np.repeat("candidate", 4)
    outcome = np.asarray([1.0, 3.0, 2.0, 4.0])
    propensity = np.repeat(0.5, 4)
    q_baseline = np.asarray([1.0, 1.0, 2.0, 2.0])
    q_policy = np.asarray([3.0, 3.0, 4.0, 4.0])

    contrast = surface.doubly_robust_policy_vs_baseline(
        observed_action=observed,
        outcome=outcome,
        observed_propensity=propensity,
        policy_action=policy,
        baseline_action="baseline",
        q_policy=q_policy,
        q_baseline=q_baseline,
    )
    np.testing.assert_allclose(contrast, np.repeat(2.0, 4))

    no_op = surface.doubly_robust_policy_vs_baseline(
        observed_action=observed,
        outcome=outcome,
        observed_propensity=propensity,
        policy_action=np.repeat("baseline", 4),
        baseline_action="baseline",
        q_policy=q_baseline,
        q_baseline=q_baseline,
    )
    np.testing.assert_array_equal(no_op, np.zeros(4))


def test_panel_identity_is_bound_to_the_complete_spec() -> None:
    spec = _spec()
    panel = _panel(spec)
    changed_spec = replace(spec, economic_epsilon_usdc=0.051)

    with pytest.raises(ValueError, match="surface_spec_sha256"):
        surface.evaluate_joint_quote_direct_value_surface(panel, changed_spec)
