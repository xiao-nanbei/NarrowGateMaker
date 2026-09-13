from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (
    OPEConfig,
    evaluate_fixed_holdout_policy,
    evaluate_offline_policy,
    make_day_folds,
)


def _randomized_panel(*, days: int = 12, rows_per_day: int = 80) -> pd.DataFrame:
    rng = np.random.default_rng(20260712)
    rows = []
    for day_idx in range(days):
        day = f"2026-01-{day_idx + 1:02d}"
        for row_idx in range(rows_per_day):
            x = float(rng.normal())
            action = "keep" if rng.random() < 0.5 else "pause"
            reward = 1.0 + 2.0 * (action == "keep") + 0.15 * x
            rows.append(
                {
                    "day": day,
                    "decision_id": f"{day}-{row_idx}",
                    "side": "BUY",
                    "inventory_ratio": x,
                    "action": action,
                    "behavior_propensity": 0.5,
                    "candidate_action": "keep",
                    "reward": reward,
                }
            )
    return pd.DataFrame(rows)


def _small_config(**overrides) -> OPEConfig:
    values = {
        "split_mode": "chronological",
        "min_train_days": 4,
        "test_days": 2,
        "embargo_days": 1,
        "min_train_rows": 100,
        "min_action_rows": 30,
        "min_effective_sample_size": 20.0,
        "bootstrap_trials": 50,
        "random_seed": 7,
    }
    values.update(overrides)
    return OPEConfig(**values)


def test_propensity_floor_and_weight_cap_define_one_unclipped_support_region() -> None:
    config = OPEConfig()

    assert config.min_behavior_propensity == pytest.approx(0.05)
    assert 1.0 / config.min_behavior_propensity <= config.max_importance_weight

    with pytest.raises(ValueError, match="deterministic action admitted.*clipped"):
        OPEConfig(
            min_behavior_propensity=0.02,
            max_importance_weight=20.0,
        )


def test_doubly_robust_ope_recovers_randomized_action_value() -> None:
    rows, folds, actions, summary = evaluate_offline_policy(
        _randomized_panel(),
        feature_names=["side", "inventory_ratio"],
        config=_small_config(),
    )

    assert not rows.empty
    assert len(folds) >= 2
    assert set(actions["action"]) == {"keep", "pause"}
    assert summary["formal_estimate_valid"] is True
    assert summary["estimand"]["kind"] == (
        "candidate_policy_value_with_bounded_unsupported_mass"
    )
    assert summary["overlap"]["importance_weight_clipped_rows"] == 0
    assert set(rows["ope_formal_estimate_valid"]) == {1}
    assert summary["overlap"]["mean_unsupported_candidate_mass"] < 0.01
    assert summary["estimators"]["candidate_clipped_dr_value"] == pytest.approx(
        3.0, abs=0.15
    )
    assert summary["estimators"]["candidate_minus_behavior_dr_uplift"] == pytest.approx(
        1.0, abs=0.15
    )
    interval = summary["day_cluster_bootstrap"]
    assert interval["uplift_p025"] > 0.5
    assert interval["uplift_p975"] < 1.5
    assert summary["daily_uplift"]["positive_rate"] > 0.9


def test_logged_behavior_probability_vector_skips_propensity_model() -> None:
    frame = _randomized_panel()
    frame["behavior_prob_keep"] = 0.5
    frame["behavior_prob_pause"] = 0.5

    rows, folds, _, summary = evaluate_offline_policy(
        frame,
        feature_names=["side", "inventory_ratio"],
        config=_small_config(),
    )

    assert summary["formal_estimate_valid"] is True
    assert set(folds["propensity_source"]) == {"logged_probability_vector"}
    assert set(rows["ope_behavior_prob_keep"]) == {0.5}
    assert set(rows["ope_behavior_prob_pause"]) == {0.5}


def test_logged_behavior_probability_vector_rejects_propensity_mismatch() -> None:
    frame = _randomized_panel()
    frame["behavior_prob_keep"] = 0.5
    frame["behavior_prob_pause"] = 0.5
    frame.loc[frame.index[0], "behavior_propensity"] = 0.4

    with pytest.raises(ValueError, match="disagrees"):
        evaluate_offline_policy(
            frame,
            feature_names=["side", "inventory_ratio"],
            config=_small_config(),
        )


def test_never_logged_candidate_action_fails_overlap_gate() -> None:
    frame = _randomized_panel(days=8, rows_per_day=30)
    frame["action"] = "pause"
    frame["candidate_action"] = "recenter_1tick"
    frame["behavior_propensity"] = 1.0

    rows, _, actions, summary = evaluate_offline_policy(
        frame,
        feature_names=["side", "inventory_ratio"],
        config=_small_config(
            min_train_days=3,
            min_train_rows=60,
            min_action_rows=10,
            min_effective_sample_size=1.0,
            bootstrap_trials=0,
        ),
    )

    assert not rows.empty
    assert "recenter_1tick" in set(actions["action"])
    assert summary["formal_estimate_valid"] is False
    assert summary["status"] == "diagnostic_only_overlap_failed"
    assert summary["prediction_coverage"] == 0.0
    assert summary["overlap"]["unsupported_actions"] == ["recenter_1tick"]
    assert set(rows["ope_formal_estimate_valid"]) == {0}
    assert rows["ope_prediction_valid"].eq(0).all()
    assert math.isnan(summary["estimators"]["candidate_clipped_dr_value"])


def test_clipped_weight_failure_invalidates_rows_and_summary_estimators() -> None:
    frame = _randomized_panel()
    keep_rows = frame["action"].eq("keep")
    frame.loc[keep_rows, "behavior_propensity"] = 0.01

    rows, _, _, summary = evaluate_offline_policy(
        frame,
        feature_names=["side", "inventory_ratio"],
        config=_small_config(max_unsupported_mass=1.0),
    )

    assert summary["formal_estimate_valid"] is False
    assert summary["overlap"]["importance_weight_clipped_rows"] > 0
    assert math.isfinite(
        summary["diagnostic_estimators"]["candidate_clipped_dr_value"]
    )
    assert math.isnan(summary["estimators"]["candidate_direct_value"])
    assert math.isnan(summary["estimators"]["candidate_clipped_ips_value"])
    assert math.isnan(summary["estimators"]["candidate_clipped_dr_value"])
    assert rows["ope_prediction_valid"].eq(0).all()
    assert rows["ope_dr_value"].isna().all()


def test_reward_components_are_fill_value_minus_campaign_and_queue_cost() -> None:
    frame = _randomized_panel()
    frame = frame.drop(columns="reward")
    frame["fill_value"] = 4.0 + 2.0 * (frame["action"] == "keep").astype(float)
    frame["campaign_cost"] = 1.0
    frame["queue_cost"] = 0.5

    rows, _, _, summary = evaluate_offline_policy(
        frame,
        feature_names=["side", "inventory_ratio"],
        config=_small_config(),
    )

    assert set(np.round(rows["_ope_reward"].unique(), 6)) == {2.5, 4.5}
    assert summary["estimators"]["candidate_clipped_dr_value"] == pytest.approx(
        4.5, abs=0.12
    )


def test_formal_ope_rejects_unregistered_terminal_feature() -> None:
    frame = _randomized_panel()
    frame["terminal_campaign_pnl"] = 1.0
    with pytest.raises(ValueError, match="unregistered features"):
        evaluate_offline_policy(
            frame,
            feature_names=["inventory_ratio", "terminal_campaign_pnl"],
            config=_small_config(),
        )


def test_formal_ope_rejects_future_feature_timestamp(tmp_path) -> None:
    frame = _randomized_panel()
    frame["decision_ts_ns"] = np.arange(len(frame), dtype=np.int64) * 1_000_000
    frame["external_state"] = 1.0
    frame["external_ready_ts_ns"] = frame["decision_ts_ns"] + 1
    registry = tmp_path / "features.json"
    registry.write_text(
        """
{
  "features": [
    {
      "name": "external_state",
      "kind": "numeric",
      "available_at": "decision",
      "source_timestamp_col": "external_ready_ts_ns",
      "max_age_ms": 100
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="after the action decision"):
        evaluate_offline_policy(
            frame,
            feature_names=["side", "inventory_ratio", "external_state"],
            feature_registry_path=registry,
            config=_small_config(),
        )


def test_missing_candidate_action_is_rejected() -> None:
    frame = _randomized_panel()
    frame.loc[0, "candidate_action"] = None

    with pytest.raises(ValueError, match="missing/empty actions"):
        evaluate_offline_policy(
            frame,
            feature_names=["side", "inventory_ratio"],
            config=_small_config(),
        )


def test_candidate_probabilities_must_sum_to_one() -> None:
    frame = _randomized_panel().drop(columns="candidate_action")
    frame["candidate_prob_keep"] = 0.4
    frame["candidate_prob_pause"] = 0.4

    with pytest.raises(ValueError, match="must sum to 1"):
        evaluate_offline_policy(
            frame,
            feature_names=["side", "inventory_ratio"],
            config=_small_config(),
        )


def test_chronological_folds_train_only_on_past_with_embargo() -> None:
    days = [f"2026-03-{idx:02d}" for idx in range(1, 11)]
    folds = make_day_folds(
        days,
        OPEConfig(
            split_mode="chronological",
            min_train_days=3,
            test_days=2,
            embargo_days=1,
        ),
    )

    first = folds[0]
    assert first.train_days == tuple(days[:3])
    assert first.test_days == tuple(days[4:6])
    assert max(first.train_days) < min(first.test_days)


def test_fixed_holdout_fits_development_and_scores_only_later_days() -> None:
    frame = _randomized_panel(days=12)
    development = frame[frame["day"] <= "2026-01-08"].copy()
    later = frame[frame["day"] > "2026-01-08"].copy()

    rows, folds, _, summary = evaluate_fixed_holdout_policy(
        development,
        later,
        feature_names=["side", "inventory_ratio"],
        config=_small_config(min_train_rows=100, min_action_rows=30),
    )

    assert set(rows["day"]) == set(later["day"])
    assert set(folds.iloc[0]["train_days"]) == set(development["day"])
    assert set(folds.iloc[0]["test_days"]) == set(later["day"])
    assert summary["evaluation_design"]["holdout_used_for_fit"] is False
    assert summary["estimators"]["candidate_clipped_dr_value"] == pytest.approx(
        3.0, abs=0.15
    )
