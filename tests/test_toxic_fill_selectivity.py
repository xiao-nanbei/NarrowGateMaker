from __future__ import annotations

import math

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.toxic_fill_selectivity import (
    paired_dr_selectivity,
    randomized_panel_selectivity,
    toxic_fill_selectivity,
)


def test_selectivity_rewards_disproportionate_toxic_fill_removal() -> None:
    result = toxic_fill_selectivity(
        baseline_fill_rate=0.40,
        candidate_fill_rate=0.30,
        baseline_toxic_fill_rate=0.20,
        candidate_toxic_fill_rate=0.08,
    )

    assert result.fills_retention == pytest.approx(0.75)
    assert result.toxic_fills_retention == pytest.approx(0.40)
    assert result.fill_reduction == pytest.approx(0.25)
    assert result.toxic_fill_reduction == pytest.approx(0.60)
    assert result.toxic_reduction_surplus == pytest.approx(0.35)
    assert result.toxic_reduction_leverage == pytest.approx(2.4)
    assert result.toxic_selectivity_log_ratio > 0.0
    assert result.nonlinear_selectivity_score > 0.0


def test_proportional_fill_removal_has_zero_selectivity() -> None:
    result = toxic_fill_selectivity(
        baseline_fill_rate=0.40,
        candidate_fill_rate=0.20,
        baseline_toxic_fill_rate=0.10,
        candidate_toxic_fill_rate=0.05,
    )

    assert result.toxic_reduction_surplus == pytest.approx(0.0)
    assert result.toxic_selectivity_log_ratio == pytest.approx(0.0)
    assert result.nonlinear_selectivity_score == pytest.approx(0.0)


def test_more_selective_volume_with_no_fill_loss_is_finite() -> None:
    result = toxic_fill_selectivity(
        baseline_fill_rate=0.40,
        candidate_fill_rate=0.40,
        baseline_toxic_fill_rate=0.10,
        candidate_toxic_fill_rate=0.05,
    )

    assert result.toxic_reduction_leverage is None
    assert math.isfinite(result.toxic_selectivity_log_ratio)
    assert result.toxic_reduction_surplus == pytest.approx(0.5)


def _dr_rows(values: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02"],
            "decision_id": ["a", "b", "c", "d"],
            "ope_dr_value": values,
        }
    )


def test_day_clustered_dr_selectivity_uses_one_denominator() -> None:
    summary = paired_dr_selectivity(
        candidate_fill_rows=_dr_rows([0.3, 0.3, 0.3, 0.3]),
        baseline_fill_rows=_dr_rows([0.4, 0.4, 0.4, 0.4]),
        candidate_toxic_rows=_dr_rows([0.08, 0.08, 0.08, 0.08]),
        baseline_toxic_rows=_dr_rows([0.2, 0.2, 0.2, 0.2]),
        bootstrap_trials=200,
        random_seed=7,
    )

    assert summary["rows"] == 4
    assert summary["days"] == 2
    assert summary["point"]["toxic_reduction_surplus"] == pytest.approx(0.35)
    assert (
        summary["day_cluster_bootstrap"]["intervals"]
        ["toxic_reduction_surplus"]["p025"]
        > 0.0
    )


def test_dr_selectivity_rejects_mismatched_decision_sets() -> None:
    candidate = _dr_rows([0.3, 0.3, 0.3, 0.3])
    candidate.loc[3, "decision_id"] = "different"

    with pytest.raises(ValueError, match="do not share one denominator"):
        paired_dr_selectivity(
            candidate_fill_rows=candidate,
            baseline_fill_rows=_dr_rows([0.4, 0.4, 0.4, 0.4]),
            candidate_toxic_rows=_dr_rows([0.08, 0.08, 0.08, 0.08]),
            baseline_toxic_rows=_dr_rows([0.2, 0.2, 0.2, 0.2]),
            bootstrap_trials=10,
        )


def test_randomized_selectivity_rewards_superlinear_toxic_removal() -> None:
    rows = []
    for day_index, day in enumerate(("2026-01-01", "2026-01-02")):
        for action in ("keep", "cancel_until_state_exit"):
            for index in range(20):
                baseline = action == "keep"
                filled = index < (10 if baseline else 8)
                toxic = filled and index < (6 if baseline else 2)
                rows.append(
                    {
                        "day": day,
                        "decision_id": f"{day_index}-{action}-{index}",
                        "action": action,
                        "behavior_propensity": 0.5,
                        "intervention_fill_count": int(filled),
                        "fill_value_markout_bps": -1.0 if toxic else 1.0,
                        "fill_value_horizon_censored": 0,
                    }
                )
    summary = randomized_panel_selectivity(
        pd.DataFrame(rows),
        candidate_action="cancel_until_state_exit",
        baseline_action="keep",
        bootstrap_trials=100,
        random_seed=7,
    )

    assert summary["point"]["fills_retention"] == pytest.approx(0.8)
    assert summary["point"]["toxic_fills_retention"] == pytest.approx(1.0 / 3.0)
    assert summary["point"]["toxic_reduction_surplus"] > 0.0
    assert summary["point"]["toxic_reduction_leverage"] > 1.0
