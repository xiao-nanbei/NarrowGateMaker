from __future__ import annotations

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.randomized_action_contrast import (
    build_randomized_itt_report,
    randomized_itt_contrast,
    validate_randomized_panel,
)


def _panel() -> pd.DataFrame:
    rows = []
    for day_index, day in enumerate(("2026-01-01", "2026-01-02", "2026-01-03")):
        for action in ("keep", "cancel_until_state_exit"):
            for row_index in range(10):
                candidate = action == "cancel_until_state_exit"
                filled = row_index < (6 if not candidate else 4)
                toxic = filled and row_index < (4 if not candidate else 1)
                reward = 0.04 if candidate else 0.01
                terminal = 0.03 if candidate else 0.00
                rows.append(
                    {
                        "day": day,
                        "decision_id": f"{day_index}-{action}-{row_index}",
                        "campaign_id": f"{action}-{row_index}",
                        "side": "BUY" if row_index % 2 == 0 else "SELL",
                        "action": action,
                        "behavior_propensity": 0.5,
                        "one_intervention_per_campaign": 1,
                        "reward": reward,
                        "fill_value": 0.0,
                        "campaign_cost": -reward,
                        "queue_cost": 0.0,
                        "reward_identity_error": 0.0,
                        "terminal_campaign_pnl": terminal,
                        "campaign_mae": -0.10 if candidate else -0.20,
                        "decision_to_terminal_s": 50.0 if candidate else 100.0,
                        "repair_event": 1,
                        "campaign_censored": 0,
                        "intervention_fill_count": int(filled),
                        "fill_value_markout_bps": -1.0 if toxic else 1.0,
                        "fill_value_horizon_censored": 0,
                        "native_exchange_seed_supported": 1,
                        "native_exchange_outcome_supported": 1,
                    }
                )
    return pd.DataFrame(rows)


def test_randomized_itt_recovers_logged_action_difference() -> None:
    panel = _panel()
    contrast = randomized_itt_contrast(
        panel,
        outcome="reward",
        bootstrap_trials=100,
        random_seed=7,
    )

    assert contrast["uplift"] == pytest.approx(0.03)
    assert contrast["interval"]["p025"] == pytest.approx(0.03)
    assert contrast["daily_positive_rate"] == pytest.approx(1.0)
    assert contrast["arms"]["keep"]["effective_sample_size"] == pytest.approx(30.0)


def test_report_keeps_strategy_and_intervention_retention_separate() -> None:
    panel = _panel()
    daily = pd.DataFrame(
        {
            "control_fills_total": [100, 100, 100],
            "randomized_fills_total": [99, 99, 99],
            "randomized_campaign_count": [50, 50, 50],
            "pnl_delta": [0.1, 0.2, 0.3],
        }
    )
    report = build_randomized_itt_report(
        panel,
        metadata={
            "panel_role": "development",
            "native_action_support": {
                "seed_gate": True,
                "path_gate": True,
            },
        },
        daily=daily,
        bootstrap_trials=100,
        random_seed=7,
    )

    assert report["total_strategy"]["fills_retention"] == pytest.approx(0.99)
    assert report["toxic_fill_selectivity"]["pooled"]["point"][
        "fills_retention"
    ] == pytest.approx(2.0 / 3.0)
    assert report["toxic_fill_selectivity"]["pooled"]["point"][
        "toxic_reduction_surplus"
    ] > 0.0


def test_panel_rejects_more_than_one_intervention_per_campaign() -> None:
    panel = _panel()
    panel.loc[1, "campaign_id"] = panel.loc[0, "campaign_id"]

    with pytest.raises(ValueError, match="one intervention per campaign"):
        validate_randomized_panel(panel)
