from __future__ import annotations

import pandas as pd
import pytest

from research.families.f03_causal_13_head.audit.owner_amended_economic_rescore import (
    _campaign_day_values,
    _loss_fill_selectivity,
)


def test_loss_fill_selectivity_detects_more_than_proportional_improvement() -> None:
    daily = pd.DataFrame(
        [
            {
                "day": day,
                "arm": arm,
                "terminal_mtm_pnl_usdc": pnl,
                "fills_total": fills,
            }
            for day in ("2026-01-01", "2026-01-02", "2026-01-03")
            for arm, pnl, fills in (
                ("ml_off", -10.0, 100),
                ("ml_on", -5.0, 90),
            )
        ]
    )

    result = _loss_fill_selectivity(daily, draws=200, seed=7)

    assert result["relative_loss_reduction"]["point_estimate"] == pytest.approx(0.5)
    assert result["relative_fill_reduction"]["point_estimate"] == pytest.approx(0.1)
    assert result["loss_reduction_to_fill_reduction_ratio"][
        "point_estimate"
    ] == pytest.approx(5.0)


def test_campaign_day_values_separates_closed_from_administrative_day_end() -> None:
    daily = pd.DataFrame(
        [
            {"day": "2026-01-01", "arm": arm}
            for arm in ("ml_off", "ml_on")
        ]
    )
    campaigns = pd.DataFrame(
        [
            {
                "day": "2026-01-01",
                "arm": "ml_off",
                "closed": True,
                "terminal_value_usdc": -1.0,
            },
            {
                "day": "2026-01-01",
                "arm": "ml_off",
                "closed": False,
                "terminal_value_usdc": -0.2,
            },
            {
                "day": "2026-01-01",
                "arm": "ml_on",
                "closed": True,
                "terminal_value_usdc": -0.4,
            },
        ]
    )

    result = _campaign_day_values(campaigns, daily).set_index("arm")

    assert result.loc["ml_off", "closed_campaign_value_usdc"] == pytest.approx(-1.0)
    assert result.loc["ml_off", "day_end_open_mtm_value_usdc"] == pytest.approx(-0.2)
    assert result.loc["ml_on", "closed_campaign_value_usdc"] == pytest.approx(-0.4)
    assert result.loc["ml_on", "day_end_open_mtm_value_usdc"] == pytest.approx(0.0)
