from __future__ import annotations

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.toxic_fill_selectivity_v2 import (
    conservative_randomized_selectivity,
    is_toxic_net_markout,
    maker_signed_net_markout_bps,
    quantity_weighted_rates,
)


def test_maker_signed_markout_is_symmetric_and_fee_adjusted() -> None:
    buy = maker_signed_net_markout_bps(
        side="BUY",
        fill_price=100.0,
        future_bbo_mid=101.0,
        maker_fee_bps=0.2,
    )
    sell = maker_signed_net_markout_bps(
        side="SELL",
        fill_price=101.0,
        future_bbo_mid=100.0,
        maker_fee_bps=0.2,
    )
    assert buy == pytest.approx(99.8)
    assert sell == pytest.approx(10000.0 / 101.0 - 0.2)
    assert is_toxic_net_markout(-0.01, epsilon_toxic_bps=0.0)
    assert not is_toxic_net_markout(0.0, epsilon_toxic_bps=0.0)


def test_quantity_rates_use_mean_decision_fraction() -> None:
    panel = pd.DataFrame(
        [
            {
                "decision_id": "a",
                "action": "control",
                "assigned_qty_btc": 0.001,
                "filled_qty_btc": 0.001,
                "known_toxic_filled_qty_btc": 0.0005,
                "unlabeled_filled_qty_btc": 0.0,
            },
            {
                "decision_id": "b",
                "action": "control",
                "assigned_qty_btc": 0.002,
                "filled_qty_btc": 0.001,
                "known_toxic_filled_qty_btc": 0.0,
                "unlabeled_filled_qty_btc": 0.0005,
            },
        ]
    )
    rates = quantity_weighted_rates(panel, action="control")
    assert rates.mean_fill_fraction == pytest.approx(0.75)
    assert rates.mean_known_toxic_fraction == pytest.approx(0.25)
    assert rates.mean_toxic_fraction_upper == pytest.approx(0.375)


def test_conservative_censoring_is_adverse_to_candidate() -> None:
    panel = pd.DataFrame(
        [
            {
                "decision_id": "control",
                "action": "control",
                "assigned_qty_btc": 0.001,
                "filled_qty_btc": 0.001,
                "known_toxic_filled_qty_btc": 0.0002,
                "unlabeled_filled_qty_btc": 0.0004,
            },
            {
                "decision_id": "candidate",
                "action": "candidate",
                "assigned_qty_btc": 0.001,
                "filled_qty_btc": 0.0008,
                "known_toxic_filled_qty_btc": 0.0001,
                "unlabeled_filled_qty_btc": 0.0003,
            },
        ]
    )
    result = conservative_randomized_selectivity(
        panel,
        baseline_action="control",
        candidate_action="candidate",
    )
    point = result["conservative_selectivity"]
    assert point["baseline_toxic_fill_rate"] == pytest.approx(0.2)
    assert point["candidate_toxic_fill_rate"] == pytest.approx(0.4)
    assert point["toxic_reduction_surplus"] < 0.0


def test_quantity_contract_rejects_overfilled_decision() -> None:
    panel = pd.DataFrame(
        [
            {
                "decision_id": "a",
                "action": "control",
                "assigned_qty_btc": 0.001,
                "filled_qty_btc": 0.002,
                "known_toxic_filled_qty_btc": 0.0,
                "unlabeled_filled_qty_btc": 0.0,
            }
        ]
    )
    with pytest.raises(ValueError, match="filled quantity"):
        quantity_weighted_rates(panel, action="control")
