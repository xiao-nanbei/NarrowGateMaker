from __future__ import annotations

import pandas as pd
import pytest

from research.families.f03_causal_13_head.audit.full_path_ml_ab import (
    panel_evidence,
    reconstruct_campaigns,
)


def _fill(
    side: str,
    ts: int,
    price: float,
    qty: float,
    before: float,
    after: float,
    *,
    fee_rate: float = 0.0,
) -> dict:
    markout = 1.0
    return {
        "side": side,
        "fill_ts": ts,
        "quote_px": price,
        "fill_qty": qty,
        "inventory_before_fill": before,
        "inventory_after_fill": after,
        "markout_30s": markout,
        "ev_30s": markout - fee_rate * price,
    }


def test_reconstruct_campaigns_closes_and_marks_day_end_inventory() -> None:
    fills = [
        _fill("BUY", 1_000, 100.0, 0.001, 0.0, 0.001),
        _fill("SELL", 2_000, 101.0, 0.001, 0.001, 0.0),
        _fill("SELL", 3_000, 102.0, 0.001, 0.0, -0.001, fee_rate=0.001),
    ]
    campaigns = reconstruct_campaigns(
        fills,
        day="2026-01-01",
        panel_role="historical",
        arm="ml_off",
        terminal_mark_price=100.0,
        order_size=0.001,
    )
    assert len(campaigns) == 2
    assert campaigns[0]["closed"] is True
    assert campaigns[0]["inventory_side"] == "LONG"
    assert campaigns[0]["terminal_value_usdc"] == pytest.approx(0.001)
    assert campaigns[1]["closed"] is False
    assert campaigns[1]["inventory_side"] == "SHORT"
    assert campaigns[1]["fees_usdc"] == pytest.approx(0.000102)
    assert campaigns[1]["terminal_value_usdc"] == pytest.approx(0.001898)


def test_reconstruct_campaigns_rejects_inventory_discontinuity() -> None:
    fills = [
        _fill("BUY", 1_000, 100.0, 0.001, 0.0, 0.001),
        _fill("SELL", 2_000, 101.0, 0.001, 0.002, 0.001),
    ]
    with pytest.raises(ValueError, match="inventory trace discontinuity"):
        reconstruct_campaigns(
            fills,
            day="2026-01-01",
            panel_role="historical",
            arm="ml_off",
            terminal_mark_price=100.0,
            order_size=0.001,
        )


def test_panel_evidence_nulls_ranking_when_inventory_gate_fails() -> None:
    daily = pd.DataFrame(
        [
            {
                "day": day,
                "arm": arm,
                "terminal_mtm_pnl_usdc": pnl,
                "pnl_usdc": pnl,
                "fills_total": 100 if arm == "ml_off" else 95,
                "abs_inventory_time_btc_s": 10.0 if arm == "ml_off" else 12.0,
                "buy_maker_value_30s_bps": 0.1,
                "sell_maker_value_30s_bps": 0.1,
                "campaign_q10_usdc": -0.1 if arm == "ml_off" else -0.09,
                "campaign_cvar10_usdc": -0.2 if arm == "ml_off" else -0.19,
                "multi_level_long_terminal_value_usdc": -1.0,
                "multi_level_short_terminal_value_usdc": -1.0,
                "multi_level_long_negative_value_usdc": -1.0,
                "multi_level_short_negative_value_usdc": -1.0,
            }
            for day, off, on in (
                ("2026-01-01", 0.0, 1.0),
                ("2026-01-02", 0.0, 1.0),
                ("2026-01-03", 0.0, 1.0),
            )
            for arm, pnl in (("ml_off", off), ("ml_on", on))
        ]
    )
    campaigns = pd.DataFrame(
        [
            {
                "arm": arm,
                "terminal_value_usdc": value,
                "closed": True,
                "multi_level": False,
                "inventory_side": "LONG",
            }
            for arm, value in (("ml_off", -0.1), ("ml_on", -0.05))
        ]
    )
    result = panel_evidence(
        daily,
        campaigns,
        gates={
            "minimum_fill_retention": 0.90,
            "maximum_inventory_time_ratio": 1.05,
            "side_maker_value_tolerance_bps": 0.05,
        },
        bootstrap_draws=100,
        bootstrap_seed=7,
    )
    assert result["hard_gates"]["primary_pnl_lcb_positive"] is True
    assert result["hard_gates"]["inventory_time_nonworse"] is False
    assert result["ranking_score"] is None
