from __future__ import annotations

import json

import pandas as pd

from research.families.f10_live_replay_attribution.audit import (
    live_120h_adverse_selection_diagnostic as diagnostic,
)


def test_live_120h_spec_contract() -> None:
    spec = json.loads(diagnostic.DEFAULT_SPEC.read_text())
    diagnostic.validate_spec(spec)


def test_live_120h_recomputes_value_and_campaign_tail() -> None:
    fills = pd.DataFrame(
        {
            "timestamp": [1_700_000_000.0, 1_700_000_001.0, 1_700_000_002.0],
            "side": ["BUY", "SELL", "SELL"],
            "role": ["opener", "opener", "add"],
            "commission": [0.0, 0.1, 0.0],
            "age_ms": [500.0, 5_000.0, 5_100.0],
            "entry_edge_bps": [2.0, 3.0, 3.0],
            "observation_delay_10s": [1.0, 2.0, 3.0],
            "value_10s_bps": [-1.0, -1.0, None],
            "value_10s_usdc": [-0.01, -0.02, None],
            "market_move_10s_bps": [-3.0, -4.0, None],
        }
    )
    campaigns = pd.DataFrame(
        {
            "campaign_sequence": [1, 2, 3],
            "opening_side": ["LONG", "SHORT", "SHORT"],
            "max_abs_position": [0.001, 0.002, 0.001],
            "pnl_usdc": [0.03, -0.06, -0.03],
        }
    )

    report = diagnostic.evaluate_frames(fills, campaigns)

    assert report["all"]["fills"] == 3
    assert report["all"]["valid_10s"] == 2
    assert report["all"]["value_10s_bps_mean"] == -1.0
    assert report["all_daily_value_means_negative"] is True
    assert report["age_slices"]["under_1s"]["fills"] == 1
    assert report["age_slices"]["4p5_to_5p5s"]["fills"] == 1
    assert report["campaigns"]["multi_inventory_campaigns"] == 1
    assert report["campaigns"]["short_campaign_pnl_usdc_sum"] == -0.09
    assert report["contracts"]["action_or_live_authorization"] is False
