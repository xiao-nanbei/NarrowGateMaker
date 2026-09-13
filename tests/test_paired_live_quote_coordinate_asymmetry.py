from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f10_live_replay_attribution.audit.paired_live_quote_coordinate_asymmetry import (
    evaluate_frames,
    pair_quote_decisions,
)


def _decision_rows() -> pd.DataFrame:
    rows = []
    for day, timestamp, bid, ask in (
        ("d1", 1_780_000_000.0, 99.0, 101.0),
        ("d2", 1_780_086_400.0, 99.0, 101.2),
    ):
        del day
        common = {
            "allow_post": 1,
            "allow_exposure_increase": 1,
            "mode": "normal",
            "reason_text": "none",
            "inventory_ratio": 0.0,
            "markout_ema": 0.0,
            "microprice_shift_bps": 0.0,
            "spread_mult": 1.0,
            "mid": 100.0,
            "action": "replace",
        }
        rows.append(
            {
                **common,
                "timestamp": timestamp,
                "side": "BUY",
                "base_price": 99.0,
                "final_price": bid,
            }
        )
        rows.append(
            {
                **common,
                "timestamp": timestamp + 0.001,
                "side": "SELL",
                "base_price": 101.0,
                "final_price": ask,
            }
        )
    return pd.DataFrame(rows)


def _fills() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "timestamp": 1_780_000_010.0,
                "side": "BUY",
                "role": "opener",
                "age_ms": 1000.0,
                "entry_edge_bps": 2.8,
                "market_move_10s_bps": -3.0,
                "value_10s_bps": -0.2,
                "observation_delay_10s": 1.0,
            },
            {
                "timestamp": 1_780_000_020.0,
                "side": "SELL",
                "role": "opener",
                "age_ms": 1000.0,
                "entry_edge_bps": 2.81,
                "market_move_10s_bps": -3.2,
                "value_10s_bps": -0.39,
                "observation_delay_10s": 1.0,
            },
            {
                "timestamp": 1_780_000_030.0,
                "side": "SELL",
                "role": "opener",
                "age_ms": np.nan,
                "entry_edge_bps": -50.0,
                "market_move_10s_bps": 50.0,
                "value_10s_bps": 0.0,
                "observation_delay_10s": 20_000.0,
            },
            {
                "timestamp": 1_780_086_410.0,
                "side": "BUY",
                "role": "reducing",
                "age_ms": 2000.0,
                "entry_edge_bps": 2.7,
                "market_move_10s_bps": -2.8,
                "value_10s_bps": -0.1,
                "observation_delay_10s": 2.0,
            },
            {
                "timestamp": 1_780_086_420.0,
                "side": "SELL",
                "role": "reducing",
                "age_ms": 2000.0,
                "entry_edge_bps": 2.7,
                "market_move_10s_bps": -2.9,
                "value_10s_bps": -0.2,
                "observation_delay_10s": 2.0,
            },
        ]
    )


def test_pair_quote_decisions_fails_on_incomplete_or_reordered_cycles():
    frame = _decision_rows()
    paired = pair_quote_decisions(frame)
    assert len(paired) == 2
    assert paired["pair_delay_ms"].max() == pytest.approx(1.0, abs=1e-4)

    with pytest.raises(ValueError, match="complete BUY/SELL pairs"):
        pair_quote_decisions(frame.iloc[:-1])
    reordered = frame.copy()
    reordered.loc[0, "side"] = "SELL"
    with pytest.raises(ValueError, match="strict BUY then SELL"):
        pair_quote_decisions(reordered)


def test_evaluator_excludes_missing_lifecycle_rows_from_entry_edge_authority():
    result = evaluate_frames(
        _decision_rows(),
        _fills(),
        start_ts=1_780_000_000.0,
        end_ts=1_780_086_500.0,
        tick_size=0.1,
        markout_spread_scale=0.2,
        markout_side_asymmetry_sign=-1.0,
        maximum_future_observation_delay_s=10.0,
    )
    identity = result["opener_entry_edge_identity"]
    assert identity["all_opener_fills"] == 3
    assert identity["exact_lifecycle_opener_fills"] == 2
    assert identity["missing_lifecycle_opener_fills"] == 1
    assert identity["mixed_identity_sell_minus_buy_bps"] < -20.0
    assert identity["exact_lifecycle_sell_minus_buy_bps"] == pytest.approx(0.01)
    assert identity["mixed_identity_gap_withdrawn"] is True
    assert result["fresh_10s_fill_value_sensitivity"]["fills"] == 4
    assert result["decision"]["structural_sell_quote_too_close_supported"] is False
    assert result["decision"]["historical_0p40bps_fill_edge_gap_valid"] is False


def test_maker_signed_markout_requires_positive_asymmetry_sign():
    result = evaluate_frames(
        _decision_rows(),
        _fills(),
        start_ts=1_780_000_000.0,
        end_ts=1_780_086_500.0,
        tick_size=0.1,
        markout_spread_scale=0.2,
        markout_side_asymmetry_sign=-1.0,
    )
    contract = result["markout_asymmetry_contract"]
    assert contract["required_semantic_sign"] == 1.0
    assert contract["semantic_contract_valid"] is False
    assert result["decision"]["baseline_markout_asymmetry_semantics_requires_correction"] is True
