from __future__ import annotations

import numpy as np
import pytest

from research.families.f09_campaign_action_uplift.causal_path_features import compute_causal_path_features


def _book():
    ts = np.array([1_000, 1_100, 1_200, 1_300], dtype=np.int64)
    bid_px = np.array([[100.0], [99.8], [99.9], [100.0]])
    ask_px = bid_px + 0.2
    bid_qty = np.array([[10.0], [5.0], [7.5], [10.0]])
    ask_qty = np.array([[10.0], [10.0], [10.0], [10.0]])
    return ts, bid_px, bid_qty, ask_px, ask_qty


def test_buy_path_detects_adverse_shock_refill_and_recovery() -> None:
    ts, bid_px, bid_qty, ask_px, ask_qty = _book()
    result = compute_causal_path_features(
        side="BUY",
        start_ts_ms=1_000,
        decision_ts_ms=1_300,
        trade_ts_ms=np.array([1_050, 1_150, 1_250]),
        trade_qty=np.array([2.0, 2.0, 1.0]),
        is_buyer_maker=np.array([True, True, False]),
        l2_ts_ms=ts,
        l2_bid_px=bid_px,
        l2_bid_qty=bid_qty,
        l2_ask_px=ask_px,
        l2_ask_qty=ask_qty,
        near_levels=1,
    )

    assert result["path_feature_valid"] == 1.0
    assert result["path_log_elapsed_s"] == pytest.approx(np.log1p(0.3))
    assert result["shock_adverse_flow_imbalance_since_fill"] == pytest.approx(0.6)
    assert result["shock_adverse_move_bps"] > 0.0
    assert result["refill_depletion_ratio"] == pytest.approx(0.5)
    assert result["refill_recovery_ratio"] == pytest.approx(1.0)
    assert result["refill_half_life_observed"] == 1.0
    assert result["shock_log1p_adverse_qty_to_depth_since_fill"] > 0.0
    assert result["refill_log1p_current_vs_start_ratio"] > 0.0
    assert result["recovery_price_ratio"] > 0.9
    assert result["recovery_current_adverse_bps"] == pytest.approx(0.0)


def test_sell_path_uses_symmetric_adverse_direction() -> None:
    ts, bid_px, bid_qty, ask_px, ask_qty = _book()
    # Mirror the price and ask-depth path upward for a short inventory shock.
    sell_bid = np.array([[100.0], [100.2], [100.1], [100.0]])
    sell_ask = sell_bid + 0.2
    sell_ask_qty = np.array([[10.0], [4.0], [7.0], [10.0]])
    result = compute_causal_path_features(
        side="SELL",
        start_ts_ms=1_000,
        decision_ts_ms=1_300,
        trade_ts_ms=np.array([1_050, 1_150]),
        trade_qty=np.array([2.0, 1.0]),
        is_buyer_maker=np.array([False, True]),
        l2_ts_ms=ts,
        l2_bid_px=sell_bid,
        l2_bid_qty=bid_qty,
        l2_ask_px=sell_ask,
        l2_ask_qty=sell_ask_qty,
        near_levels=1,
    )

    assert result["shock_adverse_flow_imbalance_since_fill"] == pytest.approx(1 / 3)
    assert result["shock_adverse_move_bps"] > 0.0
    assert result["refill_depletion_ratio"] == pytest.approx(0.6)
    assert result["recovery_price_ratio"] > 0.9


def test_future_events_do_not_change_decision_time_features() -> None:
    ts, bid_px, bid_qty, ask_px, ask_qty = _book()
    kwargs = dict(
        side="BUY",
        start_ts_ms=1_000,
        decision_ts_ms=1_200,
        trade_ts_ms=np.array([1_050, 1_150]),
        trade_qty=np.array([1.0, 1.0]),
        is_buyer_maker=np.array([True, False]),
        l2_ts_ms=ts,
        l2_bid_px=bid_px,
        l2_bid_qty=bid_qty,
        l2_ask_px=ask_px,
        l2_ask_qty=ask_qty,
        near_levels=1,
    )
    original = compute_causal_path_features(**kwargs)
    kwargs["trade_ts_ms"] = np.append(kwargs["trade_ts_ms"], 1_250)
    kwargs["trade_qty"] = np.append(kwargs["trade_qty"], 1_000.0)
    kwargs["is_buyer_maker"] = np.append(kwargs["is_buyer_maker"], True)
    changed = compute_causal_path_features(**kwargs)

    assert changed == original
