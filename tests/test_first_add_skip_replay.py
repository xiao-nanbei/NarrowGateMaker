from __future__ import annotations

import numpy as np
import pandas as pd

from models.backtest_tick import simulate_tick


def test_buy_first_add_skip_has_one_campaign_intervention() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": np.arange(0, 8_000, 1_000, dtype=np.int64),
            "price": np.full(8, 100.0),
            "quantity": np.zeros(8),
            "is_buyer_maker": np.ones(8, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "initial_inventory": 0.001,
        "initial_entry_price": 100.0,
        "replay_event_clock": "trade",
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "sell_add_skip_ope_enabled": True,
        "trace_sell_add_skip_ope_max": 10,
        "sell_add_skip_ope_probabilities": {
            "baseline": 0.5,
            "skip_one_add_cycle": 0.5,
        },
        "sell_add_skip_ope_seed": 7,
        "sell_add_skip_ope_sides": ["BUY"],
        "sell_add_skip_ope_family_id": (
            "buy_first_add_skip_marginal_value_v1"
        ),
        "sell_add_skip_min_followup_s": 0.0,
    }

    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
    )
    trace = result["_sell_add_skip_ope_trace"]

    assert result["sell_add_skip_ope_assignment_count"] == 1
    assert result["sell_add_skip_ope_sides"] == ["BUY"]
    assert len(trace) == 1
    assert trace[0]["family_id"] == "buy_first_add_skip_marginal_value_v1"
    assert trace[0]["side"] == "BUY"
    assert trace[0]["inventory_role"] == "add"
    assert trace[0]["behavior_propensity"] == 0.5
    assert trace[0]["one_intervention_per_campaign"] == 1
    assert abs(trace[0]["reward_identity_error"]) < 1e-12


def test_sell_campaign_stop_add_persists_until_flat() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": np.arange(0, 12_000, 1_000, dtype=np.int64),
            "price": np.full(12, 100.0),
            "quantity": np.zeros(12),
            "is_buyer_maker": np.zeros(12, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "initial_inventory": -0.001,
        "initial_entry_price": 100.0,
        "replay_event_clock": "trade",
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "sell_add_skip_ope_enabled": True,
        "trace_sell_add_skip_ope_max": 10,
        "sell_add_skip_ope_probabilities": {
            "baseline": 0.5,
            "stop_add_until_flat": 0.5,
        },
        "sell_add_skip_ope_seed": 0,
        "sell_add_skip_ope_sides": ["SELL"],
        "sell_add_skip_ope_family_id": "sell_campaign_add_permission_v1",
        "sell_add_skip_ope_mode": "until_flat",
        "sell_add_skip_min_followup_s": 0.0,
    }

    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
    )
    trace = result["_sell_add_skip_ope_trace"]

    assert result["sell_add_skip_ope_assignment_count"] == 1
    assert result["sell_add_skip_ope_mode"] == "until_flat"
    assert len(trace) == 1
    assert trace[0]["side"] == "SELL"
    assert trace[0]["action"] == "stop_add_until_flat"
    assert trace[0]["blocked_quote_cycles"] > 1
    assert trace[0]["intervention_order_submit_count"] == 0
    assert trace[0]["one_intervention_per_campaign"] == 1
    assert abs(trace[0]["reward_identity_error"]) < 1e-12
