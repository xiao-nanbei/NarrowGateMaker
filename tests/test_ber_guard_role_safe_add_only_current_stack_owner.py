from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from strategy.quote_core import (
    QuoteCoreConfig,
    QuotePrediction,
    QuoteState,
    apply_final_spread_cap_preserve_side,
    ber_inventory_role_for_target,
    compose_ber_exposure_add_only_quote,
    compute_quote_core,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_spec_20260808.json"
)


def test_frozen_identity_is_owner_only_and_changes_role_mapping_only() -> None:
    payload = json.loads(SPEC.read_text(encoding="utf-8"))
    assert payload["identity"] == (
        "ber_guard_role_safe_add_only_current_stack_owner_v1"
    )
    assert payload["progression_route"] == "owner_risk_accepted"
    assert payload["progression"]["research_supported_promotion_allowed"] is False
    control = payload["arms"]["control"]
    candidate = payload["arms"]["candidate"]
    assert control["ber_guard_thresh"] == candidate["ber_guard_thresh"] == 1.2
    assert control["ber_spread_mult"] == candidate["ber_spread_mult"] == 2.0
    assert control["ber_exposure_add_only"] is False
    assert candidate["ber_exposure_add_only"] is True
    assert payload["development_panel"]["day_count"] == 40
    assert len(set(payload["development_panel"]["days"])) == 40


@pytest.mark.parametrize(
    ("side", "inventory", "quantity", "expected"),
    [
        ("BUY", 0.0, 0.001, "opener"),
        ("SELL", 0.0, 0.001, "opener"),
        ("BUY", 0.001, 0.001, "add"),
        ("SELL", -0.001, 0.001, "add"),
        ("SELL", 0.001, 0.001, "reducing"),
        ("BUY", -0.001, 0.001, "reducing"),
        ("SELL", 0.0004, 0.001, "mixed_cross_zero"),
        ("BUY", -0.0004, 0.001, "mixed_cross_zero"),
    ],
)
def test_quantity_aware_ber_role_classifier(
    side: str,
    inventory: float,
    quantity: float,
    expected: str,
) -> None:
    assert (
        ber_inventory_role_for_target(side, inventory, quantity) == expected
    )


def _source_quotes():
    cfg = QuoteCoreConfig(
        gamma=0.01,
        kappa=1.0,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.01,
        ber_spread_mult=2.0,
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=0.0,
    )
    prediction = QuotePrediction()
    common = dict(
        mid=64_000.0,
        inventory=0.001,
        sigma_sq=1.0,
        trade_intensity=100.0,
        best_bid=63_999.9,
        best_ask=64_000.1,
        position_open=True,
    )
    ber_quote = compute_quote_core(
        QuoteState(**common, ber_active=True), cfg, prediction
    )
    bypass_quote = compute_quote_core(
        QuoteState(**common, ber_active=False), cfg, prediction
    )
    return ber_quote, bypass_quote


def test_role_safe_composition_preserves_add_and_reducing_sources() -> None:
    ber_quote, bypass_quote = _source_quotes()
    long_pair = compose_ber_exposure_add_only_quote(
        ber_quote=ber_quote,
        bypass_quote=bypass_quote,
        inventory=0.001,
        target_buy_quantity=0.001,
        target_sell_quantity=0.001,
    )
    assert long_pair.bid_price == ber_quote.bid_price
    assert long_pair.ask_price == bypass_quote.ask_price
    assert long_pair.quote_context["BUY"]["ber_inventory_role"] == "add"
    assert long_pair.quote_context["SELL"]["ber_inventory_role"] == "reducing"

    flat_pair = compose_ber_exposure_add_only_quote(
        ber_quote=ber_quote,
        bypass_quote=bypass_quote,
        inventory=0.0,
        target_buy_quantity=0.001,
        target_sell_quantity=0.001,
    )
    assert flat_pair.bid_price == bypass_quote.bid_price
    assert flat_pair.ask_price == bypass_quote.ask_price

    mixed_pair = compose_ber_exposure_add_only_quote(
        ber_quote=ber_quote,
        bypass_quote=bypass_quote,
        inventory=0.0004,
        target_buy_quantity=0.001,
        target_sell_quantity=0.001,
    )
    assert mixed_pair.ask_price == ber_quote.ask_price
    assert mixed_pair.quote_context["SELL"][
        "ber_mixed_cross_zero_fail_closed"
    ] is True


def test_role_safe_cap_moves_only_add_side() -> None:
    bid, ask, hit, _, feasible = apply_final_spread_cap_preserve_side(
        100.0,
        98.0,
        101.0,
        2.0,
        0.1,
        preserve_side="SELL",
    )
    assert hit is True
    assert feasible is True
    assert ask == 101.0
    assert bid == pytest.approx(99.0)


def test_python_cpp_role_safe_ber_signal_and_path_lockstep() -> None:
    pytest.importorskip("narrowgate_cpp")
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 181_000, 1_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.full(ts.size, 100.0, dtype=np.float64),
            "quantity": np.zeros(ts.size, dtype=np.float64),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
        }
    )
    var_ti = np.where(ts < 60_000, 10.0, 100.0).astype(np.float64)
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "maker_fee": 0.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "initial_inventory": 0.001,
        "initial_entry_price": 100.0,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "replay_event_clock": "trade",
        "use_bar_pricing": True,
        "ml_enabled": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 0.0,
        "ber_guard_thresh": 1.2,
        "ber_spread_mult": 2.0,
        "ber_exposure_add_only": True,
        "collect_curves": False,
    }
    common = (
        trades,
        ts,
        np.ones(ts.size, dtype=np.float64),
        params,
    )
    py = bt._simulate_tick_with_engine(
        "python", *common, var_ti=var_ti, var_retsq=np.zeros(ts.size)
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp", *common, var_ti=var_ti, var_retsq=np.zeros(ts.size)
    )

    for field in (
        "n_requotes",
        "fills_total",
        "ber_active_count",
        "ber_feature_publish_count",
        "ber_role_safe_decision_count",
        "ber_role_safe_buy_add_count",
        "ber_role_safe_sell_add_count",
        "ber_role_safe_pair_change_count",
        "ber_role_safe_bid_change_count",
        "ber_role_safe_ask_change_count",
        "ber_role_safe_source_mismatch_count",
        "ber_role_safe_cap_infeasible_count",
    ):
        assert cpp[field] == py[field], field
    for field in (
        "ber_held_input_end",
        "ber_ema_fast_end",
        "ber_ema_slow_end",
        "pnl",
        "final_inventory",
    ):
        assert cpp[field] == pytest.approx(py[field], abs=1e-10), field
    assert py["ber_active_count"] > 0
    assert py["ber_role_safe_buy_add_count"] > 0
    assert py["ber_role_safe_pair_change_count"] > 0
    assert py["ber_role_safe_source_mismatch_count"] == 0
