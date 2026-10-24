"""Shared replacement/sizing rules at the actual live order-state boundary."""

import itertools
import math
import random

import pytest

from strategy.inventory_soft_control import pre_generic_sizes
from strategy.order_manager import OrderManager, OrderState, Side
from strategy.replace_threshold import (
    cancel_first_replace,
    coalesce_pending_replace,
    replace_throttle_blocks,
    replace_throttle_parameters,
)
from tests.test_live_replace_throttle import _engine


@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("inventory", [-0.1, -1e-12, -0.0, 0.0, 1e-12, 0.1])
def test_role_and_original_live_adapter_agree(side, inventory):
    expected_increasing = (side == "BUY" and inventory >= 0) or (
        side == "SELL" and inventory <= 0
    )
    expected = (2.0, 500.0, True) if expected_increasing else (1.0, 250.0, False)
    assert replace_throttle_parameters(
        side=side, inventory=inventory, ordinary_ticks=2.0,
        ordinary_interval_ms=500.0, reducing_ticks=1.0,
        reducing_interval_ms=250.0,
    ) == expected
    assert _engine()._replace_throttle_params(Side(side), inventory) == expected


@pytest.mark.parametrize("delta", [0.0, 1.0, 2.0 - 2e-9, 2.0 - 0.5e-9, 2.0, 3.0])
@pytest.mark.parametrize("age", [0.0, 499.999999, 500.0, 500.000001])
@pytest.mark.parametrize("price_limit,age_limit", [(0.0, 0.0), (2.0, 500.0)])
def test_throttle_boundaries_preserve_independent_reasons(delta, age, price_limit, age_limit):
    assert replace_throttle_blocks(
        price_delta_ticks=delta, age_ms=age,
        min_ticks=price_limit, min_interval_ms=age_limit,
    ) == (
        price_limit > 0 and delta + 1e-9 < price_limit,
        age_limit > 0 and age < age_limit,
    )


def test_pending_and_cancel_first_truth_tables_preserve_original_rule_order():
    for enabled, needs, allowed, active, increasing, force in itertools.product(
        (False, True), repeat=6,
    ):
        expected = False
        if enabled and needs and not force and allowed and active:
            expected = increasing
        assert cancel_first_replace(
            enabled=enabled, needs_update=needs, can_post=allowed,
            active=active, exposure_increasing=increasing, force_update=force,
        ) is expected
        for pending in (False, True):
            assert coalesce_pending_replace(
                enabled=enabled, needs_update=needs, can_post=allowed, pending=pending,
            ) is bool(enabled and needs and allowed and pending)


def _event(cid, *, status, cumulative, last_quantity, trade_id, timestamp):
    return {
        "c": cid, "i": 17, "s": "BTCUSDC", "S": "BUY", "X": status,
        "o": "LIMIT", "p": "100.0", "q": "0.003", "z": str(cumulative),
        "l": str(last_quantity), "L": "100.0", "ap": "100.0",
        "x": "TRADE" if last_quantity else "CANCELED",
        "n": "0", "N": "USDC", "t": trade_id, "T": timestamp // 1_000_000,
        "_local_receive_ts_ns": timestamp,
    }


@pytest.mark.parametrize("terminal", ["CANCELED", "FILLED"])
def test_live_pending_fill_and_terminal_use_shared_rule_without_releasing_early(
    monkeypatch, terminal,
):
    import strategy.maker_engine as live

    engine = _engine()
    engine.orders = OrderManager()
    observed = []

    def record_shared_rule(**kwargs):
        observed.append(kwargs)
        return coalesce_pending_replace(**kwargs)

    monkeypatch.setattr(live, "coalesce_pending_replace", record_shared_rule)
    now = 1_750_000_000_000_000_000
    monkeypatch.setattr("strategy.order_manager.time.time_ns", lambda: now)
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 100.0, 0.003)
    order = engine.orders.get_order(cid)

    def coalesced():
        return engine._apply_pending_replace_coalesce(
            side=Side.BUY, now_ts=now / 1e9, q=0.0, target_price=99.0,
            order=order, needs_update=True, can_post=True,
        )

    assert order.state == OrderState.PENDING_NEW
    assert coalesced()
    engine.orders.confirm_new(cid, 17)
    assert not coalesced()
    engine.orders.mark_pending_cancel(cid)
    assert coalesced()
    engine.orders.on_order_update(_event(
        cid, status="PARTIALLY_FILLED", cumulative=0.001, last_quantity=0.001,
        trade_id=1, timestamp=now + 1_000_000,
    ))
    assert order.remaining_qty == 0.002
    assert order.state == OrderState.PENDING_CANCEL
    assert coalesced()  # Partial fill is not a cancel ACK or ownership release.
    engine.orders.on_order_update(_event(
        cid, status=terminal, cumulative=0.003 if terminal == "FILLED" else 0.001,
        last_quantity=0.002 if terminal == "FILLED" else 0.0,
        trade_id=2, timestamp=now + 2_000_000,
    ))
    assert order.is_terminal
    assert not coalesced()
    assert [row["pending"] for row in observed] == [True, False, True, True, False]
    assert engine.orders.active_count() == 0


def test_eta_symmetric_sizing_matches_both_previous_formulas_exactly():
    rng = random.Random(31)
    for _ in range(1000):
        base = rng.randint(1, 20) * 0.001
        lot = 0.001
        inventory = rng.randint(-100, 100) * lot
        maximum = rng.randint(1, 100) * lot
        eta = rng.choice([0.0, 0.01, 0.7, 20.0])
        symmetric = rng.choice([False, True])
        bid = ask = base
        if eta > 0.0 and maximum > 1e-10:
            normalized = inventory / maximum
            if inventory > 0.0:
                bid = base * math.exp(-eta * normalized)
                bid = math.floor(bid / lot) * lot
                if bid < lot:
                    bid = lot
            if inventory < 0.0:
                ask = base * math.exp(eta * normalized)
                ask = math.floor(ask / lot) * lot
                if ask < lot:
                    ask = lot
        if symmetric:
            if bid < ask:
                ask = bid
            elif ask < bid:
                bid = ask
        assert pre_generic_sizes(
            order_size=base, lot=lot, inventory=inventory,
            max_inventory=maximum, eta=eta, symmetric=symmetric,
        ) == (bid, ask)


def test_actual_replay_consumes_shared_sizing_and_replacement_rules(monkeypatch):
    import numpy as np

    from models.backtest_tick import simulate_tick
    from narrowgate.replay.executor import action_policy, inventory_policy, rest_gateway
    from tests.test_python_planned_maintenance_replay import (
        _async_fifo_params, _inputs, _params,
    )

    calls = {}

    def record(module, name):
        original = getattr(module, name)
        calls[name] = 0

        def checked(*args, **kwargs):
            calls[name] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(module, name, checked)

    for name in (
        "replace_throttle_parameters", "replace_throttle_blocks", "cancel_first_replace",
    ):
        record(rest_gateway, name)
    record(inventory_policy, "pre_generic_sizes")
    record(action_policy, "coalesce_pending_replace")
    trades, bbo = _inputs(crossing_fill_ts_ms=2200)
    params = {
        **_params(), **_async_fifo_params(), "planned_quote_stop_ts_ms": 0,
        "replace_min_price_change_ticks": 10,
        "replace_min_interval_ms": 500,
    }
    result = simulate_tick(
        trades, np.array([0]), np.array([1.0]), params, bbo_data=bbo,
    )
    assert result.get("completed", True)
    assert all(count > 0 for count in calls.values()), calls
