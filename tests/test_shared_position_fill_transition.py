"""Preserve original live/replay arithmetic while sharing the fill transition."""

import random

import pytest

from strategy.fill_accounting import (
    PositionAccountingBasis,
    PositionFillState,
    apply_position_fill,
)
from strategy.inventory_manager import InventoryManager
from strategy.replay_controls import ConsecutiveLossCooldown


def _original_position_transition(state, side, qty, price, fee, accumulated):
    """Independent value oracle for the two pre-extraction adapter ledgers.

    Deliberately retain original operation ordering, including the different
    opening-fee grouping and reversal quantity; exact equality is the contract.
    No production module imports or executes this reference implementation.
    """
    q, entry, cost, open_fee, trip = (
        state.inventory, state.average_entry, state.cost_basis,
        state.opening_fee, state.round_trip_pnl,
    )
    signed = qty if side == "BUY" else -qty
    realized, closed, closed_pnl, opened, reduced = 0.0, False, 0.0, False, False
    if (q == 0.0 if accumulated else abs(q) <= 1e-10):
        q, entry, cost, open_fee, trip = signed, price, price * abs(signed), fee, 0.0
        opened = True
    elif (q > 0 and signed > 0) or (q < 0 and signed < 0):
        new_abs = abs(q) + qty
        if accumulated:
            cost += price * qty
            entry = cost / new_abs
        else:
            cost = entry * abs(q) + price * qty
            entry = cost / new_abs
        q += signed
        open_fee += fee
    else:
        reduced = True
        old_q = q
        close_qty = min(qty, abs(q))
        closing_fee = fee * close_qty / qty
        opening_fee = fee - closing_fee
        if abs(q) > 1e-10:
            share = open_fee * (close_qty / abs(q)) if accumulated else open_fee * close_qty / abs(q)
        else:
            share = open_fee
        if q > 0:
            realized = (price - entry) * close_qty - closing_fee - share
        else:
            realized = (entry - price) * close_qty - closing_fee - share
        open_fee -= share
        trip += realized
        remaining = abs(q) - close_qty
        if remaining < 1e-10:
            closed, closed_pnl = True, float(trip)
            flip = qty - close_qty
            if flip > 1e-10:
                q = signed + old_q if accumulated else flip if signed > 0 else -flip
                entry, cost, open_fee, opened = price, price * abs(q), opening_fee, True
            else:
                q, entry, cost, open_fee = 0.0, 0.0, 0.0, 0.0
            trip = 0.0
        else:
            q += signed
            cost = entry * abs(q)
    return PositionFillState(q, entry, cost, open_fee, trip), realized, reduced, closed, closed_pnl, opened


@pytest.mark.parametrize("basis", list(PositionAccountingBasis))
def test_position_transition_exact_original_values_across_fill_sequences(basis):
    rng = random.Random(77319)
    state = PositionFillState(0.0, 0.0, 0.0, 0.0, 0.0)
    for i in range(15000):
        side = rng.choice(("BUY", "SELL"))
        quantity = rng.randint(1, 11) * .001
        if i % 11 == 0 and state.inventory:
            side = "SELL" if state.inventory > 0 else "BUY"
            quantity = abs(state.inventory)  # Full closures as well as flips.
        price = rng.uniform(10000., 200000.)
        fee = rng.choice((-.0001, 0., .0002)) * price * quantity
        result = apply_position_fill(state, side=side, quantity=quantity, price=price, commission=fee, basis=basis)
        expected = _original_position_transition(
            state, side, quantity, price, fee,
            basis is PositionAccountingBasis.ACCUMULATED_COST,
        )
        assert (result.state, result.realized_pnl, result.reduced_position,
                result.closed_round_trip, result.closed_round_trip_pnl,
                result.opened_position) == expected
        state = result.state


@pytest.mark.parametrize("basis", list(PositionAccountingBasis))
@pytest.mark.parametrize("inventory", [0., -1e-11, 1e-11, -1e-10, 1e-10, -.003, .003])
@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_existing_dust_and_fee_basis_are_not_silently_normalized(basis, inventory, side):
    state = PositionFillState(inventory, 120000., abs(inventory) * 120000., -.0123, -.321)
    result = apply_position_fill(state, side=side, quantity=.003, price=119900., commission=-.008, basis=basis)
    expected = _original_position_transition(
        state, side, .003, 119900., -.008,
        basis is PositionAccountingBasis.ACCUMULATED_COST,
    )
    assert (result.state, result.realized_pnl, result.reduced_position,
            result.closed_round_trip, result.closed_round_trip_pnl,
            result.opened_position) == expected


def test_live_identity_deduplication_precedes_shared_accounting():
    live = InventoryManager()
    fill = dict(side="BUY", qty=.002, price=100., commission=-.001,
                order_id="1", trade_id="1", cumulative_filled_qty=.002)
    assert live.on_fill(**fill) == .002
    before = (live.snapshot, live._open_commission, live._total_commission)
    assert live.on_fill(**fill) == 0.
    assert (live.snapshot, live._open_commission, live._total_commission) == before


def test_live_and_replay_share_close_and_flip_economics_with_adapter_risk_policy():
    live = InventoryManager()
    replay = ConsecutiveLossCooldown(max_consecutive_losses=2, cooldown_ms=1000)
    fills = [("BUY", .002, 100., .002), ("BUY", .001, 101., -.001),
             ("SELL", .001, 99., -.001), ("SELL", .003, 98., .003),
             ("BUY", .001, 101., .001)]
    for side, qty, price, fee in fills:
        live.on_fill(side, qty, price, commission=fee)
        replay.on_fill(side=side, quantity=qty, price=price, commission=fee)
        assert live.snapshot.qty == pytest.approx(replay.inventory)
        assert live.snapshot.avg_entry_price == pytest.approx(replay.avg_entry)
        assert live._open_commission == pytest.approx(replay.open_commission)
        assert live._round_trip_rpnl == pytest.approx(replay.round_trip_pnl)
        assert live._consecutive_losses == replay.consecutive_losses
    assert replay.threshold_pending
    assert replay.on_policy_clock(100) == "triggered"
    assert replay.active(100)
