"""One fee-allocation owner across live, cooldown replay and cash accounting."""

import random

import pytest

from models.replay import continuous_accounting
from strategy import inventory_manager, replay_controls
from strategy.fill_accounting import apply_position_fill, split_fill_fee


@pytest.mark.parametrize("fee", [0., .037, -.037])
@pytest.mark.parametrize("closed,quantity", [(0., .003), (.001, .003), (.003, .003)])
def test_signed_fee_allocation(fee, closed, quantity):
    closing, opening = split_fill_fee(fee, closed, quantity)
    assert closing == fee * closed / quantity
    assert opening == fee - closing
    assert closing + opening == pytest.approx(fee)


def test_preserves_legacy_float_operation_order():
    rng = random.Random(831)
    for _ in range(10000):
        quantity = 10 ** rng.uniform(-8, 4)
        closed = quantity * rng.random()
        fee = rng.uniform(-100, 100)
        closing = fee * closed / quantity
        assert split_fill_fee(fee, closed, quantity) == (closing, fee - closing)


def test_all_adapters_use_the_same_owner():
    for adapter in (inventory_manager, replay_controls):
        assert adapter.apply_position_fill is apply_position_fill
    assert continuous_accounting.split_fill_fee is split_fill_fee
