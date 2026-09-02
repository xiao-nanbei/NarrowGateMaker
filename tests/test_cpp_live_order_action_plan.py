from __future__ import annotations

import random
from types import SimpleNamespace

import pytest

from strategy.maker_engine import MakerEngine
from strategy.native_order_action import (
    CheckedNativeOrderActionPlanner,
    NativeOrderActionBoundaryError,
    NativeOrderSideBoundary,
    checked_lattice_units,
    checked_quote_atoms,
)
from strategy.order_manager import Order, OrderState, Side
from strategy.quote_core import _exposure_increasing

cpp = pytest.importorskip("narrowgate_cpp")


ROUTE_ALLOWED = 1 << 0
ALLOW_POST = 1 << 1
ALLOW_EXPOSURE = 1 << 2
FORCE_UPDATE = 1 << 3
USE_PROVIDED_UPDATE = 1 << 4
PROVIDED_NEEDS_UPDATE = 1 << 5

PENDING_COALESCE = 1 << 0
CANCEL_FIRST_EXPOSURE = 1 << 1

R_ROUTE_DISABLED = 1 << 0
R_INVENTORY_LIMIT = 1 << 1
R_POLICY_POST = 1 << 2
R_POLICY_EXPOSURE = 1 << 3
R_PRICE_DRIFT = 1 << 4
R_TTL = 1 << 5
R_EXISTING_POSITION_VALUE = 1 << 6
R_THROTTLE_PRICE = 1 << 7
R_THROTTLE_AGE = 1 << 8
R_PENDING = 1 << 9
R_MIN_QTY = 1 << 10
R_MIN_NOTIONAL = 1 << 11
R_POSITION_VALUE_CAP = 1 << 12
R_INVENTORY_QTY_CAP = 1 << 13
R_CLOSE_QTY_CAP = 1 << 14
R_CONFIGURED_CANCEL_FIRST = 1 << 15


def _context(
    *, inventory: int = 0, value_limit_lots: int = 20
) -> tuple[int | float, ...]:
    mid_notional_per_lot = 6_000_000_000
    return (
        inventory,
        26,
        value_limit_lots * mid_notional_per_lot,
        mid_notional_per_lot,
        10_000,
        1,
        500_000_000,
        0.1,
    )


def _replace(*, flags: int = PENDING_COALESCE) -> tuple[int | float, ...]:
    return (0.1, 0.001, 5.0, 0.0, 0.0, 0.0, 0.0, flags)


def _side(
    *,
    state=None,
    target: int = 600_000,
    desired: int = 1,
    probe: int = 1,
    existing: int | None = None,
    remaining: int | None = None,
    age_ms: float = 1_000.0,
    ttl_ms: float = 0.0,
    flags: int = ROUTE_ALLOWED | ALLOW_POST | ALLOW_EXPOSURE,
) -> tuple[object, ...]:
    if state is None:
        state = cpp.LivePlannerOrderState.Empty
    if existing is None:
        existing = 0 if state == cpp.LivePlannerOrderState.Empty else 600_000
    if remaining is None:
        remaining = 0 if state == cpp.LivePlannerOrderState.Empty else 1
    update_scalar = (
        abs(target * 0.1 - existing * 0.1) / 0.1
        if flags & USE_PROVIDED_UPDATE and existing > 0
        else ttl_ms
    )
    price_identity = target * 0.1 if flags & USE_PROVIDED_UPDATE else existing
    return (
        target,
        desired,
        probe,
        price_identity,
        remaining,
        age_ms,
        update_scalar,
        state,
        flags,
    )


def _state_code(state: object) -> int:
    mapping = {
        cpp.LivePlannerOrderState.Empty: 0,
        cpp.LivePlannerOrderState.PendingNew: 1,
        cpp.LivePlannerOrderState.Active: 2,
        cpp.LivePlannerOrderState.PendingCancel: 3,
        cpp.LivePlannerOrderState.Terminal: 4,
    }
    return mapping[state]


def _reference_side(
    side: str,
    context: tuple[int | float, ...],
    replace: tuple[int | float, ...],
    values: tuple[object, ...],
) -> dict[str, object]:
    (
        inventory,
        max_inventory,
        max_position_atoms,
        mid_notional_per_lot,
        _atoms_per_tick_lot,
        min_qty,
        _min_notional_atoms,
        threshold_bps,
    ) = context
    (
        tick_size,
        lot_size,
        min_notional_b0,
        add_min_delta_ticks,
        reducing_min_delta_ticks,
        add_min_interval_ms,
        reducing_min_interval_ms,
        replace_flags,
    ) = replace
    (
        target,
        desired,
        probe,
        existing_or_target,
        remaining,
        age_ms,
        update_scalar,
        state,
        flags,
    ) = values
    state_code = _state_code(state)
    active = state_code in {1, 2}
    pending = state_code in {1, 3}
    existing = (
        0
        if flags & USE_PROVIDED_UPDATE
        else existing_or_target
    )
    target_price_b0 = (
        existing_or_target
        if flags & USE_PROVIDED_UPDATE
        else target * tick_size
    )
    buy = side == "BUY"
    exposure = (
        inventory >= 0 or probe > -inventory
        if buy
        else inventory <= 0 or probe > inventory
    )
    inv_room = max(0, max_inventory - inventory if buy else max_inventory + inventory)
    value_limit_lots = max_position_atoms // mid_notional_per_lot
    value_room = max(0, value_limit_lots - inventory if buy else value_limit_lots + inventory)
    can_after_inventory = inventory < max_inventory if buy else inventory > -max_inventory
    allow_post = bool(flags & ALLOW_POST)
    allow_exposure = bool(flags & ALLOW_EXPOSURE)
    can_post = can_after_inventory and allow_post and (allow_exposure or not exposure)

    reason = 0
    if not can_after_inventory:
        reason |= R_INVENTORY_LIMIT
    if not allow_post:
        reason |= R_POLICY_POST
    if not allow_exposure and exposure:
        reason |= R_POLICY_EXPOSURE

    force_update = bool(flags & FORCE_UPDATE)
    if flags & USE_PROVIDED_UPDATE:
        needs_update = bool(flags & PROVIDED_NEEDS_UPDATE)
    else:
        needs_update = True
        if active and existing > 0:
            target_price = target * tick_size
            existing_price = existing * tick_size
            needs_update = (
                abs(target_price - existing_price) / existing_price
                > threshold_bps / 10_000.0
            )
            if needs_update:
                reason |= R_PRICE_DRIFT
            if update_scalar > 0 and age_ms >= update_scalar:
                needs_update = True
                force_update = True
                reason |= R_TTL
    if force_update:
        needs_update = True

    target_qty = desired
    if buy:
        if inventory > 0:
            if inv_room >= 1:
                if target_qty > inv_room:
                    reason |= R_INVENTORY_QTY_CAP
                    target_qty = inv_room
            else:
                if target_qty > 0:
                    reason |= R_INVENTORY_QTY_CAP
                target_qty = 0
        elif inventory < -1:
            close_cap = abs(inventory)
            close_valid = (
                close_cap >= min_qty
                and close_cap * lot_size * target_price_b0
                >= min_notional_b0
            )
            if close_valid and target_qty > close_cap:
                reason |= R_CLOSE_QTY_CAP
                target_qty = close_cap
    else:
        if inventory < 0:
            if inv_room >= 1:
                if target_qty > inv_room:
                    reason |= R_INVENTORY_QTY_CAP
                    target_qty = inv_room
            else:
                if target_qty > 0:
                    reason |= R_INVENTORY_QTY_CAP
                target_qty = 0
        elif inventory > 1:
            close_cap = inventory
            close_valid = (
                close_cap >= min_qty
                and close_cap * lot_size * target_price_b0
                >= min_notional_b0
            )
            if close_valid and target_qty > close_cap:
                reason |= R_CLOSE_QTY_CAP
                target_qty = close_cap
    if exposure and target_qty > value_room:
        reason |= R_POSITION_VALUE_CAP
        target_qty = value_room
    target_qty = max(0, target_qty)

    remaining_exposure = (
        inventory >= 0 or remaining > -inventory
        if buy
        else inventory <= 0 or remaining > inventory
    )
    if active and remaining > 0 and remaining_exposure and value_room < remaining:
        needs_update = True
        force_update = True
        reason |= R_EXISTING_POSITION_VALUE

    add_replace = inventory >= 0 if buy else inventory <= 0
    min_delta_ticks = (
        add_min_delta_ticks if add_replace else reducing_min_delta_ticks
    )
    min_interval_ms = add_min_interval_ms if add_replace else reducing_min_interval_ms
    # MakerEngine always applies the replace throttle to an active order. In
    # the provided-decision ABI, ``existing_or_target`` stores the exact raw
    # target price (the C++ union's other member), so ``existing`` is
    # intentionally zero and must not suppress the throttle.
    if (
        needs_update
        and not force_update
        and active
        and (bool(flags & USE_PROVIDED_UPDATE) or existing > 0)
    ):
        price_delta_ticks = (
            update_scalar
            if flags & USE_PROVIDED_UPDATE
            else abs(target * tick_size - existing * tick_size) / tick_size
        )
        throttle_price = (
            min_delta_ticks > 0
            and price_delta_ticks + 1e-9 < min_delta_ticks
        )
        throttle_age = min_interval_ms > 0 and age_ms < min_interval_ms
        if throttle_price:
            reason |= R_THROTTLE_PRICE
        if throttle_age:
            reason |= R_THROTTLE_AGE
        if throttle_price or throttle_age:
            needs_update = False

    quantity_ok = target_qty >= min_qty
    notional_ok = (
        target_qty * lot_size * target_price_b0 >= min_notional_b0
    )
    filter_valid = quantity_ok and notional_ok
    if not quantity_ok:
        reason |= R_MIN_QTY
    if not notional_ok:
        reason |= R_MIN_NOTIONAL

    cancel_existing = False
    if not flags & ROUTE_ALLOWED:
        action = "route_disabled"
        reason |= R_ROUTE_DISABLED
        needs_update = False
    elif not can_post:
        action = "pause"
        cancel_existing = needs_update and active
    elif not needs_update and active:
        action = "keep"
    elif needs_update and pending and replace_flags & PENDING_COALESCE:
        action = "pending"
        reason |= R_PENDING
    elif needs_update and active:
        action = "cancel_first"
        cancel_existing = True
        if not force_update and add_replace and replace_flags & CANCEL_FIRST_EXPOSURE:
            reason |= R_CONFIGURED_CANCEL_FIRST
    elif needs_update and not filter_valid:
        action = "skip_filter"
    elif needs_update:
        action = "place"
    else:
        action = "none"

    return {
        "target_price_ticks": target,
        "target_quantity_lots": target_qty,
        "inventory_room_lots": inv_room,
        "position_value_room_lots": value_room,
        "existing_remaining_lots": remaining,
        "reason_mask": reason,
        "action_name": action,
        "exposure_increasing": exposure,
        "can_post_after_inventory": can_after_inventory,
        "can_post": can_post,
        "needs_update": needs_update,
        "force_update": force_update,
        "order_active": active,
        "order_pending": pending,
        "filter_valid": filter_valid,
        "cancel_existing": cancel_existing,
    }


def _assert_plan(actual, expected: dict[str, object]) -> None:
    for field, value in expected.items():
        assert getattr(actual, field) == value, field


def test_native_order_action_plan_uses_fixed_x86_cache_line_pods() -> None:
    assert cpp.LIVE_ORDER_ACTION_PLAN_CONTEXT_BYTES == 64
    assert cpp.LIVE_ORDER_ACTION_PLAN_REPLACE_BYTES == 64
    assert cpp.LIVE_ORDER_ACTION_PLAN_SIDE_INPUT_BYTES == 64
    assert cpp.LIVE_ORDER_ACTION_PLAN_SIDE_RESULT_BYTES == 64
    assert cpp.LIVE_ORDER_ACTION_PLAN_DUAL_RESULT_BYTES == 128


def _checked_planner(
    *,
    max_position_value: float = 3_000.0,
    min_notional: float = 5.0,
    add_min_price_change_ticks: float = 0.0,
    add_min_interval_ms: float = 0.0,
) -> CheckedNativeOrderActionPlanner:
    return CheckedNativeOrderActionPlanner(
        cpp,
        max_inventory=0.026,
        max_position_value=max_position_value,
        tick_size=0.1,
        lot_size=0.001,
        min_quantity=0.001,
        min_notional=min_notional,
        requote_threshold_bps=0.1,
        add_min_price_change_ticks=add_min_price_change_ticks,
        reducing_min_price_change_ticks=add_min_price_change_ticks,
        add_min_interval_ms=add_min_interval_ms,
        reducing_min_interval_ms=add_min_interval_ms,
        replace_pending_coalesce=True,
        replace_cancel_first_exposure_increasing=False,
    )


def _checked_boundary(
    *,
    price: float,
    quantity: float = 0.001,
    probe: float = 0.001,
    order: Order | None = None,
    needs_update: bool = True,
    force_update: bool = False,
) -> NativeOrderSideBoundary:
    return NativeOrderSideBoundary(
        target_price=price,
        desired_quantity=quantity,
        exposure_probe_quantity=probe,
        order=order,
        needs_update=needs_update,
        force_update=force_update,
        route_allowed=True,
        allow_post=True,
        allow_exposure_increase=True,
    )


def test_checked_native_boundary_rejects_fractional_wire_lattice() -> None:
    assert checked_lattice_units(60_000.1, 0.1, name="price") == 600_001
    assert checked_lattice_units(0.003, 0.001, name="quantity") == 3
    assert checked_quote_atoms(0.000_000_01, name="quote") == 1

    with pytest.raises(NativeOrderActionBoundaryError, match="off the"):
        checked_lattice_units(60_000.15, 0.1, name="price")
    with pytest.raises(NativeOrderActionBoundaryError, match="off the"):
        checked_lattice_units(0.0015, 0.001, name="quantity")
    with pytest.raises(NativeOrderActionBoundaryError, match="quote atoms"):
        checked_quote_atoms(0.000_000_005, name="quote")


@pytest.mark.parametrize(
    ("target_price", "minimum_ticks", "expected_action"),
    [
        (60_000.1, 1.0, "cancel_first"),
        (60_000.1, 1.000000001, "keep"),
        (600_001 * 0.1, 1.000000001, "cancel_first"),
        (600_001 * 0.1, 1.000000002, "keep"),
    ],
)
def test_checked_adapter_preserves_b0_one_nanotick_throttle_boundary(
    target_price: float,
    minimum_ticks: float,
    expected_action: str,
) -> None:
    order = Order(
        client_order_id="existing-buy",
        symbol="BTCUSDC",
        side=Side.BUY,
        price=60_000.0,
        quantity=0.001,
        state=OrderState.OPEN,
        create_time=999.0,
    )
    planner = _checked_planner(
        add_min_price_change_ticks=minimum_ticks,
        add_min_interval_ms=1_000.0,
    )
    plan = planner.compute(
        inventory=0.0,
        mid=60_000.05,
        now_ts=1_000.0,
        buy=_checked_boundary(price=target_price, order=order),
        sell=_checked_boundary(price=60_000.2),
    )

    assert plan.buy.action_name == expected_action


def test_checked_adapter_matches_b0_caps_filters_and_cross_zero_order() -> None:
    capped = _checked_planner(max_position_value=120.0).compute(
        inventory=0.0,
        mid=60_000.0,
        now_ts=1_000.0,
        buy=_checked_boundary(price=60_000.0, quantity=0.004),
        sell=_checked_boundary(price=60_000.1, quantity=0.004),
    )
    assert capped.buy.target_quantity_lots == 2
    assert capped.sell.target_quantity_lots == 2

    exact_minimum = _checked_planner(min_notional=60.0).compute(
        inventory=0.0,
        mid=60_000.0,
        now_ts=1_000.0,
        buy=_checked_boundary(price=60_000.0),
        sell=_checked_boundary(price=59_999.9),
    )
    assert exact_minimum.buy.action == cpp.LiveOrderAction.Place
    assert exact_minimum.buy.filter_valid
    assert exact_minimum.sell.action == cpp.LiveOrderAction.SkipFilter
    assert not exact_minimum.sell.filter_valid

    one_lot_close = _checked_planner().compute(
        inventory=-0.001,
        mid=60_000.0,
        now_ts=1_000.0,
        buy=_checked_boundary(
            price=60_000.0,
            quantity=0.003,
            probe=0.001,
        ),
        sell=_checked_boundary(price=60_000.1),
    )
    assert not one_lot_close.buy.exposure_increasing
    assert one_lot_close.buy.target_quantity_lots == 3

    crosses_flat = _checked_planner().compute(
        inventory=-0.002,
        mid=60_000.0,
        now_ts=1_000.0,
        buy=_checked_boundary(
            price=60_000.0,
            quantity=0.003,
            probe=0.003,
        ),
        sell=_checked_boundary(price=60_000.1),
    )
    assert crosses_flat.buy.exposure_increasing
    assert crosses_flat.buy.target_quantity_lots == 2


def test_checked_adapter_preserves_binary64_min_notional_equality() -> None:
    # Decimal arithmetic says these are equal, while B0's binary64 product is
    # one ULP below the configured threshold. The native path must preserve
    # the existing skip instead of silently replacing it with decimal math.
    target_price = 93_137.0
    quantity = 0.015
    minimum = 1_397.055
    assert quantity * target_price < minimum

    plan = _checked_planner(min_notional=minimum).compute(
        inventory=0.0,
        mid=target_price,
        now_ts=1_000.0,
        buy=_checked_boundary(price=target_price, quantity=quantity),
        sell=_checked_boundary(price=target_price + 0.1),
    )
    assert plan.buy.action == cpp.LiveOrderAction.SkipFilter
    assert not plan.buy.filter_valid


@pytest.mark.parametrize(
    ("inventory", "buy_exposure", "sell_exposure"),
    [
        (0, True, True),
        (2, True, False),
        (-2, False, True),
    ],
)
def test_native_order_action_plan_classifies_open_add_and_reduce(
    inventory: int,
    buy_exposure: bool,
    sell_exposure: bool,
) -> None:
    result = cpp.compute_live_order_action_plan(
        _context(inventory=inventory),
        _replace(),
        _side(),
        _side(),
    )
    assert result.buy.exposure_increasing is buy_exposure
    assert result.sell.exposure_increasing is sell_exposure


def test_native_order_action_plan_cross_zero_matches_quote_core() -> None:
    for inventory_lots in range(-5, 6):
        for quantity_lots in range(1, 9):
            result = cpp.compute_live_order_action_plan(
                _context(inventory=inventory_lots),
                _replace(),
                _side(desired=quantity_lots, probe=quantity_lots),
                _side(desired=quantity_lots, probe=quantity_lots),
            )
            inventory = inventory_lots * 0.001
            quantity = quantity_lots * 0.001
            assert result.buy.exposure_increasing is _exposure_increasing(
                "BUY", inventory, quantity, 0.001
            )
            assert result.sell.exposure_increasing is _exposure_increasing(
                "SELL", inventory, quantity, 0.001
            )


def test_native_order_action_plan_position_value_cap_matches_maker_engine() -> None:
    lot = 0.001
    mid = 60_000.0
    mid_notional_atoms_per_lot = 6_000_000_000
    cases = (
        ("BUY", 0, 4, 120.0),
        ("BUY", 5, 4, 360.0),
        ("BUY", -2, 2, 180.0),
        ("SELL", 0, 4, 120.0),
        ("SELL", -5, 4, 360.0),
        ("SELL", 2, 2, 180.0),
    )
    for side_name, inventory_lots, requested_lots, max_value in cases:
        context = list(_context(inventory=inventory_lots))
        context[1] = 100
        context[2] = round(max_value * 100_000_000)
        context[3] = mid_notional_atoms_per_lot
        buy = _side(desired=requested_lots, probe=requested_lots)
        sell = _side(desired=requested_lots, probe=requested_lots)
        result = cpp.compute_live_order_action_plan(
            tuple(context), _replace(), buy, sell
        )
        actual = result.buy if side_name == "BUY" else result.sell
        expected = MakerEngine._cap_exposure_qty_by_position_value(
            side=Side.BUY if side_name == "BUY" else Side.SELL,
            current_qty=inventory_lots * lot,
            mid=mid,
            requested_qty=requested_lots * lot,
            max_position_value=max_value,
            lot=lot,
        )
        assert actual.target_quantity_lots == round(expected / lot)


def test_native_order_action_plan_keeps_replaces_and_coalesces() -> None:
    active = _side(state=cpp.LivePlannerOrderState.Active)
    kept = cpp.compute_live_order_action_plan(_context(), _replace(), active, active)
    assert kept.buy.action == cpp.LiveOrderAction.Keep
    assert kept.sell.action == cpp.LiveOrderAction.Keep

    moved = _side(state=cpp.LivePlannerOrderState.Active, target=600_010)
    replaced = cpp.compute_live_order_action_plan(_context(), _replace(), moved, active)
    assert replaced.buy.action == cpp.LiveOrderAction.CancelFirst
    assert replaced.buy.cancel_existing

    pending = _side(state=cpp.LivePlannerOrderState.PendingCancel)
    coalesced = cpp.compute_live_order_action_plan(_context(), _replace(), pending, active)
    assert coalesced.buy.action == cpp.LiveOrderAction.Pending
    assert coalesced.buy.order_pending


def test_native_order_action_plan_ttl_bypasses_replace_throttle() -> None:
    replace = (
        0.1,
        0.001,
        5.0,
        0.0,
        0.0,
        10_000.0,
        10_000.0,
        PENDING_COALESCE,
    )
    expired = _side(
        state=cpp.LivePlannerOrderState.Active,
        age_ms=2_000.0,
        ttl_ms=1_000.0,
    )
    result = cpp.compute_live_order_action_plan(
        _context(), replace, expired, _side()
    )
    assert result.buy.action == cpp.LiveOrderAction.CancelFirst
    assert result.buy.force_update


def test_native_order_action_plan_uses_frozen_pretransform_b0_update_state() -> None:
    active = _side(
        state=cpp.LivePlannerOrderState.Active,
        target=600_100,
        flags=(
            ROUTE_ALLOWED
            | ALLOW_POST
            | ALLOW_EXPOSURE
            | USE_PROVIDED_UPDATE
        ),
    )
    kept = cpp.compute_live_order_action_plan(
        _context(), _replace(), active, _side()
    )
    assert kept.buy.action == cpp.LiveOrderAction.Keep
    assert not kept.buy.needs_update

    forced_values = list(active)
    forced_values[8] |= FORCE_UPDATE
    forced = cpp.compute_live_order_action_plan(
        _context(), _replace(), tuple(forced_values), _side()
    )
    assert forced.buy.action == cpp.LiveOrderAction.CancelFirst
    assert forced.buy.needs_update
    assert forced.buy.force_update


@pytest.mark.parametrize("min_ticks", [1.0, 1.000000001, 1.000000002, 2.0])
def test_native_replace_one_nanotick_tolerance_matches_maker_engine(
    min_ticks: float,
) -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(
        tick_size=0.1,
        strategy=SimpleNamespace(
            replace_min_price_change_ticks=min_ticks,
            replace_min_price_change_ticks_reducing=min_ticks,
            replace_min_interval_ms=0.0,
            replace_min_interval_ms_reducing=0.0,
        ),
    )
    engine._replace_throttle_counts = {"BUY": 0, "SELL": 0}
    engine._last_replace_throttle_log = {"BUY": 0.0, "SELL": 0.0}
    order = SimpleNamespace(
        is_active=True,
        price=600_000 * 0.1,
        create_time=0.0,
    )
    expected_needs_update = MakerEngine._apply_replace_throttle(
        engine,
        side=Side.BUY,
        now_ts=1.0,
        q=0.0,
        # Quote prices reach this planner after tick multiplication.  Use the
        # same binary64 reconstruction as the integer-tick native boundary.
        target_price=600_001 * 0.1,
        order=order,
        needs_update=True,
        force_update=False,
    )
    replace = (
        0.1,
        0.001,
        5.0,
        min_ticks,
        min_ticks,
        0.0,
        0.0,
        PENDING_COALESCE,
    )
    values = _side(
        state=cpp.LivePlannerOrderState.Active,
        target=600_001,
        existing=600_000,
        age_ms=1_000.0,
    )
    result = cpp.compute_live_order_action_plan(
        (*_context()[:-1], 0.0),
        replace,
        values,
        _side(),
    )
    assert result.buy.needs_update is expected_needs_update
    assert result.buy.action == (
        cpp.LiveOrderAction.CancelFirst
        if expected_needs_update
        else cpp.LiveOrderAction.Keep
    )


def test_pending_new_maps_to_active_and_pending_like_order_manager() -> None:
    order = Order(
        client_order_id="ng-test",
        symbol="BTCUSDC",
        side=Side.BUY,
        price=60_000.0,
        quantity=0.001,
        state=OrderState.PENDING_NEW,
    )
    assert order.is_active
    pending_new = _side(
        state=cpp.LivePlannerOrderState.PendingNew,
        target=600_010,
        existing=600_000,
    )
    result = cpp.compute_live_order_action_plan(
        _context(), _replace(), pending_new, _side()
    )
    assert result.buy.order_active
    assert result.buy.order_pending
    assert result.buy.action == cpp.LiveOrderAction.Pending


def test_native_order_action_plan_caps_before_exchange_filters() -> None:
    result = cpp.compute_live_order_action_plan(
        _context(value_limit_lots=0),
        _replace(),
        _side(),
        _side(),
    )
    assert result.buy.target_quantity_lots == 0
    assert result.sell.target_quantity_lots == 0
    assert result.buy.action == cpp.LiveOrderAction.SkipFilter
    assert result.sell.action == cpp.LiveOrderAction.SkipFilter
    assert not result.buy.filter_valid
    assert not result.sell.filter_valid


def test_position_value_zero_and_infinity_match_b0_sentinels() -> None:
    zero = list(_context(inventory=-2, value_limit_lots=0))
    zero[2] = 0
    crossing = _side(desired=3, probe=3)
    zero_result = cpp.compute_live_order_action_plan(
        tuple(zero),
        _replace(),
        crossing,
        _side(),
    )
    assert zero_result.buy.exposure_increasing
    assert zero_result.buy.target_quantity_lots == 0

    unlimited = _checked_planner(max_position_value=float("inf")).compute(
        inventory=0.025,
        mid=60_000.0,
        now_ts=1_000.0,
        buy=_checked_boundary(price=60_000.0, quantity=0.001),
        sell=_checked_boundary(price=60_000.1, quantity=0.007),
    )
    assert unlimited.sell.target_quantity_lots == 7


def test_native_order_action_plan_is_fail_closed_on_invalid_integer_contract() -> None:
    context = list(_context())
    context[3] = 0
    result = cpp.compute_live_order_action_plan(
        tuple(context), _replace(), _side(), _side()
    )
    assert result.buy.action == cpp.LiveOrderAction.Invalid
    assert result.sell.action == cpp.LiveOrderAction.Invalid


def test_native_order_action_plan_matches_reference_over_frozen_lattice() -> None:
    rng = random.Random(0x4E474150)
    states = (
        cpp.LivePlannerOrderState.Empty,
        cpp.LivePlannerOrderState.PendingNew,
        cpp.LivePlannerOrderState.Active,
        cpp.LivePlannerOrderState.PendingCancel,
        cpp.LivePlannerOrderState.Terminal,
    )
    for _ in range(2_000):
        inventory = rng.randint(-32, 32)
        context = list(
            _context(
                inventory=inventory,
                value_limit_lots=rng.randint(1, 35),
            )
        )
        context[7] = rng.choice((0.0, 0.01, 0.1, 0.5))
        context = tuple(context)
        replace = (
            0.1,
            0.001,
            5.0,
            rng.choice((0.0, 1.0, 1.000000001, 2.0)),
            rng.choice((0.0, 1.0, 1.000000001, 2.0)),
            rng.choice((0.0, 500.0, 2_000.0)),
            rng.choice((0.0, 500.0, 2_000.0)),
            rng.choice((0, PENDING_COALESCE, PENDING_COALESCE | CANCEL_FIRST_EXPOSURE)),
        )

        def random_side() -> tuple[object, ...]:
            state = rng.choice(states)
            existing = (
                rng.randint(599_980, 600_020)
                if state in {
                    cpp.LivePlannerOrderState.PendingNew,
                    cpp.LivePlannerOrderState.Active,
                    cpp.LivePlannerOrderState.PendingCancel,
                }
                else 0
            )
            flags = rng.randint(0, 63)
            return _side(
                state=state,
                target=rng.randint(599_980, 600_020),
                desired=rng.randint(0, 5),
                probe=rng.randint(1, 3),
                existing=existing,
                remaining=rng.randint(0, 5),
                age_ms=rng.choice((0.0, 250.0, 1_000.0, 3_000.0)),
                ttl_ms=rng.choice((0.0, 500.0, 2_000.0)),
                flags=flags,
            )

        buy_values = random_side()
        sell_values = random_side()
        actual = cpp.compute_live_order_action_plan(
            context,
            replace,
            buy_values,
            sell_values,
        )
        _assert_plan(
            actual.buy,
            _reference_side("BUY", context, replace, buy_values),
        )
        _assert_plan(
            actual.sell,
            _reference_side("SELL", context, replace, sell_values),
        )
