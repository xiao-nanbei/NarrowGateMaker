from __future__ import annotations

import pytest

import strategy.inventory_manager as inventory_module
from strategy.inventory_manager import InventoryManager


def _fill(
    inventory: InventoryManager,
    side: str,
    quantity: float,
    price: float,
    commission: float,
    *,
    order_id: int,
    trade_id: int,
    cumulative: float,
    trade_time_ms: int,
) -> None:
    inventory.on_fill(
        side,
        quantity,
        price,
        commission,
        trade_time_ms,
        order_id=order_id,
        trade_id=trade_id,
        cumulative_filled_qty=cumulative,
    )


def _identity(
    *,
    side: str,
    order_id: int,
    quantity: float,
    price: float,
    commission: float,
    trade_time_ms: int,
    cumulative: float,
) -> dict[str, object]:
    return {
        "side": side,
        "order_id": order_id,
        "quantity": quantity,
        "price": price,
        "commission": commission,
        "trade_time_ms": trade_time_ms,
        "cumulative_filled_qty": cumulative,
    }


def test_flip_splits_fee_and_campaign_at_zero_boundary() -> None:
    inventory = InventoryManager()
    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        0.005,
        order_id=1,
        trade_id=11,
        cumulative=0.001,
        trade_time_ms=1_000,
    )
    old_campaign_id = inventory.campaign_snapshot().campaign_id

    _fill(
        inventory,
        "SELL",
        0.002,
        120.0,
        0.02,
        order_id=2,
        trade_id=12,
        cumulative=0.002,
        trade_time_ms=2_000,
    )

    assert inventory.snapshot.qty == pytest.approx(-0.001)
    assert inventory.snapshot.realized_pnl == pytest.approx(0.005)
    assert inventory._open_commission == pytest.approx(0.01)
    closing_fee = 0.02 * 0.001 / 0.002
    assert closing_fee + inventory._open_commission == pytest.approx(0.02)
    assert inventory._round_trip_rpnl == pytest.approx(0.0)
    campaign = inventory.campaign_snapshot()
    assert campaign.active
    assert campaign.campaign_id == old_campaign_id + 1
    assert campaign.side == "SHORT"
    assert campaign.fills == 1
    assert campaign.exposure_increasing_fills == 1
    assert campaign.reducing_fills == 0
    assert campaign.volume == pytest.approx(0.001)
    assert campaign.realized_pnl == pytest.approx(-0.01)
    assert campaign.total_pnl == pytest.approx(-0.01)
    assert campaign.adverse_excursion == pytest.approx(-0.01)
    assert inventory._campaign_last_terminal_reason == "flip"


def test_flip_preserves_signed_maker_rebate() -> None:
    inventory = InventoryManager()
    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        -0.005,
        order_id=1,
        trade_id=21,
        cumulative=0.001,
        trade_time_ms=1_000,
    )
    _fill(
        inventory,
        "SELL",
        0.002,
        120.0,
        -0.02,
        order_id=2,
        trade_id=22,
        cumulative=0.002,
        trade_time_ms=2_000,
    )

    assert inventory.snapshot.realized_pnl == pytest.approx(0.035)
    assert inventory._open_commission == pytest.approx(-0.01)
    assert inventory.campaign_snapshot().total_pnl == pytest.approx(0.01)


@pytest.mark.parametrize(
    ("commission", "expected_daily_pnl"),
    [(0.005, -0.005), (-0.005, 0.005)],
)
def test_open_commission_or_rebate_enters_marked_daily_pnl_immediately(
    commission: float,
    expected_daily_pnl: float,
) -> None:
    inventory = InventoryManager()
    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        commission,
        order_id=3,
        trade_id=23,
        cumulative=0.001,
        trade_time_ms=1_000,
    )

    assert inventory.daily_pnl == pytest.approx(expected_daily_pnl)


def test_utc_rollover_baselines_already_booked_open_commission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_s = 86_399.0
    monkeypatch.setattr(inventory_module.time, "time", lambda: now_s)
    inventory = InventoryManager()
    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        0.005,
        order_id=4,
        trade_id=24,
        cumulative=0.001,
        trade_time_ms=86_399_000,
    )
    assert inventory.daily_pnl == pytest.approx(-0.005)

    now_s = 86_401.0
    inventory.update_mark_price(100.0)
    assert inventory.daily_pnl == pytest.approx(0.0)

    _fill(
        inventory,
        "SELL",
        0.001,
        100.0,
        0.002,
        order_id=5,
        trade_id=25,
        cumulative=0.001,
        trade_time_ms=86_401_000,
    )
    assert inventory.daily_pnl == pytest.approx(-0.002)


def test_utc_rollover_preserves_session_loss_sequence_and_marked_high_water(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_s = 86_399.0
    monkeypatch.setattr(inventory_module.time, "time", lambda: now_s)
    inventory = InventoryManager()
    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        0.0,
        order_id=6,
        trade_id=26,
        cumulative=0.001,
        trade_time_ms=86_399_000,
    )
    inventory.update_mark_price(110.0)
    assert inventory.drawdown == pytest.approx(0.0)

    inventory.update_mark_price(90.0)
    assert inventory.drawdown == pytest.approx(0.02)
    _fill(
        inventory,
        "SELL",
        0.001,
        90.0,
        0.0,
        order_id=7,
        trade_id=27,
        cumulative=0.001,
        trade_time_ms=86_399_500,
    )
    assert inventory.consecutive_losses == 1
    assert inventory.drawdown == pytest.approx(0.02)

    now_s = 86_401.0
    inventory.update_mark_price(90.0)

    assert inventory.daily_pnl == pytest.approx(0.0)
    assert inventory.consecutive_losses == 1
    assert inventory.drawdown == pytest.approx(0.02)
    exposure = inventory.inventory_exposure_snapshot()
    assert exposure["session_marked_pnl"] == pytest.approx(-0.01)
    assert exposure["session_marked_high_water"] == pytest.approx(0.01)
    assert exposure["session_marked_drawdown"] == pytest.approx(0.02)


def test_equal_quantity_snapshot_installs_identity_barrier_and_deduplicates() -> None:
    inventory = InventoryManager()
    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        0.001,
        order_id=10,
        trade_id=31,
        cumulative=0.001,
        trade_time_ms=1_000,
    )

    inventory.sync_from_exchange(
        0.001,
        100.0,
        snapshot_update_time_ms=1_500,
        order_cumulative_filled_qty={10: 0.001},
        included_trade_ids={31},
        included_trade_identities={
            31: _identity(
                side="BUY",
                order_id=10,
                quantity=0.001,
                price=100.0,
                commission=0.001,
                trade_time_ms=1_000,
                cumulative=0.001,
            )
        },
    )
    barrier = inventory.reconciliation_snapshot()
    assert barrier["snapshot_update_time_ms"] == 1_500
    assert barrier["retained_post_snapshot_fill_count"] == 0

    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        0.001,
        order_id=10,
        trade_id=32,
        cumulative=0.001,
        trade_time_ms=1_000,
    )
    assert inventory.snapshot.qty == pytest.approx(0.001)
    assert inventory.snapshot.total_traded_volume == pytest.approx(0.001)


def test_snapshot_preserves_proven_post_snapshot_fill_until_cursor_catches_up() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={},
    )
    _fill(
        inventory,
        "BUY",
        0.001,
        101.0,
        0.001,
        order_id=20,
        trade_id=41,
        cumulative=0.001,
        trade_time_ms=1_100,
    )

    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={20: 0.0},
    )
    assert inventory.snapshot.qty == pytest.approx(0.001)
    assert inventory.snapshot.avg_entry_price == pytest.approx(101.0)
    assert inventory.reconciliation_snapshot()[
        "retained_post_snapshot_fill_count"
    ] == 1

    inventory.sync_from_exchange(
        0.001,
        101.0,
        snapshot_update_time_ms=1_200,
        order_cumulative_filled_qty={20: 0.001},
        included_trade_ids={41},
        included_trade_identities={
            41: _identity(
                side="BUY",
                order_id=20,
                quantity=0.001,
                price=101.0,
                commission=0.001,
                trade_time_ms=1_100,
                cumulative=0.001,
            )
        },
    )
    assert inventory.snapshot.qty == pytest.approx(0.001)
    assert inventory.reconciliation_snapshot()[
        "retained_post_snapshot_fill_count"
    ] == 0


def test_included_trade_id_cannot_replace_order_cursor_coverage() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={20: 0.0},
    )
    _fill(
        inventory,
        "BUY",
        0.001,
        101.0,
        0.001,
        order_id=20,
        trade_id=42,
        cumulative=0.001,
        trade_time_ms=1_100,
    )

    with pytest.raises(RuntimeError, match="without an order cumulative cursor"):
        inventory.sync_from_exchange(
            0.0,
            0.0,
            snapshot_update_time_ms=1_000,
            order_cumulative_filled_qty={20: 0.0},
            included_trade_ids={42},
            included_trade_identities={
                42: _identity(
                    side="BUY",
                    order_id=20,
                    quantity=0.001,
                    price=101.0,
                    commission=0.001,
                    trade_time_ms=1_100,
                    cumulative=0.001,
                )
            },
        )

    assert inventory.snapshot.qty == pytest.approx(0.001)
    assert inventory.reconciliation_snapshot()["snapshot_update_time_ms"] == 1_000


def test_cumulative_fill_must_start_at_proven_order_cursor() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.001,
        100.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={30: 0.001},
    )

    with pytest.raises(RuntimeError, match="not contiguous"):
        _fill(
            inventory,
            "BUY",
            0.002,
            110.0,
            0.02,
            order_id=30,
            trade_id=51,
            cumulative=0.002,
            trade_time_ms=1_100,
        )

    assert inventory.snapshot.qty == pytest.approx(0.001)
    assert inventory._total_commission == pytest.approx(0.0)


def test_first_fill_with_unknown_cumulative_prefix_fails_closed() -> None:
    inventory = InventoryManager()

    with pytest.raises(RuntimeError, match="not contiguous"):
        _fill(
            inventory,
            "BUY",
            0.001,
            100.0,
            0.001,
            order_id=31,
            trade_id=52,
            cumulative=0.002,
            trade_time_ms=1_100,
        )

    assert inventory.snapshot.qty == pytest.approx(0.0)
    assert inventory.reconciliation_snapshot()[
        "local_order_cumulative_filled_qty"
    ] == {}


def test_ambiguous_legacy_fill_fails_closed_at_identity_barrier() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={},
    )
    with pytest.raises(RuntimeError, match="lacks order cumulative identity"):
        inventory.on_fill(
            "BUY",
            0.001,
            100.0,
            trade_time_ms=1_000,
        )

    with pytest.raises(RuntimeError, match="lacks order cumulative identity"):
        inventory.on_fill(
            "BUY",
            0.001,
            100.0,
            trade_time_ms=1_100,
        )


def test_identified_fill_at_snapshot_time_requires_snapshot_order_cursor() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={},
    )

    with pytest.raises(RuntimeError, match="lacks its snapshot order cursor"):
        _fill(
            inventory,
            "BUY",
            0.001,
            100.0,
            0.001,
            order_id=99,
            trade_id=999,
            cumulative=0.001,
            trade_time_ms=1_000,
        )
    assert inventory.snapshot.qty == pytest.approx(0.0)
    assert inventory.reconciliation_snapshot()["tracked_trade_identity_count"] == 0


@pytest.mark.parametrize(
    ("field", "drifted_value"),
    [
        ("side", "SELL"),
        ("qty", 0.0005),
        ("price", 101.0),
        ("commission", -0.001),
        ("trade_time_ms", 1_001),
        ("order_id", 2),
        ("cumulative_filled_qty", 0.002),
    ],
)
def test_duplicate_trade_id_requires_exact_immutable_identity(
    field: str,
    drifted_value: object,
) -> None:
    inventory = InventoryManager()
    fill = {
        "side": "BUY",
        "qty": 0.001,
        "price": 100.0,
        "commission": 0.001,
        "trade_time_ms": 1_000,
        "order_id": 1,
        "trade_id": 81,
        "cumulative_filled_qty": 0.001,
    }
    assert inventory.on_fill(**fill) == pytest.approx(0.001)
    assert inventory.on_fill(**fill) == pytest.approx(0.0)

    drifted = dict(fill)
    drifted[field] = drifted_value
    with pytest.raises(RuntimeError, match="changed exact fill identity"):
        inventory.on_fill(**drifted)
    assert inventory.snapshot.qty == pytest.approx(0.001)
    assert inventory.snapshot.total_traded_volume == pytest.approx(0.001)


def test_trade_identity_is_not_evicted_after_long_process_lifetime() -> None:
    inventory = InventoryManager()
    quantity = 0.000001
    for offset in range(8_300):
        inventory.on_fill(
            "BUY",
            quantity,
            100.0,
            0.0,
            offset + 1,
            order_id=900,
            trade_id=100_000 + offset,
            cumulative_filled_qty=(offset + 1) * quantity,
        )
    before = inventory.snapshot

    applied = inventory.on_fill(
        "BUY",
        quantity,
        100.0,
        0.0,
        1,
        order_id=900,
        trade_id=100_000,
        cumulative_filled_qty=quantity,
    )

    assert applied == pytest.approx(0.0)
    assert inventory.snapshot.qty == pytest.approx(before.qty)
    assert inventory.snapshot.total_traded_volume == pytest.approx(
        before.total_traded_volume
    )
    assert inventory.reconciliation_snapshot()[
        "tracked_trade_identity_count"
    ] == 8_300


def test_snapshot_cannot_omit_a_previously_bound_order_cursor() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.001,
        100.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={45: 0.001},
    )

    with pytest.raises(RuntimeError, match="omitted a previously bound"):
        inventory.sync_from_exchange(
            0.001,
            100.0,
            snapshot_update_time_ms=1_100,
            order_cumulative_filled_qty={},
        )
    assert inventory.reconciliation_snapshot()["snapshot_update_time_ms"] == 1_000


def test_pre_snapshot_identified_fill_requires_snapshot_cursor() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={},
    )
    _fill(
        inventory,
        "BUY",
        0.001,
        101.0,
        0.001,
        order_id=46,
        trade_id=72,
        cumulative=0.001,
        trade_time_ms=1_050,
    )

    with pytest.raises(RuntimeError, match="omitted the identity cursor"):
        inventory.sync_from_exchange(
            0.001,
            101.0,
            snapshot_update_time_ms=1_100,
            order_cumulative_filled_qty={},
        )
    assert inventory.reconciliation_snapshot()["snapshot_update_time_ms"] == 1_000


def test_noninitial_snapshot_requires_authoritative_trade_pipeline_first() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={},
    )

    trade_identity = _identity(
        side="BUY",
        order_id=50,
        quantity=0.001,
        price=101.0,
        commission=-0.001,
        trade_time_ms=1_100,
        cumulative=0.001,
    )
    with pytest.raises(RuntimeError, match="unknown local trade identity"):
        inventory.sync_from_exchange(
            0.001,
            101.0,
            snapshot_update_time_ms=1_200,
            order_cumulative_filled_qty={50: 0.001},
            included_trade_ids={71},
            included_trade_identities={71: trade_identity},
        )
    assert inventory.reconciliation_snapshot()["snapshot_update_time_ms"] == 1_000
    assert inventory.sync_adjust_snapshot()["seq"] == 0

    # OrderManager's REST reconciliation path delivers this through the same
    # fill callback as websocket execution reports before barrier installation.
    _fill(
        inventory,
        "BUY",
        0.001,
        101.0,
        -0.001,
        order_id=50,
        trade_id=71,
        cumulative=0.001,
        trade_time_ms=1_100,
    )
    result = inventory.sync_from_exchange(
        0.001,
        101.0,
        snapshot_update_time_ms=1_200,
        order_cumulative_filled_qty={50: 0.001},
        included_trade_ids={71},
        included_trade_identities={71: trade_identity},
    )

    assert result["seeded"] is False
    assert result["position_changed"] is False
    assert inventory.snapshot.qty == pytest.approx(0.001)
    assert inventory._total_commission == pytest.approx(-0.001)
    assert inventory.campaign_snapshot().fills == 1
    assert inventory.campaign_snapshot().total_pnl == pytest.approx(0.001)
    assert inventory.sync_adjust_snapshot()["seq"] == 0


def test_net_zero_cursor_advances_require_locally_applied_exact_trades() -> None:
    inventory = InventoryManager()
    inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={},
    )
    identities = {
        1: _identity(
            side="BUY",
            order_id=10,
            quantity=0.001,
            price=100.0,
            commission=0.001,
            trade_time_ms=1_100,
            cumulative=0.001,
        ),
        2: _identity(
            side="SELL",
            order_id=11,
            quantity=0.001,
            price=101.0,
            commission=-0.001,
            trade_time_ms=1_150,
            cumulative=0.001,
        ),
    }

    with pytest.raises(RuntimeError, match="unknown local trade identity"):
        inventory.sync_from_exchange(
            0.0,
            0.0,
            snapshot_update_time_ms=1_200,
            order_cumulative_filled_qty={10: 0.001, 11: 0.001},
            included_trade_ids={1, 2},
            included_trade_identities=identities,
        )
    assert inventory.reconciliation_snapshot()["snapshot_update_time_ms"] == 1_000
    assert inventory.reconciliation_snapshot()[
        "order_cumulative_filled_qty"
    ] == {}

    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        0.001,
        order_id=10,
        trade_id=1,
        cumulative=0.001,
        trade_time_ms=1_100,
    )
    _fill(
        inventory,
        "SELL",
        0.001,
        101.0,
        -0.001,
        order_id=11,
        trade_id=2,
        cumulative=0.001,
        trade_time_ms=1_150,
    )
    assert inventory.snapshot.qty == pytest.approx(0.0)

    result = inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_200,
        order_cumulative_filled_qty={10: 0.001, 11: 0.001},
        included_trade_ids={1, 2},
        included_trade_identities=identities,
    )
    assert result["seeded"] is False
    assert inventory.reconciliation_snapshot()["snapshot_update_time_ms"] == 1_200
    assert inventory.snapshot.total_traded_volume == pytest.approx(0.002)
    assert inventory._total_commission == pytest.approx(0.0)


def test_sync_crosses_zero_without_counting_a_fill() -> None:
    inventory = InventoryManager()
    _fill(
        inventory,
        "BUY",
        0.001,
        100.0,
        0.001,
        order_id=40,
        trade_id=61,
        cumulative=0.001,
        trade_time_ms=1_000,
    )
    old_campaign_id = inventory.campaign_snapshot().campaign_id

    inventory.sync_from_exchange(
        -0.001,
        120.0,
        snapshot_update_time_ms=2_000,
        order_cumulative_filled_qty={40: 0.001},
        included_trade_ids={61},
        included_trade_identities={
            61: _identity(
                side="BUY",
                order_id=40,
                quantity=0.001,
                price=100.0,
                commission=0.001,
                trade_time_ms=1_000,
                cumulative=0.001,
            )
        },
    )

    campaign = inventory.campaign_snapshot()
    assert campaign.campaign_id == old_campaign_id + 1
    assert campaign.side == "SHORT"
    assert campaign.fills == 0
    assert campaign.volume == pytest.approx(0.0)
    assert inventory._campaign_last_terminal_reason == "flip"
