import pandas as pd

from research.families.f08_side_taker_lifecycle.audit.event_identity_and_riskset_v2 import (
    aggregate_lifecycle_audits,
    audit_lifecycle_events,
)
from models.audit.order_lifecycle import OrderLifecycleRecorder


def _order(order_id: int, side: str) -> dict:
    return {
        "trace_id": order_id,
        "side": side,
        "price": 100.0 if side == "BUY" else 100.2,
        "quantity": 0.001,
        "remaining": 0.001,
        "inventory_at_submit": 0.0,
        "inventory_role_at_submit": "opener",
        "campaign_id_at_submit": 0,
        "state": "PENDING_NEW",
        "fill_eligible": True,
    }


def test_complete_start_stop_identity_passes_and_excludes_same_ms_jump() -> None:
    recorder = OrderLifecycleRecorder(
        symbol="BTCUSDC",
        lot_size=0.001,
        tick_size=0.1,
        price_jump_ticks=1.0,
        max_orders=10,
    )
    buy = _order(1, "BUY")
    sell = _order(2, "SELL")
    for order in (buy, sell):
        recorder.submit(order, 1_000)
        order["state"] = "OPEN"
        recorder.activate(order, 1_010, mid=100.1)
        recorder.bind_campaign(order, 1)

    recorder.sync_repair_state(
        1_015,
        campaign_id=1,
        campaign_active=True,
        inventory=0.001,
        active_orders=[buy, sell],
    )
    recorder.native_mid(
        1_020_000_000,
        99.9,
        segment_id=1,
        same_ms_ordering_resolved=False,
    )
    buy["remaining"] = 0.0
    recorder.fill(
        buy,
        1_020,
        fill_qty=0.001,
        remaining_before=0.001,
        remaining_after=0.0,
        fill_price=100.0,
        inventory_before=0.0,
        inventory_after=0.001,
        campaign_id=1,
    )
    recorder.campaign_repair(1, 1_030)
    sell["state"] = "PENDING_CANCEL"
    recorder.request_cancel(sell, 1_040, reason="requote")
    recorder.cancel_ack(sell, 1_045, reason="requote")

    intervals, summary = audit_lifecycle_events(
        pd.DataFrame(recorder.events()),
        require_native_book=True,
    )

    assert summary["status"] == "passed"
    assert summary["unresolved_native_same_ms_jump_rows"] == 1
    assert summary["formal_fill_hazard_interval_rows"] > 0
    jump_intervals = intervals[intervals["event_type"] == "native_price_jump"]
    assert jump_intervals["formal_fill_hazard_eligible"].eq(0).all()
    assert summary["formal_repair_hazard_interval_rows"] > 0

    events = pd.DataFrame(recorder.events())
    second_day = events.copy()
    second_day["day"] = "1970-01-02"
    cross_intervals, cross_summary = audit_lifecycle_events(
        pd.concat([events, second_day], ignore_index=True),
        require_native_book=True,
    )
    assert len(cross_intervals) == 2 * len(intervals)
    assert cross_summary["orders"] == 4
    assert cross_summary["status"] == "passed"

    summary["partition_day"] = "1970-01-01"
    second_summary = dict(summary)
    second_summary["partition_day"] = "1970-01-02"
    aggregate = aggregate_lifecycle_audits([summary, second_summary])
    assert aggregate["partition_count"] == 2
    assert aggregate["rows"] == 2 * summary["rows"]
    assert aggregate["orders"] == 2 * summary["orders"]
    assert aggregate["status"] == "passed"
