import numpy as np
import pandas as pd

from models.audit.order_lifecycle import (
    OrderLifecycleRecorder,
    build_order_risk_intervals,
)
from models.backtest_tick import simulate_tick
from models.tick_data_types import HistoricalBBOData


def _order(order_id: int, side: str = "BUY", quantity: float = 0.002) -> dict:
    return {
        "trace_id": order_id,
        "side": side,
        "price": 100.0 if side == "BUY" else 100.2,
        "quantity": quantity,
        "remaining": quantity,
        "inventory_at_submit": 0.0,
        "inventory_role_at_submit": "opener",
        "campaign_id_at_submit": 0,
        "state": "PENDING_NEW",
        "fill_eligible": True,
    }


def _recorder(max_orders: int = 10) -> OrderLifecycleRecorder:
    return OrderLifecycleRecorder(
        symbol="BTCUSDC",
        lot_size=0.001,
        tick_size=0.1,
        price_jump_ticks=1.0,
        max_orders=max_orders,
    )


def test_placement_start_stop_profile_omits_dynamic_events() -> None:
    recorder = OrderLifecycleRecorder(
        symbol="BTCUSDC",
        lot_size=0.001,
        tick_size=0.1,
        price_jump_ticks=1.0,
        max_orders=10,
        event_profile="placement_start_stop",
    )
    order = _order(1)
    recorder.submit(order, 1_000)
    recorder.activate(order, 1_010, mid=100.0)
    recorder.risk_snapshot(
        order,
        1_100,
        feature_source_ts_ns=1_100_000_000,
        feature_ready_ts_ns=1_100_000_000,
        inventory_role="opener",
        inventory=0.0,
        campaign_id=0,
        mid=99.9,
        microprice=99.9,
        top_size=1.0,
        features={},
    )
    recorder.native_mid(
        1_100_000_000,
        99.8,
        segment_id=1,
        same_ms_ordering_resolved=True,
    )
    recorder.sync_repair_state(
        1_100,
        campaign_id=1,
        campaign_active=True,
        inventory=0.001,
        active_orders=[order],
    )
    recorder.request_cancel(order, 1_200, reason="requote")
    recorder.cancel_ack(order, 1_210, reason="requote")

    events = pd.DataFrame(recorder.events())
    assert events["event_profile"].eq("placement_start_stop").all()
    assert events["event_type"].tolist() == [
        "submit",
        "activate",
        "cancel_request",
        "cancel_ack",
    ]


def test_lifecycle_submit_preserves_system_order_identity() -> None:
    recorder = _recorder()
    order = _order(2, side="SELL")
    order["reduce_only"] = True
    order["circuit_breaker_close"] = True
    recorder.submit(order, 1_000)
    submit = recorder.events()[0]
    assert submit["reduce_only"] == 1
    assert submit["circuit_breaker_close"] == 1


def _legacy_risk_intervals(events: pd.DataFrame) -> pd.DataFrame:
    identity = ["day", "order_id"]
    ordered = events.sort_values(
        [*identity, "event_ts_ns", "event_seq"],
        kind="mergesort",
    )
    rows = []
    for _, group in ordered.groupby(identity, sort=False):
        records = group.to_dict("records")
        for index, current in enumerate(records[:-1]):
            following = records[index + 1]
            state = str(current["state_after"])
            remaining = float(current.get("remaining_qty", 0.0) or 0.0)
            end_ns = int(following["event_ts_ns"])
            row = dict(current)
            row.update(
                {
                    "schema_version": "local_order_risk_interval.v2",
                    "risk_interval_start_ts_ns": int(
                        current["event_ts_ns"]
                    ),
                    "risk_interval_start_event_seq": int(
                        current["event_seq"]
                    ),
                    "risk_interval_end_ts_ns": end_ns,
                    "risk_interval_end_event_seq": int(
                        following["event_seq"]
                    ),
                    "interval_ms": max(
                        0.0,
                        (
                            end_ns - int(current["event_ts_ns"])
                        )
                        / 1_000_000.0,
                    ),
                    "next_event_type": str(following["event_type"]),
                    "next_event_ts_ns": end_ns,
                    "next_event_seq": int(following["event_seq"]),
                    "fill_at_risk": int(
                        state in {"open", "pending_cancel"}
                        and remaining > 0.0
                    ),
                    "cancel_at_risk": int(
                        state in {"pending_new", "open", "pending_cancel"}
                        and remaining > 0.0
                    ),
                    "jump_at_risk": int(
                        state in {"open", "pending_cancel"}
                        and remaining > 0.0
                    ),
                    "repair_at_risk": int(
                        bool(current.get("repair_at_risk", 0))
                    ),
                    "censor_ts_ns": end_ns,
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


def test_partial_fill_during_pending_cancel_preserves_start_stop_path() -> None:
    recorder = _recorder()
    order = _order(1)
    recorder.submit(order, 1_000)
    order["state"] = "OPEN"
    recorder.activate(order, 1_010, mid=100.1)
    order["state"] = "PENDING_CANCEL"
    recorder.request_cancel(order, 1_020, reason="requote")
    order["remaining"] = 0.001
    recorder.fill(
        order,
        1_025,
        fill_qty=0.001,
        remaining_before=0.002,
        remaining_after=0.001,
        fill_price=100.0,
        inventory_before=0.0,
        inventory_after=0.001,
        campaign_id=1,
    )
    recorder.cancel_ack(order, 1_030, reason="requote")

    events = pd.DataFrame(recorder.events())
    assert events["event_type"].tolist() == [
        "submit",
        "activate",
        "cancel_request",
        "partial_fill",
        "cancel_ack",
    ]
    partial = events.loc[events["event_type"] == "partial_fill"].iloc[0]
    assert partial["state_after"] == "pending_cancel"
    assert partial["fill_while_cancel_pending_qty"] == 0.001
    ack = events.loc[events["event_type"] == "cancel_ack"].iloc[0]
    assert ack["remaining_qty_at_cancel_ack"] == 0.001

    intervals = build_order_risk_intervals(events)
    pending = intervals.loc[intervals["state_after"] == "pending_cancel"]
    assert len(pending) == 2
    assert pending["fill_at_risk"].eq(1).all()

    second_day = events.copy()
    second_day["day"] = "1970-01-02"
    cross_day = build_order_risk_intervals(
        pd.concat([events, second_day], ignore_index=True)
    )
    assert len(cross_day) == 2 * len(intervals)


def test_vectorized_risk_intervals_match_legacy_rows_and_dtypes() -> None:
    recorder = _recorder()
    for order_id, side in ((30, "BUY"), (31, "SELL")):
        order = _order(order_id, side=side)
        recorder.submit(order, 1_000)
        order["state"] = "OPEN"
        recorder.activate(order, 1_010, mid=100.1)
        recorder.request_cancel(order, 1_020, reason="requote")
        recorder.cancel_ack(order, 1_030, reason="requote")
    events = pd.DataFrame(recorder.events())

    actual = build_order_risk_intervals(events)
    expected = _legacy_risk_intervals(events)

    pd.testing.assert_frame_equal(actual, expected)


def test_native_jump_is_nonabsorbing_and_same_ms_fill_is_ambiguous() -> None:
    recorder = _recorder()
    order = _order(2, quantity=0.001)
    recorder.submit(order, 2_000)
    order["state"] = "OPEN"
    recorder.activate(order, 2_010, mid=100.1)
    recorder.native_mid(
        2_020_000_000,
        99.9,
        segment_id=3,
        same_ms_ordering_resolved=False,
    )
    order["remaining"] = 0.0
    recorder.fill(
        order,
        2_020,
        fill_qty=0.001,
        remaining_before=0.001,
        remaining_after=0.0,
        fill_price=100.0,
        inventory_before=0.0,
        inventory_after=0.001,
        campaign_id=1,
    )

    events = pd.DataFrame(recorder.events())
    assert events["event_type"].tolist()[-2:] == [
        "native_price_jump",
        "full_fill",
    ]
    same_ms = events[events["event_ts_ns"] == 2_020_000_000]
    assert same_ms["same_ms_ordering_resolved"].eq(0).all()
    assert same_ms["same_ms_cross_stream_ambiguity"].eq(1).all()


def test_events_no_copy_view_matches_default_copy() -> None:
    recorder = _recorder()
    order = _order(20)
    recorder.submit(order, 2_100)
    order["state"] = "OPEN"
    recorder.activate(order, 2_110, mid=100.1)

    copied = recorder.events()
    direct = recorder.events(copy_rows=False)

    assert copied == direct
    assert copied is not direct
    assert copied[0] is not direct[0]


def test_campaign_repair_uses_delayed_entry() -> None:
    recorder = _recorder()
    opener = _order(3, side="BUY", quantity=0.001)
    reducing = _order(4, side="SELL", quantity=0.001)
    for order in (opener, reducing):
        recorder.submit(order, 3_000)
        order["state"] = "OPEN"
        recorder.activate(order, 3_010, mid=100.1)
        recorder.bind_campaign(order, 1)

    recorder.sync_repair_state(
        3_015,
        campaign_id=1,
        campaign_active=True,
        inventory=0.001,
        active_orders=[opener],
    )
    recorder.sync_repair_state(
        3_020,
        campaign_id=1,
        campaign_active=True,
        inventory=0.001,
        active_orders=[opener, reducing],
    )
    recorder.campaign_repair(1, 3_030)

    events = pd.DataFrame(recorder.events())
    enter = events[events["event_type"] == "repair_risk_enter"]
    repair = events[events["event_type"] == "campaign_repair"]
    assert len(enter) == 2
    assert enter["event_ts_ns"].eq(3_020_000_000).all()
    assert len(repair) == 2
    assert repair["repair_ts_ns"].eq(3_030_000_000).all()


def test_campaign_repair_accepts_replay_integer_order_state() -> None:
    recorder = _recorder()
    opener = _order(5, side="BUY", quantity=0.001)
    reducing = _order(6, side="SELL", quantity=0.001)
    for order in (opener, reducing):
        recorder.submit(order, 4_000)
        order["state"] = 1
        recorder.activate(order, 4_010, mid=100.1)
        recorder.bind_campaign(order, 2)

    recorder.sync_repair_state(
        4_020,
        campaign_id=2,
        campaign_active=True,
        inventory=0.001,
        active_orders=[opener, reducing],
    )

    events = pd.DataFrame(recorder.events())
    enter = events[events["event_type"] == "repair_risk_enter"]
    assert len(enter) == 2
    assert enter["event_ts_ns"].eq(4_020_000_000).all()


def test_unchanged_repair_state_skips_history_scan_but_new_member_enters() -> None:
    recorder = _recorder()
    opener = _order(7, side="BUY", quantity=0.001)
    reducing = _order(8, side="SELL", quantity=0.001)
    for order in (opener, reducing):
        recorder.submit(order, 5_000)
        order["state"] = "OPEN"
        recorder.activate(order, 5_010, mid=100.1)
        recorder.bind_campaign(order, 3)

    state = {
        "campaign_id": 3,
        "campaign_active": True,
        "inventory": 0.001,
        "active_orders": [opener, reducing],
    }
    recorder.sync_repair_state(5_020, **state)
    recorder.sync_repair_state(5_025, **state)

    late_order = _order(9, side="BUY", quantity=0.001)
    recorder.submit(late_order, 5_030)
    late_order["state"] = "OPEN"
    recorder.activate(late_order, 5_031, mid=100.1)
    recorder.bind_campaign(late_order, 3)
    state["active_orders"].append(late_order)
    recorder.sync_repair_state(5_035, **state)

    events = pd.DataFrame(recorder.events())
    enter = events[events["event_type"] == "repair_risk_enter"]
    assert len(enter) == 3
    enter_times = {
        int(row.order_id): int(row.event_ts_ns)
        for row in enter.itertuples(index=False)
    }
    assert enter_times == {
        7: 5_020_000_000,
        8: 5_020_000_000,
        9: 5_035_000_000,
    }


def test_authoritative_replay_emits_lifecycle_trace() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": np.arange(0, 5_000, 1_000, dtype=np.int64),
            "price": np.full(5, 100.0),
            "quantity": np.zeros(5),
            "is_buyer_maker": np.ones(5, dtype=np.uint8),
        }
    )
    bbo_ts = np.arange(0, 4_501, 500, dtype=np.int64)
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts,
        best_bid=np.full(bbo_ts.size, 99.9),
        best_ask=np.full(bbo_ts.size, 100.1),
        bid_qty=np.full(bbo_ts.size, 1.0),
        ask_qty=np.full(bbo_ts.size, 1.0),
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
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "trace_local_order_lifecycle_max": 20,
        "local_order_value_price_jump_ticks": 1.0,
    }

    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        bbo_data=bbo,
    )
    lifecycle = pd.DataFrame(result["_local_order_lifecycle_trace"])

    assert not lifecycle.empty
    assert {"submit", "activate", "cancel_request", "cancel_ack"}.issubset(
        set(lifecycle["event_type"])
    )
    snapshots = lifecycle[lifecycle["event_type"].eq("risk_snapshot")]
    assert not snapshots.empty
    assert snapshots["feature_source_ts_ns"].le(
        snapshots["feature_ready_ts_ns"]
    ).all()
    assert snapshots["feature_ready_ts_ns"].le(
        snapshots["event_ts_ns"]
    ).all()
    assert lifecycle.groupby("order_id")["event_seq"].apply(
        lambda values: values.is_monotonic_increasing
    ).all()
