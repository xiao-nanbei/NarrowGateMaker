import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models.backtest_tick import (
    _advance_monotonic_visibility_cutoff,
    _exec_book_visibility_delay_ms,
    _load_exec_book_visibility_profile,
    _load_exec_source_stratified_visibility_profile,
    _load_live_requote_clock,
    _paired_exec_book_visibility_state,
    _paired_exec_source_visible_ts,
    _sampled_joint_exec_book_visibility_state,
    _source_stratified_exec_visibility_state,
)
from models.exchange_book_replay import (
    HistoricalMessageDeliverySchedule,
    ReceiveTimeCooldownReplayAdapter,
)
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data


def test_message_delivery_holds_later_low_latency_until_predecessor() -> None:
    exchange = np.asarray([100, 200, 300], dtype=np.int64)
    proposed = np.asarray([1_000, 210, 310], dtype=np.int64)
    schedule = HistoricalMessageDeliverySchedule(exchange, exchange + 5, proposed)

    assert schedule.ready_ns_for_channel().tolist() == [1_000, 1_000, 1_000]
    assert schedule.exchange_ns_for_channel().tolist() == exchange.tolist()
    assert schedule.latest_visible_index(999) == -1
    assert schedule.latest_visible_index(1_000) == -1
    assert schedule.latest_visible_index(1_000, inclusive=True) == 2
    assert schedule.latest_visible_index(1_001) == 2
    assert schedule.latest_visible_index(999) == -1
    assert proposed.tolist() == [1_000, 210, 310]
    assert schedule.stats_dict()["head_of_line_clamped_events"] == 2
    assert schedule.stats_dict()["max_head_of_line_delay_ns"] == 790


@pytest.mark.parametrize("shared_connection", [False, True])
def test_message_delivery_channel_local_index_and_shared_connection(shared_connection) -> None:
    schedule = HistoricalMessageDeliverySchedule(
        [100, 200, 300, 400],
        [101, 201, 301, 401],
        [1_000, 210, 310, 410],
        channel_ids=["bbo", "depth", "bbo", "depth"],
        connection_ids=["public"] * 4 if shared_connection else None,
    )

    assert schedule.latest_visible_index(500, channel="bbo") == -1
    assert schedule.latest_visible_index(500, channel="depth") == (-1 if shared_connection else 1)
    assert schedule.latest_visible_index(1_001, channel="bbo") == 1
    assert schedule.latest_visible_index(1_001, channel="depth") == 1
    assert schedule.exchange_ns_for_channel("depth").tolist() == [200, 400]
    assert schedule.ready_ns_for_channel("depth").tolist() == (
        [1_000, 1_000] if shared_connection else [210, 410]
    )
    with pytest.raises(ValueError, match="channel is required"):
        schedule.latest_visible_index(1_001)


def test_message_delivery_preserves_submillisecond_boundary_and_immutable_inputs() -> None:
    exchange = np.asarray([1_000_000], dtype=np.int64)
    schedule = HistoricalMessageDeliverySchedule(exchange, [1_000_001], [1_000_003])
    exchange[0] = 0

    assert schedule.exchange_ns_for_channel().tolist() == [1_000_000]
    assert schedule.latest_visible_index(1_000_000) == -1
    assert schedule.latest_visible_index(1_000_003) == -1
    assert schedule.latest_visible_index(1_000_003, inclusive=True) == 0
    assert schedule.latest_visible_index(1_000_004) == 0
    with pytest.raises(ValueError):
        schedule.ready_ns_for_channel()[0] = 0
    with pytest.raises(ValueError):
        schedule.ready_ns_for_channel().setflags(write=True)
    with pytest.raises(ValueError, match="integer nanosecond"):
        schedule.latest_visible_index(1_000_003.5)


@pytest.mark.parametrize(
    ("exchange", "receive", "ready", "error"),
    [
        ([2], [1], [3], "exchange <= receive <= ready"),
        ([1], [3], [2], "exchange <= receive <= ready"),
        ([1], [-1], [2], "nonnegative int64"),
        ([1.5], [2], [3], "integer ns array"),
        ([1], [2, 3], [4], "must be aligned"),
        ([[1]], [2], [3], "one-dimensional"),
    ],
)
def test_message_delivery_rejects_invalid_aligned_physical_clocks(exchange, receive, ready, error):
    with pytest.raises(ValueError, match=error):
        HistoricalMessageDeliverySchedule(exchange, receive, ready)


def test_message_delivery_rejects_cross_connection_reordering_within_channel() -> None:
    with pytest.raises(ValueError, match="channel delivery order regressed"):
        HistoricalMessageDeliverySchedule(
            [100, 200], [110, 210], [1_000, 220], connection_ids=["old", "new"]
        )


def test_message_delivery_empty_source_has_no_visible_row() -> None:
    schedule = HistoricalMessageDeliverySchedule([], [], [])
    assert schedule.latest_visible_index(0) == -1
    assert schedule.latest_visible_index(1_000, inclusive=True) == -1
    assert schedule.stats_dict()["message_count"] == 0


class _ReceiveTimeTestPolicy:
    def __init__(self):
        from strategy.boolean_cooldown_buy_e3 import ReceiveTimeFullMidEmaWindows

        self.windows = ReceiveTimeFullMidEmaWindows(warmup_s=2048, max_feature_age_s=5)
        self.callbacks = []
        self.calls = []

    def observe_depth(self, **kwargs):
        self.callbacks.append(kwargs)
        self.windows.observe_depth(**kwargs)

    def evaluate(self, **kwargs):
        self.calls.append(kwargs)
        _, reason, _, _ = self.windows.feature_row(decision_ts_ns=kwargs["decision_ts_ns"])
        return SimpleNamespace(
            action_id="CONTROL_85N", duration_ms=kwargs["baseline_duration_ms"],
            fallback_reason=reason, matched_rule_index=None, support_valid=False,
            policy_sha256="synthetic-policy", predicate_bundle_sha256="synthetic-predicates",
        )

    def audit(self):
        return self.windows.audit()


def _receive_time_adapter_fixture():
    source_ms = np.asarray([10, 20, 30, 40], dtype=np.int64)
    depth = HistoricalL2Data(
        ts_ms=source_ms, bid_px=np.array([[99.], [100.], [101.], [102.]]),
        ask_px=np.array([[101.], [102.], [103.], [104.]]),
        bid_qty=np.ones((4, 1)), ask_qty=np.ones((4, 1)),
    )
    # First two packets belong to one receive bucket despite different source
    # buckets/delays. Callback ready is not the receive aggregation clock.
    schedule = HistoricalMessageDeliverySchedule(
        source_ms * 1_000_000, np.array([110, 120, 220, 215]) * 1_000_000,
        np.array([150, 200, 300, 250]) * 1_000_000,
    )
    policies = {side: _ReceiveTimeTestPolicy() for side in ("BUY", "SELL")}
    return ReceiveTimeCooldownReplayAdapter(depth, schedule, policies=policies), policies


def _capture_receive_time(adapter, cutoff_ms, side="BUY", baseline_ms=170000):
    cutoff_ns = cutoff_ms * 1_000_000
    return adapter.capture_exposure_fill(
        assignment_id=f"fill-{cutoff_ms}-{side}", fill_exchange_ts_ns=cutoff_ns - 1,
        fill_visible_ts_ns=cutoff_ns,
        m0_context={"fill_visible_ts_ns": cutoff_ns, "side": side,
                    "baseline_duration_ms": baseline_ms, "campaign_age_s": 2.5},
    )


def test_receive_time_policy_streams_callbacks_once_and_preserves_live_fallback():
    adapter, policies = _receive_time_adapter_fixture()
    first = _capture_receive_time(adapter, 200)
    assert len(policies["BUY"].callbacks) == len(policies["SELL"].callbacks) == 1
    assert first.fallback_reason == "no_completed_receive_time_window"
    assert not first.policy_input_valid
    assert adapter.evaluate(first, 170000).duration_ms == 170000
    assert first.source_bundle_sha256 == ""

    second = _capture_receive_time(adapter, 300, side="SELL")
    assert len(policies["BUY"].callbacks) == 2  # same-time ready rows excluded
    assert second.fallback_reason == "no_completed_receive_time_window"
    third = _capture_receive_time(adapter, 301)
    assert len(policies["BUY"].callbacks) == len(policies["SELL"].callbacks) == 4
    assert [row["receive_ts_ns"] for row in policies["BUY"].callbacks] == [
        110_000_000, 120_000_000, 220_000_000, 220_000_000,
    ]
    assert third.fallback_reason == "receive_time_ema_warmup_incomplete"
    # Source timestamps are all below 100ms, but the actual receive windows
    # have closed the 100–200ms bucket using the final 120ms mid (101).
    assert policies["BUY"].windows.audit()["completed_windows"] == 1
    assert policies["BUY"].windows.audit()["feature_ready_ts_ns"] == 220_000_000
    frozen = adapter.evaluate(first, 170000)
    assert frozen.fallback_reason == "no_completed_receive_time_window"
    assert len(policies["BUY"].calls) == 2  # evaluate does not rerun policy
    assert adapter.audit()["receive_head_of_line_clamped_events"] == 1
    assert adapter.audit()["depth_callbacks_consumed"] == 4
    with pytest.raises(ValueError, match="baseline changed"):
        adapter.evaluate(first, 85000)
    with pytest.raises(ValueError, match="causal/monotonic"):
        _capture_receive_time(adapter, 299)


def test_receive_time_policy_preserves_supported_action_and_freezes_context(monkeypatch):
    adapter, policies = _receive_time_adapter_fixture()
    monkeypatch.setattr(policies["BUY"], "evaluate", lambda **kwargs: SimpleNamespace(
        action_id="SELECTED_DURATION", duration_ms=123456, fallback_reason=None,
        matched_rule_index=3, support_valid=True, policy_sha256="synthetic-policy",
        predicate_bundle_sha256="synthetic-predicates",
    ))
    snapshot = _capture_receive_time(adapter, 200)
    decision = adapter.evaluate(snapshot, 170000)
    assert snapshot.policy_input_valid
    assert snapshot.fallback_policy_id is None
    assert decision.duration_ms == 123456
    assert decision.action_id == "SELECTED_DURATION"
    assert decision.matched_rule_index == 3
    assert decision.snapshot_id == snapshot.snapshot_id
    with pytest.raises(TypeError):
        snapshot.m0_context["baseline_duration_ms"] = 85000


def _message_schedule_replay_inputs(*, crossing_fill: bool = False):
    source_ms = np.arange(0, 4_001, 1_000, dtype=np.int64)
    trade_ms = np.asarray([0, 600, 700, 1_200, 1_300, 2_300, 3_300, 4_000])
    prices = np.full(trade_ms.size, 100.0)
    quantities = np.zeros(trade_ms.size)
    if crossing_fill:
        prices[3], quantities[3] = 80.0, 10.0
    trades = pd.DataFrame({
        "transact_time": trade_ms,
        "price": prices,
        "quantity": quantities,
        "is_buyer_maker": np.ones(trade_ms.size, dtype=np.uint8),
    })
    bbo = HistoricalBBOData(
        ts_ms=source_ms,
        best_bid=99.9 + np.arange(source_ms.size) * 0.1,
        best_ask=100.1 + np.arange(source_ms.size) * 0.1,
        bid_qty=np.ones(source_ms.size),
        ask_qty=np.ones(source_ms.size),
    )
    depth_mid = 100.0 + np.arange(source_ms.size) * 0.2
    l2 = HistoricalL2Data(
        ts_ms=source_ms,
        bid_px=(depth_mid - 0.1)[:, None],
        ask_px=(depth_mid + 0.1)[:, None],
        bid_qty=np.ones((source_ms.size, 1)),
        ask_qty=np.ones((source_ms.size, 1)),
    )

    def clock(exchange_ms, ready_ms):
        exchange_ns = np.asarray(exchange_ms, dtype=np.int64) * 1_000_000
        return {
            "exchange_ts_ns": exchange_ns,
            "receive_ts_ns": exchange_ns + 1,
            "feature_ready_ts_ns": np.asarray(ready_ms, dtype=np.int64) * 1_000_000 + 1,
        }

    delivery = {
        "bbo": clock(source_ms, [0, 3_000, 2_100, 3_100, 4_100]),
        "depth": clock(source_ms, [0, 1_500, 2_500, 3_500, 4_500]),
        "trade": clock([0, 600, 2_300, 4_000], [0, 2_500, 3_500, 4_500]),
        "variance": clock(source_ms, [0, 1_100, 3_900, 3_901, 4_100]),
        "prediction": clock(source_ms, [0, 2_100, 2_600, 3_100, 4_100]),
    }
    delivery["trade"]["last_child_row_index"] = np.asarray([0, 4, 6, 7])
    params = {
        "gamma": 0.01, "kappa": 1.0, "order_size": 0.001, "max_inventory": 0.01,
        "requote_interval": 1.0, "rq_min": 1.0, "rq_max": 1.0, "requote_clock": "fixed",
        "maker_fee": 0.0, "taker_fee": 0.0, "tick_size": 0.1, "lot_size": 0.001,
        "queue_base": 0.0, "queue_decay": 0.0, "maker_fill_prob": 1.0,
        "use_bar_pricing": False, "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100, "max_exec_book_age_s": 0.0,
        "collect_curves": False, "position_timeout": 0.0,
        "ml_enabled": True, "vol_blend": 0.1,
        "markout_ema_span_fills": 0, "trace_decisions_max": 100,
        "trace_quotes_max": 100, "trace_fills_max": 100,
        "exec_book_visibility_mode": "message_schedule", "_exec_message_delivery": delivery,
    }
    return {
        "trades_df": trades, "var_ts_ms": source_ms,
        "var_ssq": np.asarray([1.0, 4.0, 9.0, 16.0, 25.0]), "params": params,
        "bbo_data": bbo, "l2_data": l2,
        "ml_data": (
            source_ms, np.full(source_ms.size, 0.5),
            np.arange(2.0, 7.0), np.zeros(source_ms.size),
        ),
    }


def test_message_schedule_replay_uses_independent_ready_sources_and_parent_children() -> None:
    inputs = _message_schedule_replay_inputs()
    result = bt._simulate_tick_with_engine("python", **inputs)
    rows = pd.DataFrame(result["_decision_trace"])
    rows = rows[rows["side"] == "BUY"].set_index("ts_ms")
    assert rows.index.tolist() == [1_000, 2_000, 3_000, 4_000]
    assert rows["decision_visible_bbo_index"].tolist() == [0, 0, 0, 3]
    assert rows["decision_visible_l2_index"].tolist() == [0, 1, 2, 3]
    assert rows["feature_ready_generation_index"].tolist() == [0, 1, 1, 3]
    assert rows["prediction_generation_index"].tolist() == [0, 0, 2, 3]
    assert rows["sigma_sq_raw"].tolist() == [1.0, 4.0, 4.0, 16.0]
    assert rows["decision_visible_trade_cutoff_ts_ms"].tolist() == [0, 0, 600, 2_300]
    event_rows, _ = bt.build_replay_event_clock(
        inputs["trades_df"], mode="merged", interval_ms=100,
        bbo_data=inputs["bbo_data"], l2_data=inputs["l2_data"],
    )
    execution_indices = np.flatnonzero(event_rows["_is_execution_trade"])
    assert rows["decision_visible_trade_index"].tolist() == execution_indices[[0, 0, 4, 6]].tolist()
    assert result["exec_message_delivery_sources"]["bbo"]["head_of_line_clamped_events"] == 1


@pytest.mark.parametrize("late_trade_ready_ns,third_mark", [
    (2_500_000_001, 101.0),
    (3_000_000_000, 102.0),  # A callback at decision time is not yet visible.
])
def test_message_quote_depth_mid_and_callback_ordered_unrealized_pnl(
    monkeypatch, late_trade_ready_ns, third_mark,
):
    inputs = _message_schedule_replay_inputs()
    inputs["params"].update({"initial_inventory": 0.002, "initial_entry_price": 99.0})
    bbo = inputs["bbo_data"]
    midpoint = np.asarray([100.5, 102.0, 104.0, 106.0, 108.0])
    bbo.best_bid[:] = midpoint - 0.1
    bbo.best_ask[:] = midpoint + 0.1
    delivery = inputs["params"]["_exec_message_delivery"]
    delivery["bbo"]["feature_ready_ts_ns"][:] = [
        1, 1_500_000_001, 4_000_000_000, 4_100_000_001, 4_500_000_001,
    ]
    delivery["trade"]["feature_ready_ts_ns"][:] = [
        1, late_trade_ready_ns, 4_000_000_000, 4_500_000_001,
    ]
    # Zero is a missing source timestamp in the live snapshot contract.
    inputs["var_ts_ms"][:] += 10_000
    inputs["trades_df"]["transact_time"] += 10_000
    for feed in delivery.values():
        for field in ("exchange_ts_ns", "receive_ts_ns", "feature_ready_ts_ns"):
            feed[field] += 10_000_000_000
    # Parent source=600ms maps to final child row4, not a timer/parent index.
    # Its old exchange-time price arrives after the source=1000ms BBO.
    inputs["trades_df"].loc[4, "price"] = 101.0
    inputs["trades_df"].loc[6, "price"] = 110.0
    states = []
    original = bt.compute_quote_core

    def capture(state, *args, **kwargs):
        states.append(state)
        return original(state, *args, **kwargs)

    monkeypatch.setattr(bt, "compute_quote_core", capture)
    bt._simulate_tick_with_engine("python", **inputs)
    assert len(states) == 4
    # The fresher BBO is a post-only guard, never the depth pricing midpoint.
    assert [s.mid for s in states] == pytest.approx([100.0, 100.2, 100.4, 100.6])
    assert [s.best_bid for s in states] == pytest.approx([100.4, 101.9, 101.9, 101.9])
    assert [s.best_ask for s in states] == pytest.approx([100.6, 102.1, 102.1, 102.1])
    assert [s.inventory for s in states] == pytest.approx([0.002] * 4)
    # At first callback tie the documented fixed BBO tie-break applies. At
    # 4s both future BBO/trade callbacks are excluded (strict ready < decision).
    marks = [100.5, 102.0, third_mark, 101.0]
    assert [s.unrealized_pnl for s in states] == pytest.approx(
        [(mark - 99.0) * 0.002 for mark in marks],
    )


@pytest.mark.parametrize("receive_ms,visible_limit_s,source_limit_s,expected_first", [
    (600, 0.75, 0.75, 1_000),  # total source age 1s, both separate ages pass
    (1_100, 5.0, 1.0, None),  # fresh BBO cannot hide old depth source
    (100, 0.5, 5.0, None),  # low transport lag cannot hide old receipt
])
def test_message_schedule_splits_mandatory_depth_freshness(
    receive_ms, visible_limit_s, source_limit_s, expected_first,
) -> None:
    inputs = _message_schedule_replay_inputs()
    inputs["params"].update({
        "max_exec_book_age_s": 0.75,
        "max_exec_book_visible_age_s": visible_limit_s,
        "max_exec_book_source_lag_s": source_limit_s,
    })
    depth = inputs["params"]["_exec_message_delivery"]["depth"]
    depth["receive_ts_ns"][0] = receive_ms * 1_000_000
    depth["feature_ready_ts_ns"][0] = receive_ms * 1_000_000 + 1
    depth["receive_ts_ns"][1:] = np.maximum(
        depth["receive_ts_ns"][1:], depth["receive_ts_ns"][0],
    )
    depth["feature_ready_ts_ns"][1:] = 10_000_000_000
    result = bt._simulate_tick_with_engine("python", **inputs)
    times = [row["ts_ms"] for row in result["_decision_trace"] if row["side"] == "BUY"]
    assert (times[0] if times else None) == expected_first
    assert result["stale_book_skip_count"] > 0


def test_message_schedule_replay_preserves_preprojected_shared_connection_hold() -> None:
    inputs = _message_schedule_replay_inputs()
    delivery = inputs["params"]["_exec_message_delivery"]
    fields = ("exchange_ts_ns", "receive_ts_ns", "feature_ready_ts_ns")
    joint = HistoricalMessageDeliverySchedule(
        *(np.column_stack([delivery["bbo"][field], delivery["depth"][field]]).ravel()
          for field in fields),
        channel_ids=["bbo", "depth"] * 5,
        connection_ids=["public"] * 10,
    )
    for feed in ("bbo", "depth"):
        delivery[feed]["feature_ready_ts_ns"] = joint.ready_ns_for_channel(feed)
    result = bt._simulate_tick_with_engine("python", **inputs)
    rows = [row for row in result["_decision_trace"] if row["side"] == "BUY"]
    assert [row["decision_visible_bbo_index"] for row in rows] == [0, 0, 0, 3]
    assert [row["decision_visible_l2_index"] for row in rows] == [0, 0, 0, 3]
    assert joint.stats_dict()["head_of_line_clamped_events"] == 3


@pytest.mark.parametrize("feed", ["depth", "trade", "variance", "prediction"])
def test_message_schedule_replay_waits_for_every_required_source(feed) -> None:
    inputs = _message_schedule_replay_inputs()
    # Exactly 2s is not yet visible to the 2s decision; later low-lag rows in
    # this source remain blocked by its first undelivered message.
    inputs["params"]["_exec_message_delivery"][feed]["feature_ready_ts_ns"][0] = 2_000_000_000
    result = bt._simulate_tick_with_engine("python", **inputs)
    decisions = result["_decision_trace"]
    assert decisions and min(row["ts_ms"] for row in decisions) == 3_000
    assert all(row["quote_ts"] >= 3_000 for row in result["_quote_trace"])
    assert result["exec_message_missing_source_skip_count"] >= 2


@pytest.mark.parametrize("case,reason", [
    ("visible_age", "stale_book_ticker_visible_age"),
    ("source_lag", "stale_book_ticker_source_lag"),
    ("missing", "missing_or_crossed_book_ticker"),
    ("crossed", "missing_or_crossed_book_ticker"),
    ("fresh", ""),
])
def test_message_bbo_guard_reuses_live_freshness_and_depth_fallback(monkeypatch, case, reason):
    from strategy.signal import QuoteDecisionSnapshot

    inputs = _message_schedule_replay_inputs()
    # All source arrays share this epoch-ms vector; move it once off synthetic zero.
    inputs["var_ts_ms"][:] += 10_000
    inputs["trades_df"]["transact_time"] += 10_000
    delivery = inputs["params"]["_exec_message_delivery"]
    for row in delivery.values():
        for name in ("exchange_ts_ns", "receive_ts_ns", "feature_ready_ts_ns"):
            row[name] += 10_000_000_000
    inputs["bbo_data"].best_bid[0] = 90.0
    inputs["bbo_data"].best_ask[0] = 91.0
    inputs["params"].update({
        "max_exec_book_visible_age_s": 0.5 if case == "visible_age" else 2.0,
        "max_exec_book_source_lag_s": 0.5 if case == "source_lag" else 2.0,
    })
    depth_receive = 10_900_000_000 if case == "visible_age" else 10_100_000_000
    delivery["depth"]["receive_ts_ns"][0] = depth_receive
    delivery["depth"]["feature_ready_ts_ns"][0] = depth_receive + 1
    if case == "source_lag":
        delivery["bbo"]["receive_ts_ns"][0] = 10_900_000_000
        delivery["bbo"]["feature_ready_ts_ns"][0] = 10_900_000_001
    elif case == "missing":
        delivery["bbo"]["feature_ready_ts_ns"][:] = 20_000_000_000
    elif case == "crossed":
        inputs["bbo_data"].best_ask[0] = 89.0
    observed = []
    original = QuoteDecisionSnapshot.post_only_guard
    def capture(snapshot, **kwargs):
        guard = original(snapshot, **kwargs)
        observed.append((snapshot.capture_ts_ns, guard))
        return guard
    monkeypatch.setattr(QuoteDecisionSnapshot, "post_only_guard", capture)
    result = bt._simulate_tick_with_engine("python", **inputs)
    first = next(guard for timestamp, guard in observed if timestamp == 11_000_000_000)
    assert first.fallback_reason == reason
    assert first.source == ("depth" if reason else "book_ticker")
    assert first.best_bid == pytest.approx(99.9 if reason else 90.0)
    assert any(row["ts_ms"] == 11_000 for row in result["_decision_trace"])


def test_message_bbo_clock_before_exchange_is_rejected():
    inputs = _message_schedule_replay_inputs()
    inputs["params"]["_exec_message_delivery"]["bbo"]["receive_ts_ns"][1] = 1
    with pytest.raises(ValueError, match="exchange <= receive <= ready"):
        bt._simulate_tick_with_engine("python", **inputs)


def test_message_guard_does_not_change_identical_bbo_depth_projection(monkeypatch):
    from strategy.signal import QuoteDecisionSnapshot

    def replay():
        inputs = _message_schedule_replay_inputs()
        inputs["bbo_data"].best_bid[:] = inputs["l2_data"].bid_px[:, 0]
        inputs["bbo_data"].best_ask[:] = inputs["l2_data"].ask_px[:, 0]
        delivery = inputs["params"]["_exec_message_delivery"]
        delivery["bbo"] = {key: value.copy() for key, value in delivery["depth"].items()}
        return bt._simulate_tick_with_engine("python", **inputs)
    corrected = replay()
    monkeypatch.setattr(
        QuoteDecisionSnapshot, "post_only_guard", lambda snapshot, **_: SimpleNamespace(
            best_bid=snapshot.book_ticker_bid, best_ask=snapshot.book_ticker_ask,
            source="book_ticker", fallback_reason="",
        ),
    )
    legacy = replay()
    assert corrected["_quote_trace"] == legacy["_quote_trace"]
    assert corrected["_fill_trace"] == legacy["_fill_trace"]


def test_message_schedule_replay_rejects_parent_ready_before_last_child() -> None:
    inputs = _message_schedule_replay_inputs()
    inputs["params"]["_exec_message_delivery"]["trade"]["feature_ready_ts_ns"][1] = 1_000_000_000
    with pytest.raises(ValueError, match="child|parent"):
        bt._simulate_tick_with_engine("python", **inputs)


@pytest.mark.parametrize("field", ["connection_ids", "channel_ids", "source_ordinal"])
def test_message_schedule_replay_rejects_unconsumed_joint_ordering_fields(field) -> None:
    inputs = _message_schedule_replay_inputs()
    inputs["params"]["_exec_message_delivery"]["bbo"][field] = np.arange(5)
    with pytest.raises(ValueError, match="joint|ordering|unsupported"):
        bt._simulate_tick_with_engine("python", **inputs)


def test_message_schedule_markout_uses_same_depth_delivery_without_resampling(monkeypatch) -> None:
    inputs = _message_schedule_replay_inputs(crossing_fill=True)
    inputs["params"].update(markout_ema_span_fills=1, markout_horizon_s=0.5)
    inputs["params"]["_exec_message_delivery"]["depth"]["feature_ready_ts_ns"][1] = 1_700_000_000

    def no_legacy_sampling(*args, **kwargs):
        raise AssertionError("message schedule must not resample visibility")

    monkeypatch.setattr(bt, "_exec_book_visibility_delay_ms", no_legacy_sampling)
    monkeypatch.setattr(bt, "_source_stratified_exec_visibility_state", no_legacy_sampling)
    result = bt._simulate_tick_with_engine("python", **inputs)
    assert result["fills_bid"] == 1
    assert result["markout_count"] == 1
    # The updated depth becomes ready exactly at the markout observation and
    # is excluded, even though exchange truth has already consumed it.
    assert result["avg_markout_bid"] == pytest.approx(100.0 - result["buy_avg_fill_price"])


def test_message_schedule_markout_does_not_use_unreceived_depth_for_restored_fill() -> None:
    inputs = _message_schedule_replay_inputs()
    inputs["params"].update(
        markout_ema_span_fills=1,
        markout_horizon_s=0.5,
        requote_interval=100.0,
        rq_min=100.0,
        rq_max=100.0,
        initial_live_state={"active_orders": [{
            "side": "BUY", "price": 98.0, "quantity": 0.001, "status": "OPEN",
        }]},
    )
    inputs["params"]["_exec_message_delivery"]["depth"]["feature_ready_ts_ns"][:] = (
        5_000_000_000
    )
    # The inherited exchange order fills at the first physical event, before
    # the local control wake. Only its pending markout survives the outage;
    # the test must not require an OPEN order to evade stale-book cancellation.
    inputs["trades_df"].loc[0, ["price", "quantity"]] = [80.0, 10.0]
    result = bt._simulate_tick_with_engine("python", **inputs)
    assert result["fills_bid"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 0
    assert result["markout_count"] == 0


def test_message_pending_markout_survives_thirty_second_depth_outage() -> None:
    inputs = _message_schedule_replay_inputs()
    inputs["params"].update(
        markout_ema_span_fills=1, markout_horizon_s=0.5,
        requote_interval=100.0, rq_min=100.0, rq_max=100.0,
        initial_live_state={"active_orders": [{
            "side": "BUY", "price": 98.0, "quantity": 0.001, "status": "OPEN",
        }]},
    )
    # Complete the inherited order before the first local control wake. The
    # thirty-second wait concerns a pending observation, not a surviving order.
    inputs["trades_df"].loc[0, ["price", "quantity"]] = [80.0, 10.0]
    inputs["trades_df"].loc[7, "transact_time"] = 40_000
    delivery = inputs["params"]["_exec_message_delivery"]
    delivery["trade"]["feature_ready_ts_ns"][-1] = 40_000_000_001
    delivery["depth"]["feature_ready_ts_ns"][:] = 35_000_000_001
    result = bt._simulate_tick_with_engine("python", **inputs)
    assert result["fills_bid"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 0
    assert result["markout_count"] == 1
    assert result["avg_markout_bid"] == pytest.approx(100.8 - 98.0)


def test_message_l2_flow_window_ends_at_visible_depth_source_time() -> None:
    inputs = _message_schedule_replay_inputs()
    inputs["params"]["l2_refill_cancel_lookback_s"] = 2.0
    inputs["l2_data"].bid_qty[:, 0] = [1, 2, 1, 4, 1]
    inputs["l2_data"].ask_qty[:, 0] = [1, 2, 1, 4, 1]
    result = bt._simulate_tick_with_engine("python", **inputs)
    row = next(
        r for r in result["_decision_trace"] if r["ts_ms"] == 4_000 and r["side"] == "BUY"
    )
    # Last received depth is source=3s: include source=1,2,3s, not
    # observation=4s minus 2s, which incorrectly drops source=1s.
    assert row["l2_book_refresh_ratio"] == pytest.approx(1.0)
    assert row["l2_book_cancel_ratio"] == pytest.approx(0.5 / 3)
    assert row["l2_quote_flip_rate"] == pytest.approx(2 / 3)


def test_delayed_fill_snapshot_keeps_exchange_clock_and_received_market_state() -> None:
    inputs = _message_schedule_replay_inputs(crossing_fill=True)
    captures = []

    def capture(**kwargs):
        captures.append(kwargs)
        return SimpleNamespace(
            snapshot_id="synthetic", assignment_id=kwargs["assignment_id"],
            policy_input_valid=True, fallback_policy_id=None, fallback_reason=None,
            source_bundle_sha256="synthetic",
        )

    inputs["params"].update(
        _private_fill_visibility_latency_samples_ms=np.asarray([50.0]),
        fill_cooldown=1.0,
        trace_cooldown_duration_opportunities_max=10,
        cooldown_v2_snapshot_emitter=SimpleNamespace(
            capture_exposure_fill=capture, audit=lambda: {},
        ),
    )
    result = bt._simulate_tick_with_engine("python", **inputs)
    assert len(captures) == 1
    assert captures[0]["fill_exchange_ts_ns"] == 1_200_000_000
    assert captures[0]["fill_visible_ts_ns"] == 1_250_000_000
    row = result["_cooldown_duration_opportunity_trace"][0]
    assert row["canonical_mid"] == pytest.approx(100.0)
    assert row["best_bid"] == pytest.approx(99.9)
    assert row["decision_visible_bbo_index"] == 0
    assert row["decision_visible_l2_index"] == 0


def test_visibility_profile_loads_receive_time_book_ages(tmp_path) -> None:
    source = tmp_path / "quote_decisions.csv"
    pd.DataFrame({"depth_age_s": [0.100, 0.200, 1.739, None]}).to_csv(
        source,
        index=False,
    )

    profile = _load_exec_book_visibility_profile(source)

    assert profile["exec_book_visibility_age_column"] == "depth_age_s"
    assert profile["exec_book_visibility_delay_samples_ms"].tolist() == [
        100.0,
        200.0,
        1739.0,
    ]
    assert profile["exec_book_visibility_delay_mean_ms"] == 679.6666666666666


def test_visibility_sample_is_deterministic_and_empirical() -> None:
    samples = np.asarray([82.0, 116.0, 820.0, 1739.0], dtype=np.float64)
    first = _exec_book_visibility_delay_ms(
        1_785_528_006_426,
        mean_ms=0.0,
        jitter_ms=0.0,
        seed=20260718,
        samples_ms=samples,
    )
    second = _exec_book_visibility_delay_ms(
        1_785_528_006_426,
        mean_ms=0.0,
        jitter_ms=0.0,
        seed=20260718,
        samples_ms=samples,
    )

    assert first == second
    assert first in {82, 116, 820, 1739}


def test_mean_plus_jitter_fallback_is_bounded() -> None:
    delay = _exec_book_visibility_delay_ms(
        123_456,
        mean_ms=100.0,
        jitter_ms=25.0,
        seed=7,
    )
    assert 75 <= delay <= 125


def test_visibility_profile_preserves_paired_book_depth_and_mid(tmp_path) -> None:
    source = tmp_path / "live_perf.csv"
    pd.DataFrame(
        {
            "timestamp": [1_784_505_604.294, 1_784_505_627.291],
            "event": ["requote", "requote"],
            "status": ["ok", "ok"],
            "exec_book_age_s": [1.475, 0.001],
            "exec_depth_age_s": [4.221, 1.279],
            "exec_trade_age_s": [1.238, 0.422],
            "mid": [64659.65, 64611.05],
        }
    ).to_csv(source, index=False)

    profile = _load_exec_book_visibility_profile(source)
    state = _paired_exec_book_visibility_state(
        1_784_505_604_294,
        timestamps_ms=profile["exec_book_visibility_paired_ts_ms"],
        book_delays_ms=profile["exec_book_visibility_paired_delay_ms"],
        depth_delays_ms=profile["exec_depth_visibility_paired_delay_ms"],
        trade_delays_ms=profile["exec_trade_visibility_paired_delay_ms"],
        observed_mid=profile["exec_book_visibility_paired_mid"],
        max_gap_ms=5,
    )

    assert state == (1475, 4221, 1238, 64659.65)
    assert profile["exec_book_visibility_paired_count"] == 2
    assert profile["exec_book_visibility_trade_age_column"] == "exec_trade_age_s"


def test_sampled_joint_visibility_is_deterministic_and_preserves_source_row() -> None:
    book = np.asarray([10.0, 20.0, 30.0])
    depth = np.asarray([110.0, 120.0, 130.0])
    trade = np.asarray([210.0, 220.0, 230.0])

    first = _sampled_joint_exec_book_visibility_state(
        1_777_777_777_000,
        seed=20260826,
        book_delays_ms=book,
        depth_delays_ms=depth,
        trade_delays_ms=trade,
    )
    second = _sampled_joint_exec_book_visibility_state(
        1_777_777_777_000,
        seed=20260826,
        book_delays_ms=book,
        depth_delays_ms=depth,
        trade_delays_ms=trade,
    )

    assert first == second
    assert first in {(10, 110, 210), (20, 120, 220), (30, 130, 230)}


def test_sampled_joint_visibility_rejects_unaligned_sources() -> None:
    with np.testing.assert_raises_regex(ValueError, "aligned book/depth/trade"):
        _sampled_joint_exec_book_visibility_state(
            1,
            seed=2,
            book_delays_ms=np.asarray([10.0]),
            depth_delays_ms=np.asarray([20.0, 30.0]),
            trade_delays_ms=np.asarray([40.0]),
        )


def test_sampled_visibility_cutoffs_never_forget_visible_book_or_depth() -> None:
    cutoffs: dict[str, int] = {}

    # A later high-latency draw must not move either receive stream backward.
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="bbo", candidate_ts_ms=9_900
    ) == 9_900
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="bbo", candidate_ts_ms=9_750
    ) == 9_900

    # Decision and markout consumers share the L2 feed cursor in both orders:
    # markout cannot forget a decision-visible update, and a later decision
    # cannot forget an update already consumed by markout.
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="l2", candidate_ts_ms=9_800
    ) == 9_800
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="l2", candidate_ts_ms=9_700
    ) == 9_800
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="l2", candidate_ts_ms=10_050
    ) == 10_050
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="l2", candidate_ts_ms=9_950
    ) == 10_050

    # The same source-profile draw also feeds causal signal/feature consumers.
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="trade", candidate_ts_ms=9_850
    ) == 9_850
    assert _advance_monotonic_visibility_cutoff(
        cutoffs, feed="trade", candidate_ts_ms=9_600
    ) == 9_850


def test_source_stratified_visibility_uses_joint_transport_row_and_identity(
    tmp_path,
) -> None:
    payload = {
        "schema": "market_data_latency_profile.v1",
        "profile_id": "aws_prior_test",
        "groups": [],
        "source_stratified_sampling": {
            "schema": "market_data_latency_source_stratified.v1",
            "authority": "diagnostic_non_authoritative",
            "promotion_eligible": False,
            "joint_bucket_ms": 1_000,
            "sources": [
                {
                    "market_id": "binance:perp:BTCUSDC",
                    "transport": "websocket",
                    "strata": [
                        {
                            "utc_day": "2026-08-12",
                            "window_id": "window_a",
                            "book_visibility_lag_ms_samples": [11.0, 22.0],
                            "trade_visibility_lag_ms_samples": [101.0, 202.0],
                        }
                    ],
                }
            ],
        },
    }
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    simulator, actual_sha = _load_exec_source_stratified_visibility_profile(
        path,
        expected_sha256=sha256,
        expected_profile_id="aws_prior_test",
        market_id="binance:perp:BTCUSDC",
        transport="websocket",
    )

    first = _source_stratified_exec_visibility_state(
        1_777_777_777_000,
        seed=20260826,
        simulator=simulator,
        market_id="binance:perp:BTCUSDC",
        transport="websocket",
    )
    second = _source_stratified_exec_visibility_state(
        1_777_777_777_999,
        seed=20260826,
        simulator=simulator,
        market_id="binance:perp:BTCUSDC",
        transport="websocket",
    )

    assert actual_sha == sha256
    assert first == second
    assert first in {(11, 11, 101), (22, 22, 202)}
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        _load_exec_source_stratified_visibility_profile(
            path,
            expected_sha256="0" * 64,
            expected_profile_id="aws_prior_test",
            market_id="binance:perp:BTCUSDC",
            transport="websocket",
        )
    with pytest.raises(ValueError, match="profile id mismatch"):
        _load_exec_source_stratified_visibility_profile(
            path,
            expected_sha256=sha256,
            expected_profile_id="wrong_profile",
            market_id="binance:perp:BTCUSDC",
            transport="websocket",
        )
    with pytest.raises(KeyError, match="has no source"):
        _load_exec_source_stratified_visibility_profile(
            path,
            expected_sha256=sha256,
            expected_profile_id="aws_prior_test",
            market_id="binance:perp:ETHUSDC",
            transport="websocket",
        )


def test_live_telemetry_visibility_is_rebased_to_pre_order_update(tmp_path) -> None:
    source = tmp_path / "live_perf.csv"
    pd.DataFrame(
        {
            "timestamp": [10.0],
            "event": ["requote"],
            "status": ["ok"],
            "update_orders_us": [2_000_000.0],
            "exec_book_age_s": [1.0],
            "exec_depth_age_s": [4.0],
            "exec_trade_age_s": [2.5],
            "mid": [100.0],
        }
    ).to_csv(source, index=False)

    profile = _load_exec_book_visibility_profile(source)

    assert profile["exec_book_visibility_paired_ts_ms"].tolist() == [8_000]
    assert profile["exec_book_visibility_paired_delay_ms"].tolist() == [0.0]
    assert profile["exec_depth_visibility_paired_delay_ms"].tolist() == [2_000.0]
    assert profile["exec_trade_visibility_paired_delay_ms"].tolist() == [500.0]
    assert profile["exec_book_visibility_timestamp_semantics"] == "pre_order_update"


def test_live_requote_clock_uses_pre_order_update_and_pre_cancel_times(tmp_path) -> None:
    source = tmp_path / "live_perf.csv"
    pd.DataFrame(
        {
            "timestamp": [10.0, 12.0],
            "event": ["requote", "requote"],
            "status": ["ok", "stale_book"],
            "update_orders_us": [2_000_000.0, 0.0],
            "rest_cancel_all_sum_us": [0.0, 500_000.0],
        }
    ).to_csv(source, index=False)

    clock = _load_live_requote_clock(source)

    assert clock["requote_clock_ts_ms"].tolist() == [8_000, 11_500]
    assert clock["requote_clock_observed_ts_ms"].tolist() == [10_000, 12_000]
    assert clock["requote_clock_stage_lag_ms"].tolist() == [2_000.0, 500.0]
    assert clock["requote_clock_action"].tolist() == [2, 3]


def test_paired_source_boundary_is_held_through_depth_stream_stall() -> None:
    timestamps = np.asarray([4_294, 6_903, 12_081, 17_161], dtype=np.int64)
    depth_ages = np.asarray([4_221, 6_830, 12_008, 16_983], dtype=np.float64)

    assert _paired_exec_source_visible_ts(
        14_628,
        timestamps_ms=timestamps,
        source_delays_ms=depth_ages,
    ) == 73
    assert _paired_exec_source_visible_ts(
        18_000,
        timestamps_ms=timestamps,
        source_delays_ms=depth_ages,
    ) == 178


def test_refill_features_use_depth_visibility_clock() -> None:
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 6_001, 1_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.full(ts.size, 100.0),
            "quantity": np.zeros(ts.size),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    levels = 20
    bid_qty = np.ones((ts.size, levels), dtype=np.float64)
    # Top-ten depth increases while levels 11-20 decrease by the same amount.
    # Live policy observes the increase because it summarizes top ten; a
    # replay incorrectly using top twenty would report no refill.
    bid_qty[4:, :10] = 2.0
    bid_qty[4:, 10:] = 0.0
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=np.full((ts.size, levels), 99.9),
        bid_qty=bid_qty,
        ask_px=np.full((ts.size, levels), 100.1),
        ask_qty=np.ones((ts.size, levels)),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "queue_ahead_mode": "exact_level",
        "maker_fill_prob": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 20.0,
        "max_exec_book_age_s": 0.0,
        "ml_enabled": False,
        "trace_decisions_max": 100,
        "collect_curves": False,
        "l2_refill_cancel_lookback_s": 10.0,
        "exec_book_visibility_mode": "paired",
        "exec_book_visibility_paired_max_gap_ms": 5,
        "exec_depth_visibility_source_offset_ms": 17,
        "_exec_book_visibility_paired_ts_ms": ts[1:],
        "_exec_book_visibility_paired_delay_ms": np.zeros(ts.size - 1),
        "_exec_depth_visibility_paired_delay_ms": np.full(ts.size - 1, 3_000.0),
        "_exec_trade_visibility_paired_delay_ms": np.zeros(ts.size - 1),
        "_exec_book_visibility_paired_mid": np.full(ts.size - 1, 100.0),
    }

    result = bt.simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        params,
        l2_data=l2,
    )
    decisions = pd.DataFrame(result["_decision_trace"])
    delayed = decisions[(decisions["ts_ms"] == 4_000) & (decisions["side"] == "BUY")]

    assert len(delayed) == 1
    assert delayed.iloc[0]["l2_book_refresh_ratio"] == 0.0
    assert result["exec_depth_visibility_source_offset_ms"] == 17

    current_params = dict(params)
    current_params["exec_depth_visibility_source_offset_ms"] = 0
    current_params["_exec_depth_visibility_paired_delay_ms"] = np.zeros(
        ts.size - 1
    )
    current = bt.simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        current_params,
        l2_data=l2,
    )
    current_decisions = pd.DataFrame(current["_decision_trace"])
    visible = current_decisions[
        (current_decisions["ts_ms"] == 4_000)
        & (current_decisions["side"] == "BUY")
    ]

    assert len(visible) == 1
    assert visible.iloc[0]["l2_book_refresh_ratio"] > 0.0

    sampled_joint_params = dict(current_params)
    sampled_joint_params["exec_book_visibility_mode"] = "sampled_joint"
    sampled_joint = bt.simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        sampled_joint_params,
        l2_data=l2,
    )

    assert sampled_joint["_decision_trace"]


def test_source_offset_does_not_make_paired_depth_age_negative() -> None:
    bt.configure_symbol("BTCUSDC")
    trade_ts = np.arange(0, 6_001, 1_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": trade_ts,
            "price": np.full(trade_ts.size, 100.0),
            "quantity": np.zeros(trade_ts.size),
            "is_buyer_maker": np.zeros(trade_ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(trade_ts.size, dtype=np.bool_),
        }
    )
    # The source-label offset deliberately selects the 1,010ms frame for the
    # 1,000ms decision. Freshness must still come from the paired receive-time
    # observation (0ms), not from 1,000 - 1,010 = -10ms.
    l2 = HistoricalL2Data(
        ts_ms=np.concatenate(
            [np.asarray([0], dtype=np.int64), trade_ts[1:] + 10]
        ),
        bid_px=np.full((trade_ts.size, 1), 99.9),
        bid_qty=np.ones((trade_ts.size, 1)),
        ask_px=np.full((trade_ts.size, 1), 100.1),
        ask_qty=np.ones((trade_ts.size, 1)),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "queue_ahead_mode": "exact_level",
        "maker_fill_prob": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 20.0,
        # Disable the scheduler's independent fixed-clock stale precheck. This
        # test targets the common-policy freshness value used by empirical
        # live-parity replay, where the recorded status owns that precheck.
        "max_exec_book_age_s": 0.0,
        "ml_enabled": False,
        "trace_decisions_max": 20,
        "collect_curves": False,
        "exec_book_visibility_mode": "paired",
        "exec_book_visibility_paired_max_gap_ms": 5,
        "exec_depth_visibility_source_offset_ms": 17,
        "_exec_book_visibility_paired_ts_ms": trade_ts[1:],
        "_exec_book_visibility_paired_delay_ms": np.zeros(trade_ts.size - 1),
        "_exec_depth_visibility_paired_delay_ms": np.zeros(trade_ts.size - 1),
        "_exec_trade_visibility_paired_delay_ms": np.zeros(trade_ts.size - 1),
        "_exec_book_visibility_paired_mid": np.full(trade_ts.size - 1, 100.0),
    }

    result = bt.simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        params,
        l2_data=l2,
    )
    decisions = pd.DataFrame(result["_decision_trace"])
    first = decisions[decisions["ts_ms"] > 0]

    assert len(first) >= 2
    assert (first["depth_age_s"] == 0.0).all()
    assert not first["reason_text"].str.contains("common_policy_hard_pause").any()


@pytest.mark.parametrize(
    "mode", ["sampled", "paired", "sampled_joint", "profile_source_stratified"],
)
def test_all_age_sampled_modes_hold_independent_visible_source_cutoffs(monkeypatch, mode) -> None:
    inputs = _message_schedule_replay_inputs()
    params = inputs["params"]
    params.pop("_exec_message_delivery")
    params.update(
        exec_book_visibility_mode=mode,
        _exec_book_visibility_delay_samples_ms=np.asarray([0.0, 2_000.0]),
        _exec_book_visibility_paired_ts_ms=np.asarray([1_000]),
        _exec_book_visibility_paired_delay_ms=np.asarray([0.0]),
        _exec_depth_visibility_paired_delay_ms=np.asarray([0.0]),
        _exec_trade_visibility_paired_delay_ms=np.asarray([0.0]),
        _exec_book_visibility_paired_mid=np.asarray([np.nan]),
        exec_source_stratified_profile_path="synthetic-profile.json",
    )

    def delays(now, **_):
        if now < 2_000:
            return 0, 500, 750
        if now < 3_000:
            return 2_000, 2_500, 1_900
        return (0, 0, 0) if now < 4_000 else (2_000, 2_000, 2_000)

    monkeypatch.setattr(bt, "_exec_book_visibility_delay_ms", lambda now, **kw: delays(now)[0])
    monkeypatch.setattr(bt, "_sampled_joint_exec_book_visibility_state", delays)
    monkeypatch.setattr(bt, "_source_stratified_exec_visibility_state", delays)
    monkeypatch.setattr(
        bt, "_paired_exec_book_visibility_state", lambda now, **kw: (*delays(now), np.nan),
    )
    monkeypatch.setattr(
        bt, "_load_exec_source_stratified_visibility_profile", lambda *a, **kw: (object(), ""),
    )
    observed = {feed: [] for feed in ("bbo", "l2", "trade")}

    def advance(cutoffs, *, feed, candidate_ts_ms):
        value = _advance_monotonic_visibility_cutoff(
            cutoffs, feed=feed, candidate_ts_ms=candidate_ts_ms,
        )
        observed[feed].append(value)
        return value

    monkeypatch.setattr(bt, "_advance_monotonic_visibility_cutoff", advance)
    result = bt._simulate_tick_with_engine("python", **inputs)
    rows = pd.DataFrame(result["_decision_trace"])
    rows = rows[(rows["side"] == "BUY") & (rows["ts_ms"] > 0)].set_index("ts_ms")
    assert [1_000, 2_000, 3_000, 4_000] == rows.index.tolist()
    for cutoffs in observed.values():
        # The merged 100ms control wakes advance reception even when quotes
        # are not due. The 2s high-delay sample must retain the 1.9s state.
        assert len(cutoffs) == 41
        assert cutoffs == sorted(cutoffs)
        assert cutoffs[20] == cutoffs[19]
        assert cutoffs[40] == cutoffs[39]
    if mode != "sampled":
        assert observed["bbo"][20] == 1_900
        assert observed["l2"][20] == 1_400
        assert observed["trade"][20] == 1_150


@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("top_qty", [0.0, -1.0, np.nan, np.inf])
def test_ioc_does_not_invent_missing_top_liquidity(side, top_qty) -> None:
    qty, price = bt._match_ioc_order(
        side, 100.0, 0.005, [100.0], [top_qty], tick_size=0.1, lot_size=0.001,
    )
    assert qty == price == 0.0


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_ioc_sweeps_valid_limit_levels_with_vwap_and_bounds_market_depth(side) -> None:
    direction = 1 if side == "BUY" else -1
    prices = 100.0 + direction * np.asarray([0.0, 0.1, 0.1, 0.2, 1.0])
    quantities = [0.0004, 0.0006, 99.0, 0.001, 0.002]
    # Duplicate second level cannot inflate size; the final level is outside limit.
    qty, price = bt._match_ioc_order(
        side, 100.0 + direction * 0.2, 0.01, prices, quantities,
        tick_size=0.1, lot_size=0.001,
    )
    assert qty == pytest.approx(0.002)
    assert price == pytest.approx(100.0 + direction * 0.13)
    market_qty, market_price = bt._match_ioc_order(
        side, 100.0, 0.01, prices, quantities,
        tick_size=0.1, lot_size=0.001, market=True,
    )
    assert market_qty == pytest.approx(0.004)
    assert market_price == pytest.approx(100.0 + direction * 0.565)
    bounded_qty, _ = bt._match_ioc_order(
        side, 100.0, 0.001, prices, quantities,
        tick_size=0.1, lot_size=0.001, market=True,
    )
    assert bounded_qty == pytest.approx(0.001)


def test_sampled_visibility_lifecycle_trace_does_not_change_execution() -> None:
    inputs = _message_schedule_replay_inputs(crossing_fill=True)
    params = inputs["params"]
    params.pop("_exec_message_delivery")
    params.update(
        exec_book_visibility_mode="sampled",
        _exec_book_visibility_delay_samples_ms=np.asarray([0.0, 2_000.0]),
    )
    plain = bt._simulate_tick_with_engine("python", **inputs)
    params["trace_local_order_lifecycle_max"] = 100
    traced = bt._simulate_tick_with_engine("python", **inputs)
    assert plain["fills_total"] > 0
    assert plain["_fill_trace"] == traced["_fill_trace"]
    assert plain["_quote_trace"] == traced["_quote_trace"]
    for field in ("cash_before_terminal", "final_inventory", "pnl", "n_requotes"):
        assert plain[field] == traced[field]
