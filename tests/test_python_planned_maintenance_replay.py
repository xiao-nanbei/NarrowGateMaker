from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models.backtest_tick import LocalLifecycleBoundaryScheduler, simulate_tick
from models.tick_data_types import HistoricalBBOData, HistoricalExchangeBookEvent, HistoricalL2Data


def _inputs(
    *,
    crossing_fill_ts_ms: int | None = None,
    crossing_side: str = "BUY",
):
    bbo_ts_ms = np.arange(0, 4_001, 100, dtype=np.int64)
    execution_end_ms = 1_500 if crossing_fill_ts_ms is None else 3_000
    ts_ms = np.arange(0, execution_end_ms + 1, 100, dtype=np.int64)
    price = np.full(ts_ms.shape, 100.0, dtype=np.float64)
    quantity = np.zeros(ts_ms.shape, dtype=np.float64)
    buyer_maker = np.ones(ts_ms.shape, dtype=np.uint8)
    if crossing_fill_ts_ms is not None:
        index = int(np.flatnonzero(ts_ms == crossing_fill_ts_ms)[0])
        if crossing_side == "BUY":
            price[index] = 96.0
        elif crossing_side == "SELL":
            price[index] = 104.0
            buyer_maker[index] = 0
        else:
            raise ValueError(f"unsupported crossing_side={crossing_side!r}")
        quantity[index] = 10.0
    trades = pd.DataFrame(
        {
            "transact_time": ts_ms,
            "price": price,
            "quantity": quantity,
            "is_buyer_maker": buyer_maker,
        }
    )
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts_ms,
        best_bid=np.full(bbo_ts_ms.size, 99.9),
        best_ask=np.full(bbo_ts_ms.size, 100.1),
        bid_qty=np.ones(bbo_ts_ms.size),
        ask_qty=np.ones(bbo_ts_ms.size),
    )
    return trades, bbo


def _params(*, cancel_latency_ms: int = 500) -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 0.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "cancel_order_latency_ms": cancel_latency_ms,
        "planned_quote_stop_ts_ms": 2_000,
        "replay_event_clock_end_ts_ms": 4_000,
        "trace_quotes_max": 100,
        "trace_fills_max": 100,
    }


def _run(
    *,
    crossing_fill_ts_ms: int | None = None,
    crossing_side: str = "BUY",
    param_overrides: dict[str, object] | None = None,
):
    trades, bbo = _inputs(
        crossing_fill_ts_ms=crossing_fill_ts_ms,
        crossing_side=crossing_side,
    )
    params = _params()
    if param_overrides:
        params.update(param_overrides)
    return simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        bbo_data=bbo,
    )


def _write_serial_gateway_profile(
    path, masks: list[tuple[bool, ...]], *,
    cancel_clocks: tuple[float, float, float] | None = None,
    new_clocks: tuple[float, float] = (2.0, 5.0),
) -> None:
    rows = len(masks)
    present = np.asarray(masks, dtype=np.bool_)
    offsets = np.full((rows, 4), np.nan, dtype=np.float64)
    effective = np.full((rows, 4), np.nan, dtype=np.float64)
    visible = np.full((rows, 4), np.nan, dtype=np.float64)
    for row_index, mask in enumerate(present):
        ordinal = 0
        for slot_index, enabled in enumerate(mask):
            if not enabled:
                continue
            offsets[row_index, slot_index] = float(ordinal * 10)
            effective[row_index, slot_index] = 2.0
            visible[row_index, slot_index] = 5.0
            ordinal += 1
    response_fields = {}
    if cancel_clocks is not None:
        for index in range(4):
            selected = present[:, index]
            clocks = cancel_clocks if index < 2 else new_clocks
            effective[selected, index], visible[selected, index] = clocks[:2]
        upper = np.full((rows, 4), np.nan)
        upper[:, :2] = cancel_clocks[2]
        upper_mask = present.copy()
        upper_mask[:, 2:] = False
        callback = np.full((rows, 4), "rest_ack", dtype="U16")
        callback[:, :2] = "cancel_ack"
        response_fields = {
            "rest_completion_upper_bound_observed_mask": upper_mask,
            "rest_completion_upper_bound_by_next_request_ms": upper,
            "completion_source_callback_type": callback,
        }
    np.savez(
        path,
        slot_names=np.asarray(
            ["cancel_buy", "cancel_sell", "new_buy", "new_sell"]
        ),
        request_present_mask=present,
        request_start_offset_ms=offsets,
        exchange_effective_observed_mask=present,
        exchange_effective_latency_ms=effective,
        local_visibility_observed_mask=present,
        local_visibility_latency_ms=visible,
        **response_fields,
    )


def test_python_planned_maintenance_cancels_and_stops_new_quotes() -> None:
    result = _run()

    assert result["planned_quote_stop_triggered"] is True
    assert result["planned_quote_stop_trigger_ts_ms"] == 2_000
    assert result["planned_shutdown_orders_at_trigger"] == 2
    assert result["planned_shutdown_open_order_count"] == 0
    assert result["planned_shutdown_pending_new_order_count"] == 0
    assert result["planned_shutdown_pending_cancel_order_count"] == 0
    assert result["n_requotes"] == 2
    assert "new_order_exchange_effective_latency_sample_count" not in result
    assert "cancel_exchange_effective_latency_sample_count" not in result
    assert "cancel_ack_visibility_latency_sample_count" not in result
    assert "cancel_latency_split_enabled" not in result
    assert sum(
        row.get("cancel_reason") == "planned_maintenance"
        for row in result["_quote_trace"]
    ) == 2


def test_disabled_serial_rest_gateway_preserves_b0_outputs() -> None:
    baseline = _run()
    disabled = _run(
        param_overrides={
            "rest_gateway_timing_mode": "disabled",
            "rest_gateway_timing_profile_path": "/ignored/when/disabled.npz",
        }
    )

    for key in (
        "pnl",
        "final_inventory",
        "fills_bid",
        "fills_ask",
        "n_requotes",
        "planned_shutdown_orders_at_trigger",
    ):
        assert disabled[key] == pytest.approx(baseline[key])
    assert disabled["_quote_trace"] == baseline["_quote_trace"]
    assert "rest_gateway_timing_mode" not in disabled


def test_zero_main_loop_sleep_preserves_default_replay() -> None:
    baseline = _run()
    disabled = _run(param_overrides={"replay_main_loop_sleep_ms": 0})
    for key in ("pnl", "final_inventory", "n_requotes", "_quote_trace", "_fill_trace"):
        assert disabled[key] == baseline[key]
    assert "replay_main_loop_clock" not in disabled


@pytest.mark.parametrize("value", [-1, 0.5, float("nan"), float("inf")])
def test_main_loop_sleep_requires_integer_duration(value) -> None:
    with pytest.raises(ValueError, match="non-negative integer"):
        _run(param_overrides={"replay_main_loop_sleep_ms": value})


def test_main_loop_requires_serial_http_return_clock() -> None:
    with pytest.raises(ValueError, match="REST-return timing"):
        _run(param_overrides={"replay_main_loop_sleep_ms": 100})


@pytest.mark.parametrize("extra_trade_events", [False, True])
def test_main_loop_sleep_uses_actual_return_phase_not_market_or_fixed_grid(
    tmp_path, extra_trade_events,
) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(20.0, 60.0, 80.0), new_clocks=(2.0, 37.0),
    )
    trades, bbo = _inputs()
    if extra_trade_events:
        extra = trades.iloc[:8].copy()
        extra["transact_time"] = [7, 19, 113, 114, 1_001, 1_013, 1_014, 1_015]
        trades = pd.concat((trades, extra), ignore_index=True).sort_values(
            "transact_time", kind="stable", ignore_index=True,
        )
    result = simulate_tick(
        trades, np.asarray([0], dtype=np.int64), np.asarray([1.0]),
        {**_params(), "replay_purpose": "diagnostic",
         "rest_gateway_timing_mode": "sampled_serial",
         "rest_gateway_timing_profile_path": str(profile),
         "replay_main_loop_sleep_ms": 100,
         "_decision_to_gateway_latency_samples_ms": [40.0],
         "planned_quote_stop_ts_ms": 0, "trace_decisions_max": 100},
        bbo_data=bbo,
    )
    decisions = [r for r in result["_decision_trace"] if r["side"] == "BUY"]
    # First tick: compute 40 + NEW BUY 37 + NEW SELL 37 = return at 114.
    # Wakeups are 214, 314, ...; the 1s requote becomes due at 1014.
    # Later replace calls add two 80ms cancels, retain their new phase, and
    # anchor the next deadline to actual start rather than catch-up at 2000.
    assert [r["ts_ms"] for r in decisions] == [0, 1_014, 2_088, 3_162]
    assert result["replay_main_loop_requote_anchor"] == "actual_requote_start"
    assert result["replay_main_loop_unmodeled_work"] == "periodic_position_sync_and_health_io"
    assert result["rest_gateway_pending_decision_count"] == 0


def test_main_loop_keep_still_consumes_compute_then_sleeps(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(20.0, 60.0, 80.0), new_clocks=(2.0, 37.0),
    )
    result = _run(param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "replay_main_loop_sleep_ms": 100,
        "_decision_to_gateway_latency_samples_ms": [40.0],
        "requote_threshold_bps": 1.0,
        "planned_quote_stop_ts_ms": 0, "trace_decisions_max": 100,
    })
    decisions = [r for r in result["_decision_trace"] if r["side"] == "BUY"]
    assert [r["ts_ms"] for r in decisions] == [0, 1_014, 2_054, 3_094]
    assert [r["action"] for r in decisions] == ["place", "keep", "keep", "keep"]
    assert result["rest_gateway_request_count"] == 2


def test_main_loop_long_http_call_does_not_delay_exchange_or_private_fill(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(20.0, 60.0, 80.0), new_clocks=(2.0, 350.0),
    )
    result = _run(crossing_fill_ts_ms=100, param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "replay_main_loop_sleep_ms": 100,
        "_decision_to_gateway_latency_samples_ms": [40.0],
        "_private_fill_visibility_latency_samples_ms": [10.0],
        "requote_interval": 0.2, "rq_min": 0.2, "rq_max": 0.2,
        "planned_quote_stop_ts_ms": 0, "trace_decisions_max": 100,
    })
    decisions = [r for r in result["_decision_trace"] if r["side"] == "BUY"]
    assert [r["ts_ms"] for r in decisions[:2]] == [0, 840]
    assert result["_fill_trace"][0]["fill_ts"] == 100
    assert result["_fill_trace"][0]["last_private_fill_visible_ts_ms"] == 110
    assert result["private_fill_exchange_match_count"] == result["private_fill_visible_count"]


def test_zero_pre_snapshot_compute_preserves_off_output(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(20.0, 60.0, 80.0),
    )
    params = {
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "replay_main_loop_sleep_ms": 100, "_decision_to_gateway_latency_samples_ms": [40.0],
    }
    off = _run(param_overrides=params)
    zero = _run(param_overrides={**params, "_pre_snapshot_compute_latency_samples_ms": [0.0, 0.0]})
    for key in ("pnl", "final_inventory", "n_requotes", "_quote_trace", "_fill_trace"):
        assert zero[key] == off[key]
    assert not any(key.startswith("pre_snapshot_compute") for key in zero)
    baseline = _run()
    zero_without_total = _run(param_overrides={"_pre_snapshot_compute_latency_samples_ms": [0.0]})
    assert zero_without_total["_quote_trace"] == baseline["_quote_trace"]


@pytest.mark.parametrize("pre,total", [
    ([1.0], [0.0]), ([1.0, 2.0], [3.0]), ([1.0, 2.0], [3.0, np.nan]),
    ([np.nan], [3.0]), ([-1.0], [3.0]), ([[1.0]], [[3.0]]),
])
def test_pre_snapshot_compute_rejects_unpaired_or_invalid_samples(pre, total) -> None:
    with pytest.raises(ValueError, match="pre-snapshot"):
        _run(param_overrides={
            "replay_purpose": "diagnostic", "_decision_to_gateway_latency_samples_ms": total,
            "_pre_snapshot_compute_latency_samples_ms": pre,
        })


@pytest.mark.parametrize("seed", [7, 19, 73])
def test_pre_snapshot_compute_pairs_with_total_without_double_counting(tmp_path, seed) -> None:
    from models.backtest_tick import _deterministic_decision_to_gateway_latency_ms

    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(20.0, 60.0, 80.0), new_clocks=(2.0, 37.0),
    )
    pre = np.asarray([20.0, 30.0, 50.0])
    total = np.asarray([40.0, 80.0, 90.0])
    result = _run(param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "replay_main_loop_sleep_ms": 100, "decision_to_gateway_latency_seed": seed,
        "_decision_to_gateway_latency_samples_ms": total,
        "_pre_snapshot_compute_latency_samples_ms": pre,
        "trace_decisions_max": 100,
    })
    pre_ms = _deterministic_decision_to_gateway_latency_ms(pre, seed=seed, decision_ts_ms=0)
    total_ms = _deterministic_decision_to_gateway_latency_ms(total, seed=seed, decision_ts_ms=0)
    assert (pre_ms, total_ms) in {(20, 40), (30, 80), (50, 90)}
    decisions = result["_decision_trace"]
    assert decisions[0]["ts_ms"] == pre_ms
    first = [row for row in result["_quote_trace"] if row["submit_ts"] == pre_ms]
    assert [row["side"] for row in first] == ["BUY", "SELL"]
    assert [row["activate_ts"] for row in first] == [total_ms + 2, total_ms + 39]
    assert result["pre_snapshot_compute_count"] == result["pre_snapshot_compute_completed_count"]
    assert result["pre_snapshot_compute_abandoned_count"] == 0


@pytest.mark.parametrize("stop_ms,end_ms", [(100, 4_000), (0, 100)])
def test_pre_snapshot_compute_cannot_send_after_stop_or_window_end(tmp_path, stop_ms, end_ms) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(20.0, 60.0, 80.0),
    )
    trades, bbo = _inputs()
    trades = trades.loc[trades["transact_time"] <= end_ms].copy()
    result = simulate_tick(trades, np.asarray([0]), np.asarray([1.0]), {**_params(),
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile), "replay_main_loop_sleep_ms": 100,
        "_decision_to_gateway_latency_samples_ms": [300.0],
        "_pre_snapshot_compute_latency_samples_ms": [200.0],
        "planned_quote_stop_ts_ms": stop_ms, "replay_event_clock_end_ts_ms": end_ms,
    }, bbo_data=bbo)
    assert result["_quote_trace"] == []
    assert result["rest_gateway_request_count"] == 0
    assert result["pre_snapshot_compute_count"] == 1
    assert result["pre_snapshot_compute_completed_count"] == 0
    assert result["pre_snapshot_compute_abandoned_count"] == 1


def test_pre_snapshot_compute_freezes_prediction_but_captures_later_visible_state(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(20.0, 60.0, 80.0),
    )
    base = 10_000
    trades, original_bbo = _inputs(crossing_fill_ts_ms=100)
    trades = trades.loc[trades["transact_time"] <= 1_000].copy()
    trades["transact_time"] += base
    source_ms = np.concatenate(([base - 1_000], original_bbo.ts_ms + base))
    mid = np.where(source_ms < base + 100, 100.0,
                   np.where(source_ms < base + 200, 110.0, 120.0))
    bbo = HistoricalBBOData(
        ts_ms=source_ms, best_bid=mid - 0.1, best_ask=mid + 0.1,
        bid_qty=np.ones(source_ms.size), ask_qty=np.ones(source_ms.size),
    )
    depth = HistoricalL2Data(
        ts_ms=source_ms, bid_px=bbo.best_bid[:, None], ask_px=bbo.best_ask[:, None],
        bid_qty=bbo.bid_qty[:, None], ask_qty=bbo.ask_qty[:, None],
    )
    # Finalized bars remain left-labelled. The prior second's completed
    # variance is delivered during signal compute, at base + 100ms.
    feature_ms = base + np.asarray([-2_000, -1_000, 0])
    prediction_ms = base + np.asarray([-1_000, 0, 100])

    def clocks(exchange):
        ns = np.asarray(exchange, dtype=np.int64) * 1_000_000
        return {"exchange_ts_ns": ns, "receive_ts_ns": ns.copy(),
                "feature_ready_ts_ns": ns.copy()}

    variance_clock = clocks(feature_ms)
    variance_clock["receive_ts_ns"] = (
        base + np.asarray([-1_000, 100, 1_200])
    ) * 1_000_000
    variance_clock["feature_ready_ts_ns"] = variance_clock["receive_ts_ns"].copy()
    delivery = {
        "bbo": clocks(source_ms), "depth": clocks(source_ms),
        "variance": variance_clock, "prediction": clocks(prediction_ms),
        "trade": {**clocks([base - 1_000]), "last_child_row_index": np.asarray([-1])},
    }
    result = simulate_tick(
        trades, feature_ms, np.asarray([1.0, 9.0, 25.0]),
        {**_params(), "replay_purpose": "diagnostic",
         "rest_gateway_timing_mode": "sampled_serial",
         "rest_gateway_timing_profile_path": str(profile), "replay_main_loop_sleep_ms": 100,
         "_decision_to_gateway_latency_samples_ms": [250.0],
         "_pre_snapshot_compute_latency_samples_ms": [200.0],
         "_private_fill_visibility_latency_samples_ms": [50.0],
         "initial_live_state": {"active_orders": [
             {"side": "BUY", "price": 98.0, "quantity": 0.001, "status": "OPEN"}]},
         "exec_book_visibility_mode": "message_schedule", "_exec_message_delivery": delivery,
         "vol_blend": 0.1, "use_bar_pricing": False, "planned_quote_stop_ts_ms": base + 500,
         "replay_event_clock_end_ts_ms": base + 1_000, "trace_decisions_max": 100},
        ml_data=(prediction_ms, np.asarray([0.4, 0.6, 0.8]), np.ones(3),
                 np.asarray([0.001, 0.002, 0.003])),
        bbo_data=bbo, l2_data=depth,
    )
    decision = result["_decision_trace"][0]
    # Prediction ready exactly at entry and book ready exactly at capture are
    # excluded. The completed prior-second variance delivered at 100ms is known.
    assert decision["ts_ms"] == base + 200
    assert decision["prediction_generation_index"] == 0
    assert decision["pred_ret"] == pytest.approx(0.001)
    assert decision["mid"] == 110.0
    assert decision["sigma_sq_raw"] == 9.0
    assert decision["inventory"] == pytest.approx(0.001)
    fill = result["_fill_trace"][0]
    assert fill["fill_ts"] == base + 100
    assert fill["last_private_fill_visible_ts_ms"] == base + 150


@pytest.mark.parametrize("first_bar_ready_ms,second_quote_ms", [(1_001, 1_010), (2_510, 2_610)])
def test_main_loop_dynamic_rq_consumes_only_delivered_bars_before_due_check(
    tmp_path, first_bar_ready_ms, second_quote_ms,
) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(20.0, 60.0, 80.0),
    )
    base = 10_000
    trades, original_bbo = _inputs()
    trades["transact_time"] += base
    source_ms = np.concatenate(([base - 1_000], original_bbo.ts_ms + base))
    bbo = HistoricalBBOData(
        ts_ms=source_ms, best_bid=np.full(source_ms.size, 99.9),
        best_ask=np.full(source_ms.size, 100.1),
        bid_qty=np.ones(source_ms.size), ask_qty=np.ones(source_ms.size),
    )
    depth = HistoricalL2Data(
        ts_ms=source_ms, bid_px=bbo.best_bid[:, None], ask_px=bbo.best_ask[:, None],
        bid_qty=bbo.bid_qty[:, None], ask_qty=bbo.ask_qty[:, None],
    )
    # Left labels of the bars whose completion/arrival clocks follow below.
    variance_ms = base + np.asarray([-2_000, 0, 1_000, 2_000])

    def clocks(exchange, ready=None):
        exchange_ns = np.asarray(exchange, dtype=np.int64) * 1_000_000
        return {
            "exchange_ts_ns": exchange_ns, "receive_ts_ns": exchange_ns.copy(),
            "feature_ready_ts_ns": (exchange_ns.copy() if ready is None else
                                    np.asarray(ready, dtype=np.int64) * 1_000_000),
        }

    delivery = {
        "bbo": clocks(source_ms), "depth": clocks(source_ms),
        "variance": clocks(
            variance_ms, base + np.asarray([-999, first_bar_ready_ms, 2_700, 3_001]),
        ),
        "trade": {**clocks([base - 1_000]), "last_child_row_index": np.asarray([-1])},
    }
    result = simulate_tick(
        trades, variance_ms, np.ones(variance_ms.size),
        {**_params(), "replay_purpose": "diagnostic",
         "rest_gateway_timing_mode": "sampled_serial",
         "rest_gateway_timing_profile_path": str(profile),
         "replay_main_loop_sleep_ms": 100,
         "requote_interval": 5.0, "rq_min": 0.5, "rq_max": 5.0,
         "exec_book_visibility_mode": "message_schedule", "_exec_message_delivery": delivery,
         "use_bar_pricing": False, "planned_quote_stop_ts_ms": 0,
         "replay_event_clock_end_ts_ms": base + 4_000, "trace_decisions_max": 100},
        bbo_data=bbo, l2_data=depth, var_retsq=np.asarray([0.0, 1.0, 100.0, 1.0]),
    )
    decisions = [r for r in result["_decision_trace"] if r["side"] == "BUY"]
    # The first delivered squared return sets fast/slow to 1, so the interval
    # immediately becomes rq_min. No seventh-requote warmup is invented, and
    # a bar ready exactly at a wake (2510) waits for the next wake (2610).
    assert [r["ts_ms"] - base for r in decisions[:2]] == [0, second_quote_ms]
    assert result["replay_main_loop_dynamic_rq_clock"] == "delivered_1s_bars_before_due_check"


def test_zero_decision_to_gateway_latency_preserves_b0_outputs() -> None:
    baseline = _run()
    zero_delay = _run(
        param_overrides={"_decision_to_gateway_latency_samples_ms": [0.0]}
    )

    for key in (
        "pnl",
        "final_inventory",
        "fills_bid",
        "fills_ask",
        "n_requotes",
        "planned_shutdown_orders_at_trigger",
    ):
        assert zero_delay[key] == pytest.approx(baseline[key])
    assert zero_delay["_quote_trace"] == baseline["_quote_trace"]
    assert "decision_to_gateway_latency_authority" not in zero_delay


def test_decision_to_gateway_latency_shifts_requests_not_decision_snapshot() -> None:
    baseline = _run()
    delayed = _run(
        param_overrides={
            "replay_purpose": "diagnostic",
            "_decision_to_gateway_latency_samples_ms": [40.0],
        }
    )

    baseline_orders = {
        (row["side"], row["submit_ts"]): row
        for row in baseline["_quote_trace"]
        if row["submit_ts"] == 1_000
    }
    delayed_orders = {
        (row["side"], row["submit_ts"]): row
        for row in delayed["_quote_trace"]
        if row["submit_ts"] == 1_000
    }
    assert delayed_orders.keys() == baseline_orders.keys()
    for identity, row in delayed_orders.items():
        baseline_row = baseline_orders[identity]
        assert row["price"] == pytest.approx(baseline_row["price"])
        assert row["mid"] == pytest.approx(baseline_row["mid"])
        assert row["best_bid"] == pytest.approx(baseline_row["best_bid"])
        assert row["best_ask"] == pytest.approx(baseline_row["best_ask"])
        assert row["gateway_request_ts"] == row["submit_ts"] + 40
        assert row["activate_ts"] == row["gateway_request_ts"]
        # Planned shutdown is a short safety path, not another requote.
        assert row["cancel_request_ts"] == 2_000
        assert row["outcome_ts"] == 2_500
    assert delayed["decision_to_gateway_latency_authority"] == "diagnostic_only"
    assert delayed["decision_market_snapshot_clock"] == "decision_time_frozen"


def test_nonzero_decision_to_gateway_latency_requires_diagnostic_purpose() -> None:
    with pytest.raises(ValueError, match="diagnostic-only"):
        _run(
            param_overrides={
                "_decision_to_gateway_latency_samples_ms": [1.0]
            }
        )


def test_serial_rest_gateway_uses_one_row_in_live_slot_order(tmp_path) -> None:
    profile_path = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile_path,
        [
            (False, False, True, True),
            (True, True, True, True),
            (True, True, False, False),
        ],
    )

    result = _run(
        param_overrides={
            "replay_purpose": "diagnostic",
            "rest_gateway_timing_mode": "paired_npz",
            "rest_gateway_timing_profile_path": str(profile_path),
            "rest_gateway_timing_seed": 7,
        }
    )

    assert result["rest_gateway_timing_authority"] == "diagnostic_only"
    assert result["rest_gateway_timing_profile_row_count"] == 3
    assert result["rest_gateway_timing_sampled_row_count"] == 3
    final_orders = {
        row["side"]: row
        for row in result["_quote_trace"]
        if row["submit_ts"] == 1_000
    }
    assert final_orders["BUY"]["gateway_request_ts"] == 1_020
    assert final_orders["SELL"]["gateway_request_ts"] == 1_030
    assert final_orders["BUY"]["activate_ts"] == 1_022
    assert final_orders["SELL"]["activate_ts"] == 1_032
    assert final_orders["BUY"]["cancel_request_ts"] == 2_000
    assert final_orders["SELL"]["cancel_request_ts"] == 2_010
    assert final_orders["BUY"]["outcome_ts"] == 2_005
    assert final_orders["SELL"]["outcome_ts"] == 2_015


def test_serial_gateway_offsets_start_after_one_shared_decision_delay(
    tmp_path,
) -> None:
    profile_path = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile_path,
        [
            (False, False, True, True),
            (True, True, True, True),
            (True, True, False, False),
        ],
    )

    result = _run(
        param_overrides={
            "replay_purpose": "diagnostic",
            "_decision_to_gateway_latency_samples_ms": [40.0],
            "rest_gateway_timing_mode": "paired_npz",
            "rest_gateway_timing_profile_path": str(profile_path),
            "rest_gateway_timing_seed": 7,
        }
    )

    final_orders = {
        row["side"]: row
        for row in result["_quote_trace"]
        if row["submit_ts"] == 1_000
    }
    assert final_orders["BUY"]["gateway_request_ts"] == 1_060
    assert final_orders["SELL"]["gateway_request_ts"] == 1_070
    assert final_orders["BUY"]["activate_ts"] == 1_062
    assert final_orders["SELL"]["activate_ts"] == 1_072
    assert final_orders["BUY"]["cancel_request_ts"] == 2_000
    assert final_orders["SELL"]["cancel_request_ts"] == 2_010


def test_serial_rest_gateway_rejects_unobserved_request_mask(tmp_path) -> None:
    profile_path = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile_path,
        [(False, False, True, True)],
    )

    with pytest.raises(ValueError, match="no exact observed request mask"):
        _run(
            param_overrides={
                "replay_purpose": "diagnostic",
                "rest_gateway_timing_mode": "paired_npz",
                "rest_gateway_timing_profile_path": str(profile_path),
            }
        )


def test_sampled_serial_rest_preserves_request_pairs_and_live_slot_order() -> None:
    result = _run(
        param_overrides={
            "replay_purpose": "diagnostic",
            "rest_gateway_timing_mode": "sampled_serial",
            "_decision_to_gateway_latency_samples_ms": [30.0],
            "_new_order_exchange_effective_latency_samples_ms": [40.0],
            "_new_order_latency_samples_ms": [100.0],
            "_cancel_exchange_effective_latency_samples_ms": [20.0],
            "_cancel_ack_visibility_latency_samples_ms": [60.0],
        }
    )
    # No joint-mask/profile file is required: measured single-request service
    # times run in live's cancel BUY, cancel SELL, new BUY, new SELL order.
    initial = {
        row["side"]: row for row in result["_quote_trace"] if row["submit_ts"] == 0
    }
    final = {
        row["side"]: row for row in result["_quote_trace"] if row["submit_ts"] == 1_000
    }
    assert initial["BUY"]["gateway_request_ts"] == 30
    assert initial["SELL"]["gateway_request_ts"] == 130
    assert initial["BUY"]["cancel_request_ts"] == 1_030
    assert initial["BUY"]["outcome_ts"] == 1_090
    assert initial["SELL"]["cancel_request_ts"] == 1_090
    assert initial["SELL"]["outcome_ts"] == 1_150
    assert final["BUY"]["gateway_request_ts"] == 1_150
    assert final["SELL"]["gateway_request_ts"] == 1_250
    for row in result["_quote_trace"]:
        assert row["activate_ts"] - row["gateway_request_ts"] == 40
        assert row["new_ack_ts"] - row["gateway_request_ts"] == 100
    # Maintenance does not repeat the normal requote's compute delay.
    assert final["BUY"]["cancel_request_ts"] == 2_000
    assert final["SELL"]["cancel_request_ts"] == 2_060
    assert result["rest_gateway_request_count"] == 8
    assert result["rest_gateway_busy_ms"] == 640
    assert result["rest_gateway_timing_authority"] == "diagnostic_only"
    assert result["rest_gateway_sampling_assumption"] == (
        "independent_request_service_times_with_paired_effective_ack"
    )


def test_sampled_serial_busy_lane_defers_decisions_not_exchange_fills() -> None:
    result = _run(
        crossing_fill_ts_ms=1_200,
        param_overrides={
            "replay_purpose": "diagnostic",
            "rest_gateway_timing_mode": "sampled_serial",
            "_new_order_exchange_effective_latency_samples_ms": [100.0],
            "_new_order_latency_samples_ms": [800.0],
            "_cancel_exchange_effective_latency_samples_ms": [50.0],
            "_cancel_ack_visibility_latency_samples_ms": [300.0],
            "trace_decisions_max": 100,
        },
    )
    assert result["rest_gateway_decision_deferral_count"] > 0
    assert result["fills_bid"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 1_200
    decision_times = {row["ts_ms"] for row in result["_decision_trace"]}
    assert not any(0 < ts < 1_600 for ts in decision_times)
    assert result["rest_gateway_request_wait_ms"] > 0


def test_sampled_serial_keeps_multielement_service_draws() -> None:
    pairs = {(13, 113), (29, 229), (47, 347)}
    observed = set()
    for seed in (7, 19, 73):
        params = {
            "replay_purpose": "diagnostic",
            "latency_seed": seed,
            "_new_order_exchange_effective_latency_samples_ms": [13.0, 29.0, 47.0],
            "_new_order_latency_samples_ms": [113.0, 229.0, 347.0],
            "_cancel_exchange_effective_latency_samples_ms": [11.0, 23.0, 37.0],
            "_cancel_ack_visibility_latency_samples_ms": [111.0, 223.0, 337.0],
        }
        draws = {}
        for mode in ("disabled", "sampled_serial"):
            result = _run(param_overrides={**params, "rest_gateway_timing_mode": mode})
            draws[mode] = {}
            for row in result["_quote_trace"]:
                if row["submit_ts"] != 0:
                    continue
                request = row.get("gateway_request_ts", row["submit_ts"])
                pair = (row["activate_ts"] - request, row["new_ack_ts"] - request)
                assert pair in pairs
                draws[mode][row["side"]] = pair
                observed.add(pair)
        assert draws["disabled"] == draws["sampled_serial"]
    assert len(observed) > 1


def test_sampled_serial_zero_service_times_and_disabled_preserve_b0() -> None:
    zeros = {
        "cancel_order_latency_ms": 0,
        "_new_order_exchange_effective_latency_samples_ms": [0.0],
        "_new_order_latency_samples_ms": [0.0],
        "_cancel_exchange_effective_latency_samples_ms": [0.0],
        "_cancel_ack_visibility_latency_samples_ms": [0.0],
    }
    baseline = _run(param_overrides=zeros)
    for mode in ("disabled", "sampled_serial"):
        result = _run(param_overrides={
            **zeros, "replay_purpose": "diagnostic", "rest_gateway_timing_mode": mode,
        })
        for key in ("pnl", "final_inventory", "fills_bid", "fills_ask", "n_requotes"):
            assert result[key] == baseline[key]
        assert [
            (row["side"], row["price"], row["submit_ts"], row["outcome_ts"])
            for row in result["_quote_trace"]
        ] == [
            (row["side"], row["price"], row["submit_ts"], row["outcome_ts"])
            for row in baseline["_quote_trace"]
        ]


@pytest.mark.parametrize("response_ms,expect_replace", [(40.0, False), (80.0, True)])
def test_serial_http_return_controls_replacement_not_private_ack(
    tmp_path, response_ms, expect_replace,
) -> None:
    profile = tmp_path / "gateway.npz"
    # A single request sample per side is enough; no observed whole-decision
    # mask is manufactured or required by this independent-request simulation.
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(20.0, 60.0, response_ms),
    )
    result = _run(param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "trace_decisions_max": 100,
    })
    initial = {row["side"]: row for row in result["_quote_trace"] if row["submit_ts"] == 0}
    replacements = [row for row in result["_quote_trace"] if row["submit_ts"] == 1_000]
    assert initial["BUY"]["cancel_request_ts"] == 1_000
    assert initial["SELL"]["cancel_request_ts"] == 1_000 + response_ms
    assert initial["BUY"]["outcome_ts"] == 1_060
    assert len(replacements) == (2 if expect_replace else 0)
    assert result["rest_gateway_return_pending_coalesce_count"] == (0 if expect_replace else 2)
    assert result["rest_gateway_response_clock_semantics"] == "paired_observed_upper_bound"
    if expect_replace:
        assert min(row["gateway_request_ts"] for row in replacements) == 1_160
    else:
        decisions = [row for row in result["_decision_trace"] if row["ts_ms"] == 1_000]
        assert {row["action"] for row in decisions} == {"pending_coalesce"}
        # The next HTTP request is allowed before the previous private callback.
        assert initial["SELL"]["cancel_request_ts"] < initial["BUY"]["outcome_ts"]


def test_serial_http_return_observes_full_fill_during_cancel(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(150.0, 400.0, 200.0),
    )
    result = _run(crossing_fill_ts_ms=1_100, param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "_private_fill_visibility_latency_samples_ms": [10.0],
    })
    replacements = [row for row in result["_quote_trace"] if row["submit_ts"] == 1_000]
    # BUY filled and became locally terminal at 1110, before REST returned at
    # 1200, although the separate cancel callback would not arrive until 1400.
    assert result["fills_bid"] == 1
    assert {row["side"] for row in replacements} == {"BUY"}
    assert replacements[0]["gateway_request_ts"] == 1_400
    assert result["rest_gateway_return_pending_coalesce_count"] == 1


def test_serial_inventory_limit_cancel_keeps_exchange_exposure_until_effective(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(200.0, 400.0, 250.0),
    )
    trades, bbo = _inputs(crossing_fill_ts_ms=100)
    for timestamp in (100, 200, 400):
        index = int(np.flatnonzero(trades["transact_time"].to_numpy() == timestamp)[0])
        trades.loc[index, ["price", "quantity"]] = [96.0, 0.001]
    result = simulate_tick(
        trades, np.asarray([0], dtype=np.int64), np.asarray([1.0]),
        {
            **_params(), "replay_purpose": "diagnostic",
            "rest_gateway_timing_mode": "sampled_serial",
            "rest_gateway_timing_profile_path": str(profile),
            "_private_fill_visibility_latency_samples_ms": [10.0],
            "initial_inventory": 0.001, "initial_entry_price": 100.0,
            "max_inventory": 0.002, "requote_interval": 100.0,
            "rq_min": 100.0, "rq_max": 100.0,
            "replace_min_price_change_ticks": 1_000_000.0,
            "initial_live_state": {"active_orders": [{
                "side": "BUY", "price": 98.0, "quantity": 0.003,
                "remaining": 0.003, "status": "OPEN",
            }]},
        }, bbo_data=bbo,
    )
    # The first callback reaches the local limit at 110ms. The order still
    # matches at 200ms, before CANCEL takes effect at 310ms; the 400ms trade
    # cannot match, even though the private cancel callback arrives at 510ms.
    assert result["buy_fill_qty"] == pytest.approx(0.002)
    assert [row["fill_ts"] for row in result["_fill_trace"]] == [100, 200]
    cancels = [row for row in result["_quote_trace"]
               if row.get("cancel_reason") == "inventory_limit"]
    assert len(cancels) == 1
    assert cancels[0]["cancel_request_ts"] == 110
    assert cancels[0]["cancel_effective_ts"] == 310
    assert cancels[0]["cancel_ack_ts"] == 510
    assert cancels[0]["outcome_ts"] == 510


@pytest.mark.parametrize("private_delay_ms", [1_200.0, 5_000.0])
def test_serial_cancel_cannot_terminalize_exchange_filled_order(
    tmp_path, private_delay_ms,
) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(200.0, 400.0, 250.0),
    )
    result = _run(crossing_fill_ts_ms=300, param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "_private_fill_visibility_latency_samples_ms": [private_delay_ms],
        "planned_quote_stop_ts_ms": 500,
    })
    buy = [row for row in result["_quote_trace"] if row["side"] == "BUY"]
    assert len(buy) == 1
    assert buy[0]["cancel_request_ts"] == 500
    assert buy[0]["cancel_rest_return_ts"] == 750
    assert buy[0]["cancel_terminal_suppressed_full_fill"] is True
    assert buy[0]["cancel_ack_ts"] == -1
    # The hypothetical success callback at 900 cannot release ownership.
    # Resolve only on the actual fill callback, or censor at the input bound.
    received = private_delay_ms == 1_200.0
    assert buy[0]["outcome"] == ("fill" if received else "open_end")
    if received:
        assert buy[0]["outcome_ts"] == 1_500
    assert result["private_fill_exchange_match_count"] == 1
    assert result["private_fill_visible_count"] == int(received)
    assert result["economic_pnl_complete"] is received
    assert result["exchange_inventory_at_window_end"] == pytest.approx(0.001)
    assert result["exchange_pending_quantity"] == pytest.approx(0.0 if received else 0.001)
    if not received:
        assert result["economic_pnl_status"] == "incomplete_pending_private_fills"
        assert result["pnl_clock_scope"] == "local_fill_visibility_at_window_end"
        assert result["final_inventory"] == 0.0


@pytest.mark.parametrize("status", ["OPEN", "PENDING_CANCEL"])
def test_serial_restored_orders_do_not_dispatch_new_rest(tmp_path, status) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(20.0, 60.0, 40.0), new_clocks=(2.0, 2_500.0),
    )
    result = _run(param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
        "planned_quote_stop_ts_ms": 100,
        "replace_min_price_change_ticks": 1_000_000.0,
        "initial_live_state": {"active_orders": [
            {"side": side, "price": price, "quantity": 0.001, "status": status,
             "cancel_request_ts_ms": -100, "cancel_effective_ts_ms": 300,
             "cancel_ts_ms": 500}
            for side, price in (("BUY", 98.0), ("SELL", 102.0))
        ]},
    })
    assert result["initial_live_state_orders_restored"] == 2
    assert result["rest_gateway_request_count"] == (2 if status == "OPEN" else 0)
    assert result["rest_gateway_decision_deferral_count"] == 0
    assert result["planned_shutdown_open_order_count"] == 0
    assert all(row["restored_order"] for row in result["_quote_trace"])
    assert all(row["new_rest_return_ts"] == -1 for row in result["_quote_trace"])


def test_serial_http_stop_drops_unsent_new_intent_and_drains_inflight_submit(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(20.0, 60.0, 40.0), new_clocks=(2.0, 2_500.0),
    )
    result = _run(param_overrides={
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
    })
    assert {row["side"] for row in result["_quote_trace"]} == {"BUY"}
    assert result["rest_gateway_request_count"] == 2
    assert result["planned_shutdown_open_order_count"] == 0
    assert result["planned_shutdown_pending_new_order_count"] == 0
    assert result["rest_gateway_pending_decision_count"] == 0


def test_serial_http_zero_profile_and_multiline_pairs(tmp_path) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        cancel_clocks=(0.0, 0.0, 0.0), new_clocks=(0.0, 0.0),
    )
    zeros = {
        "cancel_order_latency_ms": 0,
        "_new_order_exchange_effective_latency_samples_ms": [0.0],
        "_new_order_latency_samples_ms": [0.0],
        "_cancel_exchange_effective_latency_samples_ms": [0.0],
        "_cancel_ack_visibility_latency_samples_ms": [0.0],
    }
    baseline = _run(param_overrides=zeros)
    delayed = _run(param_overrides={
        **zeros, "replay_purpose": "diagnostic",
        "rest_gateway_timing_mode": "sampled_serial",
        "rest_gateway_timing_profile_path": str(profile),
    })
    for key in ("pnl", "fills_bid", "fills_ask", "n_requotes", "final_inventory"):
        assert delayed[key] == baseline[key]
    assert [(r["side"], r["submit_ts"], r["outcome_ts"]) for r in delayed["_quote_trace"]] == [
        (r["side"], r["submit_ts"], r["outcome_ts"]) for r in baseline["_quote_trace"]
    ]
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)] * 3,
        cancel_clocks=(10.0, 30.0, 20.0), new_clocks=(2.0, 5.0),
    )
    with np.load(profile, allow_pickle=False) as archive:
        arrays = {key: archive[key].copy() for key in archive.files}
    for i, scale in enumerate((1.0, 2.0, 3.0)):
        for key in (
            "exchange_effective_latency_ms", "local_visibility_latency_ms",
            "rest_completion_upper_bound_by_next_request_ms",
        ):
            arrays[key][i] *= scale
    np.savez(profile, **arrays)
    seen = set()
    for seed in (7, 19, 73):
        result = _run(param_overrides={
            "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
            "rest_gateway_timing_profile_path": str(profile), "latency_seed": seed,
        })
        for row in result["_quote_trace"]:
            if row["submit_ts"] != 0:
                continue
            request = row["gateway_request_ts"]
            new_pair = (row["activate_ts"] - request, row["new_rest_return_ts"] - request)
            assert new_pair in {(2, 5), (4, 10), (6, 15)}
            seen.add(new_pair)
            cancel = row["cancel_request_ts"]
            triple = (
                row["cancel_effective_ts"] - cancel, row["cancel_ack_ts"] - cancel,
                row["cancel_rest_return_ts"] - cancel,
            )
            assert triple in {(10, 30, 20), (20, 60, 40), (30, 90, 60)}
    assert len(seen) > 1


@pytest.mark.parametrize("main_loop_sleep_ms", [0, 100])
def test_serial_http_continuation_merges_native_book_boundaries(
    tmp_path, main_loop_sleep_ms,
) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)], cancel_clocks=(20.0, 60.0, 80.0),
    )
    base = 1_700_000_000_000
    trades, bbo = _inputs()
    trades["transact_time"] += base
    bbo = HistoricalBBOData(
        ts_ms=bbo.ts_ms + base, best_bid=bbo.best_bid, best_ask=bbo.best_ask,
        bid_qty=bbo.bid_qty, ask_qty=bbo.ask_qty,
    )
    snapshot = HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC", event_type="snapshot",
        exchange_ts_ns=(base - 100) * 1_000_000,
        local_receive_ts_ns=(base - 99) * 1_000_000,
        last_update_id=1, levels=(("bid", 960, 1.0), ("bid", 999, 1.0),
                                  ("ask", 1001, 1.0), ("ask", 1040, 1.0)),
    )
    result = simulate_tick(
        trades, np.asarray([base]), np.asarray([1.0]),
        {**_params(), "replay_purpose": "diagnostic",
         "rest_gateway_timing_mode": "sampled_serial",
         "rest_gateway_timing_profile_path": str(profile),
         "replay_main_loop_sleep_ms": main_loop_sleep_ms,
         "planned_quote_stop_ts_ms": base + 2_000,
         "replay_event_clock_end_ts_ms": base + 4_000,
         "exchange_book_queue_mode": "diagnostic"},
        bbo_data=bbo, exchange_book_event_tape=[snapshot],
    )
    phase_ms = 10 if main_loop_sleep_ms else 0
    replacements = [row for row in result["_quote_trace"]
                    if row["submit_ts"] == base + 1_000 + phase_ms]
    assert min(row["gateway_request_ts"] for row in replacements) == base + 1_160 + phase_ms
    assert result["rest_gateway_pending_decision_count"] == 0


def test_python_planned_maintenance_preserves_fill_risk_until_cancel_ack() -> None:
    result = _run(crossing_fill_ts_ms=2_200)

    assert result["planned_quote_stop_triggered"] is True
    assert result["fills_bid"] == 1
    assert result["fills_while_pending_cancel"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 2_200
    assert result["planned_shutdown_open_order_count"] == 0
    assert result["planned_shutdown_pending_new_order_count"] == 0
    assert result["planned_shutdown_pending_cancel_order_count"] == 0


def test_passive_fill_publisher_preserves_sell_accounting_and_trace() -> None:
    result = _run(crossing_fill_ts_ms=2_200, crossing_side="SELL")

    assert result["fills_ask"] == 1
    assert result["sell_fill_qty"] == pytest.approx(0.001)
    assert result["fills_while_pending_cancel"] == 1
    assert result["_fill_trace"][0]["side"] == "SELL"
    assert result["_fill_trace"][0]["fill_ts"] == 2_200


def test_zero_private_fill_visibility_preserves_b0_outputs() -> None:
    baseline = _run(crossing_fill_ts_ms=2_200)
    zero_delay = _run(
        crossing_fill_ts_ms=2_200,
        param_overrides={"_private_fill_visibility_latency_samples_ms": [0.0]},
    )

    for key in (
        "final_inventory",
        "pnl",
        "fills_bid",
        "fills_ask",
        "buy_fill_qty",
        "sell_fill_qty",
        "signed_inventory_time_s",
        "abs_inventory_time_s",
        "inventory_pnl",
    ):
        assert zero_delay[key] == pytest.approx(baseline[key])
    assert zero_delay["_fill_trace"] == baseline["_fill_trace"]
    assert zero_delay["_quote_trace"] == baseline["_quote_trace"]


def test_private_fill_visibility_delays_local_state_not_exchange_fill_time() -> None:
    result = _run(
        crossing_fill_ts_ms=2_200,
        param_overrides={"_private_fill_visibility_latency_samples_ms": [300.0]},
    )

    assert result["fills_bid"] == 1
    assert result["private_fill_exchange_match_count"] == 1
    assert result["private_fill_visible_count"] == 1
    assert result["private_fill_pending_visibility_count"] == 0
    assert result["_fill_trace"][0]["fill_ts"] == 2_200
    assert result["_fill_trace"][0]["last_exchange_fill_ts_ms"] == 2_200
    assert result["_fill_trace"][0]["last_private_fill_visible_ts_ms"] == 2_500
    fill_outcomes = [
        row for row in result["_quote_trace"] if row["outcome"] == "fill"
    ]
    assert [row["outcome_ts"] for row in fill_outcomes] == [2_500]
    assert result["signed_inventory_time_s"] == pytest.approx(0.0015)


def test_private_fill_can_publish_after_cancel_ack_removed_local_order() -> None:
    result = _run(
        crossing_fill_ts_ms=2_200,
        param_overrides={"_private_fill_visibility_latency_samples_ms": [500.0]},
    )

    fill_outcomes = [
        row for row in result["_quote_trace"] if row["outcome"] == "fill"
    ]
    assert result["fills_bid"] == 1
    assert [row["outcome_ts"] for row in fill_outcomes] == [2_700]


def test_private_fill_supports_exchange_fill_before_new_ack() -> None:
    result = _run(
        crossing_fill_ts_ms=300,
        param_overrides={
            "_new_order_exchange_effective_latency_samples_ms": [100.0],
            "_new_order_latency_samples_ms": [400.0],
            "_private_fill_visibility_latency_samples_ms": [50.0],
        },
    )

    assert result["fills_bid"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 300
    fill_outcomes = [
        row for row in result["_quote_trace"] if row["outcome"] == "fill"
    ]
    assert [row["outcome_ts"] for row in fill_outcomes] == [350]


def test_exchange_reservation_prevents_refill_before_private_visibility() -> None:
    trades, bbo = _inputs(crossing_fill_ts_ms=2_200)
    second = int(np.flatnonzero(trades["transact_time"].to_numpy() == 2_300)[0])
    trades.loc[second, "price"] = 96.0
    trades.loc[second, "quantity"] = 10.0
    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        {
            **_params(),
            "_private_fill_visibility_latency_samples_ms": [500.0],
        },
        bbo_data=bbo,
    )

    assert result["fills_bid"] == 1
    assert result["buy_fill_qty"] == pytest.approx(0.001)


def test_python_split_cancel_stops_matching_before_local_ack() -> None:
    result = _run(
        crossing_fill_ts_ms=2_200,
        param_overrides={
            "_new_order_latency_samples_ms": [900.0],
            "_new_order_exchange_effective_latency_samples_ms": [300.0],
            "_cancel_exchange_effective_latency_samples_ms": [100.0],
            "_cancel_ack_visibility_latency_samples_ms": [450.0],
        },
    )

    assert result["cancel_latency_split_enabled"] is True
    assert result["exchange_order_ack_replayed"] is True
    assert result["fills_bid"] == 0
    planned_cancels = [
        row
        for row in result["_quote_trace"]
        if row.get("cancel_reason") == "planned_maintenance"
    ]
    assert planned_cancels
    # There is no 2450-ms market/clock row and no native book scheduler.  The
    # generic lifecycle scheduler must still publish the ACK at its exact ms.
    assert {row["outcome_ts"] for row in planned_cancels} == {2_450}
    assert {row["cancel_request_ts"] for row in planned_cancels} == {2_000}
    assert {row["cancel_effective_ts"] for row in planned_cancels} == {2_100}
    assert {row["cancel_ack_ts"] for row in planned_cancels} == {2_450}
    assert {
        row["activate_ts"] - row["submit_ts"] for row in planned_cancels
    } == {300}


def test_python_split_cancel_requires_row_aligned_latency_samples() -> None:
    with pytest.raises(ValueError, match="must have equal length"):
        _run(
            param_overrides={
                "_cancel_exchange_effective_latency_samples_ms": [100.0, 200.0],
                "_cancel_ack_visibility_latency_samples_ms": [400.0],
            }
        )


def test_lifecycle_boundaries_do_not_double_count_inventory_path_time() -> None:
    common = {
        "initial_inventory": 0.003,
        "initial_entry_price": 100.0,
    }
    baseline = _run(param_overrides=common)
    split = _run(
        param_overrides={
            **common,
            "_new_order_exchange_effective_latency_samples_ms": [300.0],
            "_new_order_latency_samples_ms": [650.0],
            "_cancel_exchange_effective_latency_samples_ms": [100.0],
            "_cancel_ack_visibility_latency_samples_ms": [450.0],
        }
    )

    for key in (
        "signed_inventory_time_s",
        "abs_inventory_time_s",
        "sq_inventory_time_s",
        "signed_notional_inventory_time_s",
        "notional_inventory_time_s",
        "inventory_pnl",
    ):
        assert split[key] == pytest.approx(baseline[key])


def test_python_split_new_requires_row_aligned_latency_samples() -> None:
    with pytest.raises(ValueError, match="must have equal length"):
        _run(
            param_overrides={
                "_new_order_exchange_effective_latency_samples_ms": [100.0, 200.0],
                "_new_order_latency_samples_ms": [400.0],
            }
        )


def test_local_lifecycle_scheduler_orders_ties_and_is_idempotent() -> None:
    scheduler = LocalLifecycleBoundaryScheduler()
    assert scheduler.schedule(
        ts_ms=125,
        phase="cancel_ack",
        event_id="cancel-ack",
    )
    assert scheduler.schedule(
        ts_ms=125,
        phase="exchange_effective",
        event_id="exchange-effective",
    )
    assert scheduler.schedule(
        ts_ms=125,
        phase="private_fill_visible",
        event_id="private-fill",
    )
    assert not scheduler.schedule(
        ts_ms=125,
        phase="private_fill_visible",
        event_id="private-fill",
    )
    assert scheduler.drain(through_ts_ms=125, inclusive=False) == []
    drained = scheduler.drain(through_ts_ms=125, inclusive=True)
    assert [row["event_id"] for row in drained] == [
        "exchange-effective",
        "private-fill",
        "cancel-ack",
    ]


def test_split_new_ack_publishes_at_exact_no_native_boundary() -> None:
    result = _run(
        param_overrides={
            "_new_order_exchange_effective_latency_samples_ms": [100.0],
            "_new_order_latency_samples_ms": [350.0],
        }
    )
    assert result["_quote_trace"]
    assert {row["activate_ts"] - row["submit_ts"] for row in result["_quote_trace"]} == {
        100
    }
    assert {row["new_ack_ts"] - row["submit_ts"] for row in result["_quote_trace"]} == {
        350
    }
    assert all(row["exchange_accepted"] for row in result["_quote_trace"])
    assert all(row["local_new_ack_published"] for row in result["_quote_trace"])


def test_split_new_ack_wins_same_ms_market_tie() -> None:
    result = _run(
        crossing_fill_ts_ms=400,
        param_overrides={
            "_new_order_exchange_effective_latency_samples_ms": [100.0],
            "_new_order_latency_samples_ms": [400.0],
        },
    )
    assert result["fills_bid"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 400


def test_split_new_pre_ack_fill_fails_closed_until_private_visibility_exists() -> None:
    with pytest.raises(RuntimeError, match="pre-ACK exchange fill"):
        _run(
            crossing_fill_ts_ms=300,
            param_overrides={
                "_new_order_exchange_effective_latency_samples_ms": [100.0],
                "_new_order_latency_samples_ms": [400.0],
            },
        )


def test_split_new_ack_and_cancel_effective_tie_is_deterministic() -> None:
    result = _run(
        param_overrides={
            "_new_order_exchange_effective_latency_samples_ms": [300.0],
            "_new_order_latency_samples_ms": [1_100.0],
            "_cancel_exchange_effective_latency_samples_ms": [100.0],
            "_cancel_ack_visibility_latency_samples_ms": [450.0],
        }
    )
    planned = [
        row
        for row in result["_quote_trace"]
        if row.get("cancel_reason") == "planned_maintenance"
    ]
    assert planned
    tied = [row for row in planned if row["submit_ts"] == 1_000]
    assert tied and all(row["local_new_ack_published"] for row in tied)
    assert {row["outcome_ts"] for row in planned} == {2_450}


def test_split_new_ack_precedes_same_ms_cancel_ack() -> None:
    result = _run(
        param_overrides={
            "_new_order_exchange_effective_latency_samples_ms": [300.0],
            "_new_order_latency_samples_ms": [1_450.0],
            "_cancel_exchange_effective_latency_samples_ms": [100.0],
            "_cancel_ack_visibility_latency_samples_ms": [450.0],
        }
    )
    tied = [
        row
        for row in result["_quote_trace"]
        if row.get("cancel_reason") == "planned_maintenance"
        and row["submit_ts"] == 1_000
    ]
    assert tied
    assert all(row["new_ack_ts"] == row["outcome_ts"] == 2_450 for row in tied)
    assert all(row["local_new_ack_published"] for row in tied)
