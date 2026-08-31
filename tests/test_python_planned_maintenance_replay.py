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
    keep_until_stop: bool = False,
    param_overrides: dict[str, object] | None = None,
):
    trades, bbo = _inputs(
        crossing_fill_ts_ms=crossing_fill_ts_ms,
        crossing_side=crossing_side,
    )
    params = _params()
    if keep_until_stop:
        # These cases test stop/ACK/fill clocks, not replacement ownership.
        params["requote_threshold_bps"] = 1.0
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


@pytest.fixture(params=["python", "cpp"])
def control_backend(request):
    if request.param == "cpp":
        pytest.importorskip("narrowgate_cpp")
        from models.backtest_tick import _simulate_tick_cpp

        return _simulate_tick_cpp
    return simulate_tick


@pytest.mark.parametrize("initial_sign", [-1, 1])
def test_position_timeout_enters_limit_close_without_inventing_fill(
    control_backend, initial_sign,
) -> None:
    trades, bbo = _inputs()
    result = control_backend(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**_params(cancel_latency_ms=0), "planned_quote_stop_ts_ms": 0,
         "position_timeout": 0.5, "initial_inventory": initial_sign * 0.001,
         "initial_entry_price": 100.0, "circuit_breaker_sigma": 0.0,
         "use_bar_pricing": False, "replay_event_clock_end_ts_ms": 3_000},
        bbo_data=bbo,
    )
    assert result["n_timeouts"] == 1
    assert result["final_inventory"] == pytest.approx(initial_sign * 0.001)
    assert result["fills_bid"] == result["fills_ask"] == 0
    assert result["circuit_breaker_closing"]
    assert result["circuit_breaker_close_place_count"] == 1
    closing = [row for row in result["_quote_trace"] if row["submit_ts"] >= 2_000]
    assert len(closing) == 1
    # Legacy native trace does not expose these order flags; its closing
    # counter and actual side/price/time still verify the same transition.
    if control_backend is simulate_tick:
        assert all(row["reduce_only"] and row["circuit_breaker_close"] for row in closing)
    assert {row["side"] for row in closing} == {"SELL" if initial_sign > 0 else "BUY"}
    assert {row["price"] for row in closing} == {100.0}
    assert min(row["submit_ts"] for row in closing) >= 2_000


@pytest.mark.parametrize("cancel_ack_ms", [0, 1_500])
def test_ordinary_replacement_waits_for_local_cancel_ack(control_backend, cancel_ack_ms) -> None:
    trades, bbo = _inputs()
    result = control_backend(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**_params(cancel_latency_ms=0), "planned_quote_stop_ts_ms": 0,
         "replace_pending_coalesce": False, "trace_decisions_max": 100,
         "_cancel_exchange_effective_latency_samples_ms": [0.0],
         "_cancel_ack_visibility_latency_samples_ms": [float(cancel_ack_ms)]},
        bbo_data=bbo,
    )
    for side in ("BUY", "SELL"):
        orders = sorted(
            [row for row in result["_quote_trace"] if row["side"] == side],
            key=lambda row: row["submit_ts"],
        )
        assert len(orders) >= 2
        assert orders[1]["submit_ts"] == (1_000 if cancel_ack_ms == 0 else 3_000)
        for previous, following in zip(orders[:-1], orders[1:], strict=True):
            assert following["submit_ts"] >= previous["outcome_ts"]
    if cancel_ack_ms:
        assert result["decision_pending_coalesce_count"] > 0


def test_ordinary_replacement_cannot_erase_pending_new_with_zero_cancel_delay(control_backend):
    trades, bbo = _inputs()
    result = control_backend(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**_params(cancel_latency_ms=0), "planned_quote_stop_ts_ms": 0,
         "replace_pending_coalesce": False,
         "_new_order_exchange_effective_latency_samples_ms": [100.0],
         "_new_order_latency_samples_ms": [2_500.0]},
        bbo_data=bbo,
    )
    for side in ("BUY", "SELL"):
        orders = sorted(
            [row for row in result["_quote_trace"] if row["side"] == side],
            key=lambda row: row["submit_ts"],
        )
        assert [row["submit_ts"] for row in orders] == [0, 3_000]
        assert orders[0]["outcome_ts"] == 3_000


def test_stale_active_quotes_cancel_before_next_requote(control_backend) -> None:
    trades, _ = _inputs()
    bbo = HistoricalBBOData(
        ts_ms=np.asarray([0]), best_bid=np.asarray([99.9]), best_ask=np.asarray([100.1]),
        bid_qty=np.asarray([1.0]), ask_qty=np.asarray([1.0]),
    )
    result = control_backend(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**_params(), "planned_quote_stop_ts_ms": 0,
         "requote_interval": 5.0, "rq_min": 5.0, "rq_max": 5.0,
         "use_bar_pricing": False, "max_exec_book_age_s": 0.2},
        bbo_data=bbo,
    )
    canceled = [row for row in result["_quote_trace"] if row["cancel_reason"] == "stale_book"]
    assert len(canceled) == 2
    assert {row["outcome_ts"] for row in canceled} == {800}
    assert result["n_requotes"] == 1


def test_main_loop_stale_stop_runs_while_requote_is_not_due() -> None:
    trades, _ = _inputs()
    bbo = HistoricalBBOData(
        ts_ms=np.asarray([0]), best_bid=np.asarray([99.9]), best_ask=np.asarray([100.1]),
        bid_qty=np.asarray([1.0]), ask_qty=np.asarray([1.0]),
    )
    result = simulate_tick(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**_params(), "planned_quote_stop_ts_ms": 0, "use_bar_pricing": False,
         "requote_interval": 5.0, "rq_min": 5.0, "rq_max": 5.0,
         "max_exec_book_age_s": 0.2, "replay_purpose": "diagnostic",
         "rest_gateway_timing_mode": "sampled_serial", "replay_main_loop_sleep_ms": 100,
         "_serial_rest_return_samples_by_operation": {
             "new": [[0.0, 0.0, 0.0]], "cancel": [[50.0, 200.0, 100.0]],
         }, "_serial_rest_return_sample_semantics": "synthetic_split_clocks"},
        bbo_data=bbo,
    )
    canceled = [row for row in result["_quote_trace"] if row["cancel_reason"] == "stale_book"]
    assert {row["cancel_request_ts"] for row in canceled} == {300, 400}
    assert {row["cancel_ack_ts"] for row in canceled} == {500, 600}
    assert result["n_requotes"] == 1


def test_python_planned_maintenance_cancels_and_stops_new_quotes() -> None:
    result = _run(keep_until_stop=True)

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


@pytest.mark.parametrize("new_clocks,cancel_clocks", [
    ((37.0, 37.0), (80.0, 80.0, 80.0)),
    ((2.0, 37.0), (20.0, 110.0, 80.0)),
])
def test_direct_rest_return_samples_match_profile_behaviour(
    tmp_path, new_clocks, cancel_clocks,
) -> None:
    profile = tmp_path / "gateway.npz"
    _write_serial_gateway_profile(
        profile, [(True, True, True, True)],
        new_clocks=new_clocks, cancel_clocks=cancel_clocks,
    )
    common = {
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "replay_main_loop_sleep_ms": 100,
        "_decision_to_gateway_latency_samples_ms": [40.0],
        "_pre_snapshot_compute_latency_samples_ms": [10.0],
        "trace_decisions_max": 100, "trace_local_order_lifecycle_max": 100,
    }
    profiled = _run(param_overrides={
        **common, "rest_gateway_timing_profile_path": str(profile),
    })
    semantics = "measured_HTTP_duration_with_explicit_effective_and_ACK_upper_bound_proxy"
    direct = _run(param_overrides={
        **common,
        "_serial_rest_return_samples_by_operation": {
            "new": np.asarray([[*new_clocks, new_clocks[1]]]),
            "cancel": np.asarray([cancel_clocks]),
        },
        "_serial_rest_return_sample_semantics": semantics,
    })
    for key in (
        "pnl", "final_inventory", "n_requotes", "fills_bid", "fills_ask",
        "_quote_trace", "_fill_trace", "_decision_trace", "rest_gateway_request_count",
        "rest_gateway_busy_ms", "rest_gateway_pending_decision_count",
    ):
        assert direct[key] == profiled[key], key
    assert direct["rest_gateway_response_clock_semantics"] == semantics
    assert "rest_gateway_timing_profile_path" not in direct
    assert direct["rest_gateway_response_sample_counts"] == {
        "cancel_buy": 1, "cancel_sell": 1, "new_buy": 1, "new_sell": 1,
    }


@pytest.mark.parametrize("rows", [
    [], [1.0, 2.0, 3.0], [[1.0, 2.0]], [[1.0, 2.0, 3.0, 4.0]],
    [[float("nan"), 2.0, 3.0]], [[1.0, float("inf"), 3.0]],
    [[-1.0, 2.0, 3.0]], [[3.0, 2.0, 4.0]], [[3.0, 4.0, 2.0]],
])
def test_direct_rest_return_samples_reject_invalid_clock_rows(rows) -> None:
    with pytest.raises(ValueError, match="effective/ACK/HTTP triples"):
        _run(param_overrides={
            "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
            "_serial_rest_return_samples_by_operation": {
                "new": rows, "cancel": [[1.0, 2.0, 3.0]],
            },
            "_serial_rest_return_sample_semantics": "explicit_test_proxy",
        })


@pytest.mark.parametrize("overrides,message", [
    ({"_serial_rest_return_sample_semantics": ""}, "declare observed or proxy semantics"),
    ({"_serial_rest_return_samples_by_operation": {"new": [[1, 2, 3]]}},
     "require new and cancel operations"),
    ({"rest_gateway_timing_mode": "disabled"}, "sampled_serial without a profile"),
    ({"rest_gateway_timing_profile_path": "/not-read.npz"},
     "sampled_serial without a profile"),
])
def test_direct_rest_return_samples_reject_ambiguous_input_contract(overrides, message) -> None:
    with pytest.raises(ValueError, match=message):
        _run(param_overrides={
            "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
            "_serial_rest_return_samples_by_operation": {
                "new": [[1.0, 2.0, 3.0]], "cancel": [[1.0, 2.0, 3.0]],
            },
            "_serial_rest_return_sample_semantics": "explicit_test_proxy",
            **overrides,
        })


def test_direct_rest_return_samples_run_with_source_message_clock() -> None:
    trades, original_bbo = _inputs()
    base = 10_000
    trades["transact_time"] += base
    bbo = HistoricalBBOData(
        ts_ms=original_bbo.ts_ms + base,
        best_bid=original_bbo.best_bid, best_ask=original_bbo.best_ask,
        bid_qty=original_bbo.bid_qty, ask_qty=original_bbo.ask_qty,
    )
    depth = HistoricalL2Data(
        ts_ms=bbo.ts_ms, bid_px=bbo.best_bid[:, None], ask_px=bbo.best_ask[:, None],
        bid_qty=bbo.bid_qty[:, None], ask_qty=bbo.ask_qty[:, None],
    )

    def clocks(ms):
        ns = np.asarray(ms, dtype=np.int64) * 1_000_000
        return {"exchange_ts_ns": ns, "receive_ts_ns": ns.copy(),
                "feature_ready_ts_ns": ns.copy()}

    variance = clocks([base - 1_000])
    variance["receive_ts_ns"] += 1_000_000_000
    variance["feature_ready_ts_ns"] += 1_000_000_000
    result = simulate_tick(
        trades, np.asarray([base - 1_000]), np.asarray([1.0]),
        {**_params(), "replay_purpose": "diagnostic",
         "rest_gateway_timing_mode": "sampled_serial", "replay_main_loop_sleep_ms": 100,
         "_serial_rest_return_samples_by_operation": {
             "new": [[37.0, 37.0, 37.0]], "cancel": [[80.0, 80.0, 80.0]],
         },
         "_serial_rest_return_sample_semantics": "explicit_HTTP_upper_bound_proxy",
         "use_bar_pricing": False,
         "_decision_to_gateway_latency_samples_ms": [40.0],
         "_pre_snapshot_compute_latency_samples_ms": [10.0],
         "exec_book_visibility_mode": "message_schedule", "_exec_message_delivery": {
             "bbo": clocks(bbo.ts_ms), "depth": clocks(depth.ts_ms), "variance": variance,
             "trade": {**clocks([base]), "last_child_row_index": np.asarray([0])},
         },
         "planned_quote_stop_ts_ms": base + 2_000,
         "replay_event_clock_end_ts_ms": base + 4_000, "trace_decisions_max": 100},
        bbo_data=bbo, l2_data=depth,
    )
    assert result["n_requotes"] > 0
    assert result["rest_gateway_request_count"] > 0
    assert result["rest_gateway_response_clock_semantics"] == "explicit_HTTP_upper_bound_proxy"
    assert set(result["exec_message_delivery_sources"]) == {"bbo", "depth", "trade", "variance"}
    # Inputs arriving at entry are visible at the later snapshot after compute.
    assert result["_decision_trace"][0]["ts_ms"] == base + 10


def _ioc_inventory_path(
    *, initial_sign: int, maker_closes_before_ioc: bool = False, new_service_ms: float = 0.0,
):
    timestamps = np.asarray([0, 10_000, 70_000, 85_000, 100_000, 120_000, 140_000])
    prices = np.asarray([100.0, 110.0, 110.0, 90.0, 90.0, 120.0, 90.0])
    quantities = np.asarray([0.0, 0.0, 0.0, 10.0, 0.0, 10.0, 0.0])
    maker_flags = np.asarray([0, 0, 0, 1, 0, 0, 0])
    if maker_closes_before_ioc:
        timestamps[2] = 35_000
        prices[2] = 100.0
        quantities[:] = 0.0
        quantities[2] = 10.0
        maker_flags[2] = 1
    l2_ts = np.arange(0, 140_001, 1_000, dtype=np.int64)
    mid = np.where(l2_ts < 10_000, 100.0, np.where(l2_ts < 100_000, 110.0, 90.0))
    if initial_sign > 0:
        prices = 200.0 - prices
        mid = 200.0 - mid
        maker_flags = 1 - maker_flags
    trades = pd.DataFrame({
        "transact_time": timestamps, "price": prices,
        "quantity": quantities, "is_buyer_maker": maker_flags,
    })
    depth = HistoricalL2Data(
        ts_ms=l2_ts, bid_px=(mid - 0.1)[:, None], ask_px=(mid + 0.1)[:, None],
        bid_qty=np.full((l2_ts.size, 1), 0.001), ask_qty=np.full((l2_ts.size, 1), 0.001),
    )
    return simulate_tick(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**_params(), "planned_quote_stop_ts_ms": 0,
         "replay_event_clock_end_ts_ms": 140_000, "replay_clock_interval_ms": 1_000,
         "requote_interval": 10.0, "rq_min": 10.0, "rq_max": 10.0,
         "initial_inventory": initial_sign * 0.001, "initial_entry_price": 100.0,
         "use_bar_pricing": False, "circuit_breaker_sigma": 1.0,
         "pnl_volatility_horizon_s": 1.0, "circuit_breaker_exit_mode": "maker_close",
         "new_order_latency_ms": 0, "cancel_order_latency_ms": 0, "taker_fee": 0.01,
         "_private_fill_visibility_latency_samples_ms": [
             50_000.0 if maker_closes_before_ioc else 20.0,
         ],
         "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
         "_serial_rest_return_samples_by_operation": {
             "new": [[new_service_ms, new_service_ms, new_service_ms]],
             "cancel": [[0.0, 0.0, 0.0]],
         }, "_serial_rest_return_sample_semantics": "synthetic_zero_service"},
        l2_data=depth,
    )


@pytest.mark.parametrize("initial_sign", [-1, 1])
def test_ioc_physical_inventory_update_allows_later_reduce_only_maker_fill(initial_sign) -> None:
    result = _ioc_inventory_path(initial_sign=initial_sign)
    fills = result["_fill_trace"]
    assert len(fills) == 3
    assert result["circuit_breaker_close_ioc_fill_count"] == 1
    assert [row["fill_fee_rate"] for row in fills] == [0.01, 0.0, 0.0]
    assert fills[-1]["reduce_only"] is True
    assert fills[-1]["fill_ts"] == 120_000
    assert fills[0]["exchange_remaining"] == 0.0
    assert fills[0]["exchange_accepted"] is True
    assert fills[0]["local_new_ack_published"] is True
    assert fills[0]["last_exchange_fill_ts_ms"] == fills[0]["fill_ts"]
    assert fills[0]["last_private_fill_visible_ts_ms"] == fills[0]["fill_ts"]
    assert result["final_inventory"] == pytest.approx(0.0, abs=1e-12)
    assert result["exchange_inventory_at_window_end"] == pytest.approx(0.0, abs=1e-12)
    assert result["exchange_pending_quantity"] == pytest.approx(0.0, abs=1e-12)
    assert result["private_fill_pending_visibility_count"] == 0


@pytest.mark.parametrize("initial_sign", [-1, 1])
def test_ioc_reduce_only_uses_exchange_inventory_while_maker_callback_pending(initial_sign) -> None:
    result = _ioc_inventory_path(initial_sign=initial_sign, maker_closes_before_ioc=True)
    # Maker already closed the physical position at 35s but its callback waits
    # until 85s. The stale local position must not permit another IOC close.
    assert result["circuit_breaker_close_ioc_fill_count"] == 0
    assert result["circuit_breaker_close_ioc_expire_count"] > 0
    expired_ioc = [
        row for row in result["_quote_trace"]
        if row["cancel_reason"] == "ioc_no_top_liquidity"
    ]
    assert expired_ioc
    assert all(row["exchange_accepted"] is True for row in expired_ioc)
    assert len(result["_fill_trace"]) == 1
    assert result["_fill_trace"][0]["fill_fee_rate"] == 0.0
    assert result["final_inventory"] == pytest.approx(0.0, abs=1e-12)
    assert result["exchange_inventory_at_window_end"] == pytest.approx(0.0, abs=1e-12)
    assert result["exchange_pending_quantity"] == pytest.approx(0.0, abs=1e-12)


def test_ioc_trace_uses_actual_execution_boundary_not_previous_trade_timestamp() -> None:
    result = _ioc_inventory_path(initial_sign=-1, new_service_ms=17.0)
    fills = [row for row in result["_fill_trace"] if row["fill_fee_rate"] == 0.01]
    assert fills
    for fill in fills:
        assert fill["fill_ts"] >= fill["activate_ts"]
        assert fill["fill_ts"] == fill["last_exchange_fill_ts_ms"]
        assert fill["fill_ts"] == fill["last_private_fill_visible_ts_ms"]


@pytest.mark.parametrize("initial_sign", [-1, 1])
def test_synchronous_taker_close_keeps_physical_ledger_consistent(initial_sign) -> None:
    overrides = {
        "initial_inventory": initial_sign * 0.001,
        "initial_entry_price": 110.0 if initial_sign > 0 else 90.0,
        "taker_fee": 0.01, "_private_fill_visibility_latency_samples_ms": [20.0],
        "replay_purpose": "diagnostic", "rest_gateway_timing_mode": "sampled_serial",
        "_serial_rest_return_samples_by_operation": {
            "new": [[0.0, 0.0, 0.0]], "cancel": [[0.0, 0.0, 0.0]],
        }, "_serial_rest_return_sample_semantics": "synthetic_zero_service",
    }
    overrides.update(circuit_breaker_exit_mode="immediate_taker", circuit_breaker_sigma=1.0,
                     pnl_volatility_horizon_s=1.0)
    result = _run(param_overrides=overrides)
    assert result["final_inventory"] == pytest.approx(0.0, abs=1e-12)
    assert result["exchange_inventory_at_window_end"] == pytest.approx(0.0, abs=1e-12)
    assert result["exchange_pending_quantity"] == pytest.approx(0.0, abs=1e-12)
    assert result["private_fill_pending_visibility_count"] == 0


@pytest.mark.parametrize("initial_sign", [-1, 1])
def test_historical_passive_close_clip_keeps_order_during_30s_aggression(initial_sign) -> None:
    timestamps = np.asarray([0, 10_000, 65_000])
    prices = np.asarray([100.0, 110.0, 110.0])
    book_ts = np.arange(0, 65_001, 1_000, dtype=np.int64)
    mid = np.where(book_ts < 10_000, 100.0, 110.0)
    if initial_sign > 0:
        prices = 200.0 - prices
        mid = 200.0 - mid
    trades = pd.DataFrame({
        "transact_time": timestamps, "price": prices,
        "quantity": np.zeros(3), "is_buyer_maker": np.zeros(3, dtype=np.uint8),
    })
    bbo = HistoricalBBOData(
        ts_ms=book_ts, best_bid=mid - 0.1, best_ask=mid + 0.1,
        bid_qty=np.ones(book_ts.size), ask_qty=np.ones(book_ts.size),
    )
    params = {
        **_params(), "planned_quote_stop_ts_ms": 0,
        "replay_event_clock_end_ts_ms": 65_000, "replay_clock_interval_ms": 1_000,
        "requote_interval": 5.0, "rq_min": 5.0, "rq_max": 5.0,
        "requote_threshold_bps": 0.0,
        "initial_inventory": initial_sign * 0.001, "initial_entry_price": 100.0,
        "use_bar_pricing": False, "circuit_breaker_sigma": 1.0,
        "pnl_volatility_horizon_s": 1.0, "circuit_breaker_exit_mode": "maker_close",
        "new_order_latency_ms": 0, "cancel_order_latency_ms": 0,
        "replay_purpose": "diagnostic",
    }
    default = simulate_tick(trades, np.asarray([0]), np.asarray([1.0]), params, bbo_data=bbo)
    unclipped = simulate_tick(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**params, "_diagnostic_passive_close_bbo_clip": False}, bbo_data=bbo,
    )
    clipped = simulate_tick(
        trades, np.asarray([0]), np.asarray([1.0]),
        {**params, "_diagnostic_passive_close_bbo_clip": True}, bbo_data=bbo,
    )
    assert default["_quote_trace"] == unclipped["_quote_trace"]
    assert default["_fill_trace"] == unclipped["_fill_trace"]
    assert default["circuit_breaker_close_gtx_reject_count"] == 3
    assert default["circuit_breaker_close_ioc_place_count"] == 1
    assert clipped["circuit_breaker_close_gtx_reject_count"] == 0
    assert clipped["circuit_breaker_close_ioc_place_count"] == 0
    assert clipped["circuit_breaker_close_keep_count"] > 0
    closing = [row for row in clipped["_quote_trace"] if row["circuit_breaker_close"]]
    assert len(closing) == 1
    assert closing[0]["side"] == ("BUY" if initial_sign < 0 else "SELL")
    assert closing[0]["price"] == pytest.approx(110.0 if initial_sign < 0 else 90.0)
    assert clipped["_fill_trace"] == []


@pytest.mark.parametrize("purpose", ["formal", "exploratory", ""])
def test_historical_passive_close_clip_requires_diagnostic_purpose(purpose) -> None:
    trades, bbo = _inputs()
    variance_ts = np.arange(0, 4_001, 1_000, dtype=np.int64)
    with pytest.raises(ValueError, match="passive-close BBO clipping is diagnostic-only"):
        simulate_tick(
            trades, variance_ts, np.ones(variance_ts.size),
            {**_params(), "_diagnostic_passive_close_bbo_clip": True, "replay_purpose": purpose},
            bbo_data=bbo,
        )


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
def test_pre_snapshot_compute_cannot_send_after_stop_or_window_end(
    tmp_path, stop_ms, end_ms,
) -> None:
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
    assert result["rest_gateway_timing_sampled_row_count"] == 2
    initial_orders = {
        row["side"]: row
        for row in result["_quote_trace"]
        if row["submit_ts"] == 0
    }
    assert initial_orders["BUY"]["gateway_request_ts"] == 0
    assert initial_orders["SELL"]["gateway_request_ts"] == 10
    assert initial_orders["BUY"]["activate_ts"] == 2
    assert initial_orders["SELL"]["activate_ts"] == 12
    assert initial_orders["BUY"]["cancel_request_ts"] == 1_000
    assert initial_orders["SELL"]["cancel_request_ts"] == 1_010
    assert initial_orders["BUY"]["outcome_ts"] == 1_005
    assert initial_orders["SELL"]["outcome_ts"] == 1_015
    # A row's future NEW slots are not authority to pre-create replacements
    # before the preceding cancels become locally terminal.
    assert len(result["_quote_trace"]) == 2


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

    initial_orders = {
        row["side"]: row
        for row in result["_quote_trace"]
        if row["submit_ts"] == 0
    }
    assert initial_orders["BUY"]["gateway_request_ts"] == 40
    assert initial_orders["SELL"]["gateway_request_ts"] == 50
    assert initial_orders["BUY"]["activate_ts"] == 42
    assert initial_orders["SELL"]["activate_ts"] == 52
    assert initial_orders["BUY"]["cancel_request_ts"] == 1_040
    assert initial_orders["SELL"]["cancel_request_ts"] == 1_050
    assert len(result["_quote_trace"]) == 2


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
    assert final == {}
    for row in result["_quote_trace"]:
        assert row["activate_ts"] - row["gateway_request_ts"] == 40
        assert row["new_ack_ts"] - row["gateway_request_ts"] == 100
    assert result["rest_gateway_request_count"] == 4
    assert result["rest_gateway_busy_ms"] == 320
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
    result = _run(crossing_fill_ts_ms=2_200, keep_until_stop=True)

    assert result["planned_quote_stop_triggered"] is True
    assert result["fills_bid"] == 1
    assert result["fills_while_pending_cancel"] == 1
    assert result["_fill_trace"][0]["fill_ts"] == 2_200
    assert result["planned_shutdown_open_order_count"] == 0
    assert result["planned_shutdown_pending_new_order_count"] == 0
    assert result["planned_shutdown_pending_cancel_order_count"] == 0


def test_passive_fill_publisher_preserves_sell_accounting_and_trace() -> None:
    result = _run(crossing_fill_ts_ms=2_200, crossing_side="SELL", keep_until_stop=True)

    assert result["fills_ask"] == 1
    assert result["sell_fill_qty"] == pytest.approx(0.001)
    assert result["fills_while_pending_cancel"] == 1
    assert result["_fill_trace"][0]["side"] == "SELL"
    assert result["_fill_trace"][0]["fill_ts"] == 2_200


def test_zero_private_fill_visibility_preserves_b0_outputs() -> None:
    baseline = _run(crossing_fill_ts_ms=2_200, keep_until_stop=True)
    zero_delay = _run(
        crossing_fill_ts_ms=2_200,
        keep_until_stop=True,
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
        keep_until_stop=True,
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
        keep_until_stop=True,
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
            "requote_threshold_bps": 1.0,
        },
        bbo_data=bbo,
    )

    assert result["fills_bid"] == 1
    assert result["buy_fill_qty"] == pytest.approx(0.001)


def test_python_split_cancel_stops_matching_before_local_ack() -> None:
    result = _run(
        crossing_fill_ts_ms=2_200,
        keep_until_stop=True,
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
        keep_until_stop=True,
        param_overrides={
            "_new_order_exchange_effective_latency_samples_ms": [300.0],
            "_new_order_latency_samples_ms": [2_100.0],
            "replace_pending_coalesce": True,
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
    tied = [row for row in planned if row["submit_ts"] == 0]
    assert tied and all(row["local_new_ack_published"] for row in tied)
    assert {row["outcome_ts"] for row in planned} == {2_450}


def test_split_new_ack_precedes_same_ms_cancel_ack() -> None:
    result = _run(
        keep_until_stop=True,
        param_overrides={
            "_new_order_exchange_effective_latency_samples_ms": [300.0],
            "_new_order_latency_samples_ms": [2_450.0],
            "replace_pending_coalesce": True,
            "_cancel_exchange_effective_latency_samples_ms": [100.0],
            "_cancel_ack_visibility_latency_samples_ms": [450.0],
        }
    )
    tied = [
        row
        for row in result["_quote_trace"]
        if row.get("cancel_reason") == "planned_maintenance"
        and row["submit_ts"] == 0
    ]
    assert tied
    assert all(row["new_ack_ts"] == row["outcome_ts"] == 2_450 for row in tied)
    assert all(row["local_new_ack_published"] for row in tied)
