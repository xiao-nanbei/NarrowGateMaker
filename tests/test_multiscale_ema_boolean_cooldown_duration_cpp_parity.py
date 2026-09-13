from __future__ import annotations

import copy

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt

pytest.importorskip("narrowgate_cpp")


_EMPTY_I64 = np.empty(0, dtype=np.int64)
_EMPTY_F64 = np.empty(0, dtype=np.float64)
_BASE_TS_MS = 1_700_000_000_000


def _base_params(*, fill_cooldown_s: float, cancel_latency_ms: int) -> dict:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "max_inventory": 0.02,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "new_order_latency_ms": 0,
        "cancel_order_latency_ms": cancel_latency_ms,
        "latency_jitter_ms": 0,
        "latency_sampler_version": "keyed_splitmix64_v1",
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 1_000.0,
        "max_exec_book_age_s": 5.0,
        "ml_enabled": False,
        "fill_cooldown": fill_cooldown_s,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
        "trace_fills_max": 200,
        "trace_quotes_max": 200,
        "trace_cooldown_duration_opportunities_max": 200,
        "collect_curves": False,
    }


def _market_path(
    prices: list[float],
    quantities: list[float],
    buyer_maker: list[int],
) -> tuple[pd.DataFrame, bt.HistoricalBBOData]:
    assert len(prices) == len(quantities) == len(buyer_maker)
    ts_ms = _BASE_TS_MS + np.arange(len(prices), dtype=np.int64) * 1_000
    price = np.asarray(prices, dtype=np.float64)
    quantity = np.asarray(quantities, dtype=np.float64)
    trades = pd.DataFrame(
        {
            "transact_time": ts_ms,
            "price": price,
            "quantity": quantity,
            "is_buyer_maker": np.asarray(buyer_maker, dtype=np.uint8),
            "_is_execution_trade": quantity > 0.0,
        }
    )
    bbo = bt.HistoricalBBOData(
        ts_ms=ts_ms,
        best_bid=price - 0.1,
        best_ask=price + 0.1,
        bid_qty=np.ones(price.size, dtype=np.float64),
        ask_qty=np.ones(price.size, dtype=np.float64),
        source="synthetic_native_bbo",
    )
    return trades, bbo


def _same_event_partial_fill_path():
    # First fill one order unit; after its cooldown, fill a second order in
    # four same-timestamp pieces. Never rely on overlapping same-side orders
    # while replacement cancellation is still awaiting local acknowledgement.
    prices = [100.0] * 94
    quantities = [0.0] * 94
    buyer_maker = [0] * 94
    prices[1], quantities[1], buyer_maker[1] = 90.0, 0.004, 1
    prices[90:] = [90.0] * 4
    quantities[90], buyer_maker[90] = 0.001, 1
    trades, bbo = _market_path(prices, quantities, buyer_maker)
    indices = np.arange(len(trades))
    trades = trades.iloc[
        np.repeat(indices, np.where(indices == 90, 4, 1))
    ].reset_index(drop=True)
    return trades, bbo


def _right_censored_path():
    return _market_path(
        [100.0, 96.6, 96.6, 90.0, 90.0, 90.0, 80.0, 80.0, 80.0],
        [0.0, 0.001, 0.0, 0.001, 0.0, 0.0, 0.001, 0.0, 0.0],
        [0, 1, 0, 1, 0, 0, 1, 0, 0],
    )


def _completed_washout_path():
    return _market_path(
        [100.0, 96.6, 103.0, 103.0, 103.0],
        [0.0, 0.001, 0.001, 0.0, 0.0],
        [0, 1, 0, 0, 0],
    )


def _post_quarantine_side_disabled_path():
    return _market_path(
        [
            100.0,
            103.4,
            100.0,
            100.0,
            100.0,
            100.0,
            96.5,
            100.0,
            100.0,
            100.0,
            100.0,
            100.0,
        ],
        [0.0, 0.001, 0.0, 0.0, 0.0, 0.0, 0.001, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0],
    )


def _run(
    engine: str,
    trades: pd.DataFrame,
    bbo: bt.HistoricalBBOData,
    params: dict,
) -> dict:
    bt.configure_symbol("BTCUSDC")
    return bt._simulate_tick_with_engine(
        engine,
        trades,
        _EMPTY_I64,
        _EMPTY_F64,
        params,
        bbo_data=bbo,
    )


def _fork_params(
    params: dict,
    target: dict,
    *,
    action: str,
    fixed_ms: float = 500.0,
) -> dict:
    fork = copy.deepcopy(params)
    fork.update(
        {
            "cooldown_duration_fork_enabled": True,
            "cooldown_duration_fork_action": action,
            "cooldown_duration_fork_target_ordinal": int(
                target["exposure_fill_ordinal"]
            ),
            "cooldown_duration_fork_target_ts_ms": int(
                target["fill_visible_ts_ms"]
            ),
            "cooldown_duration_fork_target_side": str(target["side"]),
            "cooldown_duration_fork_target_order_id": int(target["order_id"]),
            "cooldown_duration_fork_target_campaign_id": int(
                target["campaign_id"]
            ),
            "cooldown_duration_fork_expected_baseline_ms": float(
                target["baseline_duration_ms"]
            ),
            "cooldown_duration_fork_fixed_ms": float(fixed_ms),
        }
    )
    return fork


def _assert_numeric_fields_equal(
    left: dict,
    right: dict,
    fields: tuple[str, ...],
    *,
    abs_tol: float = 1e-12,
) -> None:
    for field in fields:
        assert left[field] == pytest.approx(right[field], abs=abs_tol), field


def _assert_opportunity_parity(py_row: dict, cpp_row: dict) -> None:
    exact_fields = (
        "schema_version",
        "fill_clock_semantics",
        "live_receive_time_authority",
        "exposure_fill_ordinal",
        "fill_visible_ts_ms",
        "fill_exchange_ts_ms",
        "side",
        "role_at_fill",
        "order_id",
        "campaign_id",
        "consecutive_units_before",
        "consecutive_units_after",
        "prior_deadline_ts_ms",
        "baseline_deadline_ts_ms",
        "decision_visible_bbo_index",
        "decision_visible_l2_index",
        "market_event_index",
    )
    for field in exact_fields:
        assert cpp_row[field] == py_row[field], field
    _assert_numeric_fields_equal(
        cpp_row,
        py_row,
        (
            "inventory_before_fill_btc",
            "inventory_after_fill_btc",
            "fill_qty_btc",
            "unit_qty_btc",
            "baseline_duration_ms",
            "canonical_mid",
            "best_bid",
            "best_ask",
            "assignment_equity_usdc",
        ),
    )


def _assert_standard_fill_trace_parity(py: dict, cpp: dict) -> None:
    py_rows = py["_fill_trace"]
    cpp_rows = cpp["_fill_trace"]
    assert len(cpp_rows) == len(py_rows)
    for py_row, cpp_row in zip(py_rows, cpp_rows, strict=True):
        for field in ("side", "fill_ts", "order_id"):
            assert cpp_row[field] == py_row[field], field
        for field in (
            "quote_px",
            "fill_qty",
            "inventory_before_fill",
            "inventory_after_fill",
        ):
            assert cpp_row[field] == pytest.approx(py_row[field], abs=1e-12), field


def _assert_cpp_fork_path_matches_python_fills(
    py: dict, cpp: dict, *, first_fill_index: int = 0
) -> None:
    py_rows = py["_fill_trace"][first_fill_index:]
    cpp_rows = cpp["_cooldown_duration_fill_path"]
    assert len(cpp_rows) == len(py_rows)
    for path_ordinal, (py_row, cpp_row) in enumerate(
        zip(py_rows, cpp_rows, strict=True),
        start=1,
    ):
        assert cpp_row["path_fill_ordinal"] == path_ordinal
        assert cpp_row["fill_visible_ts_ms"] == py_row["fill_ts"]
        assert cpp_row["side"] == py_row["side"]
        assert cpp_row["order_id"] == py_row["order_id"]
        assert cpp_row["fill_price_usdc_per_btc"] == pytest.approx(
            py_row["quote_px"],
            abs=1e-12,
        )
        assert cpp_row["fill_qty_btc"] == pytest.approx(
            py_row["fill_qty"],
            abs=1e-12,
        )
        assert cpp_row["inventory_before_fill_btc"] == pytest.approx(
            py_row["inventory_before_fill"],
            abs=1e-12,
        )
        assert cpp_row["inventory_after_fill_btc"] == pytest.approx(
            py_row["inventory_after_fill"],
            abs=1e-12,
        )


def test_cpp_opportunity_census_is_path_noop_when_fork_is_disabled():
    trades, bbo = _right_censored_path()
    params = _base_params(fill_cooldown_s=2.0, cancel_latency_ms=0)
    without_census = copy.deepcopy(params)
    without_census["trace_cooldown_duration_opportunities_max"] = 0

    control = _run("cpp", trades, bbo, without_census)
    census = _run("cpp", trades, bbo, params)

    for field in (
        "pnl",
        "cash_before_terminal",
        "final_inventory",
        "fills_bid",
        "fills_ask",
        "fills_total",
        "n_requotes",
        "max_inventory",
        "abs_inventory_time_s",
        "signed_inventory_time_s",
    ):
        assert census[field] == control[field], field
    assert census["_fill_trace"] == control["_fill_trace"]
    assert census["_quote_trace"] == control["_quote_trace"]
    assert census["_cooldown_duration_opportunity_trace"]
    assert census["_cooldown_duration_fill_path"] == []
    assert census["_cooldown_duration_fork_trace"] == {}


def test_python_cpp_same_event_partial_fill_opportunity_parity():
    trades, bbo = _same_event_partial_fill_path()
    params = _base_params(fill_cooldown_s=85.0, cancel_latency_ms=10_000)
    params.update(order_size=0.004, requote_threshold_bps=1.0)

    py = _run("python", trades, bbo, params)
    cpp = _run("cpp", trades, bbo, params)
    py_rows = py["_cooldown_duration_opportunity_trace"]
    cpp_rows = cpp["_cooldown_duration_opportunity_trace"]

    assert len(py_rows) == len(cpp_rows) == 5
    partial_rows = cpp_rows[1:]
    assert len(partial_rows) == 4
    assert [row["exposure_fill_ordinal"] for row in partial_rows] == [2, 3, 4, 5]
    assert len({row["fill_visible_ts_ms"] for row in partial_rows}) == 1
    assert len({row["order_id"] for row in partial_rows}) == 1
    assert partial_rows[0]["order_id"] != cpp_rows[0]["order_id"]
    assert [row["fill_qty_btc"] for row in partial_rows] == [0.001] * 4
    assert [row["unit_qty_btc"] for row in partial_rows] == [0.004] * 4
    assert [row["consecutive_units_after"] for row in partial_rows] == [
        1.25, 1.5, 1.75, 2.0
    ]
    assert [row["baseline_duration_ms"] for row in cpp_rows] == [
        85_000.0,
        106_250.0,
        127_500.0,
        148_750.0,
        170_000.0,
    ]
    for py_row, cpp_row in zip(py_rows, cpp_rows, strict=True):
        _assert_opportunity_parity(py_row, cpp_row)


def test_python_cpp_opportunities_preserve_adaptive_multiplier():
    trades, bbo = _same_event_partial_fill_path()
    params = _base_params(fill_cooldown_s=85.0, cancel_latency_ms=10_000)
    params.update(order_size=0.004, requote_threshold_bps=1.0)
    params.update(
        {
            "adaptive_add_cooldown_enabled": True,
            "adaptive_add_cooldown_min_mult": 0.5,
            "adaptive_add_cooldown_max_mult": 3.0,
            "adaptive_add_cooldown_w_markout": 0.0,
            "adaptive_add_cooldown_w_flow": 1.0,
            "adaptive_add_cooldown_w_campaign": 0.0,
            "adaptive_add_cooldown_w_trend": 0.0,
            "adaptive_add_cooldown_w_refill_weak": 0.0,
            "adaptive_add_cooldown_w_refill_good": 0.0,
            "adaptive_add_cooldown_w_reversion": 0.0,
            "adaptive_add_cooldown_flow_ref": 1.0,
            "adaptive_add_cooldown_gate_enabled": False,
        }
    )

    py = _run("python", trades, bbo, params)
    cpp = _run("cpp", trades, bbo, params)
    py_rows = py["_cooldown_duration_opportunity_trace"]
    cpp_rows = cpp["_cooldown_duration_opportunity_trace"]

    assert [row["baseline_duration_ms"] for row in cpp_rows] == [
        85_000.0,
        132_812.5,
        191_250.0,
        260_312.5,
        340_000.0,
    ]
    for py_row, cpp_row in zip(py_rows, cpp_rows, strict=True):
        _assert_opportunity_parity(py_row, cpp_row)


@pytest.mark.parametrize("fixed_ms", [500.0, 500.5, 501.5])
def test_fixed_duration_targets_one_fill_then_same_event_fills_restore_baseline(
    fixed_ms: float,
):
    trades, bbo = _same_event_partial_fill_path()
    params = _base_params(fill_cooldown_s=85.0, cancel_latency_ms=10_000)
    params.update(order_size=0.004, requote_threshold_bps=1.0)
    census = _run("python", trades, bbo, params)
    target = census["_cooldown_duration_opportunity_trace"][1]
    fork_params = _fork_params(
        params,
        target,
        action="FIXED_DURATION_MS",
        fixed_ms=fixed_ms,
    )

    py = _run("python", trades, bbo, fork_params)
    cpp = _run("cpp", trades, bbo, fork_params)
    _assert_standard_fill_trace_parity(py, cpp)
    # The initial whole-order fill predates assignment, so only the four
    # same-timestamp partial fills belong to this fork's outcome path.
    assert len(py["_fill_trace"]) == len(cpp["_fill_trace"]) == 5
    _assert_cpp_fork_path_matches_python_fills(py, cpp, first_fill_index=1)

    cpp_path = cpp["_cooldown_duration_fill_path"]
    assert [row["target_fill"] for row in cpp_path] == [True, False, False, False]
    assert [row["applied_duration_ms"] for row in cpp_path] == [
        fixed_ms,
        127_500.0,
        148_750.0,
        170_000.0,
    ]
    assert cpp_path[-1]["applied_deadline_ts_ms"] == (
        cpp_path[-1]["fill_visible_ts_ms"] + 170_000
    )
    expected_target_deadline = round(float(target["fill_visible_ts_ms"]) + fixed_ms)
    assert cpp_path[0]["applied_deadline_ts_ms"] == expected_target_deadline
    assert cpp["_cooldown_duration_fork_trace"]["applied_deadline_ts_ms"] == (
        py["_cooldown_duration_fork_trace"]["applied_deadline_ts_ms"]
    ) == expected_target_deadline
    _assert_numeric_fields_equal(
        cpp,
        py,
        ("pnl", "cash_before_terminal", "final_inventory"),
    )


@pytest.mark.parametrize("action", ["CONTROL_85N", "FIXED_DURATION_MS"])
def test_python_cpp_right_censored_fork_trace_parity(action: str):
    trades, bbo = _right_censored_path()
    params = _base_params(fill_cooldown_s=2.0, cancel_latency_ms=0)
    census = _run("python", trades, bbo, params)
    target = census["_cooldown_duration_opportunity_trace"][0]
    fork_params = _fork_params(params, target, action=action, fixed_ms=500.0)

    py = _run("python", trades, bbo, fork_params)
    cpp = _run("cpp", trades, bbo, fork_params)
    _assert_standard_fill_trace_parity(py, cpp)
    _assert_cpp_fork_path_matches_python_fills(py, cpp)
    py_fork = py["_cooldown_duration_fork_trace"]
    cpp_fork = cpp["_cooldown_duration_fork_trace"]

    for field in (
        "schema_version",
        "action",
        "side",
        "campaign_id",
        "target_exposure_fill_ordinal",
        "target_order_id",
        "assignment_ts_ms",
        "applied_deadline_ts_ms",
        "quarantine_entered",
        "quarantine_ts_ms",
        "arm_washout_complete",
        "terminal_ts_ms",
        "terminal_reason",
        "right_censored",
        "post_assignment_buy_fill_count",
        "post_assignment_sell_fill_count",
        "active_or_pending_order_count",
        "pending_submit_count",
        "pending_cancel_count",
        "pending_ack_count",
        "campaign_active",
        "cursor_owner_count",
        "hazard_owner_count",
        "exposure_permission_change_count",
        "reducing_permission_control_checks",
    ):
        assert cpp_fork[field] == py_fork[field], field
    assert cpp_fork["assignment_to_washout_value_usdc"] is None
    assert cpp_fork["censor_time_mid_mark_usdc"] == pytest.approx(
        py_fork["censor_time_mid_mark_usdc"]
    )
    assert cpp_fork["censor_time_executable_mark_usdc"] == pytest.approx(
        py_fork["censor_time_executable_mark_usdc"]
    )
    assert cpp_fork["censor_marks_are_terminal_bounds"] is False
    assert cpp_fork["washout_protocol"] == (
        "first_flat_exposure_quarantine_scheduler_drained_v2"
    )
    assert cpp_fork["control_path_exact_until_quarantine"] is True
    assert cpp_fork["exposure_permission_change_count"] == py_fork[
        "exposure_permission_change_count"
    ]
    assert cpp_fork["reducing_permission_control_checks"] == py_fork[
        "reducing_permission_control_checks"
    ]
    assert cpp_fork["reducing_quote_change_count"] == 0
    assert cpp_fork["second_assignment_count"] == 0
    _assert_numeric_fields_equal(
        cpp_fork,
        py_fork,
        (
            "assignment_inventory_btc",
            "assignment_equity_usdc",
            "baseline_duration_ms",
            "applied_duration_ms",
            "terminal_inventory_btc",
            "terminal_mid_usdc_per_btc",
            "inventory_time_btc_s",
            "mae_usdc",
            "max_abs_inventory_btc",
        ),
    )
    _assert_numeric_fields_equal(
        cpp,
        py,
        ("pnl", "cash_before_terminal", "final_inventory"),
    )
    if action == "CONTROL_85N":
        no_fork = _run("cpp", trades, bbo, params)
        for field in (
            "pnl",
            "cash_before_terminal",
            "final_inventory",
            "fills_bid",
            "fills_ask",
            "fills_total",
            "n_requotes",
            "max_inventory",
            "abs_inventory_time_s",
            "signed_inventory_time_s",
        ):
            assert cpp[field] == no_fork[field], field
        assert cpp["_fill_trace"] == no_fork["_fill_trace"]
        assert cpp["_quote_trace"] == no_fork["_quote_trace"]


def test_completed_washout_has_explicit_zero_descendant_state():
    trades, bbo = _completed_washout_path()
    params = _base_params(fill_cooldown_s=2.0, cancel_latency_ms=0)
    census = _run("python", trades, bbo, params)
    target = census["_cooldown_duration_opportunity_trace"][0]
    fork_params = _fork_params(
        params,
        target,
        action="FIXED_DURATION_MS",
        fixed_ms=500.0,
    )

    py = _run("python", trades, bbo, fork_params)
    cpp = _run("cpp", trades, bbo, fork_params)
    py_fork = py["_cooldown_duration_fork_trace"]
    cpp_fork = cpp["_cooldown_duration_fork_trace"]

    assert cpp_fork["arm_washout_complete"] is True
    assert cpp_fork["right_censored"] is False
    assert cpp_fork["assignment_to_washout_value_usdc"] == pytest.approx(
        py_fork["assignment_to_washout_value_usdc"]
    )
    assert cpp_fork["censor_time_mid_mark_usdc"] is None
    assert cpp_fork["censor_time_executable_mark_usdc"] is None
    assert cpp_fork["censor_marks_are_terminal_bounds"] is False
    assert cpp_fork["washout_protocol"] == (
        "first_flat_exposure_quarantine_scheduler_drained_v2"
    )
    assert cpp_fork["reducing_quote_change_count"] == 0
    assert cpp_fork["second_assignment_count"] == 0
    assert abs(cpp_fork["accounting_residual_usdc"]) <= 1e-12
    assert {
        field: cpp_fork[field]
        for field in (
            "active_or_pending_order_count",
            "pending_submit_count",
            "pending_cancel_count",
            "pending_ack_count",
            "campaign_active",
            "cursor_owner_count",
            "hazard_owner_count",
        )
    } == {
        "active_or_pending_order_count": 0,
        "pending_submit_count": 0,
        "pending_cancel_count": 0,
        "pending_ack_count": 0,
        "campaign_active": False,
        "cursor_owner_count": 0,
        "hazard_owner_count": 0,
    }
    for field in (
        "pending_ack_count",
        "campaign_active",
        "cursor_owner_count",
        "hazard_owner_count",
    ):
        assert cpp_fork[field] == py_fork[field], field


def test_post_policy_side_disabled_cancel_completes_washout_on_same_event():
    trades, bbo = _post_quarantine_side_disabled_path()
    params = _base_params(fill_cooldown_s=2.0, cancel_latency_ms=0)
    params.update(
        {
            "requote_interval": 5.0,
            "rq_min": 5.0,
            "rq_max": 5.0,
        }
    )
    census = _run("python", trades, bbo, params)
    target = census["_cooldown_duration_opportunity_trace"][0]
    fork_params = _fork_params(
        params,
        target,
        action="FIXED_DURATION_MS",
        fixed_ms=500.0,
    )

    py = _run("python", trades, bbo, fork_params)
    cpp = _run("cpp", trades, bbo, fork_params)
    py_fork = py["_cooldown_duration_fork_trace"]
    cpp_fork = cpp["_cooldown_duration_fork_trace"]
    expected_terminal_ts_ms = _BASE_TS_MS + 10_000

    assert cpp["_quote_trace"][-1]["cancel_reason"] == "side_disabled"
    assert cpp["_quote_trace"][-1]["outcome_ts"] == expected_terminal_ts_ms
    assert cpp_fork["quarantine_ts_ms"] == _BASE_TS_MS + 6_000
    assert cpp_fork["terminal_ts_ms"] == expected_terminal_ts_ms
    assert py_fork["terminal_ts_ms"] == expected_terminal_ts_ms
    assert cpp_fork["terminal_reason"] == py_fork["terminal_reason"] == (
        "arm_economic_washout"
    )


def test_pending_cancel_ack_does_not_backdate_visible_washout_terminal():
    trades, bbo = _post_quarantine_side_disabled_path()
    params = _base_params(fill_cooldown_s=2.0, cancel_latency_ms=300)
    params.update(
        {
            "requote_interval": 5.0,
            "rq_min": 5.0,
            "rq_max": 5.0,
            # Keep the unchanged initial BUY until its reducing fill at 6s;
            # an unrelated 5s replacement must not erase that test event.
            "requote_threshold_bps": 1.0,
        }
    )
    census = _run("python", trades, bbo, params)
    target = census["_cooldown_duration_opportunity_trace"][0]
    fork_params = _fork_params(
        params,
        target,
        action="FIXED_DURATION_MS",
        fixed_ms=500.0,
    )

    py = _run("python", trades, bbo, fork_params)
    cpp = _run("cpp", trades, bbo, fork_params)
    py_fork = py["_cooldown_duration_fork_trace"]
    cpp_fork = cpp["_cooldown_duration_fork_trace"]
    expected_visible_terminal_ts_ms = _BASE_TS_MS + 11_000

    assert cpp["_quote_trace"][-1]["cancel_reason"] == "side_disabled"
    assert cpp["_quote_trace"][-1]["outcome_ts"] == expected_visible_terminal_ts_ms
    assert cpp_fork["quarantine_ts_ms"] == _BASE_TS_MS + 6_000
    assert cpp_fork["terminal_ts_ms"] == expected_visible_terminal_ts_ms
    assert py_fork["terminal_ts_ms"] == expected_visible_terminal_ts_ms
    assert cpp_fork["terminal_ts_ms"] > cpp_fork["quarantine_ts_ms"]
    assert cpp_fork["terminal_reason"] == py_fork["terminal_reason"] == (
        "arm_economic_washout"
    )


def test_cpp_duration_fork_target_identity_drift_fails_fast():
    trades, bbo = _right_censored_path()
    params = _base_params(fill_cooldown_s=2.0, cancel_latency_ms=0)
    census = _run("python", trades, bbo, params)
    target = dict(census["_cooldown_duration_opportunity_trace"][0])
    target["order_id"] += 1
    fork_params = _fork_params(
        params,
        target,
        action="FIXED_DURATION_MS",
        fixed_ms=500.0,
    )

    with pytest.raises(RuntimeError, match="target identity drifted"):
        _run("cpp", trades, bbo, fork_params)


def test_duration_fork_unknown_action_fails_fast():
    trades, bbo = _right_censored_path()
    params = _base_params(fill_cooldown_s=2.0, cancel_latency_ms=0)
    census = _run("python", trades, bbo, params)
    target = census["_cooldown_duration_opportunity_trace"][0]
    fork_params = _fork_params(
        params,
        target,
        action="UNREGISTERED_DURATION_ACTION",
    )

    with pytest.raises(ValueError, match="CONTROL_85N or FIXED_DURATION_MS"):
        _run("cpp", trades, bbo, fork_params)
