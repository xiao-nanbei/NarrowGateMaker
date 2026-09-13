from __future__ import annotations

import numpy as np
import pytest


cpp = pytest.importorskip("narrowgate_cpp")


def _params(*, initial_inventory: float = 0.0):
    params = cpp.TickReplayParams()
    params.order_size = 0.001
    params.max_inventory = 0.01
    params.requote_interval_s = 1.0
    params.maker_fill_prob = 1.0
    params.initial_inventory = initial_inventory
    params.initial_entry_price = 100.0 if initial_inventory else 0.0
    params.initial_sigma_sq = 1.0
    params.trace_quotes_max = 100
    params.trace_p3_reach_decisions_max = 100
    params.quote.gamma = 0.01
    params.quote.kappa = 1.0
    params.quote.p3_delta_star = 0.2
    params.quote.p3_kappa_eff = 10.0
    params.quote.historical_p3_scalar_adapter_enabled = True
    params.quote.p3_identity_required = True
    params.quote.p3_event_type = "touch"
    params.quote.p3_horizon_s = 10.0
    params.quote.p3_distance_origin = "same_side_best_bid_or_ask_at_window_start"
    params.quote.p3_distance_unit = "USDC_per_BTC"
    params.quote.p3_side = "pooled_buy_sell"
    params.quote.p3_queue_included = False
    params.quote.p3_artifact_sha256 = "c" * 64
    params.quote.regime_enabled = True
    params.quote.tick_size = 0.1
    params.quote.lot_size = 0.001
    params.quote.order_size = params.order_size
    params.quote.max_inventory = params.max_inventory
    params.quote.max_spread_bps = 1_000.0
    params.quote.dynamic_cap_enabled = False
    return params


def _replay_args(*, tox_bid: float = 0.9, tox_ask: float = 0.9):
    ts = np.arange(0, 5_000, 1_000, dtype=np.int64)
    prices = np.full(ts.size, 100.0, dtype=np.float64)
    quantities = np.zeros(ts.size, dtype=np.float64)
    makers = np.zeros(ts.size, dtype=np.uint8)
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)
    empty_matrix = np.empty((0, 0), dtype=np.float64)
    ml_ts = np.asarray([0], dtype=np.int64)
    return (
        ts,
        prices,
        quantities,
        makers,
        empty_i64,
        empty_f64,
        empty_f64,
        empty_f64,
        ml_ts,
        np.asarray([0.5], dtype=np.float64),
        np.asarray([0.0], dtype=np.float64),
        np.asarray([0.0], dtype=np.float64),
        np.asarray([tox_bid], dtype=np.float64),
        np.asarray([tox_ask], dtype=np.float64),
        empty_matrix,
        empty_matrix,
        empty_matrix,
        empty_i64,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_i64,
        empty_matrix,
        empty_matrix,
        empty_matrix,
        empty_matrix,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_f64,
    )


def _gate_payload(*, status: int = 2):
    timestamps = np.asarray([0], dtype=np.int64)
    # Four blocks: BUY opener/add and SELL opener/add. A broad synthetic grid
    # keeps this unit test focused on execution semantics rather than P3 support.
    matrix = np.full((1, 4 * 200), status, dtype=np.uint8)
    return timestamps, matrix


def _final_prices(result):
    return {
        (int(row.quote_ts), str(row.side)): float(row.final_price)
        for row in result.quote_trace
    }


def test_v5_disabled_gate_is_identical_to_v3():
    params = _params()
    replay_args = _replay_args()
    gate_ts, gate_status = _gate_payload()

    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, params)
    candidate = cpp.simulate_tick_arrays_ext_policy_v5(
        replay_args,
        gate_ts,
        gate_status,
        params,
    )

    assert candidate.summary.pnl == control.summary.pnl
    assert candidate.summary.n_requotes == control.summary.n_requotes
    assert _final_prices(candidate) == _final_prices(control)
    assert candidate.summary.p3_reach_gate_price_change_count == 0


def test_v5_gate_moves_flat_exposure_quotes_outward_by_frozen_ticks():
    replay_args = _replay_args()
    gate_ts, gate_status = _gate_payload()
    control_params = _params()
    candidate_params = _params()
    candidate_params.conditional_p3_reach_gate_enabled = True
    candidate_params.conditional_p3_reach_gate_outward_ticks = 16
    candidate_params.conditional_p3_reach_gate_grid_min_ticks = 1
    candidate_params.conditional_p3_reach_gate_buy_toxicity_threshold = 0.8
    candidate_params.conditional_p3_reach_gate_sell_toxicity_threshold = 0.8

    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, control_params)
    candidate = cpp.simulate_tick_arrays_ext_policy_v5(
        replay_args,
        gate_ts,
        gate_status,
        candidate_params,
    )

    control_prices = _final_prices(control)
    candidate_prices = _final_prices(candidate)
    assert control_prices.keys() == candidate_prices.keys()
    for key, baseline_price in control_prices.items():
        side = key[1]
        expected = baseline_price - 1.6 if side == "BUY" else baseline_price + 1.6
        assert candidate_prices[key] == pytest.approx(expected, abs=1e-12)
    assert candidate.summary.p3_reach_gate_buy_price_change_count > 0
    assert candidate.summary.p3_reach_gate_sell_price_change_count > 0
    assert {str(row.side) for row in candidate.p3_reach_decision_trace} == {"BUY", "SELL"}
    assert all(row.role == "opener" for row in candidate.p3_reach_decision_trace)
    assert all(row.price_changed for row in candidate.p3_reach_decision_trace)


def test_v5_gate_does_not_move_reducing_side():
    replay_args = _replay_args()
    gate_ts, gate_status = _gate_payload()
    control_params = _params(initial_inventory=0.001)
    candidate_params = _params(initial_inventory=0.001)
    candidate_params.conditional_p3_reach_gate_enabled = True
    candidate_params.conditional_p3_reach_gate_outward_ticks = 16
    candidate_params.conditional_p3_reach_gate_grid_min_ticks = 1
    candidate_params.conditional_p3_reach_gate_buy_toxicity_threshold = 0.8
    candidate_params.conditional_p3_reach_gate_sell_toxicity_threshold = 0.8

    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, control_params)
    candidate = cpp.simulate_tick_arrays_ext_policy_v5(
        replay_args,
        gate_ts,
        gate_status,
        candidate_params,
    )
    control_prices = _final_prices(control)
    candidate_prices = _final_prices(candidate)

    sell_keys = [key for key in control_prices if key[1] == "SELL"]
    buy_keys = [key for key in control_prices if key[1] == "BUY"]
    assert sell_keys and buy_keys
    assert all(candidate_prices[key] == control_prices[key] for key in sell_keys)
    assert all(
        candidate_prices[key] == pytest.approx(control_prices[key] - 1.6, abs=1e-12)
        for key in buy_keys
    )
    assert candidate.summary.p3_reach_gate_sell_price_change_count == 0
    assert candidate.summary.p3_reach_gate_buy_price_change_count > 0


@pytest.mark.parametrize(
    ("gate_ts", "gate_status", "message"),
    [
        (
            np.asarray([1], dtype=np.int64),
            np.full((1, 800), 2, dtype=np.uint8),
            "timestamp differs",
        ),
        (
            np.asarray([0], dtype=np.int64),
            np.full((1, 799), 2, dtype=np.uint8),
            "four equal blocks",
        ),
        (
            np.asarray([0], dtype=np.int64),
            np.full((1, 800), 3, dtype=np.uint8),
            "status must lie",
        ),
    ],
)
def test_v5_gate_payload_fails_closed(gate_ts, gate_status, message):
    params = _params()
    params.conditional_p3_reach_gate_enabled = True
    params.conditional_p3_reach_gate_grid_min_ticks = 1
    params.conditional_p3_reach_gate_buy_toxicity_threshold = 0.8
    params.conditional_p3_reach_gate_sell_toxicity_threshold = 0.8
    with pytest.raises(ValueError, match=message):
        cpp.simulate_tick_arrays_ext_policy_v5(
            _replay_args(),
            gate_ts,
            gate_status,
            params,
        )
