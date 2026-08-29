from __future__ import annotations

import numpy as np
import pytest

cpp = pytest.importorskip("narrowgate_cpp")

GRID_SIZE = 1_180


def _params(*, initial_inventory: float = 0.0):
    params = cpp.TickReplayParams()
    params.order_size = 0.001
    params.max_inventory = 0.01
    params.requote_interval_s = 1.0
    params.maker_fill_prob = 1.0
    params.initial_inventory = initial_inventory
    params.initial_entry_price = 100.0 if initial_inventory else 0.0
    params.initial_sigma_sq = 1.0
    params.trace_quotes_max = 1_000
    params.quote.gamma = 0.01
    params.quote.kappa = 1.0
    # Produces a five-tick executable BBO distance, exactly at grid_min_ticks.
    params.quote.p3_delta_star = 0.6
    params.quote.p3_kappa_eff = 10.0
    params.quote.historical_p3_scalar_adapter_enabled = True
    params.quote.p3_identity_required = True
    params.quote.p3_event_type = "touch"
    params.quote.p3_horizon_s = 10.0
    params.quote.p3_distance_origin = "same_side_best_bid_or_ask_at_window_start"
    params.quote.p3_distance_unit = "USDC_per_BTC"
    params.quote.p3_side = "pooled_buy_sell"
    params.quote.p3_queue_included = False
    params.quote.p3_artifact_sha256 = "b" * 64
    params.quote.regime_enabled = True
    params.quote.tick_size = 0.1
    params.quote.lot_size = 0.001
    params.quote.order_size = params.order_size
    params.quote.max_inventory = params.max_inventory
    params.quote.max_spread_bps = 1_000.0
    params.quote.dynamic_cap_enabled = False
    params.conditional_p3_reach_budget_grid_min_ticks = 5
    params.conditional_p3_reach_budget_buy_toxicity_threshold = 0.8
    params.conditional_p3_reach_budget_sell_toxicity_threshold = 0.8
    return params


def _replay_args(
    *,
    timestamps: np.ndarray | None = None,
    prices: np.ndarray | None = None,
    quantities: np.ndarray | None = None,
    makers: np.ndarray | None = None,
    ml_timestamps: np.ndarray | None = None,
    tox_bid: float = 0.9,
    tox_ask: float = 0.9,
):
    ts = (
        np.arange(0, 10_000, 1_000, dtype=np.int64)
        if timestamps is None
        else np.asarray(timestamps, dtype=np.int64)
    )
    px = (
        np.full(ts.size, 100.0, dtype=np.float64)
        if prices is None
        else np.asarray(prices, dtype=np.float64)
    )
    qty = (
        np.zeros(ts.size, dtype=np.float64)
        if quantities is None
        else np.asarray(quantities, dtype=np.float64)
    )
    maker = (
        np.zeros(ts.size, dtype=np.uint8)
        if makers is None
        else np.asarray(makers, dtype=np.uint8)
    )
    ml_ts = (
        np.asarray([0], dtype=np.int64)
        if ml_timestamps is None
        else np.asarray(ml_timestamps, dtype=np.int64)
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)
    empty_matrix = np.empty((0, 0), dtype=np.float64)
    return (
        ts,
        px,
        qty,
        maker,
        empty_i64,
        empty_f64,
        empty_f64,
        empty_f64,
        ml_ts,
        np.full(ml_ts.size, 0.5, dtype=np.float64),
        np.zeros(ml_ts.size, dtype=np.float64),
        np.zeros(ml_ts.size, dtype=np.float64),
        np.full(ml_ts.size, tox_bid, dtype=np.float64),
        np.full(ml_ts.size, tox_ask, dtype=np.float64),
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


def _final_prices(result):
    return {
        (int(row.quote_ts), str(row.side)): float(row.final_price)
        for row in result.quote_trace
    }


def _selected_k_rows(*rows: tuple[int, int, int, int]) -> np.ndarray:
    return np.asarray(
        [
            np.concatenate(
                [np.full(GRID_SIZE, value, dtype=np.uint8) for value in row]
            )
            for row in rows
        ],
        dtype=np.uint8,
    )


def _run_candidate(replay_args, selected_k, params, *, selected_ts=None):
    params.conditional_p3_reach_budget_policy_enabled = True
    timestamps = (
        np.asarray(replay_args[8], dtype=np.int64)
        if selected_ts is None
        else np.asarray(selected_ts, dtype=np.int64)
    )
    return cpp.simulate_tick_arrays_ext_policy_v6(
        replay_args,
        timestamps,
        np.asarray(selected_k, dtype=np.uint8),
        params,
    )


def test_selected_k_is_frozen_and_reused_without_changing_requote_clock() -> None:
    replay_args = _replay_args()
    control_params = _params()
    candidate_params = _params()
    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, control_params)
    # Columns: BUY opener/add, SELL opener/add.
    candidate = _run_candidate(
        replay_args,
        _selected_k_rows((4, 8, 6, 10)),
        candidate_params,
    )

    control_prices = _final_prices(control)
    candidate_prices = _final_prices(candidate)
    assert control_prices.keys() == candidate_prices.keys()
    for key, baseline_price in control_prices.items():
        expected = baseline_price - 0.4 if key[1] == "BUY" else baseline_price + 0.6
        assert candidate_prices[key] == pytest.approx(expected, abs=1e-12)

    summary = candidate.summary
    assert summary.n_requotes == control.summary.n_requotes
    assert summary.p3_reach_budget_bucket_eval_count == 2
    assert summary.p3_reach_budget_toxicity_trigger_count == 2
    assert summary.p3_reach_budget_activation_count == 2
    assert summary.p3_reach_budget_buy_activation_count == 1
    assert summary.p3_reach_budget_sell_activation_count == 1
    assert summary.p3_reach_budget_selected_k_sum == 10
    assert summary.p3_reach_budget_selected_k_max == 6
    assert summary.p3_reach_budget_reuse_count > 0
    assert summary.p3_reach_budget_active_end_count == 2
    assert summary.p3_reach_budget_buy_selected_k_end == 4
    assert summary.p3_reach_budget_sell_selected_k_end == 6


def test_persistent_penalty_survives_keep_decisions() -> None:
    replay_args = _replay_args()
    params = _params()
    params.requote_threshold_bps = 1.0
    candidate = _run_candidate(
        replay_args,
        _selected_k_rows((4, 0, 6, 0)),
        params,
    )

    assert candidate.summary.decision_place_count >= 2
    assert candidate.summary.decision_keep_count > 0
    assert candidate.summary.p3_reach_budget_reuse_count > 0
    assert candidate.summary.p3_reach_budget_price_change_count > 2


def test_next_canonical_bucket_expires_penalty_and_can_select_no_action() -> None:
    timestamps = np.arange(0, 16_000, 1_000, dtype=np.int64)
    ml_timestamps = np.asarray([0, 10_000], dtype=np.int64)
    replay_args = _replay_args(timestamps=timestamps, ml_timestamps=ml_timestamps)
    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, _params())
    candidate = _run_candidate(
        replay_args,
        _selected_k_rows((4, 0, 6, 0), (0, 0, 0, 0)),
        _params(),
    )

    control_prices = _final_prices(control)
    candidate_prices = _final_prices(candidate)
    before_boundary = [key for key in control_prices if key[0] < 10_000]
    after_boundary = [key for key in control_prices if key[0] >= 10_000]
    assert before_boundary and after_boundary
    assert any(candidate_prices[key] != control_prices[key] for key in before_boundary)
    assert all(candidate_prices[key] == control_prices[key] for key in after_boundary)

    summary = candidate.summary
    assert summary.p3_reach_budget_bucket_eval_count == 4
    assert summary.p3_reach_budget_activation_count == 2
    assert summary.p3_reach_budget_no_action_count == 2
    assert summary.p3_reach_budget_bucket_expiry_count == 2
    assert summary.p3_reach_budget_active_end_count == 0


def test_toxicity_p90_trigger_is_separate_from_selected_k_matrix() -> None:
    replay_args = _replay_args(tox_bid=0.79, tox_ask=0.79)
    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, _params())
    candidate = _run_candidate(
        replay_args,
        _selected_k_rows((4, 0, 6, 0)),
        _params(),
    )

    assert _final_prices(candidate) == _final_prices(control)
    assert candidate.summary.p3_reach_budget_bucket_eval_count == 2
    assert candidate.summary.p3_reach_budget_toxicity_trigger_count == 0
    assert candidate.summary.p3_reach_budget_no_action_count == 2
    assert candidate.summary.p3_reach_budget_activation_count == 0


def test_lookup_uses_role_block_and_baseline_distance_offset() -> None:
    replay_args = _replay_args()
    selected = np.full((1, 4 * GRID_SIZE), 255, dtype=np.uint8)
    # The synthetic baseline is exactly five ticks from each BBO, so offset is zero.
    selected[0, 0] = 7
    selected[0, 2 * GRID_SIZE] = 0
    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, _params())
    candidate = _run_candidate(replay_args, selected, _params())

    control_prices = _final_prices(control)
    candidate_prices = _final_prices(candidate)
    for key, baseline_price in control_prices.items():
        expected = baseline_price - 0.7 if key[1] == "BUY" else baseline_price
        assert candidate_prices[key] == pytest.approx(expected, abs=1e-12)
    assert candidate.summary.p3_reach_budget_activation_count == 1
    assert candidate.summary.p3_reach_budget_no_action_count == 1
    assert candidate.summary.p3_reach_budget_unsupported_count == 0


def test_reducing_side_is_unchanged() -> None:
    replay_args = _replay_args()
    control = cpp.simulate_tick_arrays_ext_policy_v3(
        *replay_args,
        _params(initial_inventory=0.001),
    )
    candidate = _run_candidate(
        replay_args,
        _selected_k_rows((0, 5, 0, 9)),
        _params(initial_inventory=0.001),
    )

    control_prices = _final_prices(control)
    candidate_prices = _final_prices(candidate)
    buy_keys = [key for key in control_prices if key[1] == "BUY"]
    sell_keys = [key for key in control_prices if key[1] == "SELL"]
    assert buy_keys and sell_keys
    assert all(
        candidate_prices[key] == pytest.approx(control_prices[key] - 0.5, abs=1e-12)
        for key in buy_keys
    )
    assert all(candidate_prices[key] == control_prices[key] for key in sell_keys)
    assert candidate.summary.p3_reach_budget_bucket_eval_count == 1
    assert candidate.summary.p3_reach_budget_buy_activation_count == 1
    assert candidate.summary.p3_reach_budget_sell_activation_count == 0


def test_fill_to_flat_ends_episode_without_same_bucket_requery() -> None:
    timestamps = np.asarray([0, 1_000, 2_000, 3_000], dtype=np.int64)
    replay_args = _replay_args(
        timestamps=timestamps,
        prices=np.asarray([100.0, 101.0, 101.0, 101.0], dtype=np.float64),
        quantities=np.asarray([0.0, 0.001, 0.0, 0.0], dtype=np.float64),
        makers=np.zeros(timestamps.size, dtype=np.uint8),
    )
    selected = _selected_k_rows((16, 4, 0, 0))
    control = cpp.simulate_tick_arrays_ext_policy_v3(
        *replay_args,
        _params(initial_inventory=0.001),
    )
    candidate = _run_candidate(
        replay_args,
        selected,
        _params(initial_inventory=0.001),
    )

    assert candidate.summary.fills_ask == 1
    assert candidate.summary.final_inventory == pytest.approx(0.0, abs=1e-12)
    assert candidate.summary.p3_reach_budget_activation_count == 1
    assert candidate.summary.p3_reach_budget_flat_reset_count == 1
    assert candidate.summary.p3_reach_budget_active_end_count == 0

    control_prices = _final_prices(control)
    candidate_prices = _final_prices(candidate)
    post_flat_buy = [
        key for key in control_prices if key[0] >= 1_000 and key[1] == "BUY"
    ]
    assert post_flat_buy
    assert all(candidate_prices[key] == control_prices[key] for key in post_flat_buy)


def test_hard_safety_suppresses_price_change_without_clearing_episode() -> None:
    replay_args = _replay_args()
    params = _params(initial_inventory=0.001)
    params.max_inventory = 0.001
    params.quote.max_inventory = 0.001
    candidate = _run_candidate(
        replay_args,
        _selected_k_rows((0, 5, 0, 0)),
        params,
    )

    summary = candidate.summary
    assert summary.p3_reach_budget_activation_count == 1
    assert summary.p3_reach_budget_price_change_count == 0
    assert summary.p3_reach_budget_hard_safety_suppressed_count > 0
    assert summary.p3_reach_budget_reuse_count > 0
    assert summary.p3_reach_budget_active_end_count == 1
    assert summary.p3_reach_budget_buy_selected_k_end == 5


def test_unsupported_matrix_is_baseline_and_invalid_payloads_fail_closed() -> None:
    replay_args = _replay_args()
    control = cpp.simulate_tick_arrays_ext_policy_v3(*replay_args, _params())
    unsupported = _run_candidate(
        replay_args,
        _selected_k_rows((255, 0, 255, 0)),
        _params(),
    )

    assert _final_prices(unsupported) == _final_prices(control)
    assert unsupported.summary.p3_reach_budget_unsupported_count == 2
    assert unsupported.summary.p3_reach_budget_activation_count == 0

    out_of_grid_control_params = _params()
    out_of_grid_control_params.quote.p3_delta_star = 0.5
    out_of_grid_params = _params()
    out_of_grid_params.quote.p3_delta_star = 0.5
    out_of_grid_control = cpp.simulate_tick_arrays_ext_policy_v3(
        *replay_args,
        out_of_grid_control_params,
    )
    out_of_grid = _run_candidate(
        replay_args,
        _selected_k_rows((4, 0, 6, 0)),
        out_of_grid_params,
    )
    assert _final_prices(out_of_grid) == _final_prices(out_of_grid_control)
    assert out_of_grid.summary.p3_reach_budget_unsupported_count == 2

    with pytest.raises(ValueError, match="selected-k must be 0..16 or 255"):
        _run_candidate(
            replay_args,
            _selected_k_rows((17, 0, 0, 0)),
            _params(),
        )
    with pytest.raises(ValueError, match="canonical 10s bucket"):
        noncanonical_args = _replay_args(
            ml_timestamps=np.asarray([1], dtype=np.int64)
        )
        _run_candidate(
            noncanonical_args,
            _selected_k_rows((1, 0, 0, 0)),
            _params(),
            selected_ts=np.asarray([1], dtype=np.int64),
        )
    with pytest.raises(ValueError, match="four 1180-column distance blocks"):
        _run_candidate(
            replay_args,
            np.zeros((1, 4 * GRID_SIZE - 1), dtype=np.uint8),
            _params(),
        )
    invalid_origin_params = _params()
    invalid_origin_params.conditional_p3_reach_budget_grid_min_ticks = 6
    with pytest.raises(ValueError, match="grid_min_ticks must equal 5"):
        _run_candidate(
            replay_args,
            _selected_k_rows((1, 0, 0, 0)),
            invalid_origin_params,
        )


def test_adaptive_policy_and_historical_fixed_gate_are_mutually_exclusive() -> None:
    replay_args = _replay_args()
    params = _params()
    params.conditional_p3_reach_gate_enabled = True
    with pytest.raises(ValueError, match="mutually exclusive"):
        _run_candidate(
            replay_args,
            _selected_k_rows((1, 0, 0, 0)),
            params,
        )
