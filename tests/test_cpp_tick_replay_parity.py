import math

import numpy as np
import pandas as pd
import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

from models import backtest_tick as bt  # noqa: E402
from models.tick_data_types import HistoricalL2Data  # noqa: E402
from strategy import quote_core as qc  # noqa: E402


def _make_params():
    params = narrowgate_cpp.TickReplayParams()
    params.order_size = 0.001
    params.max_inventory = 0.01
    params.requote_interval_s = 1.0
    params.maker_fee = 0.0
    params.queue_base = 0.0
    params.queue_decay = 0.0
    params.maker_fill_prob = 1.0
    params.initial_inventory = 0.0
    params.initial_entry_price = 0.0
    params.initial_sigma_sq = 1.0
    params.quote.gamma = 0.01
    params.quote.kappa = 1.0
    params.quote.tick_size = 0.1
    params.quote.lot_size = 0.001
    params.quote.order_size = params.order_size
    params.quote.max_inventory = params.max_inventory
    params.quote.maker_fee = params.maker_fee
    params.quote.max_spread_bps = 20.0
    # The mechanics fixture preserves the historical inward-cap research arm
    # explicitly. Production/missing-field defaults are tested as fail-closed
    # pause_exposure in test_cpp_quote_core_parity.py.
    params.quote.spread_cap_mode = qc.SPREAD_CAP_COMPRESS
    params.quote.dynamic_cap_enabled = True
    params.quote.dynamic_cap_base_bps = 20.0
    return params


def _python_replay(ts, price, qty, is_buyer_maker, params):
    tick = params.quote.tick_size
    lot = params.quote.lot_size
    order_size = params.order_size
    requote_ms = int(params.requote_interval_s * 1000.0)
    quote_cfg = qc.QuoteCoreConfig(
        gamma=params.quote.gamma,
        eta_inventory=(
            None
            if np.isnan(params.quote.eta_inventory)
            else params.quote.eta_inventory
        ),
        a_spread=(
            None if np.isnan(params.quote.a_spread) else params.quote.a_spread
        ),
        kappa=params.quote.kappa,
        tick_size=tick,
        lot_size=lot,
        maker_fee=params.maker_fee,
        order_size=order_size,
        max_inventory=params.max_inventory,
        quote_horizon_s=params.quote.quote_horizon_s,
        max_spread_bps=params.quote.max_spread_bps,
        dynamic_cap_enabled=params.quote.dynamic_cap_enabled,
        dynamic_cap_base_bps=params.quote.dynamic_cap_base_bps,
        spread_cap_mode=params.quote.spread_cap_mode,
    )
    cash = 0.0
    inv = params.initial_inventory
    bid_orders = []
    ask_orders = []
    last_requote = int(ts[0]) - requote_ms
    quote_mid_state = float(price[0])
    inferred_best_bid = float(price[0]) - tick
    inferred_best_ask = float(price[0]) + tick
    fills_bid = 0
    fills_ask = 0
    n_requotes = 0
    spread_sum = 0.0
    signed_inv_time = 0.0
    abs_inv_time = 0.0
    sq_inv_time = 0.0
    signed_notional_inv_time = 0.0
    notional_inv_time = 0.0
    max_abs_inv = 0.0

    def floor_lot(value):
        return math.floor(value / lot) * lot

    for i in range(len(ts)):
        t = int(ts[i])
        p = float(price[i])
        q = max(0.0, float(qty[i]))
        if is_buyer_maker[i]:
            inferred_best_bid = p
            if inferred_best_ask <= inferred_best_bid:
                inferred_best_ask = inferred_best_bid + tick
        else:
            inferred_best_ask = p
            if inferred_best_bid >= inferred_best_ask:
                inferred_best_bid = inferred_best_ask - tick
        if i > 0:
            dt_s = max(0.0, (t - int(ts[i - 1])) / 1000.0)
            mark = float(price[i - 1]) if price[i - 1] > 0.0 else p
            signed_inv_time += inv * dt_s
            abs_inv_time += abs(inv) * dt_s
            sq_inv_time += inv * inv * dt_s
            signed_notional_inv_time += inv * mark * dt_s
            notional_inv_time += abs(inv) * mark * dt_s

        if is_buyer_maker[i]:
            remaining = q
            new_orders = []
            for order_price, remaining_order in bid_orders:
                if (
                    remaining >= lot
                    and remaining_order >= lot
                    and bt._trade_crosses_order_tick("BUY", p, order_price, tick)
                ):
                    fill_qty = floor_lot(min(remaining_order, remaining))
                    if fill_qty >= lot:
                        cash -= order_price * fill_qty
                        inv += fill_qty
                        remaining_order -= fill_qty
                        remaining -= fill_qty
                        fills_bid += 1
                if remaining_order >= lot:
                    new_orders.append((order_price, remaining_order))
            bid_orders = new_orders
        else:
            remaining = q
            new_orders = []
            for order_price, remaining_order in ask_orders:
                if (
                    remaining >= lot
                    and remaining_order >= lot
                    and bt._trade_crosses_order_tick("SELL", p, order_price, tick)
                ):
                    fill_qty = floor_lot(min(remaining_order, remaining))
                    if fill_qty >= lot:
                        cash += order_price * fill_qty
                        inv -= fill_qty
                        remaining_order -= fill_qty
                        remaining -= fill_qty
                        fills_ask += 1
                if remaining_order >= lot:
                    new_orders.append((order_price, remaining_order))
            ask_orders = new_orders
        max_abs_inv = max(max_abs_inv, abs(inv))

        if t - last_requote < requote_ms:
            continue
        last_requote = t
        quote = qc._compute_quote_core_py(
            qc.QuoteState(
                mid=quote_mid_state,
                inventory=inv,
                sigma_sq=params.initial_sigma_sq,
                best_bid=inferred_best_bid,
                best_ask=inferred_best_ask,
            ),
            quote_cfg,
            qc.QuotePrediction(),
        )
        bid_orders = []
        ask_orders = []
        if inv < params.max_inventory:
            bid_size = order_size
            if inv > 0:
                room = floor_lot(max(0.0, params.max_inventory - inv))
                bid_size = min(bid_size, room) if room >= lot else 0.0
            elif inv < -lot:
                bid_size = min(bid_size, floor_lot(abs(inv)))
            if bid_size >= lot:
                bid_orders.append((quote.bid_price, bid_size))
        if inv > -params.max_inventory:
            ask_size = order_size
            if inv < 0:
                room = floor_lot(max(0.0, params.max_inventory - abs(inv)))
                ask_size = min(ask_size, room) if room >= lot else 0.0
            elif inv > lot:
                ask_size = min(ask_size, floor_lot(inv))
            if ask_size >= lot:
                ask_orders.append((quote.ask_price, ask_size))
        spread_sum += getattr(quote, "delta_after_cap", quote.spread)
        n_requotes += 1
        quote_mid_state = p

    final_price = float(price[-1])
    return {
        "pnl": cash + inv * final_price,
        "cash": cash,
        "final_inventory": inv,
        "max_abs_inventory": max_abs_inv,
        "fills_bid": fills_bid,
        "fills_ask": fills_ask,
        "fills_total": fills_bid + fills_ask,
        "n_requotes": n_requotes,
        "avg_spread": spread_sum / n_requotes if n_requotes else 0.0,
        "signed_inventory_time_s": signed_inv_time,
        "abs_inventory_time_s": abs_inv_time,
        "sq_inventory_time_s": sq_inv_time,
        "signed_notional_inventory_time_s": signed_notional_inv_time,
        "notional_inventory_time_s": notional_inv_time,
    }


def test_cpp_summary_only_matches_curve_mode():
    ts = np.arange(0, 30_000, 250, dtype=np.int64)
    price = np.rint((100.0 + np.sin(np.arange(ts.size) / 7.0) * 0.6) / 0.1) * 0.1
    qty = np.full(ts.size, 0.003, dtype=np.float64)
    is_buyer_maker = (np.arange(ts.size) % 2).astype(np.uint8)

    with_curves = _make_params()
    with_curves.collect_curves = True
    full = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, with_curves)
    pnl_curve = np.asarray(full.pnl, dtype=np.float64)
    ts_curve = np.asarray(full.pnl_ts_ms, dtype=np.int64)
    pnl_delta = np.diff(pnl_curve)
    dt_s = np.diff(ts_curve).astype(np.float64) / 1000.0
    normalized = pnl_delta / np.sqrt(dt_s)
    expected_sharpe = (
        (pnl_delta.sum() / dt_s.sum()) / normalized.std() * math.sqrt(365.25 * 86_400.0)
        if normalized.size and normalized.std() > 0.0
        else 0.0
    )
    expected_drawdown = float((np.maximum.accumulate(pnl_curve) - pnl_curve).max())
    assert full.summary.sharpe == pytest.approx(expected_sharpe, abs=1e-10)
    assert full.summary.max_drawdown == pytest.approx(expected_drawdown, abs=1e-10)

    summary_only = _make_params()
    summary_only.collect_curves = False
    compact = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, summary_only)

    assert compact.pnl_ts_ms == []
    assert compact.pnl == []
    assert compact.inventory == []
    for field in ("pnl", "final_inventory", "fills_total", "sharpe", "max_drawdown"):
        assert getattr(compact.summary, field) == pytest.approx(
            getattr(full.summary, field), abs=1e-10
        ), field


def test_cpp_tick_replay_synthetic_parity():
    n = 2_000
    ts = np.arange(n, dtype=np.int64) * 100
    price = 100.0 + np.sin(np.arange(n, dtype=np.float64) / 8.0) * 0.6
    price = np.round(price, 1).astype(np.float64)
    qty = np.full(n, 0.003, dtype=np.float64)
    is_buyer_maker = ((np.arange(n) % 2) == 0).astype(np.uint8)
    params = _make_params()
    params.quote.eta_inventory = 0.02
    params.quote.a_spread = 0.03
    params.quote.quote_horizon_s = 5.0

    py = _python_replay(ts, price, qty, is_buyer_maker, params)
    cpp = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params).summary

    assert cpp.pnl == pytest.approx(py["pnl"], abs=1e-9)
    assert cpp.cash == pytest.approx(py["cash"], abs=1e-9)
    assert cpp.final_inventory == pytest.approx(py["final_inventory"], abs=1e-12)
    assert cpp.fills_bid == py["fills_bid"]
    assert cpp.fills_ask == py["fills_ask"]
    assert cpp.fills_total == py["fills_total"]
    assert cpp.n_requotes == py["n_requotes"]
    assert cpp.avg_spread == pytest.approx(py["avg_spread"], abs=2e-5)
    assert cpp.abs_inventory_time_s == pytest.approx(py["abs_inventory_time_s"], abs=1e-12)
    assert cpp.signed_notional_inventory_time_s == pytest.approx(
        py["signed_notional_inventory_time_s"],
        abs=1e-12,
    )


def test_cpp_requote_clock_fixed_preserves_wall_clock_cadence():
    ts = np.asarray([0, 1500, 2000, 2500, 3000], dtype=np.int64)
    price = np.full(ts.size, 100.0, dtype=np.float64)
    qty = np.zeros(ts.size, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)

    trade_clock = _make_params()
    trade_clock.requote_interval_s = 1.0
    trade_clock.requote_clock_fixed = False
    fixed_clock = _make_params()
    fixed_clock.requote_interval_s = 1.0
    fixed_clock.requote_clock_fixed = True

    trade_result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, trade_clock)
    fixed_result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, fixed_clock)

    assert trade_result.summary.n_requotes == 3
    assert fixed_result.summary.n_requotes == 4


def test_cpp_replace_throttle_keeps_active_orders():
    ts = np.arange(0, 5_000, 1_000, dtype=np.int64)
    price = np.full(ts.size, 100.0, dtype=np.float64)
    qty = np.zeros(ts.size, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)

    params = _make_params()
    params.replace_min_price_change_ticks = 20.0
    params.replace_min_price_change_ticks_reducing = 20.0
    params.replace_min_interval_ms = 0.0
    params.replace_min_interval_ms_reducing = 0.0

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert result.summary.n_requotes == 5
    assert result.summary.decision_place_count == 2
    assert result.summary.decision_replace_count == 0
    assert result.summary.decision_keep_count == 8
    assert result.summary.replace_throttle_count == 8
    assert result.summary.replace_throttle_price_count == 8


def test_cpp_pending_replace_coalesce_does_not_stack_pending_new_orders():
    ts = np.arange(0, 5_000, 1_000, dtype=np.int64)
    price = np.full(ts.size, 100.0, dtype=np.float64)
    qty = np.zeros(ts.size, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)

    params = _make_params()
    params.new_order_latency_ms = 3_000
    params.replace_pending_coalesce = True

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert result.summary.n_requotes == 5
    assert result.summary.decision_place_count == 2
    assert result.summary.decision_pending_coalesce_count >= 2
    assert result.summary.max_pending_new_orders == 2


def test_cpp_buy_fill_selection_compact_scorer_hits():
    ts = np.arange(0, 3_000, 1_000, dtype=np.int64)
    price = np.full(ts.size, 100.0, dtype=np.float64)
    qty = np.zeros(ts.size, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)

    fold = narrowgate_cpp.FillSelectionFoldModel()
    fold.base_logit = 0.0
    fold.contribution_scale = 1.0
    fold.categorical_features = ["side"]
    fold.contributions = {"side": {"BUY": 10.0}}

    params = _make_params()
    params.buy_fill_selection_live_enabled = True
    params.buy_fill_selection_live_score_threshold = 0.90
    params.buy_fill_selection_live_spread_mult_cap = 1.0
    params.buy_fill_selection_models = [fold]

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert result.summary.buy_fill_selection_live_eval_count == 3
    assert result.summary.buy_fill_selection_live_hit_count == 3
    assert result.summary.buy_fill_selection_live_score_max > 0.98


def test_cpp_buy_fill_selection_static_model_rejects_legacy_abi():
    ts = np.arange(0, 3_000, 1_000, dtype=np.int64)
    price = np.full(ts.size, 100.0, dtype=np.float64)
    qty = np.zeros(ts.size, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)

    fold = narrowgate_cpp.FillSelectionFoldModel()
    fold.base_logit = 0.0
    fold.contribution_scale = 1.0
    fold.numeric_cuts = {"fill_quality_score": [0.5]}
    fold.contributions = {"fill_quality_score": {"missing": -1.0, "b01": 1.0}}

    params = _make_params()
    params.buy_fill_selection_live_enabled = True
    params.buy_fill_selection_models = [fold]

    with pytest.raises(ValueError, match="ext_policy_v3 static payload is required"):
        narrowgate_cpp.simulate_tick_arrays(
            ts,
            price,
            qty,
            is_buyer_maker,
            params,
        )


def _run_cpp_v3_buy_fill_selection(
    *,
    params,
    static_delta: np.ndarray,
    static_missing: np.ndarray,
    static_used: np.ndarray,
    ml_ready_ts: int = 0,
):
    ts = np.arange(0, 3_000, 1_000, dtype=np.int64)
    price = np.full(ts.size, 100.0, dtype=np.float64)
    qty = np.zeros(ts.size, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)
    empty_matrix = np.empty((0, 0), dtype=np.float64)
    ml_ts = np.asarray([ml_ready_ts], dtype=np.int64)
    ml_half = np.asarray([0.5], dtype=np.float64)
    ml_zero = np.asarray([0.0], dtype=np.float64)

    return narrowgate_cpp.simulate_tick_arrays_ext_policy_v3(
        ts,
        price,
        qty,
        is_buyer_maker,
        empty_i64,
        empty_f64,
        empty_f64,
        empty_f64,
        ml_ts,
        ml_half,
        ml_zero,
        ml_zero,
        ml_half,
        ml_half,
        np.ascontiguousarray(static_delta, dtype=np.float64),
        np.ascontiguousarray(static_missing, dtype=np.float64),
        np.ascontiguousarray(static_used, dtype=np.float64),
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
        params,
    )


def test_cpp_buy_fill_selection_v3_consumes_static_feature_payload():
    fold = narrowgate_cpp.FillSelectionFoldModel()
    fold.base_logit = 0.0
    fold.contribution_scale = 1.0
    fold.numeric_cuts = {"fill_quality_score": [0.5]}
    fold.contributions = {"fill_quality_score": {"b01": 10.0}}

    params = _make_params()
    params.buy_fill_selection_live_enabled = True
    params.buy_fill_selection_live_score_threshold = 0.90
    params.buy_fill_selection_models = [fold]

    result = _run_cpp_v3_buy_fill_selection(
        params=params,
        static_delta=np.asarray([[0.0], [10.0]]),
        static_missing=np.asarray([[1.0], [0.0]]),
        static_used=np.asarray([[0.0], [1.0]]),
    )

    expected = 1.0 / (1.0 + math.exp(-math.sqrt(1.0 / 5.0) * 10.0))
    assert result.summary.buy_fill_selection_live_eval_count == 3
    assert result.summary.buy_fill_selection_live_hit_count == 3
    assert result.summary.buy_fill_selection_live_score_max == pytest.approx(expected)


def test_cpp_buy_fill_selection_v3_does_not_read_static_row_before_ready_time():
    fold = narrowgate_cpp.FillSelectionFoldModel()
    fold.base_logit = 0.0
    fold.contribution_scale = 1.0
    fold.numeric_cuts = {"fill_quality_score": [0.5]}
    fold.contributions = {"fill_quality_score": {"missing": -10.0, "b01": 10.0}}

    params = _make_params()
    params.buy_fill_selection_live_enabled = True
    params.buy_fill_selection_live_score_threshold = 0.90
    params.buy_fill_selection_models = [fold]

    result = _run_cpp_v3_buy_fill_selection(
        params=params,
        static_delta=np.asarray([[-10.0], [10.0]]),
        static_missing=np.asarray([[1.0], [0.0]]),
        static_used=np.asarray([[1.0], [1.0]]),
        ml_ready_ts=2_000,
    )

    assert result.summary.buy_fill_selection_live_eval_count == 3
    assert result.summary.buy_fill_selection_live_hit_count == 1
    assert result.summary.buy_fill_selection_live_score_max > 0.98


def test_cpp_buy_fill_selection_threshold_hit_is_not_actionable_at_inventory_limit():
    fold = narrowgate_cpp.FillSelectionFoldModel()
    fold.base_logit = 0.0
    fold.contribution_scale = 1.0
    fold.categorical_features = ["side"]
    fold.contributions = {"side": {"BUY": 10.0}}

    params = _make_params()
    params.initial_inventory = params.max_inventory * 0.99
    params.buy_fill_selection_live_enabled = True
    params.buy_fill_selection_live_score_threshold = 0.90
    params.buy_fill_selection_models = [fold]

    result = _run_cpp_v3_buy_fill_selection(
        params=params,
        static_delta=np.zeros((2, 1)),
        static_missing=np.zeros((2, 1)),
        static_used=np.zeros((2, 1)),
    )

    assert result.summary.buy_fill_selection_live_eval_count == 3
    assert result.summary.buy_fill_selection_live_score_max > 0.98
    assert result.summary.buy_fill_selection_live_hit_count == 0


def test_cpp_queue_ahead_multiplier_thickens_initial_queue():
    ts = np.asarray([0, 1_000, 2_000], dtype=np.int64)
    price = np.asarray([100.0, 99.9, 100.0], dtype=np.float64)
    qty = np.asarray([0.0, 0.002, 0.0], dtype=np.float64)
    is_buyer_maker = np.asarray([0, 1, 0], dtype=np.uint8)

    baseline = _make_params()
    baseline.requote_interval_s = 10.0
    baseline.queue_base = 0.001
    baseline.queue_decay = 0.0

    thick = _make_params()
    thick.requote_interval_s = 10.0
    thick.queue_base = 0.001
    thick.queue_decay = 0.0
    thick.queue_ahead_base_mult = 3.0

    baseline_result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, baseline)
    thick_result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, thick)

    assert baseline_result.summary.fills_bid == 1
    assert thick_result.summary.fills_bid == 0


def test_cpp_queue_deplete_multiplier_increases_queue_consumption():
    ts = np.asarray([0, 1_000, 2_000], dtype=np.int64)
    price = np.asarray([100.0, 99.9, 100.0], dtype=np.float64)
    qty = np.asarray([0.0, 0.002, 0.0], dtype=np.float64)
    is_buyer_maker = np.asarray([0, 1, 0], dtype=np.uint8)

    baseline = _make_params()
    baseline.requote_interval_s = 10.0
    baseline.queue_base = 0.002
    baseline.queue_decay = 0.0

    depleted = _make_params()
    depleted.requote_interval_s = 10.0
    depleted.queue_base = 0.002
    depleted.queue_decay = 0.0
    depleted.queue_deplete_base_mult = 2.0

    baseline_result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, baseline)
    depleted_result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, depleted)

    assert baseline_result.summary.fills_bid == 0
    assert depleted_result.summary.fills_bid == 1


def test_cpp_l2_cancel_ahead_depletes_only_unprinted_queue_drop():
    ts = np.asarray([0, 1_000, 2_000, 3_000], dtype=np.int64)
    price = np.asarray([100.0, 100.0, 99.9, 100.0], dtype=np.float64)
    qty = np.asarray([0.0, 0.0, 0.003, 0.0], dtype=np.float64)
    is_buyer_maker = np.asarray([0, 0, 1, 0], dtype=np.uint8)
    l2_ts = ts.copy()
    l2_bid_px = np.full((4, 1), 99.9, dtype=np.float64)
    l2_bid_qty = np.asarray([[0.004], [0.002], [0.002], [0.002]], dtype=np.float64)
    l2_ask_px = np.full((4, 1), 100.1, dtype=np.float64)
    l2_ask_qty = np.full((4, 1), 0.004, dtype=np.float64)

    static = _make_params()
    static.requote_interval_s = 10.0
    dynamic = _make_params()
    dynamic.requote_interval_s = 10.0
    dynamic.queue_l2_cancel_ahead_enabled = True

    static_result = _simulate_policy_ext(
        ts,
        price,
        qty,
        is_buyer_maker,
        static,
        l2_ts=l2_ts,
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
    )
    dynamic_result = _simulate_policy_ext(
        ts,
        price,
        qty,
        is_buyer_maker,
        dynamic,
        l2_ts=l2_ts,
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
    )

    assert static_result.summary.fills_bid == 0
    assert dynamic_result.summary.fills_bid == 1
    assert dynamic_result.summary.queue_l2_cancel_ahead_bid_event_count == 1
    assert dynamic_result.summary.queue_l2_cancel_ahead_qty == pytest.approx(0.002)


def test_cpp_l2_cancel_ahead_does_not_double_count_same_price_trade():
    ts = np.asarray([0, 1_000, 2_000, 3_000], dtype=np.int64)
    price = np.asarray([100.0, 99.9, 100.0, 99.9], dtype=np.float64)
    qty = np.asarray([0.0, 0.002, 0.0, 0.002], dtype=np.float64)
    is_buyer_maker = np.asarray([0, 1, 0, 1], dtype=np.uint8)
    l2_ts = np.asarray([0, 2_000], dtype=np.int64)
    l2_bid_px = np.full((2, 1), 99.9, dtype=np.float64)
    l2_bid_qty = np.asarray([[0.004], [0.002]], dtype=np.float64)
    l2_ask_px = np.full((2, 1), 100.1, dtype=np.float64)
    l2_ask_qty = np.full((2, 1), 0.004, dtype=np.float64)

    params = _make_params()
    params.requote_interval_s = 10.0
    params.queue_l2_cancel_ahead_enabled = True
    result = _simulate_policy_ext(
        ts,
        price,
        qty,
        is_buyer_maker,
        params,
        l2_ts=l2_ts,
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
    )

    assert result.summary.fills_bid == 0
    assert result.summary.queue_l2_cancel_ahead_event_count == 0
    assert result.summary.queue_l2_cancel_ahead_qty == pytest.approx(0.0)


def test_python_cpp_l2_cancel_ahead_synthetic_parity():
    bt.configure_symbol("BTCUSDC")
    ts = np.asarray([0, 1_000, 2_000, 3_000], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": [100.0, 100.0, 99.9, 100.0],
            "quantity": [0.0, 0.0, 0.003, 0.0],
            "is_buyer_maker": [False, False, True, False],
            "_is_execution_trade": [False, False, True, False],
        }
    )
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=np.full((4, 1), 99.9, dtype=np.float64),
        bid_qty=np.asarray([[0.004], [0.002], [0.002], [0.002]], dtype=np.float64),
        ask_px=np.full((4, 1), 100.1, dtype=np.float64),
        ask_qty=np.full((4, 1), 0.004, dtype=np.float64),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 10.0,
        "rq_min": 10.0,
        "rq_max": 10.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "queue_ahead_mode": "exact_level",
        "queue_ahead_base_mult": 1.0,
        "queue_deplete_base_mult": 1.0,
        "maker_fill_prob": 1.0,
        "queue_l2_cancel_ahead_enabled": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 20.0,
        "spread_cap_mode": "compress",
        "ml_enabled": False,
        "trace_quotes_max": 100,
        "trace_fills_max": 100,
        "collect_curves": False,
    }

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )

    for field in (
        "fills_bid",
        "fills_ask",
        "queue_l2_cancel_ahead_event_count",
        "queue_l2_cancel_ahead_bid_event_count",
    ):
        assert cpp[field] == py[field], field
    assert cpp["queue_l2_cancel_ahead_qty"] == pytest.approx(
        py["queue_l2_cancel_ahead_qty"]
    )
    assert cpp["pnl"] == pytest.approx(py["pnl"], abs=1e-12)


def test_python_cpp_exec_book_visibility_delay_keeps_quote_clock_parity():
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 4_001, 1_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": [100.0, 110.0, 110.0, 110.0, 110.0],
            "quantity": np.zeros(ts.size),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    bid = np.asarray([99.9, 109.9, 109.9, 109.9, 109.9])
    ask = np.asarray([100.1, 110.1, 110.1, 110.1, 110.1])
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=bid[:, None],
        bid_qty=np.full((ts.size, 1), 0.01),
        ask_px=ask[:, None],
        ask_qty=np.full((ts.size, 1), 0.01),
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
        "spread_cap_mode": "compress",
        "max_exec_book_age_s": 5.0,
        "ml_enabled": False,
        "trace_quotes_max": 100,
        "collect_curves": False,
        "_exec_book_visibility_delay_samples_ms": np.asarray([1_000.0]),
        "exec_book_visibility_delay_seed": 20260718,
    }

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )

    assert py["exec_book_visibility_delay_applied_avg_ms"] == 1_000.0
    assert cpp["exec_book_visibility_delay_applied_avg_ms"] == 1_000.0
    assert cpp["n_requotes"] == py["n_requotes"]
    assert cpp["fills_total"] == py["fills_total"]
    assert cpp["pnl"] == pytest.approx(py["pnl"], abs=1e-12)
    assert cpp["_quote_trace"][0]["best_bid"] == pytest.approx(
        py["_quote_trace"][0]["best_bid"]
    )
    assert cpp["_quote_trace"][0]["best_ask"] == pytest.approx(
        py["_quote_trace"][0]["best_ask"]
    )


def _empty_i64():
    return np.empty(0, dtype=np.int64)


def _empty_f64():
    return np.empty(0, dtype=np.float64)


def _empty_l2():
    return np.empty((0, 0), dtype=np.float64)


def _simulate_policy_ext(
    ts,
    price,
    qty,
    is_buyer_maker,
    params,
    *,
    l2_ts=None,
    l2_bid_px=None,
    l2_bid_qty=None,
    l2_ask_px=None,
    l2_ask_qty=None,
    queue_base_by_trade=None,
    queue_decay_by_trade=None,
    buy_fill_prob_by_trade=None,
    sell_fill_prob_by_trade=None,
):
    args = [
        ts,
        price,
        qty,
        is_buyer_maker,
        _empty_i64(),
        _empty_f64(),
        _empty_f64(),
        _empty_f64(),
        _empty_i64(),
        _empty_f64(),
        _empty_f64(),
        _empty_f64(),
        _empty_f64(),
        _empty_f64(),
        _empty_l2(),
        _empty_l2(),
        _empty_l2(),
        _empty_i64(),
        _empty_f64(),
        _empty_f64(),
        _empty_f64(),
        _empty_f64(),
        np.ascontiguousarray(l2_ts if l2_ts is not None else _empty_i64(), dtype=np.int64),
        np.ascontiguousarray(l2_bid_px if l2_bid_px is not None else _empty_l2(), dtype=np.float64),
        np.ascontiguousarray(l2_bid_qty if l2_bid_qty is not None else _empty_l2(), dtype=np.float64),
        np.ascontiguousarray(l2_ask_px if l2_ask_px is not None else _empty_l2(), dtype=np.float64),
        np.ascontiguousarray(l2_ask_qty if l2_ask_qty is not None else _empty_l2(), dtype=np.float64),
        np.ascontiguousarray(
            queue_base_by_trade if queue_base_by_trade is not None else _empty_f64(),
            dtype=np.float64,
        ),
        np.ascontiguousarray(
            queue_decay_by_trade if queue_decay_by_trade is not None else _empty_f64(),
            dtype=np.float64,
        ),
        np.ascontiguousarray(
            buy_fill_prob_by_trade if buy_fill_prob_by_trade is not None else _empty_f64(),
            dtype=np.float64,
        ),
        np.ascontiguousarray(
            sell_fill_prob_by_trade if sell_fill_prob_by_trade is not None else _empty_f64(),
            dtype=np.float64,
        ),
        _empty_f64(),
        _empty_f64(),
    ]
    args.append(
        params,
    )
    return narrowgate_cpp.simulate_tick_arrays_ext_policy_v3(*args)


def test_cpp_policy_position_timeout_closes_inventory():
    ts = np.arange(6, dtype=np.int64) * 1_000
    price = np.full(6, 100.0, dtype=np.float64)
    qty = np.full(6, 0.01, dtype=np.float64)
    is_buyer_maker = np.zeros(6, dtype=np.uint8)
    params = _make_params()
    params.initial_inventory = 0.001
    params.initial_entry_price = 100.0
    params.position_timeout_s = 0.5
    params.taker_fee = 0.0

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert result.summary.position_timeout_count == 1
    assert result.summary.final_inventory == pytest.approx(0.0, abs=1e-12)
    assert result.summary.pnl == pytest.approx(0.0, abs=1e-12)


def test_cpp_policy_circuit_breaker_uses_price_variance_units():
    ts = np.arange(7, dtype=np.int64) * 1_000
    price = np.array(
        [100.0, 90.0, 90.0, 90.0, 90.0, 90.0, 90.0],
        dtype=np.float64,
    )
    # The first non-zero trade refreshes the inferred touch; the next one
    # consumes the reduce-only maker close order.
    qty = np.array([0.0, 0.0, 0.0, 0.0, 0.001, 0.01, 0.0], dtype=np.float64)
    is_buyer_maker = np.zeros(7, dtype=np.uint8)
    params = _make_params()
    params.initial_inventory = 0.001
    params.initial_entry_price = 100.0
    params.initial_sigma_sq = 1.0
    params.circuit_breaker_sigma = 1.0
    params.circuit_breaker_maker_close = True
    params.quote.pnl_volatility_horizon_s = 1.0
    params.taker_fee = 0.01

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert result.summary.circuit_breaker_count == 1
    assert result.summary.circuit_breaker_close_place_count >= 1
    assert result.summary.circuit_breaker_close_fill_count == 1
    assert result.summary.circuit_breaker_closing is False
    assert result.summary.final_inventory == pytest.approx(0.0, abs=1e-12)
    assert result.summary.pnl == pytest.approx(-0.01, abs=1e-12)


def test_cpp_policy_maker_close_escalates_to_ioc():
    ts = np.arange(0, 100_000, 10_000, dtype=np.int64)
    price = np.asarray([100.0] + [90.0] * (ts.size - 1), dtype=np.float64)
    qty = np.zeros(ts.size, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)
    params = _make_params()
    params.initial_inventory = 0.001
    params.initial_entry_price = 100.0
    params.initial_sigma_sq = 1.0
    params.circuit_breaker_sigma = 1.0
    params.circuit_breaker_maker_close = True
    params.quote.pnl_volatility_horizon_s = 1.0
    params.requote_interval_s = 10.0
    params.rq_min_s = 10.0
    params.rq_max_s = 10.0
    params.taker_fee = 0.01

    result = narrowgate_cpp.simulate_tick_arrays(
        ts,
        price,
        qty,
        is_buyer_maker,
        params,
    )

    assert result.summary.circuit_breaker_count == 1
    assert result.summary.circuit_breaker_close_ioc_place_count >= 1
    assert result.summary.circuit_breaker_close_ioc_fill_count == 1
    assert result.summary.circuit_breaker_close_ioc_expire_count == 0
    assert result.summary.circuit_breaker_closing is False
    assert result.summary.final_inventory == pytest.approx(0.0, abs=1e-12)


def test_python_cpp_circuit_breaker_maker_close_parity():
    bt.configure_symbol("BTCUSDC")
    trade_ts = np.asarray([0, 3_000, 5_000], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": trade_ts,
            "price": [100.0, 90.1, 90.1],
            "quantity": [0.0, 0.01, 0.0],
            "is_buyer_maker": [False, False, False],
        }
    )
    l2_ts = np.arange(0, 5_001, 1_000, dtype=np.int64)
    bid = np.asarray([99.9, 89.9, 89.9, 89.9, 89.9, 89.9])
    ask = np.asarray([100.1, 90.1, 90.1, 90.1, 90.1, 90.1])
    l2 = HistoricalL2Data(
        ts_ms=l2_ts,
        bid_px=bid[:, None],
        bid_qty=np.zeros((l2_ts.size, 1), dtype=np.float64),
        ask_px=ask[:, None],
        ask_qty=np.zeros((l2_ts.size, 1), dtype=np.float64),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.01,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "initial_inventory": 0.001,
        "initial_entry_price": 100.0,
        "circuit_breaker_sigma": 1.0,
        "circuit_breaker_exit_mode": "maker_close",
        "pnl_volatility_horizon_s": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "max_exec_book_age_s": 5.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "trace_fills_max": 100,
    }
    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )

    for field in (
        "circuit_breaker_count",
        "circuit_breaker_close_place_count",
        "circuit_breaker_close_fill_count",
        "circuit_breaker_close_gtx_reject_count",
        "circuit_breaker_close_ioc_place_count",
        "circuit_breaker_close_ioc_fill_count",
        "circuit_breaker_close_ioc_expire_count",
        "fills_bid",
        "fills_ask",
    ):
        assert cpp[field] == py[field], field
    assert cpp["final_inventory"] == pytest.approx(py["final_inventory"], abs=1e-12)
    assert cpp["pnl"] == pytest.approx(py["pnl"], abs=1e-12)
    assert len(cpp["_fill_trace"]) == len(py["_fill_trace"]) == 1
    assert cpp["_fill_trace"][0]["price"] == pytest.approx(
        py["_fill_trace"][0]["price"],
        abs=1e-12,
    )
    assert cpp["_fill_trace"][0]["quote_px"] == pytest.approx(
        py["_fill_trace"][0]["quote_px"],
        abs=1e-12,
    )


def test_python_cpp_ioc_close_uses_activation_book_without_resetting_quote_clock():
    bt.configure_symbol("BTCUSDC")
    trade_ts = np.arange(0, 100_001, 10_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": trade_ts,
            "price": np.asarray(
                [100.0] + [90.0] * (trade_ts.size - 1),
                dtype=np.float64,
            ),
            "quantity": np.zeros(trade_ts.size, dtype=np.float64),
            "is_buyer_maker": np.zeros(trade_ts.size, dtype=bool),
        }
    )
    l2_ts = np.arange(0, 100_001, 1_000, dtype=np.int64)
    bid = np.where(l2_ts < 10_000, 99.9, 89.9).astype(np.float64)
    ask = np.where(l2_ts < 10_000, 100.1, 90.1).astype(np.float64)
    bid[(l2_ts >= 60_000) & (l2_ts < 62_000)] = 89.7
    ask[(l2_ts >= 60_000) & (l2_ts < 62_000)] = 89.9
    bid[l2_ts >= 62_000] = 89.5
    ask[l2_ts >= 62_000] = 89.7
    l2 = HistoricalL2Data(
        ts_ms=l2_ts,
        bid_px=bid[:, None],
        bid_qty=np.ones((l2_ts.size, 1), dtype=np.float64),
        ask_px=ask[:, None],
        ask_qty=np.ones((l2_ts.size, 1), dtype=np.float64),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 10.0,
        "rq_min": 10.0,
        "rq_max": 10.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.01,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "initial_inventory": 0.001,
        "initial_entry_price": 100.0,
        "circuit_breaker_sigma": 1.0,
        "circuit_breaker_exit_mode": "maker_close",
        "pnl_volatility_horizon_s": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "max_exec_book_age_s": 5.0,
        "new_order_latency_ms": 1_500,
        "cancel_order_latency_ms": 0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "trace_fills_max": 100,
        "trace_quotes_max": 1_000,
    }

    results = {
        engine: bt._simulate_tick_with_engine(
            engine,
            trades,
            np.asarray([0], dtype=np.int64),
            np.asarray([1.0], dtype=np.float64),
            params,
            l2_data=l2,
        )
        for engine in ("python", "cpp")
    }
    py = results["python"]
    cpp = results["cpp"]

    assert py["circuit_breaker_close_ioc_fill_count"] == 1
    assert cpp["circuit_breaker_close_ioc_fill_count"] == 1
    assert cpp["pnl"] == pytest.approx(py["pnl"], abs=1e-12)
    assert cpp["final_inventory"] == pytest.approx(
        py["final_inventory"],
        abs=1e-12,
    )
    assert len(cpp["_fill_trace"]) == len(py["_fill_trace"]) == 1
    for field in (
        "submit_ts",
        "activate_ts",
        "quote_ts",
        "fill_ts",
        "price",
        "quote_px",
        "fill_trade_px",
        "quote_mid",
        "fill_qty",
        "fill_fee_rate",
        "fill_fee_usdc",
    ):
        assert cpp["_fill_trace"][0][field] == pytest.approx(
            py["_fill_trace"][0][field],
            abs=1e-12,
        ), field
    assert py["_fill_trace"][0]["quote_ts"] == 70_000
    assert py["_fill_trace"][0]["activate_ts"] == 71_500
    assert py["_fill_trace"][0]["fill_ts"] == 72_000
    assert py["_fill_trace"][0]["quote_px"] == pytest.approx(89.5)
    assert py["_fill_trace"][0]["fill_trade_px"] == pytest.approx(90.0)
    for result in (py, cpp):
        assert result["_fill_trace"][0]["fill_fee_rate"] == pytest.approx(0.01)
        assert result["_fill_trace"][0]["fill_fee_usdc"] == pytest.approx(
            0.001 * 89.5 * 0.01
        )
    for result in (py, cpp):
        fill_order_id = result["_fill_trace"][0]["order_id"]
        terminal_rows = [
            row
            for row in result["_quote_trace"]
            if row["order_id"] == fill_order_id
        ]
        assert len(terminal_rows) == 1
        assert terminal_rows[0]["outcome"] == "fill"
        assert terminal_rows[0]["cancel_reason"] == "ioc_fill"
        assert terminal_rows[0]["outcome_ts"] == 72_000


def test_python_cpp_gtx_rejects_resting_order_against_activation_book():
    bt.configure_symbol("BTCUSDC")
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray([0, 600], dtype=np.int64),
            "price": np.asarray([100.0, 105.1], dtype=np.float64),
            "quantity": np.asarray([0.0, 1.0], dtype=np.float64),
            "is_buyer_maker": np.asarray([False, False], dtype=bool),
        }
    )
    l2 = HistoricalL2Data(
        ts_ms=np.asarray([0, 500, 600], dtype=np.int64),
        bid_px=np.asarray([[99.9], [105.0], [105.0]], dtype=np.float64),
        bid_qty=np.ones((3, 1), dtype=np.float64),
        ask_px=np.asarray([[100.1], [105.2], [105.2]], dtype=np.float64),
        ask_qty=np.ones((3, 1), dtype=np.float64),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 10.0,
        "rq_min": 10.0,
        "rq_max": 10.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "circuit_breaker_sigma": 0.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 5.0,
        "new_order_latency_ms": 500,
        "cancel_order_latency_ms": 0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "trace_fills_max": 100,
        "trace_quotes_max": 1_000,
    }

    results = {
        engine: bt._simulate_tick_with_engine(
            engine,
            trades,
            np.asarray([0], dtype=np.int64),
            np.asarray([0.0], dtype=np.float64),
            params,
            l2_data=l2,
        )
        for engine in ("python", "cpp")
    }
    py = results["python"]
    cpp = results["cpp"]

    assert py["fills_ask"] == 0
    assert cpp["fills_ask"] == py["fills_ask"]
    assert cpp["fills_total"] == py["fills_total"]
    assert cpp["pnl"] == pytest.approx(py["pnl"], abs=1e-12)
    assert cpp["final_inventory"] == pytest.approx(
        py["final_inventory"],
        abs=1e-12,
    )
    assert cpp["_fill_trace"] == py["_fill_trace"] == []


def test_cpp_policy_emergency_taker_close_hook():
    ts = np.arange(4, dtype=np.int64) * 1_000
    price = np.full(4, 100.0, dtype=np.float64)
    qty = np.full(4, 0.01, dtype=np.float64)
    is_buyer_maker = np.zeros(4, dtype=np.uint8)
    params = _make_params()
    params.initial_inventory = params.max_inventory
    params.initial_entry_price = 100.0
    params.emergency_taker_close_enabled = True
    params.taker_fee = 0.0

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert result.summary.emergency_close_count == 1
    assert result.summary.final_inventory == pytest.approx(0.0, abs=1e-12)


def test_cpp_policy_v3_per_trade_fill_prob_controls_bid_fills():
    n = 30
    ts = np.arange(n, dtype=np.int64) * 1_000
    price = np.where((np.arange(n) % 2) == 0, 100.0, 99.7).astype(np.float64)
    qty = np.full(n, 0.01, dtype=np.float64)
    is_buyer_maker = np.ones(n, dtype=np.uint8)
    params = _make_params()
    params.queue_base = 0.0
    params.queue_decay = 0.0

    baseline = _simulate_policy_ext(ts, price, qty, is_buyer_maker, params)
    gated = _simulate_policy_ext(
        ts,
        price,
        qty,
        is_buyer_maker,
        params,
        queue_base_by_trade=np.zeros(n, dtype=np.float64),
        queue_decay_by_trade=np.zeros(n, dtype=np.float64),
        buy_fill_prob_by_trade=np.zeros(n, dtype=np.float64),
        sell_fill_prob_by_trade=np.ones(n, dtype=np.float64),
    )

    assert baseline.summary.fills_bid > 0
    assert gated.summary.fills_bid == 0


def test_cpp_policy_latency_jitter_is_seeded_and_supported():
    n = 40
    ts = np.arange(n, dtype=np.int64) * 1_000
    price = np.full(n, 100.0, dtype=np.float64)
    qty = np.zeros(n, dtype=np.float64)
    is_buyer_maker = np.zeros(n, dtype=np.uint8)
    params = _make_params()
    params.new_order_latency_ms = 2
    params.cancel_order_latency_ms = 2
    params.latency_jitter_ms = 2
    params.latency_seed = 123
    params.trace_quotes_max = 30

    first = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)
    second = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert first.summary.n_requotes == second.summary.n_requotes
    assert first.summary.pending_cancel_fills == second.summary.pending_cancel_fills
    assert [row.activate_ts for row in first.quote_trace] == [row.activate_ts for row in second.quote_trace]
    latencies = [row.activate_ts - row.submit_ts for row in first.quote_trace]
    assert latencies
    assert all(0 <= value <= 4 for value in latencies)


@pytest.mark.parametrize("operation", [1, 2, 3, 4])
@pytest.mark.parametrize("side,is_buy", [("BUY", True), ("SELL", False)])
def test_python_cpp_keyed_latency_sampler_is_identical(operation, side, is_buy):
    samples = np.asarray([1.25, 4.75, 9.0, 17.5], dtype=np.float64)
    kwargs = {
        "base_ms": 3,
        "jitter_ms": 5,
        "seed": 20260718,
        "event_ts_ms": 1_768_000_123_456,
        "operation": operation,
        "order_ts_ms": 1_768_000_120_000,
        "stress_enabled": True,
        "stress_spike_probability": 0.25,
        "stress_spike_multiplier": 7.0,
    }

    python_value = bt.deterministic_latency_ms(
        samples_ms=samples,
        side=side,
        **kwargs,
    )
    cpp_value = narrowgate_cpp.sample_keyed_latency_ms(
        kwargs["base_ms"],
        kwargs["jitter_ms"],
        samples.tolist(),
        kwargs["seed"],
        kwargs["event_ts_ms"],
        is_buy,
        kwargs["operation"],
        kwargs["order_ts_ms"],
        kwargs["stress_enabled"],
        kwargs["stress_spike_probability"],
        kwargs["stress_spike_multiplier"],
    )

    assert cpp_value == python_value


@pytest.mark.parametrize("operation", [1, 2])
@pytest.mark.parametrize(
    "seed,event_ts_ms,action_identity",
    [
        (12345, 0, 0),
        (20260725, 1_768_000_123_456, 17),
        (-7, 99_999, 4_294_967_311),
    ],
)
def test_python_cpp_keyed_random_passive_draw_is_identical(
    operation,
    seed,
    event_ts_ms,
    action_identity,
):
    python_value = bt.deterministic_random_passive_unit(
        seed=seed,
        event_ts_ms=event_ts_ms,
        action_identity=action_identity,
        operation=operation,
    )
    cpp_value = narrowgate_cpp.sample_keyed_random_passive_unit(
        seed,
        event_ts_ms,
        action_identity,
        operation,
    )

    assert cpp_value == python_value


def test_python_cpp_random_passive_uses_identical_action_path():
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 20_001, 500, dtype=np.int64)
    prices = np.rint(
        (100.0 + np.sin(np.arange(ts.size, dtype=np.float64) / 4.0) * 0.3) / 0.1
    ) * 0.1
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": prices,
            "quantity": np.zeros(ts.size, dtype=np.float64),
            "is_buyer_maker": (np.arange(ts.size) % 2).astype(np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 20.0,
        "spread_cap_mode": "compress",
        "ml_enabled": False,
        "max_exec_book_age_s": 0.0,
        "random_passive_enabled": True,
        "random_passive_seed": 20260725,
        "random_passive_side_mirror_prob": 0.5,
        "random_passive_timing_jitter_fraction": 0.35,
        "random_passive_preserve_inventory_skew": True,
        "trace_quotes_max": 200,
        "collect_curves": False,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
    )

    for field in (
        "n_requotes",
        "random_passive_mirror_eligible_count",
        "random_passive_mirror_count",
        "random_passive_timing_jitter_count",
    ):
        assert cpp[field] == py[field], field
    py_path = [
        (row["quote_ts"], row["side"], row["random_passive_mirrored"])
        for row in py["_quote_trace"]
    ]
    cpp_path = [
        (row["quote_ts"], row["side"], row["random_passive_mirrored"])
        for row in cpp["_quote_trace"]
    ]
    assert cpp_path == py_path


def test_python_cpp_random_passive_mirror_preserves_raw_tick_rounding():
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 5_001, 500, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.full(ts.size, 61_679.95),
            "quantity": np.zeros(ts.size, dtype=np.float64),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    bbo = bt.HistoricalBBOData(
        ts_ms=ts,
        best_bid=np.full(ts.size, 61_679.9),
        best_ask=np.full(ts.size, 61_680.0),
        bid_qty=np.ones(ts.size, dtype=np.float64),
        ask_qty=np.ones(ts.size, dtype=np.float64),
    )
    params = {
        "gamma": 0.05,
        "kappa": 0.073,
        "p3_kappa_eff_override": 0.0674,
        "maker_fee": 0.0,
        "max_inventory": 0.026,
        "order_size": 0.001,
        "initial_inventory": 0.001,
        "initial_entry_price": 61_679.95,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 500,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 100.0,
        "spread_cap_mode": "compress",
        "ml_enabled": False,
        "max_exec_book_age_s": 5.0,
        "random_passive_enabled": True,
        "random_passive_seed": 20260725,
        "random_passive_side_mirror_prob": 1.0,
        "random_passive_timing_jitter_fraction": 0.0,
        "random_passive_preserve_inventory_skew": False,
        "trace_quotes_max": 100,
        "collect_curves": False,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
        bbo_data=bbo,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
        bbo_data=bbo,
    )

    py_lifecycle = [
        (
            row["order_id"],
            row["side"],
            row["submit_ts"],
            row["activate_ts"],
            row["price"],
            row["outcome"],
            row["outcome_ts"],
            row["cancel_reason"],
            row["fill_qty"],
        )
        for row in py["_quote_trace"]
    ]
    cpp_lifecycle = [
        (
            row["order_id"],
            row["side"],
            row["submit_ts"],
            row["activate_ts"],
            row["price"],
            row["outcome"],
            row["outcome_ts"],
            row["cancel_reason"],
            row["fill_qty"],
        )
        for row in cpp["_quote_trace"]
    ]
    assert cpp_lifecycle == py_lifecycle
    first_buy = next(row for row in py["_quote_trace"] if row["side"] == "BUY")
    assert first_buy["random_passive_mirrored"] is True
    assert first_buy["price"] == pytest.approx(61_656.0, abs=1e-12)


def test_python_cpp_l2_path_metrics_use_same_wall_clock_frames():
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 4_000, 1_000, dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.asarray([100.0, 100.1, 100.1, 100.2]),
            "quantity": np.zeros(ts.size, dtype=np.float64),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    bid_px = np.asarray(
        [
            [99.9, 99.8],
            [100.0, 99.9],
            [100.0, 99.9],
            [100.1, 100.0],
        ],
        dtype=np.float64,
    )
    ask_px = np.asarray(
        [
            [100.1, 100.2],
            [100.2, 100.3],
            [100.2, 100.3],
            [100.3, 100.4],
        ],
        dtype=np.float64,
    )
    quantities = np.asarray(
        [
            [5.0, 5.0],
            [7.5, 7.5],
            [3.75, 3.75],
            [5.0, 5.0],
        ],
        dtype=np.float64,
    )
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=bid_px,
        bid_qty=quantities,
        ask_px=ask_px,
        ask_qty=quantities,
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 20.0,
        "spread_cap_mode": "compress",
        "max_exec_book_age_s": 5.0,
        "ml_enabled": False,
        "l2_refill_cancel_lookback_s": 2.0,
        "l2_refill_cancel_near_levels": 2,
        "l2_policy_depth_levels": 2,
        "trace_quotes_max": 100,
        "collect_curves": False,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
        l2_data=l2,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
        l2_data=l2,
    )

    def buy_row(result, quote_ts):
        return next(
            row
            for row in result["_quote_trace"]
            if row["side"] == "BUY" and row["quote_ts"] == quote_ts
        )

    py_row = buy_row(py, 2_000)
    cpp_row = buy_row(cpp, 2_000)
    for field in (
        "l2_quote_flip_rate",
        "l2_book_refresh_ratio",
        "l2_book_cancel_ratio",
        "l2_near_depth_total",
    ):
        assert cpp_row[field] == pytest.approx(py_row[field], abs=1e-15), field
    assert py_row["l2_quote_flip_rate"] == pytest.approx(1.0 / 3.0)
    assert py_row["l2_book_refresh_ratio"] == pytest.approx(1.0 / 6.0)
    assert py_row["l2_book_cancel_ratio"] == pytest.approx(1.0 / 6.0)
    assert py_row["l2_near_depth_total"] == pytest.approx(15.0)


def test_python_cpp_common_policy_prefers_wall_clock_l2_thin_depth():
    bt.configure_symbol("BTCUSDC")
    ts = np.arange(0, 7_000, 1_000, dtype=np.int64)
    mid = np.asarray([100.0, 100.0, 101.0, 101.0, 101.0, 101.0, 101.0])
    bid_px = np.column_stack([mid - offset for offset in (0.1, 0.2, 0.3, 0.4)])
    ask_px = np.column_stack([mid + offset for offset in (0.1, 0.2, 0.3, 0.4)])
    quantities = np.tile(
        np.asarray([0.5, 20.0, 20.0, 20.0], dtype=np.float64),
        (ts.size, 1),
    )
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=bid_px,
        bid_qty=quantities,
        ask_px=ask_px,
        ask_qty=quantities,
    )
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.asarray([100.0, 100.8, 101.0, 101.0, 101.0, 101.0, 101.0]),
            "quantity": np.asarray([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.asarray(
                [False, True, False, False, False, False, False],
                dtype=np.bool_,
            ),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 100.0,
        "spread_cap_mode": "compress",
        "max_exec_book_age_s": 5.0,
        "ml_enabled": False,
        "markout_horizon_s": 1.0,
        "markout_ema_span_fills": 1.0,
        "markout_spread_scale": 0.2,
        "l2_refill_cancel_near_levels": 1,
        "l2_policy_depth_levels": 4,
        "thin_depth_threshold": 10.0,
        "trace_quotes_max": 100,
        "trace_fills_max": 100,
        "collect_curves": False,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
        l2_data=l2,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
        l2_data=l2,
    )

    def rows_at(result, quote_ts):
        return {
            row["side"]: row
            for row in result["_quote_trace"]
            if row["quote_ts"] == quote_ts
        }

    py_rows = rows_at(py, 2_000)
    cpp_rows = rows_at(cpp, 2_000)
    assert set(py_rows) == {"BUY", "SELL"}
    assert set(cpp_rows) == set(py_rows)
    for side in ("BUY", "SELL"):
        assert py_rows[side]["near_depth_total"] == pytest.approx(121.0)
        assert py_rows[side]["l2_near_depth_total"] == pytest.approx(1.0)
        assert py_rows[side]["mo_ema_ask"] == pytest.approx(-0.5)
        assert cpp_rows[side]["price"] == pytest.approx(
            py_rows[side]["price"],
            abs=1e-12,
        )
        assert cpp_rows[side]["final_price"] == pytest.approx(
            py_rows[side]["final_price"],
            abs=1e-12,
        )
    assert py_rows["BUY"]["price"] == pytest.approx(100.5)
    assert py_rows["SELL"]["price"] == pytest.approx(101.5)
    assert cpp["fills_total"] == py["fills_total"] == 1
    assert cpp["pnl"] == pytest.approx(py["pnl"], abs=1e-12)


def test_python_cpp_queue_regime_rank_is_sampled_at_order_activation():
    bt.configure_symbol("BTCUSDC")
    ts = np.asarray([0, 100, 900, 1_000, 1_005, 2_000, 3_000], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.asarray([100.0, 110.0, 105.0, 105.0, 101.0, 101.0, 101.0]),
            "quantity": np.zeros(ts.size, dtype=np.float64),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    groups = [
        {
            "key": f"{side}|0|{rank_bin}|*",
            "queue_mult": multiplier,
        }
        for side in ("BUY", "SELL")
        for rank_bin, multiplier in ((0, 0.5), (1, 1.5))
    ]
    calibration = {
        "days": {
            "1970-01-01": {
                "regime": {
                    "distance_edges": [],
                    "rank_edges": [0.25],
                    "groups": groups,
                }
            }
        }
    }
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "queue_base": 1.0,
        "queue_decay": 0.0,
        "queue_ahead_base_mult": 1.0,
        "queue_ahead_buy_exposure_mult": 1.0,
        "queue_ahead_buy_reducing_mult": 1.0,
        "queue_ahead_sell_exposure_mult": 1.0,
        "queue_ahead_sell_reducing_mult": 1.0,
        "queue_regime_calibration_enabled": True,
        "_queue_calibration": calibration,
        "maker_fill_prob": 1.0,
        "new_order_latency_ms": 10,
        "cancel_order_latency_ms": 0,
        "latency_jitter_ms": 0,
        "latency_sampler_version": "keyed_splitmix64_v1",
        "use_bar_pricing": True,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 100.0,
        "spread_cap_mode": "compress",
        "max_exec_book_age_s": 0.0,
        "ml_enabled": False,
        "trace_quotes_max": 100,
        "collect_curves": False,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
    )

    def activated_rows(result):
        return sorted(
            (
                row["side"],
                row["activate_ts"],
                row["queue_init"],
            )
            for row in result["_quote_trace"]
            if row["submit_ts"] == 1_000
        )

    py_rows = activated_rows(py)
    cpp_rows = activated_rows(cpp)
    assert [(side, activate_ts) for side, activate_ts, _ in py_rows] == [
        ("BUY", 1_010),
        ("SELL", 1_010),
    ]
    assert [(side, activate_ts) for side, activate_ts, _ in cpp_rows] == [
        (side, activate_ts) for side, activate_ts, _ in py_rows
    ]
    assert len(py_rows) == len(cpp_rows)
    for py_row, cpp_row in zip(py_rows, cpp_rows):  # noqa: B905
        assert py_row[2] == pytest.approx(0.5)
        assert cpp_row[2] == pytest.approx(py_row[2], abs=1e-15)


def test_python_cpp_fallback_queue_distance_is_sampled_at_order_activation():
    bt.configure_symbol("BTCUSDC")
    ts = np.asarray([0, 1_000, 1_050, 2_000, 3_000], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.asarray([100.0, 100.0, 100.2, 100.2, 100.2]),
            "quantity": np.zeros(ts.size, dtype=np.float64),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    bbo = bt.HistoricalBBOData(
        ts_ms=np.asarray([0, 1_000, 1_050, 2_000, 3_000], dtype=np.int64),
        best_bid=np.asarray([99.9, 99.9, 100.1, 100.1, 100.1]),
        best_ask=np.asarray([100.1, 100.1, 100.3, 100.3, 100.3]),
        bid_qty=np.ones(5, dtype=np.float64),
        ask_qty=np.ones(5, dtype=np.float64),
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "queue_base": 5.0,
        "queue_decay": 0.1,
        "queue_ahead_base_mult": 1.0,
        "queue_ahead_buy_exposure_mult": 1.0,
        "queue_ahead_buy_reducing_mult": 1.0,
        "queue_ahead_sell_exposure_mult": 1.0,
        "queue_ahead_sell_reducing_mult": 1.0,
        "maker_fill_prob": 1.0,
        "new_order_latency_ms": 100,
        "cancel_order_latency_ms": 0,
        "latency_jitter_ms": 0,
        "latency_sampler_version": "keyed_splitmix64_v1",
        "use_bar_pricing": False,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 100.0,
        "spread_cap_mode": "compress",
        "max_exec_book_age_s": 5.0,
        "ml_enabled": False,
        "trace_quotes_max": 100,
        "collect_curves": False,
    }
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
        bbo_data=bbo,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
        bbo_data=bbo,
    )

    py_rows = sorted(
        (row["side"], row["price"], row["quote_ts"], row["queue_init"])
        for row in py["_quote_trace"]
        if row["submit_ts"] == 1_000
    )
    cpp_rows = sorted(
        (row["side"], row["price"], row["quote_ts"], row["queue_init"])
        for row in cpp["_quote_trace"]
        if row["submit_ts"] == 1_000
    )
    assert [(side, price, quote_ts) for side, price, quote_ts, _ in cpp_rows] == [
        (side, price, quote_ts) for side, price, quote_ts, _ in py_rows
    ]
    assert {quote_ts for _, _, quote_ts, _ in py_rows} == {1_100}
    assert len(py_rows) == len(cpp_rows)
    for py_row, cpp_row in zip(py_rows, cpp_rows):  # noqa: B905
        assert cpp_row[3] == pytest.approx(py_row[3], abs=1e-15)


def test_python_cpp_adverse_pause_preserves_reducing_side():
    bt.configure_symbol("BTCUSDC")
    ts = np.asarray([0, 1_000, 2_000, 3_000], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.full(ts.size, 100.0),
            "quantity": np.zeros(ts.size, dtype=np.float64),
            "is_buyer_maker": np.zeros(ts.size, dtype=np.uint8),
            "_is_execution_trade": np.zeros(ts.size, dtype=np.bool_),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "initial_inventory": -0.001,
        "initial_entry_price": 100.0,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "queue_base": 1.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 100.0,
        "spread_cap_mode": "compress",
        "max_exec_book_age_s": 0.0,
        "ml_enabled": True,
        "adverse_guard_enabled": True,
        "adverse_toxicity_threshold": 0.7,
        "adverse_pause": True,
        "trace_quotes_max": 100,
        "collect_curves": False,
    }
    zero = np.asarray([0.0], dtype=np.float64)
    ml_data = (
        np.asarray([0], dtype=np.int64),
        np.asarray([0.5], dtype=np.float64),
        zero,
        zero,
        np.asarray([0.9], dtype=np.float64),
        np.asarray([0.1], dtype=np.float64),
        *([zero] * len(bt.XMARKET_REPLAY_FEATURE_COLUMNS)),
        {},
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    py = bt._simulate_tick_with_engine(
        "python",
        trades,
        empty_i64,
        empty_f64,
        params,
        ml_data=ml_data,
    )
    cpp = bt._simulate_tick_with_engine(
        "cpp",
        trades,
        empty_i64,
        empty_f64,
        params,
        ml_data=ml_data,
    )

    def submitted_sides(result):
        return {
            row["side"]
            for row in result["_quote_trace"]
            if row["submit_ts"] == 1_000
        }

    assert "BUY" in submitted_sides(py)
    assert submitted_sides(cpp) == submitted_sides(py)
    py_buy = next(
        row
        for row in py["_quote_trace"]
        if row["submit_ts"] == 1_000 and row["side"] == "BUY"
    )
    cpp_buy = next(
        row
        for row in cpp["_quote_trace"]
        if row["submit_ts"] == 1_000 and row["side"] == "BUY"
    )
    assert py_buy["side_adverse_pause"] is False
    assert cpp_buy["price"] == pytest.approx(py_buy["price"], abs=1e-12)


def test_cpp_executable_random_passive_is_seeded_and_runs_full_replay():
    n = 80
    ts = np.arange(n, dtype=np.int64) * 1_000
    price = np.rint(
        (100.0 + np.sin(np.arange(n, dtype=np.float64) / 4.0) * 0.4) / 0.1
    ) * 0.1
    qty = np.full(n, 0.01, dtype=np.float64)
    is_buyer_maker = (np.arange(n) % 2).astype(np.uint8)
    params = _make_params()
    params.random_passive_enabled = True
    params.random_passive_seed = 12345
    params.random_passive_side_mirror_prob = 0.5
    params.random_passive_timing_jitter_fraction = 0.35

    first = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)
    second = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)

    assert first.summary.random_passive_mirror_eligible_count > 0
    assert first.summary.random_passive_mirror_count > 0
    assert first.summary.random_passive_timing_jitter_count > 0
    assert first.summary.random_passive_mirror_count == second.summary.random_passive_mirror_count
    assert first.summary.fills_total == second.summary.fills_total
    assert first.summary.pnl == pytest.approx(second.summary.pnl)
    assert first.summary.terminal_fee_drag == pytest.approx(0.0)
    assert first.summary.pnl == pytest.approx(first.summary.mtm_before_terminal_fee)
    assert first.summary.terminal_liquidation_fee_estimate >= 0.0


def test_cpp_policy_trace_reasons_include_fill_requote_and_open_end():
    ts = np.arange(5, dtype=np.int64) * 1_000
    price = np.array([100.0, 99.9, 100.0, 100.0, 100.0], dtype=np.float64)
    qty = np.full(ts.size, 0.01, dtype=np.float64)
    is_buyer_maker = np.array([0, 1, 0, 0, 0], dtype=np.uint8)
    params = _make_params()
    params.trace_quotes_max = 50
    params.trace_fills_max = 50

    result = narrowgate_cpp.simulate_tick_arrays(ts, price, qty, is_buyer_maker, params)
    reasons = {(row.outcome, row.cancel_reason) for row in result.quote_trace}

    assert ("fill", "fill") in reasons
    assert ("cancel", "requote_replace") in reasons
    assert ("open_end", "end_of_window") in reasons
    assert result.fill_trace
    assert [fill.fill_sequence for fill in result.fill_trace] == list(
        range(len(result.fill_trace))
    )
    for fill in result.fill_trace:
        expected_after = fill.inventory_before_fill + (fill.fill_qty if fill.side == "BUY" else -fill.fill_qty)
        assert fill.inventory_after_fill == pytest.approx(expected_after, abs=1e-12)
        assert isinstance(fill.markout_20s, float)
        assert fill.ev_20s == pytest.approx(fill.markout_20s, abs=1e-12)


def test_cpp_policy_local_extreme_and_fragile_cancel():
    ts = np.arange(8, dtype=np.int64) * 1_000
    price = np.array([100.0, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6, 100.7], dtype=np.float64)
    qty = np.full(ts.size, 0.0001, dtype=np.float64)
    is_buyer_maker = np.zeros(ts.size, dtype=np.uint8)
    l2_ts = ts.copy()
    l2_bid_px = np.full((ts.size, 1), 99.9, dtype=np.float64)
    l2_bid_qty = np.full((ts.size, 1), 0.1, dtype=np.float64)
    l2_ask_px = np.full((ts.size, 1), 100.1, dtype=np.float64)
    l2_ask_qty = np.full((ts.size, 1), 0.1, dtype=np.float64)
    params = _make_params()
    params.use_bar_pricing = False
    params.requote_interval_s = 1.0
    params.rq_min_s = 1.0
    params.rq_max_s = 1.0
    params.local_extreme_guard_enabled = True
    params.local_extreme_require_thin_depth = False
    params.local_extreme_rank_threshold = 0.7
    params.local_extreme_spread_mult = 1.2
    params.fragile_order_ttl_s = 0.5
    params.local_extreme_thin_depth_threshold = 1.0
    params.trace_quotes_max = 20

    result = _simulate_policy_ext(
        ts,
        price,
        qty,
        is_buyer_maker,
        params,
        l2_ts=l2_ts,
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
    )

    assert result.summary.local_extreme_guard_count > 0
    assert result.summary.fragile_ttl_cancel_count > 0
    assert any(row.cancel_reason == "fragile_ttl" for row in result.quote_trace)
