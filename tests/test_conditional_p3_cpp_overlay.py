from __future__ import annotations

import numpy as np
import pytest

from strategy.quote_core import SPREAD_CAP_COMPRESS


cpp = pytest.importorskip("narrowgate_cpp")


def _params():
    params = cpp.TickReplayParams()
    params.order_size = 0.001
    params.max_inventory = 0.01
    params.requote_interval_s = 1.0
    params.maker_fill_prob = 1.0
    params.initial_sigma_sq = 1.0
    params.trace_quotes_max = 100
    params.quote.gamma = 0.01
    params.quote.kappa = 1.0
    params.quote.p3_delta_star = 2.0
    params.quote.p3_kappa_eff = 10.0
    params.quote.regime_enabled = True
    params.quote.tick_size = 0.1
    params.quote.lot_size = 0.001
    params.quote.order_size = params.order_size
    params.quote.max_inventory = params.max_inventory
    params.quote.max_spread_bps = 100.0
    params.quote.spread_cap_mode = SPREAD_CAP_COMPRESS
    params.quote.dynamic_cap_enabled = False
    return params


def _base_args():
    ts = np.arange(0, 5_000, 1_000, dtype=np.int64)
    prices = np.full(ts.size, 100.0, dtype=np.float64)
    quantities = np.zeros(ts.size, dtype=np.float64)
    makers = np.zeros(ts.size, dtype=np.uint8)
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)
    empty_matrix = np.empty((0, 0), dtype=np.float64)
    return (
        ts,
        prices,
        quantities,
        makers,
        empty_i64,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_i64,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_f64,
        empty_f64,
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


def _prices(result):
    return [(int(row.quote_ts), str(row.side), float(row.final_price)) for row in result.quote_trace]


def test_constant_conditional_p3_overlay_is_noop_against_v3_abi():
    params = _params()
    args = _base_args()
    control = cpp.simulate_tick_arrays_ext_policy_v3(*args, params)
    candidate = cpp.simulate_tick_arrays_ext_policy_v4(
        *args,
        np.asarray([0], dtype=np.int64),
        np.asarray([2.0], dtype=np.float64),
        np.asarray([10.0], dtype=np.float64),
        params,
    )

    assert candidate.summary.pnl == control.summary.pnl
    assert candidate.summary.avg_spread == control.summary.avg_spread
    assert candidate.summary.n_requotes == control.summary.n_requotes
    assert _prices(candidate) == _prices(control)


def test_conditional_p3_overlay_changes_quote_floor_after_ready_time():
    params = _params()
    result = cpp.simulate_tick_arrays_ext_policy_v4(
        *_base_args(),
        np.asarray([2_000], dtype=np.int64),
        np.asarray([4.0], dtype=np.float64),
        np.asarray([10.0], dtype=np.float64),
        params,
    )
    rows = sorted(result.quote_trace, key=lambda row: (row.quote_ts, str(row.side)))
    before = [row for row in rows if row.quote_ts < 2_000]
    after = [row for row in rows if row.quote_ts >= 2_000]

    assert before and after
    assert max(row.raw_half_spread for row in before) == pytest.approx(2.0)
    assert min(row.raw_half_spread for row in after) == pytest.approx(4.0)


def test_conditional_p3_overlay_rejects_non_increasing_timestamps():
    with pytest.raises(ValueError, match="strictly increasing"):
        cpp.simulate_tick_arrays_ext_policy_v4(
            *_base_args(),
            np.asarray([0, 0], dtype=np.int64),
            np.asarray([2.0, 2.0], dtype=np.float64),
            np.asarray([10.0, 10.0], dtype=np.float64),
            _params(),
        )
