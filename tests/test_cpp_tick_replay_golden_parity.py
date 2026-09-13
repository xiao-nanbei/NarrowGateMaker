import os
from functools import lru_cache

import numpy as np
import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

from models import backtest_tick as bt


pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        os.environ.get("RUN_NARROWGATE_GOLDEN") != "1",
        reason="set RUN_NARROWGATE_GOLDEN=1 to run real-data C++ replay golden parity tests",
    ),
]


GOLDEN_WINDOWS = [
    pytest.param(
        "may_baseline",
        "2026-05-15 00:00",
        "2026-05-15 01:00",
        id="may_baseline_20260515_0000_1h",
    ),
    pytest.param(
        "may_high_activity",
        "2026-05-15 14:00",
        "2026-05-15 15:00",
        id="may_high_activity_20260515_1400_1h",
    ),
    pytest.param(
        "feb_low_activity",
        "2026-02-15 00:00",
        "2026-02-15 01:00",
        id="feb_low_activity_20260215_0000_1h",
    ),
    pytest.param(
        "jan_ab_day",
        "2026-01-15 00:00",
        "2026-01-15 01:00",
        id="jan_ab_day_20260115_0000_1h",
    ),
]


SUMMARY_FIELDS = [
    "pnl",
    "inventory_adjusted_pnl",
    "inventory_pnl",
    "fills_bid",
    "fills_ask",
    "fills_total",
    "final_inventory",
    "avg_markout",
    "markout_count",
    "abs_inventory_time_s",
    "signed_inventory_time_s",
    "sq_inventory_time_s",
    "notional_inventory_time_s",
    "n_requotes",
    "avg_spread",
    "avg_final_spread",
    "n_final_spread",
    "quote_spread_lt_100_rate",
    "quote_spread_lt_150_rate",
    "final_spread_lt_100_rate",
    "final_spread_lt_150_rate",
    "cap_hit_rate",
    "delta_cap_hit_rate",
    "final_cap_compress_rate",
    "sharpe",
    "max_drawdown",
]


@lru_cache(maxsize=1)
def _base_params():
    return bt.load_tick_base_params(
        symbol="BTCUSDC",
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=False,
        queue_base=None,
        queue_decay=None,
        queue_ahead_mode=None,
        queue_price_tolerance=None,
        min_historical_book_coverage=0.0,
        maker_fill_prob=None,
    )


@lru_cache(maxsize=8)
def _day_data(day: str):
    trades = bt.load_aggtrades(days=[day])
    bars = bt.load_1s_bars(days=[day])
    var_ts_ms, var_ssq = bt.build_rolling_variance(bars)
    _, var_ti = bt.build_trade_intensity(bars)
    _, var_retsq = bt.build_squared_returns(bars)
    return {
        "trades": trades,
        "var_ts_ms": var_ts_ms,
        "var_ssq": var_ssq,
        "var_ti": var_ti,
        "var_retsq": var_retsq,
        "bbo_data": bt.load_bbo_data(days=[day]),
        "l2_data": bt.load_l2_data(days=[day]),
    }


def _load_window(start: str, end: str):
    start_ms = bt._parse_time_filter_ms(start, "UTC")
    end_ms = bt._parse_time_filter_ms(end, "UTC")
    days = bt._days_for_time_window(start_ms, end_ms)
    assert len(days) == 1, "golden windows should stay within one UTC day"
    data = dict(_day_data(days[0]))
    data["trades"] = bt._filter_trades_by_time(data["trades"], start_ms, end_ms)
    data["ml_data"] = bt.load_ml_predictions(
        data["trades"],
        toxicity_horizon_s=_base_params().get("toxicity_horizon_s", 10),
    )
    return data


def _run(engine: str, window: dict):
    params = dict(_base_params())
    params["trace_quotes_max"] = 100_000
    params["trace_fills_max"] = 10_000
    return bt._simulate_tick_with_engine(
        engine,
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        params,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
    )


def _assert_summary_close(py: dict, cpp: dict):
    for field in SUMMARY_FIELDS:
        py_value = py[field]
        cpp_value = cpp[field]
        if isinstance(py_value, (int, np.integer)):
            assert cpp_value == py_value, field
        else:
            assert float(cpp_value) == pytest.approx(float(py_value), abs=1e-8), field

    py_pnl = np.asarray(py["_pnl_ts"], dtype=np.float64)
    cpp_pnl = np.asarray(cpp["_pnl_ts"], dtype=np.float64)
    py_ts = np.asarray(py["_ts"], dtype=np.int64)
    cpp_ts = np.asarray(cpp["_ts"], dtype=np.int64)
    assert cpp_ts.tolist() == py_ts.tolist()
    assert cpp_pnl == pytest.approx(py_pnl, abs=1e-10)

    assert len(cpp["_fill_trace"]) == len(py["_fill_trace"])
    assert len(cpp["_quote_trace"]) == len(py["_quote_trace"])


@pytest.mark.parametrize(("name", "start", "end"), GOLDEN_WINDOWS)
def test_cpp_tick_replay_real_window_golden_parity(name, start, end):
    window = _load_window(start, end)
    assert len(window["trades"]) > 0, name

    py = _run("python", window)
    cpp = _run("cpp", window)

    _assert_summary_close(py, cpp)
