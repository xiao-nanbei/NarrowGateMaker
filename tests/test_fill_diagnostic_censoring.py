import json
import math

import numpy as np
import pandas as pd
import pytest

from models.replay.fill_diagnostics import fill_markout_diagnostics, diagnostic_counts


def diagnostic(times, prices, *, side="BUY", fill=9900, end=None):
    return fill_markout_diagnostics(np.array(times), np.array(prices),
        fill_ts_ms=fill, quote_px=100, side=side, fee_rate=.001,
        outcome_end_ts_ms=end)


def test_tail_never_falls_back_or_becomes_safe():
    row = diagnostic([9900, 10000], [100, 101])
    assert math.isnan(row["markout_1s"]) and math.isnan(row["ev_1s"])
    assert row["toxic_1s"] is None
    assert row["markout_1s_requested_end_ms"] == 10900
    assert row["markout_1s_actual_end_ms"] is None
    assert row["markout_1s_censor_reason"] == "no_future_trade"
    assert diagnostic_counts([row])["1s"]["censored"] == 1


@pytest.mark.parametrize("side,expected", [("BUY", 1), ("SELL", -1)])
def test_exact_and_delayed_price_method_are_distinguished(side, expected):
    exact = diagnostic([9900, 10900], [100, 101], side=side)
    assert exact["markout_1s"] == expected
    assert exact["ev_1s"] == expected - .1
    assert exact["toxic_1s"] == (expected < 0)
    assert exact["markout_1s_fixed_horizon_valid"]
    delayed = diagnostic([9900, 100000], [100, 101], side=side)
    assert delayed["markout_1s"] == expected  # unchanged first-after price rule
    assert delayed["markout_1s_status"] == "delayed"
    assert delayed["markout_1s_gap_ms"] == 89100
    assert not delayed["markout_1s_fixed_horizon_valid"]


@pytest.mark.parametrize("end,reason", [(10000, "requested_end_after_allowed_boundary"),
                                      (11000, "actual_end_after_allowed_boundary")])
def test_allowed_outcome_boundary(end, reason):
    row = diagnostic([9900, 12000], [100, 101], end=end)
    assert math.isnan(row["markout_1s"])
    assert row["markout_1s_censor_reason"] == reason
    assert row["markout_1s_actual_end_ms"] == 12000


def test_roundtrip_preserves_unknown_and_denominators(tmp_path):
    rows = [diagnostic([9900, 10000], [100, 101]),
            diagnostic([9900, 10900], [100, 101])]
    frame = pd.DataFrame(rows)
    p = tmp_path / "diagnostics.parquet"
    frame.to_parquet(p)
    restored = pd.read_parquet(p)
    assert pd.isna(restored.loc[0, "toxic_1s"])
    assert pd.isna(restored.loc[0, "ev_1s"])
    records = json.loads(restored.to_json(orient="records"))
    assert records[0]["toxic_1s"] is None
    assert diagnostic_counts(records)["1s"] == dict(total=2, exact=1, delayed=0,
                                                    censored=1, missing=0, unclassified=0)


@pytest.mark.parametrize("mode", ["ordinary", "async", "compute", "timeout", "emergency", "close_replace"])
def test_real_replay_trace_diagnostics_do_not_change_economic_path(mode):
    from models.backtest_tick import simulate_tick
    from tests.test_tick_runtime_checkpoint import scenario, assert_same
    args, kwargs = scenario(mode)
    on = simulate_tick(*args, **kwargs)
    if mode in {"ordinary", "emergency"}:
        assert on["_fill_trace"]
    off_args = (*args[:3], {**args[3], "trace_fills_max": 0})
    off = simulate_tick(*off_args, **kwargs)
    assert all(r["markout_30s_status"] == "censored" for r in on["_fill_trace"])
    assert all(r["toxic_30s"] is None for r in on["_fill_trace"])
    for result in (on, off):
        result.pop("_fill_trace")
        result.pop("_fill_diagnostic_counts")
        result.pop("_trace_coverage")
    assert_same(on, off)


@pytest.mark.parametrize("mode", ["ordinary", "async", "compute", "timeout", "emergency", "close_replace"])
def test_trace_caps_preserve_attempt_denominators_and_economics(mode):
    from models.backtest_tick import simulate_tick
    from tests.test_tick_runtime_checkpoint import scenario, assert_same
    args, kwargs = scenario(mode)
    full = simulate_tick(*args, **kwargs)
    capped = simulate_tick(*args[:3], {**args[3], "trace_fills_max": 1,
                                     "trace_quotes_max": 1, "trace_decisions_max": 1}, **kwargs)
    for key in ("fills", "order_outcomes", "routed_decisions"):
        counts = capped["_trace_coverage"][key]
        assert counts["attempted"] == full["_trace_coverage"][key]["attempted"]
        assert counts["recorded"] <= 1
        assert counts["unrecorded"] == counts["attempted"] - counts["recorded"]
    for result in (full, capped):
        for key in ("_fill_trace", "_quote_trace", "_decision_trace", "_trace_coverage",
                    "_fill_diagnostic_counts"):
            result.pop(key, None)
    assert_same(full, capped)
