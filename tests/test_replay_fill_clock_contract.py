"""Matching clocks and strategy-visible notification clocks are distinct."""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from models import backtest_tick as bt
from tests.test_python_planned_maintenance_replay import _inputs, _params
from tests.test_python_planned_maintenance_replay import _ioc_inventory_path, _run


def test_cross_order_notification_reordering_preserves_each_match_clock(monkeypatch):
    _, bbo = _inputs()
    params = _params()
    params.update(
        requote_interval=100.0, rq_min=100.0, rq_max=100.0,
        _private_fill_visibility_latency_samples_ms=[20.0],
        initial_live_state={"active_orders": [
            {"side": "BUY", "price": 98.0, "quantity": 0.001, "status": "OPEN"},
            {"side": "SELL", "price": 102.0, "quantity": 0.001, "status": "OPEN"},
        ]},
    )
    trades = pd.DataFrame({
        "transact_time": [0, 100, 105, 200, 4000],
        "price": [100.0, 96.0, 104.0, 100.0, 100.0],
        "quantity": [0.0, 10.0, 10.0, 0.0, 0.0],
        "is_buyer_maker": [1, 1, 0, 1, 1],
    })
    sample = bt.deterministic_latency_ms

    def latency(**kwargs):
        if kwargs["operation"] == bt._LATENCY_PRIVATE_FILL:
            return 20 if kwargs["side"] == "BUY" else 2
        return sample(**kwargs)

    monkeypatch.setattr(bt, "deterministic_latency_ms", latency)
    result = bt.simulate_tick(trades, np.array([0]), np.array([1.0]), params, bbo_data=bbo)
    rows = result["_fill_trace"]
    # These are notification records: sorting them would falsify local inventory.
    assert [r["side"] for r in rows] == ["SELL", "BUY"]
    assert [r["fill_ts"] for r in rows] == [105, 100]
    assert [r["fill_clock_context"]["processed_ts_ms"] for r in rows] == [107, 120]
    assert [r["inventory_before_fill"] for r in rows] == [0.0, -0.001]
    assert [r["inventory_after_fill"] for r in rows] == [-0.001, 0.0]
    for row in rows:
        clock = row["fill_clock_context"]
        assert row["fill_ts"] == clock["match_ts_ms"] == clock["indexed_trade_ts_ms"]
        assert clock["match_ts_ms"] <= clock["visible_ts_ms"] == clock["processed_ts_ms"]
    assert result["private_fill_exchange_match_count"] == 2
    assert result["private_fill_visible_count"] == 2
    facts = result["_economic_fill_trace"]
    assert [r["match_sequence"] for r in facts] == [0, 1]
    assert [r["fill_ts"] for r in facts] == [100, 105]
    assert [r["side"] for r in facts] == ["BUY", "SELL"]
    assert [r["economic_match_sequence"] for r in rows] == [1, 0]
    # Matching inventory is required by settlement even without serial REST.
    assert result["exchange_inventory_at_window_end"] == 0.0
    assert result["economic_match_inventory"] == 0.0


@pytest.mark.parametrize("initial_sign", [-1, 1])
def test_mixed_fill_clock_context_parquet_roundtrip(tmp_path, initial_sign):
    passive = _run(
        crossing_fill_ts_ms=1_100,
        param_overrides={"_private_fill_visibility_latency_samples_ms": [20.0]},
    )["_fill_trace"]
    ioc = _ioc_inventory_path(initial_sign=initial_sign)["_fill_trace"]
    rows = passive + ioc
    clocks = [row["fill_clock_context"] for row in rows]
    assert {c["path"] for c in clocks} == {"passive", "ioc"}
    assert all(c.keys() == clocks[0].keys() for c in clocks)
    for clock in clocks:
        if clock["path"] == "passive":
            assert clock["match_processed_ts_ms"] is None
            assert clock["exchange_reserved"] is True
        else:
            assert type(clock["match_processed_ts_ms"]) is int
            assert clock["exchange_reserved"] is None
    frame = pd.DataFrame(rows)
    path = tmp_path / "fills.parquet"
    frame.to_parquet(path, index=False)
    assert pd.read_parquet(path).equals(frame)
    # Nullable timestamps remain integer-valued on disk, not floating clocks.
    schema = pq.read_schema(path).field("fill_clock_context").type
    for name in ("match_ts_ms", "visible_ts_ms", "processed_ts_ms",
                 "match_processed_ts_ms", "indexed_trade_ts_ms", "outer_loop_ts_ms"):
        assert schema.field(name).type == pa.int64()
