import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.daily_raw import sha256_file
from models import backtest_tick as replay
from models.tick_data_types import HistoricalBBOData, book_usable_mask


def test_independent_top_clock_never_refreshes_depth_and_masks_invalid(tmp_path, monkeypatch):
    day = "2025-08-29"
    root = tmp_path
    for name in ("bbo", "l2", "clock", "quality"):
        (root / name).mkdir()
    axis = np.array([100, 200, 300], dtype=np.int64)
    paths = {kind: root / kind / f"BTCUSDC-{kind}-{day}.parquet" for kind in ("bbo", "l2", "clock")}
    for kind in ("bbo", "l2"):
        pq.write_table(pa.table({"timestamp": axis}), paths[kind])
    clock = pa.table({"timestamp": axis, "last_observation_timestamp_us": [90000] * 3,
                      "observation_age_us": axis * 1000 - 90000,
                      "observation_kind": ["source_observed", "carried_forward", "carried_forward"],
                      "bbo_last_observation_timestamp_us": [90000, 190000, 190000],
                      "bbo_observation_age_us": [10000, 10000, 110000],
                      "bbo_observation_kind": ["source_observed", "source_observed", "invalid_top"],
                      "bbo_usable": [True, True, False]})
    pq.write_table(clock, paths["clock"])
    quality = root / "quality" / f"BTCUSDC-{day}.json"
    def bind():
        quality.write_text(json.dumps({"day": day, "symbol": "BTCUSDC", **{
            f"{kind}_output": {"sha256": sha256_file(path)} for kind, path in paths.items()}}))
    bind()
    monkeypatch.setattr(replay, "book_observation_inputs", lambda *a, **k: (quality, paths["clock"]))
    top, _, usable = replay._load_book_observations(paths["bbo"], axis, "bbo", return_usable=True)
    deep, _ = replay._load_book_observations(paths["l2"], axis, "l2")
    np.testing.assert_array_equal(top, [90000, 190000, 190000])
    np.testing.assert_array_equal(deep, [90000] * 3)
    np.testing.assert_array_equal(usable, [True, True, False])
    invalid = clock.set_column(clock.schema.get_field_index("bbo_usable"), "bbo_usable", pa.array([True] * 3))
    pq.write_table(invalid, paths["clock"])
    bind()
    with pytest.raises(ValueError, match="marked usable"):
        replay._load_book_observations(paths["bbo"], axis, "bbo")


def test_top_loader_preserves_explicit_invalidation_on_asof_axis(tmp_path, monkeypatch):
    day = "2025-08-29"
    path = tmp_path / f"BTCUSDC-bbo-{day}.parquet"
    axis = np.array([100, 200, 300, 400], dtype=np.int64)
    pq.write_table(pa.table({"timestamp": axis, "best_bid": [100., 101., 101., 102.],
                            "best_ask": [101., 102., 102., 103.]}), path)
    mask = np.array([True, True, False, True])
    monkeypatch.setattr(replay, "_collect_snapshot_files", lambda *a, **k: [path])
    monkeypatch.setattr(replay, "filter_paths_for_orderbook_quality", lambda paths, *a, **k: paths)
    monkeypatch.setattr(replay, "allowed_timestamp_mask", lambda ts, *a, **k: np.ones(len(ts), dtype=bool))
    monkeypatch.setattr(replay, "_load_book_observations", lambda *a, **k: (
        np.array([90_000, 190_000, 190_000, 390_000]), np.array([True, True, False, True]), mask))
    data = replay.load_bbo_data(days=[day])
    np.testing.assert_array_equal(data.ts_ms, axis)
    np.testing.assert_array_equal(data.usable, mask)
    idx = np.searchsorted(data.ts_ms, 350, side="right") - 1
    assert idx == 2 and not book_usable_mask(data)[idx]
    assert data.observation_ts_us[idx] == 190_000


def test_unknown_top_clock_remains_unknown_until_first_real_observation(tmp_path, monkeypatch):
    day = "2025-08-29"
    bbo = tmp_path / f"BTCUSDC-bbo-{day}.parquet"
    clock = tmp_path / "clock.parquet"
    quality = tmp_path / "quality.json"
    pq.write_table(pa.table({"timestamp": [100, 200]}), bbo)
    pq.write_table(pa.table({"timestamp": [100, 200],
        "bbo_last_observation_timestamp_us": pa.array([None, 190000], type=pa.int64()),
        "bbo_observation_age_us": pa.array([None, 10000], type=pa.int64()),
        "bbo_observation_kind": ["unknown", "source_observed"], "bbo_usable": [False, True]}), clock)
    quality.write_text(json.dumps({"day": day, "symbol": "BTCUSDC",
        "bbo_output": {"sha256": sha256_file(bbo)}, "clock_output": {"sha256": sha256_file(clock)}}))
    monkeypatch.setattr(replay, "book_observation_inputs", lambda *a, **k: (quality, clock))
    observed, present, usable = replay._load_book_observations(bbo, np.array([100, 200]), "bbo", return_usable=True)
    np.testing.assert_array_equal(observed, [-1, 190000])
    np.testing.assert_array_equal(present, [False, True])
    np.testing.assert_array_equal(usable, [False, True])


def _zero_trade_state_fixture(usable, *, activation_ms=0):
    """Synthetic lifecycle-only fixture: zero traded quantity, no market data."""
    import pandas as pd
    axis = np.arange(0, 2001, 100, dtype=np.int64)
    trades = pd.DataFrame({"transact_time": axis, "price": np.full(len(axis), 100.),
                           "quantity": np.zeros(len(axis)), "is_buyer_maker": np.ones(len(axis))})
    data = HistoricalBBOData(axis, np.full(len(axis), 99.9), np.full(len(axis), 100.1),
        np.ones(len(axis)), np.ones(len(axis)), usable=np.asarray(usable, dtype=bool))
    params = {"gamma": .01, "kappa": 1., "order_size": .001, "max_inventory": .01,
              "requote_interval": .1, "rq_min": .1, "rq_max": .1, "requote_clock": "fixed",
              "maker_fee": 0., "taker_fee": 0., "tick_size": .1, "lot_size": .001,
              "queue_base": 0., "queue_decay": 0., "maker_fill_prob": 1.,
              "use_bar_pricing": True, "replay_event_clock": "merged", "replay_clock_interval_ms": 100,
              "max_exec_book_age_s": 0., "collect_curves": False, "position_timeout": 0.,
              "markout_ema_span_fills": 0, "cancel_order_latency_ms": 100,
              "new_order_latency_ms": activation_ms, "planned_quote_stop_ts_ms": 1800,
              "replay_event_clock_end_ts_ms": 2000, "trace_quotes_max": 100, "trace_fills_max": 100}
    return trades, data, params


def test_invalid_top_stops_new_quotes_without_trade_touch_fallback():
    mask = np.ones(21, dtype=bool)
    mask[2:11] = False
    trades, data, params = _zero_trade_state_fixture(mask)
    result = replay.simulate_tick(trades, np.array([0]), np.array([1.]), params, bbo_data=data)
    assert result["stale_book_skip_count"] > 0
    assert result["_quote_trace"]
    assert not any(200 <= row["submit_ts"] < 1100 for row in result["_quote_trace"])
    assert any(row["submit_ts"] >= 1100 for row in result["_quote_trace"])
    assert not result["_fill_trace"]


def test_cpp_rejects_only_explicit_invalid_masks_without_silent_fallback():
    _, data, _ = _zero_trade_state_fixture([False] * 21)
    with pytest.raises(NotImplementedError, match="explicit BBO invalidation"):
        replay._simulate_tick_cpp(None, None, None, {}, bbo_data=data)


def test_unknown_gtx_activation_is_not_fabricated_as_ack_or_reject():
    mask = np.ones(21, dtype=bool)
    mask[2:11] = False
    trades, data, params = _zero_trade_state_fixture(mask, activation_ms=300)
    params["cancel_order_latency_ms"] = 1000
    result = replay.simulate_tick(trades, np.array([0]), np.array([1.]), params, bbo_data=data)
    first = [row for row in result["_quote_trace"] if row["submit_ts"] == 0]
    assert len(first) == 2
    assert all(row["activation_book_status"] == "UNKNOWN" for row in first)
    assert all(not row["exchange_accepted"] for row in first)
    assert all(not row["local_new_ack_published"] for row in first)
    assert result["gtx_rejects"] == 0
    assert not result["_fill_trace"]


@pytest.mark.parametrize("explicit", [False, True])
def test_cpp_valid_or_legacy_mask_reaches_existing_validation(monkeypatch, explicit):
    from dataclasses import replace
    _, data, _ = _zero_trade_state_fixture([True] * 21)
    if not explicit:
        data = replace(data, usable=None)
    def existing_validation(*args, **kwargs):
        raise RuntimeError("existing native validation reached")
    monkeypatch.setattr(replay, "validate_replay_initial_state", existing_validation)
    with pytest.raises(RuntimeError, match="existing native validation reached"):
        replay._simulate_tick_cpp(None, None, None, {}, bbo_data=data)


@pytest.mark.parametrize("mask", [np.ones(2, dtype=np.int64), np.ones(3, dtype=bool)])
def test_usability_mask_requires_aligned_boolean_values(mask):
    data = HistoricalBBOData(np.array([100, 200]), *[np.ones(2)] * 4, usable=mask)
    with pytest.raises(ValueError, match="boolean and align"):
        book_usable_mask(data)
