"""Independent top observations cannot repair or freshen depth/queue state."""
from copy import deepcopy

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.book_top import (
    BBO_CLOCK_FIELDS,
    TOP_SCHEMA,
    apply_top_observations,
    from_daily_book_rows,
    merge_book_rows,
    select_top_observations,
    to_daily_book_rows,
    validate_top_state,
)

BASE = 1_782_259_200_000_000


def top(offset, *, bid="100", ask="101", bid_qty="2", ask_qty="3", symbol="BTCUSDC"):
    return {"exchange": "binance-futures", "symbol": symbol,
            "timestamp": BASE + offset, "local_timestamp": BASE + offset + 7,
            "ask_amount": ask_qty, "ask_price": ask,
            "bid_price": bid, "bid_amount": bid_qty}


def tops(*rows):
    return pa.Table.from_pylist(list(rows), schema=TOP_SCHEMA)


def inputs(offsets, observed=None, *, bid=90.0, ask=91.0):
    times = [BASE + value for value in offsets]
    if observed is None:
        observed = [BASE] * len(times)
    else:
        observed = [BASE + value if value is not None else None for value in observed]
    bbo = pa.table({"timestamp": [t // 1000 for t in times],
                    "best_bid": [float(bid)] * len(times), "best_bid_qty": [2.0] * len(times),
                    "best_ask": [float(ask)] * len(times), "best_ask_qty": [3.0] * len(times)})
    clock = pa.table({
        "timestamp": pa.array([t // 1000 for t in times], type=pa.int64()),
        "exchange_cut_timestamp_us": pa.array(observed, type=pa.int64()),
        "last_provider_local_timestamp_us": pa.array(observed, type=pa.int64()),
        "exchange_age_us": pa.array([t - o if o else None for t, o in zip(times, observed, strict=True)]),
        "last_observation_timestamp_us": pa.array(observed, type=pa.int64()),
        "observation_age_us": pa.array([t - o if o else None for t, o in zip(times, observed, strict=True)]),
        "observation_kind": ["source_observed"] * len(times),
        "update_coverage": ["unobserved"] * len(times),
    })
    return bbo, clock


def test_no_top_preserves_complete_values_and_all_depth_clock_fields():
    bbo, clock = inputs([100_000, 200_000, 300_000], [0, 100_000, 200_000])
    result, extended, state, metrics = apply_top_observations(bbo, clock, tops())
    assert result.equals(bbo)
    assert extended.select(clock.column_names).equals(clock)
    assert extended.column_names == clock.column_names + list(BBO_CLOCK_FIELDS)
    assert extended["bbo_usable"].to_pylist() == [True] * 3
    assert extended["bbo_observation_age_us"].to_pylist() == [100_000] * 3
    assert state["latest_top"] is None
    assert metrics["top_selected_rows"] == 0


def test_top_gap_fills_only_bbo_actual_clock_age_and_no_future_fill():
    bbo, clock = inputs([6_000_000, 6_100_000, 6_200_000, 6_300_000])
    result, extended, _, metrics = apply_top_observations(
        bbo, clock, tops(top(6_050_000), top(6_200_000, bid="102", ask="103")))
    assert result["best_bid"].to_pylist() == [90, 100, 102, 102]
    assert extended["bbo_last_observation_timestamp_us"].to_pylist() == [
        BASE, BASE + 6_050_000, BASE + 6_200_000, BASE + 6_200_000]
    assert extended["bbo_observation_age_us"].to_pylist() == [6_000_000, 50_000, 0, 100_000]
    assert extended["bbo_observation_kind"].to_pylist() == [
        "source_observed", "source_observed", "source_observed", "carried_forward"]
    assert extended.select(clock.column_names).equals(clock)
    assert metrics["future_fill_violations"] == metrics["bbo_clock_regressions"] == 0


@pytest.mark.parametrize("invalid", [dict(bid="0"), dict(ask="99"), dict(bid_qty="NaN"), dict(ask_qty=None)])
def test_latest_invalid_ends_top_and_never_exposes_older_valid_as_fresh(invalid):
    bbo, clock = inputs([6_000_000, 6_100_000, 6_200_000, 6_300_000])
    _, extended, state, metrics = apply_top_observations(
        bbo, clock, tops(top(6_000_000), top(6_100_000, **invalid)))
    assert extended["bbo_usable"].to_pylist() == [True, False, False, False]
    assert extended["bbo_observation_kind"].to_pylist() == ["source_observed"] + ["invalid_top"] * 3
    assert extended["bbo_observation_age_us"].to_pylist() == [0, 100_000, 200_000, 300_000]
    assert state["latest_top"]["timestamp"] == BASE + 6_100_000
    assert state["last_valid_top"]["timestamp"] == BASE + 6_000_000
    assert metrics["invalid_top_rows"] == 3


def test_newer_depth_restores_invalid_top_but_older_depth_never_regresses_clock():
    bbo, clock = inputs([6_000_000, 6_100_000, 6_200_000, 6_300_000],
                        [0, 1_000_000, 2_000_000, 6_300_000])
    result, extended, _, _ = apply_top_observations(
        bbo, clock, tops(top(6_000_000), top(6_200_000, bid="0")))
    assert result["best_bid"].to_pylist() == [100, 100, 100, 90]
    assert extended["bbo_last_observation_timestamp_us"].to_pylist() == [
        BASE + 6_000_000] * 3 + [BASE + 6_300_000]
    assert extended["bbo_usable"].to_pylist() == [True, True, False, True]


def test_new_invalid_depth_cannot_reuse_valid_top_as_usable():
    bbo, clock = inputs([6_000_000], [0])
    _, _, state, _ = apply_top_observations(bbo, clock, tops(top(6_000_000)))
    bbo, clock = inputs([6_100_000], [6_100_000], bid=0)
    _, extended, _, _ = apply_top_observations(bbo, clock, tops(), state)
    assert extended["bbo_usable"].to_pylist() == [False]
    assert extended["bbo_observation_kind"].to_pylist() == ["unknown"]


def test_unknown_without_prior_observation_stays_unknown_not_zero_age():
    bbo, clock = inputs([100_000], [None], bid=np.nan, ask=np.nan)
    _, extended, state, _ = apply_top_observations(bbo, clock, tops(top(200_000)))
    assert extended["bbo_last_observation_timestamp_us"].to_pylist() == [None]
    assert extended["bbo_observation_age_us"].to_pylist() == [None]
    assert extended["bbo_observation_kind"].to_pylist() == ["unknown"]
    assert extended["bbo_usable"].to_pylist() == [False]
    assert state["latest_top"] is None


def test_cross_day_keeps_real_top_clock_and_invalid_termination():
    end = 86_400_000_000
    bbo, clock = inputs([end - 200_000, end - 100_000], [end - 10_000_000] * 2)
    _, _, state, _ = apply_top_observations(
        bbo, clock, tops(top(end - 200_000), top(end - 100_000, ask="99")))
    snapshot = deepcopy(state)
    bbo, clock = inputs([end, end + 100_000], [end - 9_000_000] * 2)
    result, extended, next_state, _ = apply_top_observations(bbo, clock, tops(), state)
    assert state == snapshot
    assert result["best_bid"].to_pylist() == [100, 100]
    assert extended["bbo_last_observation_timestamp_us"].to_pylist() == [BASE + end - 200_000] * 2
    assert extended["bbo_observation_age_us"].to_pylist() == [200_000, 300_000]
    assert extended["bbo_usable"].to_pylist() == [False, False]
    assert next_state["latest_top"] == state["latest_top"]


@pytest.mark.parametrize("change", [dict(symbol="BTCUSDT"), dict(clock="receive"),
                                    dict(last_grid_us=BASE + 9_000_000), dict(extra="no")])
def test_state_schema_clock_boundary_rejected_without_mutating(change):
    bbo, clock = inputs([6_000_000])
    _, _, state, _ = apply_top_observations(bbo, clock, tops(top(6_000_000)))
    state.update(change)
    snapshot = deepcopy(state)
    with pytest.raises(ValueError):
        validate_top_state(state, boundary_us=BASE + 6_000_000)
    assert snapshot == state


def test_future_depth_and_nonmonotonic_grids_are_rejected():
    bbo, clock = inputs([100_000], [200_000])
    with pytest.raises(ValueError, match="future"):
        apply_top_observations(bbo, clock, tops())
    bbo, clock = inputs([100_000, 100_000])
    with pytest.raises(ValueError, match="strictly increasing"):
        apply_top_observations(bbo, clock, tops())


def test_bounded_selector_keeps_invalid_latest_not_previous_valid(tmp_path):
    raw = tops(*(top(i * 100_000, bid="0" if i == 71 else "100") for i in range(100)))
    path = tmp_path / "book_ticker.parquet"
    pq.write_table(raw, path, row_group_size=10)
    _, clock = inputs([7_000_000, 7_100_000, 7_200_000])
    selected, metrics = select_top_observations(path, clock)
    assert selected["timestamp"].to_pylist() == [BASE + 7_000_000, BASE + 7_100_000, BASE + 7_200_000]
    assert selected["bid_price"].to_pylist() == ["100", "0", "100"]
    assert metrics["row_groups_read"] == 1
    assert metrics["raw_rows_read"] == 10
    assert metrics["selected_invalid_events"] == 1


def test_selector_preserves_latest_equal_timestamp_distinct_update(tmp_path):
    path = tmp_path / "book_ticker.parquet"
    pq.write_table(tops(top(6_000_000), top(6_000_000, bid="0"), top(7_000_000)), path, row_group_size=1)
    _, clock = inputs([6_000_000, 6_100_000])
    selected, _ = select_top_observations(path, clock)
    assert selected["bid_price"].to_pylist() == ["0"]


def test_selector_before_first_unknown_complete_grid_does_not_read(tmp_path):
    path = tmp_path / "book_ticker.parquet"
    pq.write_table(tops(top(7_000_000)), path)
    _, clock = inputs([6_000_000])
    selected, metrics = select_top_observations(path, clock)
    assert len(selected) == 0
    assert metrics["grids_without_prior_top"] == 1
    _, clock = inputs([100_000], [0])
    selected, metrics = select_top_observations(tmp_path / "absent.parquet", clock)
    assert len(selected) == metrics["row_groups_read"] == 0


def test_selector_requires_bounded_timestamp_statistics(tmp_path):
    path = tmp_path / "book_ticker.parquet"
    pq.write_table(tops(top(6_000_000)), path, write_statistics=False)
    _, clock = inputs([6_000_000])
    with pytest.raises(ValueError, match="statistics"):
        select_top_observations(path, clock)


def test_top_daily_roundtrip_exact_values_equal_timestamps_and_invalid():
    raw = tops(top(6_000_000, bid_qty="1.00000000000000001"), top(6_000_000, ask="NaN"))
    daily = to_daily_book_rows(raw)
    assert from_daily_book_rows(daily).equals(raw)
    assert daily["side"].to_pylist() == ["bid", "ask", "bid", "ask"]
    for name in ("is_snapshot", "queue_rebase", "native_sequence"):
        assert daily[name].to_pylist() == [False] * 4
    assert daily["observed_timestamp_us"].equals(daily["timestamp"])
    assert daily["top_only"].to_pylist() == [True] * 4


@pytest.mark.parametrize("field,value", [("is_snapshot", True), ("observation_only", False),
                                        ("queue_rebase", True), ("stream_id", "d0"),
                                        ("observed_timestamp_us", BASE - 1)])
def test_top_decode_rejects_depth_or_fabricated_observation_semantics(field, value):
    daily = to_daily_book_rows(tops(top(6_000_000)))
    rows = daily.to_pylist()
    rows[0][field] = value
    with pytest.raises(ValueError):
        from_daily_book_rows(pa.Table.from_pylist(rows, schema=daily.schema))


def test_streaming_merge_equal_time_depth_across_batches_precedes_top():
    deep = to_daily_book_rows(tops(top(5_000_000), top(6_000_000), top(7_000_000)))
    deep = deep.set_column(deep.schema.get_field_index("top_only"), "top_only", pa.array([False] * 6))
    supplement = to_daily_book_rows(tops(top(4_000_000), top(6_000_000), top(8_000_000)))
    batches = [deep.slice(0, 3), deep.slice(3, 1), deep.slice(4)]
    out_batches = list(merge_book_rows(iter(batches), supplement, batch_size=2))
    out = pa.Table.from_batches(out_batches)
    assert all(len(batch) <= 2 for batch in out_batches)
    assert out["timestamp"].to_pylist() == sorted(out["timestamp"].to_pylist())
    middle = [row["top_only"] for row in out.to_pylist() if row["timestamp"] == BASE + 6_000_000]
    assert middle == [False, False, True, True]
    assert len(out) == len(deep) + len(supplement)


def test_streaming_merge_rejects_regression():
    a = to_daily_book_rows(tops(top(7_000_000)))
    b = to_daily_book_rows(tops(top(6_000_000)))
    with pytest.raises(ValueError, match="regress"):
        list(merge_book_rows([a, b], a.slice(0, 0)))
