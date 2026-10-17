from __future__ import annotations

import copy
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from data.daily_raw import (
    BOOK_SCHEMA, DAILY_BOOK_MARKER, DAILY_BOOK_SCHEMA, _day_bounds,
    book_stream_priority, daily_book_receipt, fuse_orderbook_day,
    supplement_daily_book_day,
)


def current_day(root: Path, day="2025-08-01", *, with_top=True):
    _, end = _day_bounds(day)
    t = end - 10_000_000
    rows = []
    for side, price in [("bid", "100.000"), ("bid", "99.000"), ("ask", "101.000"), ("ask", "102.000")]:
        rows.append(dict(exchange="binance-futures", symbol="BTCUSDC", timestamp=t,
            local_timestamp=t+500, is_snapshot=True, side=side, price=price, amount="1.000",
            stream_id="s0", stream_priority=1, queue_rebase=True, observation_only=False,
            top_only=False, native_sequence=False, observed_timestamp_us=t, original_timestamp_us=None))
    rows.append({**rows[0], "timestamp": t+1_000_000, "is_snapshot": False,
                 "observed_timestamp_us": t+1_000_000, "amount": "2.000"})
    rows.append({**rows[-1], "timestamp": t+2_000_000,
                 "observed_timestamp_us": t+1_500_000, "queue_rebase": False, "observation_only": True})
    if with_top:
        for side, price in [("bid", "110.000"), ("ask", "111.000")]:
            rows.append({**rows[0], "timestamp": t+8_000_000,
                "local_timestamp": t+8_000_500, "is_snapshot": False, "side": side, "price": price,
                "stream_id": "t0", "stream_priority": 0, "queue_rebase": False,
                "observation_only": True, "top_only": True, "observed_timestamp_us": t+8_000_000,
                "original_timestamp_us": t+8_000_000})
    receipt = dict(schema=DAILY_BOOK_MARKER, day=day, symbol="BTCUSDC",
        stream_priority=book_stream_priority(["s0"]), initial_state=None, final_state=None,
        top_initial_state=None, top_final_state=None, normalized={}, output_rows=len(rows), top_rows=2 if with_top else 0)
    path = root / f"current-{day}.parquet"
    metadata = {b"narrowgate.book_fusion": DAILY_BOOK_MARKER.encode(),
        b"narrowgate.book_receipt": json.dumps(receipt).encode(), b"narrowgate.day": day.encode()}
    pq.write_table(pa.Table.from_pylist(rows, schema=DAILY_BOOK_SCHEMA).replace_schema_metadata(metadata), path)
    return path


def capture(root: Path, day="2025-08-01", *, at=None, symbol="BTCUSDC", late=False):
    _, end = _day_bounds(day)
    at = end-9_500_000 if at is None else at
    rows = [dict(exchange="binance-futures", symbol=symbol, timestamp=at, local_timestamp=at+2000,
                 is_snapshot=True, side=side, price=price, amount="3.00")
            for side, price in [("bid", "102.00"), ("bid", "101.00"), ("ask", "103.00"), ("ask", "104.00")]]
    if late:
        rows.append({**rows[0], "is_snapshot": False, "timestamp": at-1000, "amount": "4.00"})
    path = root / f"capture-{day}-{at}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=BOOK_SCHEMA), path)
    return path


def run(root, current, extra, day="2025-08-01", **kwargs):
    return supplement_daily_book_day(current, extra, root / "raw.parquet", day,
        stream_id="s1", normalized_root=root / "derived", minimum_levels=2, **kwargs)


def test_supplement_preserves_every_current_field_and_separate_top_clock(tmp_path):
    pytest.importorskip("narrowgate_cpp")
    current, extra = current_day(tmp_path), capture(tmp_path, late=True)
    before = current.read_bytes()
    result = run(tmp_path / "stage", current, extra)
    output = pq.read_table(result["path"]).replace_schema_metadata(None)
    old = output.filter(pc.not_equal(output["stream_id"], "s1"))
    assert old.equals(pq.read_table(current).replace_schema_metadata(None))
    assert current.read_bytes() == before
    assert result["rows"] == len(old)+5
    assert result["receipt"]["stream_priority"] == book_stream_priority(["s0", "s1"])
    added = output.filter(pc.equal(output["stream_id"], "s1"))
    assert added["timestamp"][-1].as_py() == added["timestamp"][0].as_py()
    assert added["observed_timestamp_us"][-1].as_py() < added["timestamp"][-1].as_py()
    assert added["original_timestamp_us"].equals(added["observed_timestamp_us"])
    assert not any(added["native_sequence"].to_pylist())
    assert result["stats"]["future_fill_violations"] == 0
    clock = pq.read_table(result["stats"]["normalized"]["clock"]["path"]).to_pydict()
    assert max(clock["bbo_last_observation_timestamp_us"]) > max(clock["last_observation_timestamp_us"])
    assert result["receipt"]["top_rows"] == 2
    assert result["initial_state"]["state"]["streams"][1]["initialized"] is False
    assert result["final_state"]["state"]["streams"][1]["initialized"] is True


def test_repeated_capture_is_content_bound_and_adds_no_rows(tmp_path):
    current, extra = current_day(tmp_path), capture(tmp_path)
    first = run(tmp_path / "first", current, extra)
    second = run(tmp_path / "second", Path(first["path"]), extra)
    assert second["input_reused"] is True
    assert second["rows"] == first["rows"]
    assert second["consumed_inputs"] == first["consumed_inputs"]
    assert len(second["consumed_inputs"]) == 1
    assert pq.read_table(first["path"]).replace_schema_metadata(None).equals(
        pq.read_table(second["path"]).replace_schema_metadata(None))
    for kind in ("bbo", "l2", "clock"):
        assert pq.read_table(first["stats"]["normalized"][kind]["path"]).equals(
            pq.read_table(second["stats"]["normalized"][kind]["path"]))
    with pytest.raises(ValueError, match="existing stream"):
        run(tmp_path / "conflict", Path(first["path"]), capture(tmp_path, at=_day_bounds("2025-08-01")[1]-4_000_000))


def test_next_day_without_capture_retains_new_stream_and_real_observation(tmp_path):
    first = run(tmp_path / "first", current_day(tmp_path), capture(tmp_path))
    original = current_day(tmp_path, "2025-08-02", with_top=False)
    previous = copy.deepcopy(first["final_state"])
    second = run(tmp_path / "second", original, None, "2025-08-02", previous_state=previous,
                 previous_top_state=first["top_final_state"])
    assert previous == first["final_state"]
    assert second["initial_state"] == previous
    assert pq.read_table(second["path"]).replace_schema_metadata(None).equals(
        pq.read_table(original).replace_schema_metadata(None))
    old_clock = previous["state"]["streams"][1]["observed_us"]
    assert second["final_state"]["state"]["streams"][1]["observed_us"] == old_clock
    clock = pq.read_table(second["stats"]["normalized"]["clock"]["path"]).to_pydict()
    assert clock["last_observation_timestamp_us"][0] < _day_bounds("2025-08-02")[0]
    assert clock["observation_age_us"][1] > clock["observation_age_us"][0]
    assert second["top_initial_state"] == first["top_final_state"]
    assert clock["bbo_last_observation_timestamp_us"][0] == first["top_final_state"]["last_bbo"]["observed_timestamp_us"]


@pytest.mark.parametrize("replacement", [None, "different-capture"])
@pytest.mark.parametrize("reuse_file", [False, True])
def test_existing_logical_capture_cannot_be_renamed_or_erased(tmp_path, replacement, reuse_file):
    current, extra = current_day(tmp_path), capture(tmp_path)
    first = run(tmp_path / "first", current, extra, logical_stream_id="purchased-capture")
    existing = Path(first["path"])
    before = existing.read_bytes()
    with pytest.raises(ValueError, match="logical identity cannot change"):
        run(tmp_path / "bad", existing, extra if reuse_file else None,
            logical_stream_id=replacement)
    assert existing.read_bytes() == before
    assert not (tmp_path / "bad/raw.parquet").exists()


def test_legacy_capture_can_bind_logical_identity_without_duplicate_observations(tmp_path):
    current, extra = current_day(tmp_path), capture(tmp_path)
    legacy = run(tmp_path / "legacy", current, extra)
    bound = run(tmp_path / "bound", Path(legacy["path"]), extra,
                logical_stream_id="purchased-capture")
    assert bound["input_reused"] is True
    assert bound["rows"] == legacy["rows"]
    assert bound["consumed_inputs"] == legacy["consumed_inputs"]
    assert bound["receipt"]["supplementation_contract"]["logical_stream_id"] == "purchased-capture"
    repeated = run(tmp_path / "repeated", Path(bound["path"]), extra,
                   logical_stream_id="purchased-capture")
    assert repeated["input_reused"] is True
    assert repeated["rows"] == bound["rows"]


@pytest.mark.parametrize("change", ["symbol", "boundary", "mapping", "levels"])
def test_incompatible_continuation_rejected_without_mutation(tmp_path, change):
    from data.daily_raw import _daily_supplement_seed
    current = current_day(tmp_path)
    _, seed = _daily_supplement_seed(daily_book_receipt(current), "2025-08-01", "s1", None, 2)
    if change == "symbol":
        seed["symbol"] = "BTCUSDT"
    elif change == "boundary":
        seed["next_day_start_us"] += 86_400_000_000
    elif change == "mapping":
        seed["stream_priority"]["preferred_index"] = 1
    else:
        seed["state"]["minimum_levels"] = 20
    before = copy.deepcopy(seed)
    with pytest.raises(ValueError, match="continuation"):
        run(tmp_path / "bad", current, None, previous_state=seed)
    assert seed == before
    assert not (tmp_path / "bad/raw.parquet").exists()


@pytest.mark.parametrize("kind", ["market", "future", "fused", "samefile"])
def test_invalid_capture_never_changes_current_file(tmp_path, kind):
    current = current_day(tmp_path)
    before = current.read_bytes()
    extra = capture(tmp_path, symbol="BTCUSDT" if kind == "market" else "BTCUSDC",
                    at=_day_bounds("2025-08-01")[1] if kind == "future" else None)
    if kind == "fused":
        table = pq.read_table(extra)
        pq.write_table(table.replace_schema_metadata({b"narrowgate.book_fusion": b"reconstructed_fusion.v1"}), extra)
    if kind == "samefile":
        extra = current
    with pytest.raises(ValueError):
        run(tmp_path / "invalid", current, extra)
    assert current.read_bytes() == before
    assert not (tmp_path / "invalid/raw.parquet").exists()


def test_failed_rebuild_keeps_existing_current_and_seed(tmp_path, monkeypatch):
    import data.normalize_tardis_orderbook as normalizer
    current, extra = current_day(tmp_path), capture(tmp_path)
    before = current.read_bytes()
    monkeypatch.setattr(normalizer, "iter_fused_book_batches",
        lambda *a, **k: (_ for _ in ()).throw(ValueError("injected reconstruction failure")))
    with pytest.raises(ValueError, match="injected"):
        run(tmp_path / "failed", current, extra)
    assert current.read_bytes() == before
    assert extra.exists()
    assert not (tmp_path / "failed/raw.parquet").exists()


def test_fusion_entry_requires_explicit_contract_and_never_retires_inputs(tmp_path, monkeypatch):
    import data.daily_raw as daily
    current, extra = current_day(tmp_path), capture(tmp_path)
    calls = []
    monkeypatch.setattr(daily, "supplement_daily_book_day", lambda *a, **k: calls.append((a, k)) or {"status": "STAGED"})
    result = fuse_orderbook_day({"canonical": current, "tardis": extra}, tmp_path / "staged.parquet", "2025-08-01",
        normalized_root=tmp_path / "derived", unified_output=True, supplement_stream_id="s1")
    assert result == {"status": "STAGED"}
    assert calls[0][0][:2] == (current, extra)
    assert calls[0][1]["stream_id"] == "s1"
    with pytest.raises(ValueError, match="caller-owned retirement"):
        fuse_orderbook_day({"canonical": current}, tmp_path / "bad.parquet", "2025-08-01",
            normalized_root=tmp_path / "derived", unified_output=True, supplement_stream_id="s1", retire_sources=True)


def test_merge_keeps_old_before_new_ties_even_across_batches():
    from data.daily_raw import _merge_daily_streams
    def table(times, label):
        return pa.table({"timestamp": times, "identity": [label]*len(times)})
    result = pa.concat_tables(_merge_daily_streams(
        [table([1, 2], "old-a"), table([2, 2], "old-b"), table([4], "old-c")],
        [table([2, 3], "new-a"), table([3, 4], "new-b")]))
    assert result.to_pydict() == {
        "timestamp": [1, 2, 2, 2, 2, 3, 3, 4, 4],
        "identity": ["old-a", "old-a", "old-b", "old-b", "new-a", "new-a", "new-b", "old-c", "new-b"]}


def test_capture_can_fill_explicitly_empty_supplement_slot_later(tmp_path):
    current = current_day(tmp_path)
    first = run(tmp_path / "carry-only", current, None)
    second = run(tmp_path / "filled", Path(first["path"]), capture(tmp_path))
    assert second["rows"] == first["rows"]+4
    assert not second["input_reused"]
    assert second["initial_state"] == first["initial_state"]


def test_unchanged_old_stream_wins_equal_exchange_clock(tmp_path):
    current = current_day(tmp_path, with_top=False)
    at = _day_bounds("2025-08-01")[1]-10_000_000
    result = run(tmp_path / "ties", current, capture(tmp_path, at=at))
    l2 = pq.read_table(result["stats"]["normalized"]["l2"]["path"])
    assert l2["bid_px_1"][0].as_py() == 100.
    assert result["initial_state"]["state"]["preferred"] == 0


def test_current_receive_clock_contract_cannot_be_silently_reinterpreted(tmp_path):
    current = current_day(tmp_path)
    table = pq.read_table(current)
    metadata = dict(table.schema.metadata)
    metadata[b"narrowgate.timestamp_unit"] = b"receive_microseconds"
    pq.write_table(table.replace_schema_metadata(metadata), current)
    before = current.read_bytes()
    with pytest.raises(ValueError, match="incompatible clock"):
        run(tmp_path / "wrong-clock", current, None)
    assert current.read_bytes() == before


def native_capture(root, day="2025-08-01", *, messages=None):
    at = _day_bounds(day)[1] - 7_000_000
    messages = messages or [(True, at, 100, 100, None, 100),
                            (False, at + 100_000, 101, 101, 100, None)]
    rows = []
    for snapshot, stamp, first, final, previous, last in messages:
        for side, price in (("bid", "102"), ("bid", "101"), ("ask", "103"), ("ask", "104")):
            rows.append(dict(exchange="binance-futures", symbol="BTCUSDC", timestamp=stamp,
                local_timestamp=stamp+700, received_time=stamp//1000+1,
                event_type="snapshot" if snapshot else "update", is_snapshot=snapshot,
                event_time=stamp//1000, transaction_time=stamp//1000-1,
                first_update_id=first, final_update_id=final, prev_final_update_id=previous,
                last_update_id=last, side=side, price=price, amount="3", quantity="3"))
    path = root / f"native-{day}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=BOOK_SCHEMA), path)
    return path


def test_native_snapshot_delta_clock_ids_and_existing_rows_are_preserved(tmp_path):
    extra = native_capture(tmp_path)
    current = current_day(tmp_path)
    before = pq.read_table(extra)
    result = run(tmp_path / "native", current, extra, capture_mode="binance_futures_native")
    output = pq.read_table(result["path"]).replace_schema_metadata(None)
    old = output.filter(pc.not_equal(output["stream_id"], "s1"))
    assert old.equals(pq.read_table(current).replace_schema_metadata(None))
    added = output.filter(pc.equal(output["stream_id"], "s1"))
    for name in ("event_time", "transaction_time", "received_time", "first_update_id", "final_update_id",
                 "prev_final_update_id", "last_update_id", "is_snapshot"):
        assert added[name].equals(before[name])
    assert all(added["native_sequence"].to_pylist())
    assert result["final_state"]["state"]["streams"][1]["last_id"] == 101
    assert result["capture_clock_selection"]["event_rows"] == 8
    assert result["capture_contract"]["clock"] == "exchange_event_ms_then_transaction_ms"
    assert result["stats"]["future_fill_violations"] == 0
    repeat = run(tmp_path / "repeat", Path(result["path"]), extra, capture_mode="binance_futures_native")
    assert repeat["input_reused"]
    assert repeat["consumed_inputs"] == result["consumed_inputs"]
    assert repeat["capture_clock_selection"] == result["capture_clock_selection"]
    assert repeat["rows"] == result["rows"]


def test_native_deltas_cannot_bootstrap_from_existing_book_or_top(tmp_path):
    t = _day_bounds("2025-08-01")[1] - 20_000_000
    extra = native_capture(tmp_path, messages=[(False, t, 101, 101, 100, None)])
    result = run(tmp_path / "delta", current_day(tmp_path), extra, capture_mode="binance_futures_native")
    source = result["final_state"]["state"]["streams"][1]
    assert source["initialized"] is False
    assert source["observed_us"] == 0
    assert source["levels"] == []
    assert result["stats"]["pre_snapshot_messages"] == 1
    assert result["stats"]["native_sequence_authority"] is False
    clock = pq.read_table(result["stats"]["normalized"]["clock"]["path"])
    assert clock["last_observation_timestamp_us"][0].as_py() > t
    assert all(x > 0 for x in clock["last_observation_timestamp_us"].to_pylist())


def test_native_broken_chain_does_not_refresh_and_needs_real_snapshot(tmp_path):
    t = _day_bounds("2025-08-01")[1] - 7_000_000
    messages = [(True, t, 100, 100, None, 100),
                (False, t+100_000, 101, 101, 100, None),
                (False, t+200_000, 104, 104, 103, None),
                (False, t+300_000, 105, 105, 104, None)]
    extra = native_capture(tmp_path, messages=messages)
    result = run(tmp_path / "broken", current_day(tmp_path), extra, capture_mode="binance_futures_native")
    source = result["final_state"]["state"]["streams"][1]
    assert source["initialized"] is False
    assert source["observed_us"] == t+100_000
    assert result["stats"]["sequence_gaps"] == 1
    assert result["stats"]["pre_snapshot_messages"] == 1
    native_capture(tmp_path, messages=[*messages, (True, t+400_000, 105, 105, None, 105)])
    restored = run(tmp_path / "restored", current_day(tmp_path), extra, capture_mode="binance_futures_native")
    assert restored["final_state"]["state"]["streams"][1]["initialized"] is True
    assert restored["final_state"]["state"]["streams"][1]["observed_us"] == t+400_000


def test_native_exchange_fallback_has_explicit_row_boundaries(tmp_path):
    extra = native_capture(tmp_path)
    table = pq.read_table(extra)
    event = table["event_time"].to_pylist()
    event[4:] = [None] * 4
    table = table.set_column(table.schema.get_field_index("event_time"), "event_time", pa.array(event, pa.int64()))
    pq.write_table(table, extra)
    result = run(tmp_path / "fallback", current_day(tmp_path), extra, capture_mode="binance_futures_native")
    assert result["capture_clock_selection"] == {
        "input_rows": 8, "event_rows": 4, "transaction_fallback_rows": 4,
        "transaction_fallback_row_ranges": [[4, 8]], "row_ranges": "zero_based_half_open_input_order"}
    assert result["stats"]["source_stats"]["s1"]["exchange_T_fallback_rows"] == 4
    source = result["final_state"]["state"]["streams"][1]
    assert source["observed_us"] == table["transaction_time"][-1].as_py()*1000


@pytest.mark.parametrize("change", ["false_snapshot", "missing_id", "bad_units", "no_clock", "rounded"])
def test_invalid_native_messages_are_atomic(tmp_path, change):
    extra = native_capture(tmp_path)
    rows = pq.read_table(extra).to_pylist()
    for row in rows[:4]:
        if change == "false_snapshot":
            row["event_type"] = "update"
        elif change == "missing_id":
            row["last_update_id"] = row["final_update_id"] = None
        elif change == "bad_units":
            row["event_time"] *= 1000
        elif change == "no_clock":
            row["event_time"] = row["transaction_time"] = None
        else:
            row["event_time"] = _day_bounds("2025-08-01")[0]//1000+3_600_000
            row["transaction_time"] = None
    pq.write_table(pa.Table.from_pylist(rows, schema=BOOK_SCHEMA), extra)
    current = current_day(tmp_path)
    before = current.read_bytes()
    with pytest.raises(ValueError):
        run(tmp_path / "bad", current, extra, capture_mode="binance_futures_native")
    assert current.read_bytes() == before
    assert not (tmp_path / "bad/raw.parquet").exists()


def test_native_cross_day_binding_and_missing_day_carry(tmp_path):
    first = run(tmp_path / "one", current_day(tmp_path), native_capture(tmp_path), capture_mode="binance_futures_native")
    previous = copy.deepcopy(first["final_state"])
    next_current = current_day(tmp_path, "2025-08-02", with_top=False)
    second = run(tmp_path / "two", next_current, None, "2025-08-02", previous_state=previous,
                 capture_mode="binance_futures_native")
    assert second["initial_state"] == previous
    assert previous == first["final_state"]
    assert second["final_state"]["state"]["streams"][1] == previous["state"]["streams"][1]
    clock = pq.read_table(second["stats"]["normalized"]["clock"]["path"])
    assert clock["observation_age_us"][1].as_py() > clock["observation_age_us"][0].as_py()
    assert clock["last_observation_timestamp_us"][0].as_py() < _day_bounds("2025-08-02")[0]
    with pytest.raises(ValueError, match="capture mode/symbol/clock"):
        run(tmp_path / "mode-switch", next_current, None, "2025-08-02", previous_state=previous)
    bad = copy.deepcopy(previous)
    bad["capture_contract"]["clock"] = "received"
    with pytest.raises(ValueError, match="capture mode/symbol/clock"):
        run(tmp_path / "clock-switch", next_current, None, "2025-08-02", previous_state=bad,
            capture_mode="binance_futures_native")


def test_tardis_continuation_cannot_seed_native_stream(tmp_path):
    old = run(tmp_path / "tardis", current_day(tmp_path), capture(tmp_path))
    with pytest.raises(ValueError, match="capture mode/symbol/clock"):
        run(tmp_path / "wrong", current_day(tmp_path, "2025-08-02"), None, "2025-08-02",
            previous_state=old["final_state"], capture_mode="binance_futures_native")


def test_native_next_day_delta_uses_only_bound_previous_native_sequence(tmp_path):
    first = run(tmp_path / "one", current_day(tmp_path), native_capture(tmp_path), capture_mode="binance_futures_native")
    day = "2025-08-02"
    stamp = _day_bounds(day)[1]-7_000_000
    extra = native_capture(tmp_path, day, messages=[(False, stamp, 102, 102, 101, None)])
    second = run(tmp_path / "two", current_day(tmp_path, day), extra, day,
                 previous_state=first["final_state"], capture_mode="binance_futures_native")
    assert second["stats"]["pre_snapshot_messages"] == 0
    assert second["stats"]["sequence_gaps"] == 0
    state = second["final_state"]["state"]["streams"][1]
    assert state["last_id"] == 102
    assert state["observed_us"] == stamp


def test_native_mode_cannot_relabel_a_current_base_slot(tmp_path):
    with pytest.raises(ValueError, match="unbound source slot"):
        supplement_daily_book_day(current_day(tmp_path), None, tmp_path / "bad/raw.parquet", "2025-08-01",
            stream_id="s0", normalized_root=tmp_path / "bad/views", minimum_levels=2,
            capture_mode="binance_futures_native")


def layout_day(root, day, slots, *, initial=None):
    """Synthetic canonical layout; do not invent aliases between old base slots."""
    path = current_day(root, day, with_top=False)
    table = pq.read_table(path)
    receipt = daily_book_receipt(path)
    receipt["stream_priority"] = book_stream_priority([f"s{i}" for i in range(slots)],
                                                      preferred_index=1 if slots == 3 else 0)
    receipt["initial_state"] = initial
    rank = -1 if slots == 3 else 1
    table = table.set_column(table.schema.get_field_index("stream_priority"), "stream_priority",
                             pa.array([rank] * len(table), type=pa.int16()))
    metadata = dict(table.schema.metadata)
    metadata[b"narrowgate.book_receipt"] = json.dumps(receipt).encode()
    pq.write_table(table.replace_schema_metadata(metadata), path)
    return path


def seam_run(root, current, day, slots, *, previous=None, previous_slot=None, extra=None, **kwargs):
    return supplement_daily_book_day(current, extra, root / "raw.parquet", day,
        stream_id=f"s{slots}", normalized_root=root / "derived", minimum_levels=2,
        previous_state=previous, logical_stream_id="additional-l2-v1",
        previous_stream_id=previous_slot, **kwargs)


def test_native_mode_survives_explicit_three_one_three_layout_seams(tmp_path):
    previous, previous_slot, original_source = None, None, None
    for day, slots in [("2025-08-01", 3), ("2025-08-02", 1), ("2025-08-03", 3)]:
        result = seam_run(tmp_path / day, layout_day(tmp_path, day, slots), day, slots,
            previous=previous, previous_slot=previous_slot,
            extra=native_capture(tmp_path, day) if previous is None else None,
            capture_mode="binance_futures_native")
        source = result["final_state"]["state"]["streams"][slots]
        if original_source is None:
            original_source = copy.deepcopy(source)
        assert source == original_source
        assert result["final_state"]["capture_contract"]["stream_id"] == f"s{slots}"
        assert result["final_state"]["capture_contract"]["logical_stream_id"] == "additional-l2-v1"
        previous, previous_slot = result["final_state"], f"s{slots}"


def test_opt_in_same_layout_retains_exact_existing_full_state(tmp_path):
    first = seam_run(tmp_path / "one", layout_day(tmp_path, "2025-08-01", 3), "2025-08-01", 3,
                     extra=capture(tmp_path))
    second = seam_run(tmp_path / "two", layout_day(tmp_path, "2025-08-02", 3), "2025-08-02", 3,
                      previous=first["final_state"], previous_slot="s3")
    assert "seam_transition" not in second
    assert second["initial_state"] == first["final_state"]
    assert second["final_state"]["supplementation"] == {
        "logical_stream_id": "additional-l2-v1", "stream_id": "s3"}


def test_three_to_one_to_three_seams_preserve_only_named_supplement_and_real_clocks(tmp_path):
    first = seam_run(tmp_path / "one", layout_day(tmp_path, "2025-08-01", 3), "2025-08-01", 3,
                     extra=capture(tmp_path, at=_day_bounds("2025-08-01")[1]-1_000_000))
    previous = copy.deepcopy(first["final_state"])
    old_supplement = previous["state"]["streams"][3]
    second_raw = layout_day(tmp_path, "2025-08-02", 1)
    second = seam_run(tmp_path / "two", second_raw, "2025-08-02", 1,
                      previous=previous, previous_slot="s3")
    assert first["final_state"] == previous
    assert second["initial_state"]["state"]["streams"][1] == old_supplement
    assert second["initial_state"]["state"]["streams"][0]["initialized"] is False
    assert second["seam_transition"]["opening_view"] == "carried_supplement"
    assert second["seam_transition"]["source_observation_created"] is False
    assert pq.read_table(second["path"]).replace_schema_metadata(None).equals(
        pq.read_table(second_raw).replace_schema_metadata(None))
    clock = pq.read_table(second["stats"]["normalized"]["clock"]["path"],
                          columns=["timestamp", "last_observation_timestamp_us", "observation_age_us"])
    assert clock["last_observation_timestamp_us"][0].as_py() == old_supplement["observed_us"]
    assert clock["observation_age_us"][1].as_py() > clock["observation_age_us"][0].as_py()
    third = seam_run(tmp_path / "three", layout_day(tmp_path, "2025-08-03", 3), "2025-08-03", 3,
                     previous=second["final_state"], previous_slot="s1")
    assert third["initial_state"]["state"]["streams"][3] == old_supplement
    assert all(not row["initialized"] for row in third["initial_state"]["state"]["streams"][:3])
    assert third["seam_transition"]["opening_view"] == "unknown_until_observation"
    assert third["initial_state"]["state"]["global_observed_us"] == second["final_state"]["state"]["global_observed_us"]
    assert third["final_state"]["supplementation"]["stream_id"] == "s3"
    assert third["stats"]["future_fill_violations"] == 0


def test_seam_no_base_initial_and_no_supplement_is_missing_rows_not_zero_fresh_clock(tmp_path):
    first = seam_run(tmp_path / "one", layout_day(tmp_path, "2025-08-01", 3), "2025-08-01", 3)
    second = seam_run(tmp_path / "two", layout_day(tmp_path, "2025-08-02", 1), "2025-08-02", 1,
                      previous=first["final_state"], previous_slot="s3")
    assert second["seam_transition"]["opening_view"] == "unknown_until_observation"
    assert second["initial_state"]["state"]["global_observed_us"] == first["final_state"]["state"]["global_observed_us"]
    assert second["initial_state"]["state"]["selected_stream"] == -1
    clock = pq.read_table(second["stats"]["normalized"]["clock"]["path"],
                          columns=["timestamp", "last_observation_timestamp_us"])
    assert pc.min(clock["last_observation_timestamp_us"]).as_py() > 0
    assert clock["timestamp"][0].as_py() * 1000 > _day_bounds("2025-08-02")[0]
    assert second["stats"]["future_fill_violations"] == 0


def test_second_cohort_crosses_already_bound_four_two_four_layouts(tmp_path):
    first = seam_run(tmp_path / "first", layout_day(tmp_path, "2025-08-01", 3), "2025-08-01", 3,
                     extra=capture(tmp_path, at=_day_bounds("2025-08-01")[1]-1_000_000))
    second = seam_run(tmp_path / "second", layout_day(tmp_path, "2025-08-02", 1), "2025-08-02", 1,
                      previous=first["final_state"], previous_slot="s3")
    third = seam_run(tmp_path / "third", layout_day(tmp_path, "2025-08-03", 3), "2025-08-03", 3,
                     previous=second["final_state"], previous_slot="s1")
    second_current, third_current = Path(second["path"]), Path(third["path"])
    original = {path: path.read_bytes() for path in (second_current, third_current)}
    repeated_second = seam_run(tmp_path / "next-second", second_current, "2025-08-02", 1,
                               previous=first["final_state"], previous_slot="s3")
    repeated_third = seam_run(tmp_path / "next-third", third_current, "2025-08-03", 3,
                              previous=repeated_second["final_state"], previous_slot="s1")
    for old, new, slot in ((second, repeated_second, 1), (third, repeated_third, 3)):
        assert new["seam_transition"]["current_supplement_reused"] is True
        assert new["rows"] == old["rows"]
        assert new["stats"]["future_fill_violations"] == 0
        assert pq.read_table(new["path"]).replace_schema_metadata(None).equals(
            pq.read_table(old["path"]).replace_schema_metadata(None))
        assert new["initial_state"]["state"]["streams"][slot] == old["initial_state"]["state"]["streams"][slot]
    assert all(path.read_bytes() == value for path, value in original.items())


def test_second_cohort_inherits_repaired_previous_capture_not_old_composite_state(tmp_path):
    first = seam_run(tmp_path / "first", layout_day(tmp_path, "2025-08-01", 3), "2025-08-01", 3)
    second = seam_run(tmp_path / "second", layout_day(tmp_path, "2025-08-02", 1), "2025-08-02", 1,
                      previous=first["final_state"], previous_slot="s3")
    assert not second["initial_state"]["state"]["streams"][1]["initialized"]
    repaired_first = seam_run(tmp_path / "repaired-first", Path(first["path"]), "2025-08-01", 3,
                              extra=capture(tmp_path, at=_day_bounds("2025-08-01")[1]-1_000_000))
    repaired_second = seam_run(tmp_path / "repaired-second", Path(second["path"]), "2025-08-02", 1,
                               previous=repaired_first["final_state"], previous_slot="s3")
    new_source = repaired_first["final_state"]["state"]["streams"][3]
    assert new_source["initialized"]
    assert repaired_second["initial_state"]["state"]["streams"][1] == new_source
    assert repaired_second["rows"] == second["rows"]
    assert repaired_second["stats"]["future_fill_violations"] == 0
    assert repaired_second["seam_transition"]["opening_view"] == "carried_supplement"


@pytest.mark.parametrize("change", ["legacy_unbound", "different_logical", "initial_binding_missing",
                                     "previous_binding_missing"])
def test_composite_seam_cannot_adopt_an_unproven_existing_capture(tmp_path, change):
    from data.daily_raw import _daily_supplement_seam_seed
    first = seam_run(tmp_path / "first", layout_day(tmp_path, "2025-08-01", 3), "2025-08-01", 3)
    second = seam_run(tmp_path / "second", layout_day(tmp_path, "2025-08-02", 1), "2025-08-02", 1,
                      previous=first["final_state"], previous_slot="s3")
    receipt = copy.deepcopy(second["receipt"])
    previous = copy.deepcopy(first["final_state"])
    if change == "legacy_unbound":
        receipt["supplementation_contract"].pop("logical_stream_id")
    elif change == "different_logical":
        receipt["supplementation_contract"]["logical_stream_id"] = "other-capture"
    elif change == "initial_binding_missing":
        receipt["initial_state"].pop("supplementation")
    else:
        previous.pop("supplementation")
    before = copy.deepcopy(receipt)
    with pytest.raises(ValueError, match="explicitly bound"):
        _daily_supplement_seam_seed(receipt, "2025-08-02", "s1", previous,
                                    "s3", "additional-l2-v1", 2)
    assert receipt == before


@pytest.mark.parametrize("case", ["wrong_slot", "wrong_logical", "future", "wrong_day", "no_opt_in"])
def test_seam_rejects_forged_mapping_future_clock_and_implicit_aliases(tmp_path, case):
    from data.daily_raw import _daily_supplement_seed
    current = layout_day(tmp_path, "2025-08-01", 3)
    _, previous = _daily_supplement_seed(daily_book_receipt(current), "2025-08-01", "s3", None, 2)
    previous["next_day_start_us"] = _day_bounds("2025-08-02")[0]
    previous["supplementation"] = {"logical_stream_id": "additional-l2-v1", "stream_id": "s3"}
    next_raw = layout_day(tmp_path, "2025-08-02", 1)
    kwargs = {"logical_stream_id": "additional-l2-v1", "previous_stream_id": "s3"}
    if case == "wrong_slot":
        kwargs["previous_stream_id"] = "s0"
    elif case == "wrong_logical":
        kwargs["logical_stream_id"] = "different-l2"
    elif case == "future":
        previous["state"]["streams"][3]["observed_us"] = previous["next_day_start_us"]+1
    elif case == "wrong_day":
        previous["next_day_start_us"] += 86_400_000_000
    else:
        kwargs = {}
        previous.pop("supplementation")
    before = copy.deepcopy(previous)
    with pytest.raises(ValueError):
        supplement_daily_book_day(next_raw, None, tmp_path / "bad.parquet", "2025-08-02",
            stream_id="s1", previous_state=previous, normalized_root=tmp_path / "derived", minimum_levels=2, **kwargs)
    assert previous == before
    assert not (tmp_path / "bad.parquet").exists()


def test_seam_failure_is_atomic_for_previous_continuation_and_current_rows(tmp_path, monkeypatch):
    import data.normalize_tardis_orderbook as normalizer
    first = seam_run(tmp_path / "one", layout_day(tmp_path, "2025-08-01", 3), "2025-08-01", 3,
                     extra=capture(tmp_path))
    previous = copy.deepcopy(first["final_state"])
    current = layout_day(tmp_path, "2025-08-02", 1)
    before = current.read_bytes()
    monkeypatch.setattr(normalizer, "iter_fused_book_batches",
                        lambda *a, **k: (_ for _ in ()).throw(ValueError("seam injected failure")))
    with pytest.raises(ValueError, match="injected"):
        seam_run(tmp_path / "failed", current, "2025-08-02", 1, previous=previous, previous_slot="s3")
    assert previous == first["final_state"] and current.read_bytes() == before
    assert not (tmp_path / "failed/raw.parquet").exists()


def test_seam_preserves_current_initial_base_instead_of_aliasing_previous_base(tmp_path):
    from data.daily_raw import _daily_supplement_seed, _daily_supplement_seam_seed
    previous_raw = layout_day(tmp_path, "2025-08-01", 1)
    _, previous = _daily_supplement_seed(daily_book_receipt(previous_raw), "2025-08-01", "s1", None, 2)
    boundary = _day_bounds("2025-08-02")[0]
    previous["next_day_start_us"] = boundary
    current = layout_day(tmp_path, "2025-08-02", 3)
    receipt = daily_book_receipt(current)
    _, initial = _daily_supplement_seed(receipt, "2025-08-02", "s3", None, 2)
    initial["state"]["streams"].pop()
    initial["state"]["stream_count"] = 3
    initial["stream_priority"] = receipt["stream_priority"]
    initial["pending_rows"].pop("s3", None)
    initial["pending_observations"].pop("s3", None)
    receipt["initial_state"] = initial
    before = copy.deepcopy(initial)
    _, state, note = _daily_supplement_seam_seed(receipt, "2025-08-02", "s3", previous,
                                                "s1", "additional-l2-v1", 2)
    assert state["state"]["streams"][:3] == before["state"]["streams"]
    assert state["stream_priority"]["preferred_index"] == 1
    assert note["base_seed"] == "current_canonical_initial"
    assert initial == before


def test_seam_deferred_observations_keep_real_clock_and_remap_only_supplement_slot(tmp_path):
    from data.daily_raw import _daily_supplement_seed, _daily_supplement_seam_seed
    first = layout_day(tmp_path, "2025-08-01", 3)
    _, previous = _daily_supplement_seed(daily_book_receipt(first), "2025-08-01", "s3", None, 2)
    boundary = _day_bounds("2025-08-02")[0]
    previous["next_day_start_us"] = boundary
    stamp = boundary+500_000
    row = dict(exchange="binance-futures", symbol="BTCUSDC", timestamp=stamp,
               local_timestamp=stamp+1000, is_snapshot=True, side="bid", price="100", amount="1",
               stream_id="s3", stream_priority=-4, queue_rebase=True, observation_only=False,
               top_only=False, native_sequence=False, observed_timestamp_us=stamp,
               original_timestamp_us=stamp)
    previous["pending_observations"]["s3"] = pa.Table.from_pylist([row], schema=DAILY_BOOK_SCHEMA).to_pylist()
    previous["pending_rows"]["s3"] = [[stamp, stamp, stamp+1000, 1, 0, -1, -1, -1, -1, -1,
                                           0, 100*100_000_000, 100_000_000, 0]]
    before = copy.deepcopy(previous)
    second = layout_day(tmp_path, "2025-08-02", 1)
    _, seed, _ = _daily_supplement_seam_seed(daily_book_receipt(second), "2025-08-02", "s1", previous,
                                           "s3", "additional-l2-v1", 2)
    copied = seed["pending_observations"]["s1"][0]
    assert copied["stream_id"] == "s1" and copied["stream_priority"] == -2
    assert copied["timestamp"] == copied["observed_timestamp_us"] == stamp
    assert seed["pending_rows"]["s1"] == before["pending_rows"]["s3"]
    assert seed["state"]["streams"][1]["observed_us"] == 0
    assert previous == before


def test_missing_deferred_identity_cannot_be_dropped_at_seam(tmp_path):
    from data.daily_raw import _daily_supplement_seed, _daily_supplement_seam_seed
    first = layout_day(tmp_path, "2025-08-01", 3)
    _, previous = _daily_supplement_seed(daily_book_receipt(first), "2025-08-01", "s3", None, 2)
    previous["next_day_start_us"] = _day_bounds("2025-08-02")[0]
    previous["pending_rows"]["s3"] = [[1]*14]
    second = layout_day(tmp_path, "2025-08-02", 1)
    with pytest.raises(ValueError, match="original deferred observations"):
        _daily_supplement_seam_seed(daily_book_receipt(second), "2025-08-02", "s1", previous,
                                    "s3", "additional-l2-v1", 2)


def test_last_original_base_slot_cannot_impersonate_an_added_stream(tmp_path):
    from data.daily_raw import _daily_supplement_seed, _daily_supplement_seam_seed
    first = layout_day(tmp_path, "2025-08-01", 3)
    _, previous = _daily_supplement_seed(daily_book_receipt(first), "2025-08-01", "s3", None, 2)
    previous["state"]["streams"].pop()
    previous["state"]["stream_count"] = 3
    previous["stream_priority"] = book_stream_priority(["s0", "s1", "s2"], preferred_index=1)
    previous["next_day_start_us"] = _day_bounds("2025-08-02")[0]
    second = layout_day(tmp_path, "2025-08-02", 1)
    with pytest.raises(ValueError, match="one/three base-slot"):
        _daily_supplement_seam_seed(daily_book_receipt(second), "2025-08-02", "s1", previous,
                                    "s2", "additional-l2-v1", 2)


def test_parallel_supplement_triplet_bytes_and_cross_day_state_match_serial(tmp_path):
    current, extra = current_day(tmp_path), capture(tmp_path)
    next_current = current_day(tmp_path, "2025-08-02", with_top=False)
    input_bytes = current.read_bytes(), extra.read_bytes(), next_current.read_bytes()
    first = [run(tmp_path / f"first-{workers}", current, extra, write_workers=workers) for workers in (1, 2)]
    second = [run(tmp_path / f"second-{workers}", next_current, None, "2025-08-02",
                  previous_state=result["final_state"], previous_top_state=result["top_final_state"],
                  write_workers=workers) for workers, result in zip((1, 2), first, strict=True)]
    for pair in (first, second):
        assert pair[0]["sha256"] == pair[1]["sha256"]
        assert pair[0]["final_state"] == pair[1]["final_state"]
        assert pair[0]["top_final_state"] == pair[1]["top_final_state"]
        for kind in ("bbo", "l2", "clock"):
            assert pair[0]["stats"]["normalized"][kind]["sha256"] == pair[1]["stats"]["normalized"][kind]["sha256"]
        runtime = pair[1]["stats"]["write_pipeline"]
        assert runtime["async_batches"] > 0 and runtime["pending_batches"] == 0
        assert runtime["closed"] and not runtime["failure_observed"]
        assert runtime["peak_pending_bytes"] <= runtime["max_pending_bytes"]
    assert second[1]["initial_state"] == first[1]["final_state"]
    assert input_bytes == (current.read_bytes(), extra.read_bytes(), next_current.read_bytes())


def test_parallel_failure_after_source_updates_keeps_previous_continuation(tmp_path, monkeypatch):
    from data.normalize_tardis_orderbook import _ParquetPairWriter
    first = run(tmp_path / "first", current_day(tmp_path), capture(tmp_path), write_workers=2)
    current = current_day(tmp_path, "2025-08-02")
    previous, original = copy.deepcopy(first["final_state"]), current.read_bytes()
    def fail(self, tables):
        self.bbo_writer.write_table(tables[0])
        raise OSError("synthetic asynchronous successor failure")
    monkeypatch.setattr(_ParquetPairWriter, "_write_tables", fail)
    with pytest.raises(OSError, match="successor failure"):
        run(tmp_path / "failed", current, None, "2025-08-02", previous_state=previous,
            previous_top_state=first["top_final_state"], write_workers=2)
    assert previous == first["final_state"] and current.read_bytes() == original
    assert not (tmp_path / "failed/raw.parquet").exists()
