from __future__ import annotations

from pathlib import Path
import pickle
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from models.backtest_tick import simulate_tick
from models.exchange_book_replay import (
    HistoricalExchangeBookScheduler,
    HistoricalExchangeBookVisibilityScheduler,
    HistoricalMessageDeliverySchedule,
    build_configured_cooldown_policy_adapter,
)
from models.tick_data_types import (
    HistoricalBBOData,
    HistoricalExchangeBookEvent,
    HistoricalL2Data,
)

BASE_MS = 1_700_000_000_000


@pytest.mark.parametrize("native", [False, True])
def test_canonical_daily_plan_selects_sequence_capability_without_supplier_dirs(tmp_path, monkeypatch, native):
    from data_paths import daily_market_path
    from models.exchange_book_replay import PlannedExchangeBookTape

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    monkeypatch.delenv("NARROWGATE_DAILY_ORDERBOOK_ROOT", raising=False)
    day = "2026-01-01"
    path = daily_market_path(day, "BTCUSDC", "incremental_book_L2")
    path.parent.mkdir(parents=True)
    ms = 1_767_225_600_100
    frame = pd.DataFrame({"exchange": ["binance-futures"] * 3, "symbol": ["BTCUSDC"] * 3,
                          "timestamp": [ms*1000, ms*1000, (ms+100)*1000],
                          "local_timestamp": [ms*1000+10, ms*1000+10, (ms+100)*1000+10],
                          "is_snapshot": [True, True, False], "side": ["bid", "ask", "bid"],
                          "price": ["100", "101", "100"], "amount": ["1", "1", "2"],
                          "last_update_id": [100, 100, None] if native else [None]*3,
                          "final_update_id": [100, 100, 101] if native else [None]*3})
    if native:
        frame["event_type"] = ["snapshot", "snapshot", "update"]
        frame["event_time"] = [ms, ms, ms+100]
        frame["transaction_time"] = [ms, ms, ms+100]
        frame["received_time"] = [ms*1000000+10000, ms*1000000+10000, (ms+100)*1000000+10000]
        frame["quantity"] = frame["amount"]
        frame["first_update_id"] = [None, None, 101]
        frame["prev_final_update_id"] = [None, None, 100]
        frame["source_hour"] = 0
        frame["source_row"] = range(3)
        hours = []
        for hour in range(24):
            hourly = frame.copy()
            hourly["source_hour"] = hour
            for name in ("event_time", "transaction_time"):
                hourly[name] += hour * 3_600_000
            for name in ("timestamp", "local_timestamp"):
                hourly[name] += hour * 3_600_000_000
            hourly["received_time"] += hour * 3_600_000_000_000
            hours.append(hourly)
        frame = pd.concat(hours, ignore_index=True)
    frame.to_parquet(path)
    plan = {"symbol": "BTCUSDC", "days": [{"day": day, "cache_enabled": False}]}
    events = list(PlannedExchangeBookTape(plan, days=[day], symbol="BTCUSDC", tick_size=.1))
    assert len(events) == (48 if native else 2)
    assert len(events[0].levels) == 2
    assert events[1].levels == (("bid", 1000, 2.),)
    assert events[0].last_update_id == (100 if native else None)
    assert events[1].final_update_id == (101 if native else None)
    assert events[1].sequence_scope == ("exchange_sequence" if native else "provider_ordered")


def test_fused_daily_book_native_ids_are_provenance_not_native_decoder(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data_paths import daily_market_path
    from models.exchange_book_replay import PlannedExchangeBookTape

    monkeypatch.setenv("NARROWGATE_RAW_DATA_ROOT", str(tmp_path / "raw"))
    day = "2026-01-01"
    path = daily_market_path(day, "BTCUSDC", "incremental_book_L2")
    path.parent.mkdir(parents=True)
    table = pa.table({
        "exchange": ["binance-futures"] * 3, "symbol": ["BTCUSDC"] * 3,
        "timestamp": [1_767_225_600_100_000] * 2 + [1_767_225_600_200_000],
        "local_timestamp": [1_767_225_600_110_000] * 2 + [None],
        "is_snapshot": [True, True, False], "side": ["bid", "ask", "bid"],
        "price": ["100", "101", "100"], "amount": ["1", "1", "2"],
        "last_update_id": [100, 100, None], "final_update_id": [100, 100, 101],
    }).replace_schema_metadata({b"narrowgate.book_fusion": b"reconstructed_fusion.v1"})
    pq.write_table(table, path)
    events = list(PlannedExchangeBookTape({"symbol": "BTCUSDC"}, days=[day], symbol="BTCUSDC", tick_size=.1))
    assert len(events) == 2
    assert all(event.sequence_scope == "provider_ordered" for event in events)
    assert all(event.final_update_id is None for event in events)
    assert events[1].local_receive_ts_ns == 0
    assert all(event.fusion_reason == "unknown_reconstruction" for event in events)
    assert all(event.source_observed_ts_ns == 0 for event in events)
    scheduler = HistoricalExchangeBookScheduler(events, strict_sequence=False)
    step = scheduler.advance_to(events[-1].exchange_ts_ns)
    assert step.snapshot_reset and not step.level_changes
    assert scheduler.lookup("bid", 999).status == "unknown"


def _reconstructed_tape(tmp_path, messages, *, metadata_key=b"narrowgate.book_fusion",
                        metadata_value=b"reconstructed_fusion.v1"):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from models.exchange_book_replay import TardisExchangeBookTape

    rows = []
    for offset, snapshot, reason, source, observed, levels in messages:
        for side, price, quantity in levels:
            rows.append({"exchange": "binance-futures", "symbol": "BTCUSDC",
                         "timestamp": BASE_MS * 1000 + offset,
                         "local_timestamp": BASE_MS * 1000 + offset + 1000,
                         "is_snapshot": snapshot, "side": side, "price": str(price),
                         "amount": str(quantity), "source_id": source, "fusion_reason": reason,
                         "source_observed_timestamp_us": BASE_MS * 1000 + observed})
    path = tmp_path / "reconstructed.parquet"
    pq.write_table(pa.Table.from_pylist(rows).replace_schema_metadata({metadata_key: metadata_value}), path)
    return TardisExchangeBookTape([path], symbol="BTCUSDC", tick_size=1.)


@pytest.mark.parametrize("reason", ["source_switch", "source_snapshot_reset"])
@pytest.mark.parametrize("full_snapshot", [False, True])
def test_reconstructed_rebase_updates_state_without_cancel_ahead_evidence(tmp_path, reason, full_snapshot):
    initial = (("bid", 100, 1.), ("bid", 98, 2.), ("ask", 101, 1.), ("ask", 103, 2.))
    replacement = (("bid", 100, 3.), ("bid", 97, 4.), ("ask", 101, 1.))
    changes = replacement if full_snapshot else ((*replacement, ("bid", 98, 0.), ("ask", 103, 0.)))
    tape = _reconstructed_tape(tmp_path, [
        (1000, True, "source_update", "cryptohft", 1000, initial),
        (2000, full_snapshot, reason, "tardis", 2000, changes),
        (3000, False, "source_update", "tardis", 3000, (("bid", 100, 0.),)),
    ])
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    initial_step = scheduler.advance_to(BASE_MS * 1_000_000 + 1_000_000)
    assert initial_step.snapshot_reset and not initial_step.level_changes
    assert scheduler.lookup("bid", 99).status == "unknown"
    assert scheduler.preview_at(BASE_MS * 1_000_000 + 2_000_000).snapshot_or_gap
    rebase = scheduler.advance_to(BASE_MS * 1_000_000 + 2_000_000)
    assert rebase.snapshot_reset and not rebase.level_changes
    assert scheduler.lookup("bid", 100).quantity == 3.
    assert scheduler.lookup("bid", 97).quantity == 4.
    for side, tick in (("bid", 98), ("bid", 99), ("ask", 103)):
        lookup = scheduler.lookup(side, tick)
        assert lookup.status == "unknown" and not lookup.strict_usable
    assert "reconstructed_state_not_native_delta" in scheduler.evidence_scope
    delta = scheduler.advance_to(BASE_MS * 1_000_000 + 3_000_000)
    assert len(delta.level_changes) == 1
    assert delta.level_changes[0].quantity_before == 3.
    assert delta.level_changes[0].quantity_after == 0.
    assert scheduler.lookup("bid", 100).status == "known_zero"
    assert tape.identity()["native_delta_authority"] is False
    with pytest.raises(ValueError, match="strict exchange sequence"):
        HistoricalExchangeBookScheduler(tape).advance_to(BASE_MS * 1_000_000 + 3_000_000)


def test_reconstructed_carried_observation_clock_survives_reader_and_checkpoint(tmp_path):
    from models.exchange_book_replay import ReconstructedExchangeBookEvent

    levels = (("bid", 100, 1.), ("ask", 101, 1.))
    tape = _reconstructed_tape(tmp_path, [
        (1000, True, "carried_opening_snapshot", "cryptohft", -500_000, levels),
        (2000, False, "observation_refresh", "cryptohft", 1500, (("bid", 100, 1.),)),
    ])
    events = list(tape)
    assert isinstance(events[0], ReconstructedExchangeBookEvent)
    assert events[0].source_id == "cryptohft"
    assert events[0].fusion_reason == "carried_opening_snapshot"
    assert events[0].event_time_ns == events[0].source_observed_ts_ns < events[0].exchange_ts_ns
    assert events[0].exchange_ts_source == "unknown"
    scheduler = HistoricalExchangeBookScheduler(events, strict_sequence=False)
    scheduler.advance_to(events[0].exchange_ts_ns)
    assert scheduler.last_source_observed_ts_ns == events[0].source_observed_ts_ns
    state = pickle.loads(pickle.dumps(scheduler.checkpoint()))
    restored = HistoricalExchangeBookScheduler.from_checkpoint(state, [])
    step = restored.advance_to(events[1].exchange_ts_ns)
    assert not step.level_changes and not step.snapshot_reset
    assert restored.last_source_observed_ts_ns == events[1].source_observed_ts_ns
    assert isinstance(step.source_events[0], ReconstructedExchangeBookEvent)


def test_fusion_reasons_and_source_identity_split_same_timestamp_messages(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import data.normalize_tardis_orderbook as normalizer

    tape = _reconstructed_tape(tmp_path, [
        (1000, True, "source_update", "cryptohft", 1000, (("bid", 100, 1.), ("ask", 101, 1.))),
        (2000, False, "source_update", "cryptohft", 2000, (("bid", 100, 2.),)),
        (2000, False, "source_switch", "tardis", 2000, (("bid", 100, 3.),)),
    ])
    def small_batches(path):
        parquet = pq.ParquetFile(path)
        return pa.RecordBatchReader.from_batches(parquet.schema_arrow, parquet.iter_batches(batch_size=1))
    monkeypatch.setattr(normalizer, "_open_csv", small_batches)
    events = list(tape)
    assert len(events) == 3 and len(events[0].levels) == 2
    assert [e.fusion_reason for e in events] == ["source_update", "source_update", "source_switch"]
    assert [e.source_id for e in events] == ["cryptohft", "cryptohft", "tardis"]


@pytest.mark.parametrize("field,value", [("source_observed_timestamp_us", BASE_MS * 1000 + 2000),
                                          ("source_observed_timestamp_us", None),
                                          ("fusion_reason", "made_up"), ("source_id", None)])
def test_fused_source_annotations_cannot_invent_observation_or_reason(tmp_path, field, value):
    import pyarrow as pa
    import pyarrow.parquet as pq

    tape = _reconstructed_tape(tmp_path, [
        (1000, True, "source_update", "tardis", 1000, (("bid", 100, 1.), ("ask", 101, 1.))),
    ])
    path = tape.paths[0]
    table = pq.read_table(path)
    table = table.set_column(table.schema.get_field_index(field), field,
                             pa.array([value] * len(table), type=table.schema.field(field).type))
    pq.write_table(table, path)
    with pytest.raises(ValueError, match="reconstructed"):
        list(tape)


@pytest.mark.parametrize("metadata_key", [b"narrowgate.book_fusion", b"narrowgate.schema", b"narrowgate.book_union"])
def test_observed_union_cannot_enter_legacy_single_provider_reader(tmp_path, metadata_key):
    from models.exchange_book_replay import PlannedExchangeBookTape

    tape = _reconstructed_tape(tmp_path, [
        (1000, True, "source_update", "tardis", 1000, (("bid", 100, 1.), ("ask", 101, 1.))),
    ], metadata_key=metadata_key, metadata_value=b"observed_union.v1")
    with pytest.raises(ValueError, match="source-aware adapter|union source schema"):
        list(tape)
    with pytest.raises(ValueError, match="source-aware adapter|union source schema"):
        list(PlannedExchangeBookTape({"symbol": "BTCUSDC", "days": [
            {"day": "2026-01-01", "raw_file": str(tape.paths[0])},
        ]}, days=["2026-01-01"], symbol="BTCUSDC", tick_size=1.))


UNION_START_US = 1_767_225_600_000_000
UNION_LEVELS = (("bid", 100, 1.), ("bid", 99, 1.), ("ask", 101, 1.), ("ask", 102, 1.))


def _union_message(source, t, levels=UNION_LEVELS, *, snapshot=False, identifier=101, previous=100, observed=None):
    return {"source": source, "t": t, "levels": levels, "snapshot": snapshot,
            "identifier": identifier, "previous": previous, "observed": t if observed is None else observed}


def _observed_tape(tmp_path, messages, *, seed=None, terminal=None, priority=None):
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data.daily_raw import UNION_BOOK_SCHEMA
    from models.exchange_book_replay import TardisExchangeBookTape

    rows = []
    for m in messages:
        native = m.get("native", m["source"] == "cryptohft")
        for side, price, quantity in m["levels"]:
            rows.append({"exchange": "binance-futures", "symbol": "BTCUSDC",
                "timestamp": UNION_START_US + m["t"], "source_timestamp_us": UNION_START_US + m["observed"],
                "local_timestamp": UNION_START_US + m["t"] + 1000,
                "source_observed_timestamp_us": UNION_START_US + m["observed"],
                "is_snapshot": m["snapshot"], "side": side, "price": str(price), "amount": str(quantity),
                "quantity": str(quantity), "source_id": m["source"], "source_native_sequence": native,
                "fusion_reason": "source_observation", "event_type": "snapshot" if m["snapshot"] else "update",
                "event_time": (UNION_START_US + m["observed"]) // 1000 if native else None,
                "transaction_time": (UNION_START_US + m["observed"]) // 1000 if native else None,
                "first_update_id": m["identifier"] if native and not m["snapshot"] else None,
                "final_update_id": m["identifier"] if native else None,
                "last_update_id": m["identifier"] if native and m["snapshot"] else None,
                "prev_final_update_id": m["previous"] if native and not m["snapshot"] else None})
    receipt = {"initial_continuation": seed, "stats": {"continuation": terminal}}
    if priority is not None:
        receipt["stream_priority"] = priority
    path = tmp_path / "union.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=UNION_BOOK_SCHEMA).replace_schema_metadata({
        b"narrowgate.book_fusion": b"observed_union.v1", b"narrowgate.day": b"2026-01-01",
        b"narrowgate.fusion_receipt": json.dumps(receipt).encode()}), path)
    return TardisExchangeBookTape([path], symbol="BTCUSDC", tick_size=1.)


def _native_union_state(messages, *, preferred=1):
    cpp = pytest.importorskip("narrowgate_cpp")
    rows, sources = [], []
    for m in messages:
        native = m["source"] == "cryptohft"
        for side, price, quantity in m["levels"]:
            rows.append([UNION_START_US + m["t"], UNION_START_US + m["observed"],
                UNION_START_US + m["t"] + 1000, int(m["snapshot"]), int(native),
                m["identifier"] if native and not m["snapshot"] else -1,
                m["identifier"] if native else -1,
                m["previous"] if native and not m["snapshot"] else -1,
                m["identifier"] if native and m["snapshot"] else -1,
                (UNION_START_US + m["observed"]) // 1000 if native else -1,
                int(side == "ask"), round(price * 1e8), round(quantity * 1e8), len(rows)])
            sources.append(0 if native else 1)
    kernel = cpp.BookFusion(2, preferred, 2)
    kernel.configure_raw_diff_output(False)
    kernel.push_rows(np.asarray(sources, dtype=np.int64), np.asarray(rows, dtype=np.int64))
    kernel.finish()
    return kernel.continuation()


def test_observed_union_source_messages_stay_atomic_across_batches_and_tie(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import data.normalize_tardis_orderbook as normalizer
    from models.exchange_book_replay import ObservedUnionExchangeBookEvent, PlannedExchangeBookTape

    messages = [_union_message("cryptohft", 1000, snapshot=True, identifier=100),
                _union_message("tardis", 1000, tuple((s, p, 2.) for s, p, _ in UNION_LEVELS), snapshot=True),
                _union_message("cryptohft", 2000, (("bid", 100, 3.),)),
                _union_message("tardis", 2000, (("bid", 100, 4.),))]
    tape = _observed_tape(tmp_path, messages)
    def small(path):
        p = pq.ParquetFile(path)
        return pa.RecordBatchReader.from_batches(p.schema_arrow, p.iter_batches(batch_size=1))
    monkeypatch.setattr(normalizer, "_open_csv", small)
    events = list(tape)
    assert len(events) == 4 and all(isinstance(e, ObservedUnionExchangeBookEvent) for e in events)
    assert [len(e.levels) for e in events] == [4, 4, 1, 1]
    assert events[0].last_update_id == 100 and events[1].last_update_id is None
    planned = PlannedExchangeBookTape({"symbol": "BTCUSDC", "days": [
        {"day": "2026-01-01", "raw_file": str(tape.paths[0])}]}, days=["2026-01-01"], symbol="BTCUSDC", tick_size=1.)
    scheduler = HistoricalExchangeBookScheduler(planned, strict_sequence=False, union_minimum_levels=2)
    first = scheduler.advance_to((UNION_START_US + 1000) * 1000)
    assert first.snapshot_reset and not first.level_changes
    assert scheduler.lookup("bid", 100).quantity == 2.
    second = scheduler.advance_to((UNION_START_US + 2000) * 1000)
    assert len(second.level_changes) == 1
    assert (second.level_changes[0].quantity_before, second.level_changes[0].quantity_after) == (2., 4.)
    assert scheduler._union_selected == "tardis"
    assert scheduler.book.bid_levels is scheduler._union_sources["tardis"].scheduler.book.bid_levels
    assert "observed_source_union" == tape.identity()["source"]
    with pytest.raises(ValueError, match="strict global exchange sequence"):
        HistoricalExchangeBookScheduler(tape, union_minimum_levels=2).advance_to(events[-1].exchange_ts_ns)


def test_anonymous_union_priority_is_persisted_isolates_streams_and_protects_queue(tmp_path):
    from data.daily_raw import book_stream_priority

    priority = book_stream_priority(["stream-z", "stream-a"], preferred_index=0)
    two = tuple((s, p, 2.) for s, p, _ in UNION_LEVELS)
    tape = _observed_tape(tmp_path, [
        _union_message("stream-z", 1000, snapshot=True),
        _union_message("stream-a", 1000, two, snapshot=True),
        _union_message("stream-a", 2000, (("bid", 100, 7.),)),
        _union_message("stream-z", 3000, (("bid", 100, 3.),)),
        _union_message("stream-z", 4000, (("bid", 100, 3.),)),
        _union_message("stream-z", 5000, (("bid", 100, 4.), ("bid", 97, 1.),
                                            ("ask", 101, 4.), ("ask", 104, 1.)), snapshot=True),
    ], priority=priority)
    events = list(tape)
    assert [event.stream_priority for event in events[:2]] == [1, -2]
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    def advance(offset):
        return scheduler.advance_to((UNION_START_US + offset) * 1000)
    assert advance(1000).snapshot_reset
    assert scheduler._union_selected == "stream-z"
    assert scheduler.lookup("bid", 100).quantity == 1.
    assert advance(2000).snapshot_reset
    assert scheduler.lookup("bid", 100).quantity == 7.
    switch = advance(3000)
    assert switch.snapshot_reset and not switch.level_changes
    assert scheduler.lookup("ask", 101).quantity == 1.  # Other stream's 2 is not inherited.
    assert not advance(4000).level_changes  # Fresh unchanged observation is not queue progress.
    reset = advance(5000)
    assert reset.snapshot_reset and not reset.level_changes
    assert scheduler.lookup("bid", 99).status == "unknown"
    restored = HistoricalExchangeBookScheduler.from_checkpoint(scheduler.checkpoint(), [])
    assert restored._union_stream_ranks == scheduler._union_stream_ranks
    assert restored.top_levels(2) == scheduler.top_levels(2)


def test_anonymous_union_requires_priority_and_rejects_priority_drift(tmp_path):
    from dataclasses import replace
    from data.daily_raw import book_stream_priority

    messages = [_union_message("stream-0", 1000, snapshot=True),
                _union_message("stream-0", 2000, (("bid", 100, 2.),))]
    with pytest.raises(ValueError, match="not declared"):
        list(_observed_tape(tmp_path, messages))
    tape = _observed_tape(tmp_path, messages, priority=book_stream_priority(["stream-0"]))
    events = list(tape)
    events[1] = replace(events[1], stream_priority=events[1].stream_priority - 1)
    with pytest.raises(ValueError, match="priority changed"):
        HistoricalExchangeBookScheduler(events, strict_sequence=False, union_minimum_levels=2).advance_to(events[-1].exchange_ts_ns)


def test_anonymous_reconstructed_reset_and_observation_refresh_are_not_queue_flow(tmp_path):
    tape = _reconstructed_tape(tmp_path, [
        (1000, True, "source_update", "stream-0", 1000, UNION_LEVELS),
        (2000, False, "source_switch", "stream-1", 2000, (("bid", 100, 3.),)),
        (3000, False, "observation_refresh", "stream-1", 3000, (("bid", 100, 3.),)),
    ])
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    scheduler.advance_to(BASE_MS * 1_000_000 + 1_000_000)
    switch = scheduler.advance_to(BASE_MS * 1_000_000 + 2_000_000)
    assert switch.snapshot_reset and not switch.level_changes
    refresh = scheduler.advance_to(BASE_MS * 1_000_000 + 3_000_000)
    assert not refresh.snapshot_reset and not refresh.level_changes
    assert scheduler.last_source_observed_ts_ns == BASE_MS * 1_000_000 + 3_000_000


def test_old_union_without_priority_footer_preserves_three_slot_ties(tmp_path):
    def snapshot(source, quantity):
        return _union_message(source, 1000, tuple((s, p, quantity) for s, p, _ in UNION_LEVELS),
                              snapshot=True, identifier=100)
    for sources, expected in [(["canonical", "cryptohft"], "cryptohft"),
                              (["canonical", "cryptohft", "tardis"], "tardis")]:
        tape = _observed_tape(tmp_path, [snapshot(source, index + 1.) for index, source in enumerate(sources)])
        scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
        scheduler.advance_to((UNION_START_US + 1000) * 1000)
        assert scheduler._union_selected == expected


def test_old_seed_kernel_preference_is_used_without_new_priority_footer(tmp_path):
    earlier = [_union_message("cryptohft", -2000, snapshot=True, identifier=100),
               _union_message("tardis", -2000, tuple((s, p, 2.) for s, p, _ in UNION_LEVELS), snapshot=True)]
    seed = {"symbol": "BTCUSDC", "source_ids": ["cryptohft", "tardis"],
            "next_day_start_us": UNION_START_US, "kernel": _native_union_state(earlier, preferred=0)}
    messages = [_union_message("cryptohft", 1000, (("bid", 100, 3.),)),
                _union_message("tardis", 1000, (("bid", 100, 4.),))]
    tape = _observed_tape(tmp_path, messages, seed=seed)
    assert list(tape)[0].stream_priority == 1
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    scheduler.advance_to((UNION_START_US + 1000) * 1000)
    assert scheduler._union_selected == "cryptohft"
    assert scheduler.lookup("bid", 100).quantity == 3.


@pytest.mark.parametrize("with_seed", [False, True])
def test_daily_book_physical_projection_preserves_union_events_and_seed(tmp_path, with_seed):
    import json
    import pyarrow.parquet as pq
    from data.daily_raw import DAILY_BOOK_SCHEMA, daily_book_receipt, unify_orderbook_day
    from models.exchange_book_replay import TardisExchangeBookTape

    earlier = [_union_message("cryptohft", -2000, snapshot=True, identifier=100),
               _union_message("tardis", -2000, snapshot=True)]
    seed = {"symbol": "BTCUSDC", "source_ids": ["cryptohft", "tardis"],
            "next_day_start_us": UNION_START_US, "kernel": _native_union_state(earlier, preferred=0)} if with_seed else None
    messages = ([] if with_seed else [_union_message("cryptohft", 1000, snapshot=True, identifier=100),
                                      _union_message("tardis", 1000, snapshot=True)])
    messages += [_union_message("cryptohft", 2000, (("bid", 100, 3.),)),
                 _union_message("tardis", 3000, (("bid", 100, 4.),))]
    old = _observed_tape(tmp_path, messages, seed=seed)
    output = tmp_path / "daily.parquet"
    result = unify_orderbook_day(old.paths[0], output, "2026-01-01")
    assert pq.ParquetFile(output).schema_arrow.remove_metadata() == DAILY_BOOK_SCHEMA
    receipt = daily_book_receipt(output)
    encoded = json.dumps(receipt)
    for word in ("tardis", "cryptohft", "source_id", "fusion_reason", "raw_diff", "legacy"):
        assert word not in encoded
    assert result["rows"] == pq.ParquetFile(old.paths[0]).metadata.num_rows
    new = TardisExchangeBookTape([output], symbol="BTCUSDC", tick_size=1.)
    left = HistoricalExchangeBookScheduler(old, strict_sequence=False, union_minimum_levels=2)
    right = HistoricalExchangeBookScheduler(new, strict_sequence=False, union_minimum_levels=2)
    assert right.last_source_observed_ts_ns == left.last_source_observed_ts_ns
    for timestamp in (1000, 2000, 3000):
        a = left.advance_to((UNION_START_US + timestamp) * 1000)
        b = right.advance_to((UNION_START_US + timestamp) * 1000)
        assert (a.snapshot_reset, a.level_changes) == (b.snapshot_reset, b.level_changes)
        assert left.top_levels(2) == right.top_levels(2)
        assert left.last_source_observed_ts_ns == right.last_source_observed_ts_ns


def test_daily_book_selected_patches_remain_one_stream_and_queue_rebase(tmp_path):
    from datetime import datetime, timezone
    import pyarrow.parquet as pq
    from data.daily_raw import unify_orderbook_day
    from models.exchange_book_replay import TardisExchangeBookTape

    old = _reconstructed_tape(tmp_path, [
        (1000, True, "source_update", "cryptohft", 1000, UNION_LEVELS),
        (2000, False, "source_switch", "tardis", 2000, (("bid", 100, 3.),)),
        (3000, False, "observation_refresh", "tardis", 2500, (("bid", 100, 3.),)),
        (4000, False, "source_update", "cryptohft", 4000, (("bid", 100, 2.),)),
    ])
    day = datetime.fromtimestamp(BASE_MS / 1000, timezone.utc).date().isoformat()
    output = tmp_path / "daily.parquet"
    unify_orderbook_day(old.paths[0], output, day)
    table = pq.read_table(output)
    assert set(table["stream_id"].to_pylist()) == {"s0"}
    assert table["original_timestamp_us"].null_count == len(table)
    assert not any(table["native_sequence"].to_pylist())
    new = TardisExchangeBookTape([output], symbol="BTCUSDC", tick_size=1.)
    left = HistoricalExchangeBookScheduler(old, strict_sequence=False)
    right = HistoricalExchangeBookScheduler(new, strict_sequence=False, union_minimum_levels=2)
    for offset in (1000, 2000, 3000, 4000):
        a = left.advance_to(BASE_MS * 1000_000 + offset * 1000)
        b = right.advance_to(BASE_MS * 1000_000 + offset * 1000)
        assert (a.snapshot_reset, a.level_changes) == (b.snapshot_reset, b.level_changes)
        assert left.top_levels(2) == right.top_levels(2)
        assert left.last_source_observed_ts_ns == right.last_source_observed_ts_ns


def test_daily_book_unified_normalizer_preserves_union_seed_and_ages(tmp_path):
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data.daily_raw import unify_orderbook_day
    from data.normalize_tardis_orderbook import iter_fused_book_batches, reconstruct_l2

    earlier = [_union_message("cryptohft", -2000, snapshot=True, identifier=100),
               _union_message("tardis", -2000, snapshot=True)]
    seed = {"symbol": "BTCUSDC", "source_ids": ["cryptohft", "tardis"],
            "next_day_start_us": UNION_START_US, "kernel": _native_union_state(earlier, preferred=0)}
    old = _observed_tape(tmp_path, [_union_message("cryptohft", 2000, (("bid", 100, 3.),))], seed=seed)
    output = tmp_path / "daily.parquet"
    unify_orderbook_day(old.paths[0], output, "2026-01-01")
    table = pq.read_table(output)
    top = table.to_pylist()[0]
    top.update(timestamp=UNION_START_US + 6001000, observed_timestamp_us=UNION_START_US + 6001000,
               original_timestamp_us=UNION_START_US + 6001000, local_timestamp=UNION_START_US + 6002000,
               stream_id="t0", stream_priority=0,
               top_only=True, observation_only=True, native_sequence=False, is_snapshot=False,
               queue_rebase=False, side="bid", price="500.0")
    top_ask = {**top, "side": "ask", "price": "501.0"}
    expanded = pa.concat_tables([table, pa.Table.from_pylist([top, top_ask], schema=table.schema)])
    metadata = {key: value for key, value in pq.ParquetFile(output).metadata.metadata.items() if key != b"ARROW:schema"}
    receipt = json.loads(metadata[b"narrowgate.book_receipt"])
    receipt.update(output_rows=len(expanded), top_rows=2)
    metadata[b"narrowgate.book_receipt"] = json.dumps(receipt).encode()
    pq.write_table(expanded.replace_schema_metadata(metadata), output)
    from models.exchange_book_replay import TardisExchangeBookTape
    events = list(TardisExchangeBookTape([output], symbol="BTCUSDC", tick_size=1.))
    assert len(events) == 1 and events[0].source_observed_ts_ns == (UNION_START_US + 2000) * 1000
    batches, stats = iter_fused_book_batches({"canonical": output}, "2026-01-01",
        observed_union=True, normalized_root=tmp_path / "normalized", minimum_levels=2,
        output_start_us=UNION_START_US, output_end_us=UNION_START_US + 7000000)
    list(batches)
    assert stats["status"] == "COMPLETED"
    clock = pq.read_table(stats["normalized"]["clock"]["path"]).to_pydict()
    observations = clock["last_observation_timestamp_us"]
    assert max(observations) == UNION_START_US + 2000
    assert clock["observation_age_us"][-1] > clock["observation_age_us"][1]
    assert clock["bbo_last_observation_timestamp_us"][-1] == UNION_START_US + 6001000
    assert pq.read_table(stats["normalized"]["bbo"]["path"])["best_bid"][-1].as_py() == 500.
    assert pq.read_table(stats["normalized"]["l2"]["path"])["bid_px_1"][-1].as_py() == 100.
    with pytest.raises(ValueError, match="multi-stream books"):
        reconstruct_l2(output, output_root=tmp_path / "wrong", day="2026-01-01", levels=2)


@pytest.mark.parametrize("failure", ["decode_top", "apply_top", "write_top", "publish_triplet"])
@pytest.mark.parametrize("existing_outputs", [False, True])
def test_daily_book_top_failure_does_not_publish_partial_triplet(tmp_path, monkeypatch,
                                                               failure, existing_outputs):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import data.book_top as book_top
    import data.normalize_tardis_orderbook as normalizer
    from data.daily_raw import unify_orderbook_day

    old = _observed_tape(tmp_path, [_union_message("tardis", 1000, snapshot=True)])
    raw = tmp_path / "daily.parquet"
    unify_orderbook_day(old.paths[0], raw, "2026-01-01")
    root = tmp_path / "normalized"
    targets = [root / kind / f"BTCUSDC-{kind}-2026-01-01.parquet"
               for kind in ("bbo", "l2", "clock")]
    before = {}
    for target in targets:
        target.parent.mkdir(parents=True)
        if existing_outputs:
            pq.write_table(pa.table({"existing_verified_value": [1]}), target)
            before[target] = target.read_bytes()

    def fail(*args, **kwargs):
        raise RuntimeError("injected top-publication failure")

    if failure == "decode_top":
        monkeypatch.setattr(book_top, "from_daily_book_rows", fail)
    elif failure == "apply_top":
        monkeypatch.setattr(book_top, "apply_top_observations", fail)
    elif failure == "write_top":
        original_write = pq.write_table
        def write(table, where, *args, **kwargs):
            if Path(where) == targets[2].with_suffix(".parquet.tmp"):
                fail()
            return original_write(table, where, *args, **kwargs)
        monkeypatch.setattr(pq, "write_table", write)
    else:
        original_replace = normalizer.os.replace
        def replace(source, target, *args, **kwargs):
            if Path(source) == targets[1].with_suffix(".parquet.tmp"):
                fail()
            return original_replace(source, target, *args, **kwargs)
        monkeypatch.setattr(normalizer.os, "replace", replace)

    batches, stats = normalizer.iter_fused_book_batches({"canonical": raw}, "2026-01-01",
        observed_union=True, normalized_root=root, minimum_levels=2,
        output_start_us=UNION_START_US, output_end_us=UNION_START_US + 1_000_000)
    with pytest.raises(RuntimeError, match="injected top-publication"):
        list(batches)
    assert stats["status"] == "FAILED"
    assert "normalized" not in stats
    for target in targets:
        if existing_outputs:
            assert target.read_bytes() == before[target]
        else:
            assert not target.exists()
        assert not target.with_suffix(".parquet.tmp").exists()


def test_daily_book_selected_view_final_state_is_one_anonymous_stream(tmp_path):
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data.daily_raw import daily_book_continuation, daily_book_receipt, unify_orderbook_day

    old = _reconstructed_tape(tmp_path, [(1000, True, "source_update", "tardis", 1000, UNION_LEVELS)])
    final = {"symbol": "BTCUSDC", "source_ids": ["cryptohft", "tardis"],
             "next_day_start_us": UNION_START_US + 86400000000, "kernel": _native_union_state([
                 _union_message("cryptohft", -2000, snapshot=True, identifier=100),
                 _union_message("tardis", -1000, snapshot=True)])}
    table = pq.read_table(old.paths[0])
    for field in ("timestamp", "local_timestamp", "source_observed_timestamp_us"):
        table = table.set_column(table.schema.get_field_index(field), field,
                                 pa.array([UNION_START_US + 1000] * len(table)))
    metadata = dict(table.schema.metadata)
    metadata[b"narrowgate.fusion_receipt"] = json.dumps({"stats": {"continuation": final}}).encode()
    pq.write_table(table.replace_schema_metadata(metadata), old.paths[0])
    output = tmp_path / "daily.parquet"
    unify_orderbook_day(old.paths[0], output, "2026-01-01")
    packed = daily_book_receipt(output)["final_state"]
    restored = daily_book_continuation(packed)
    assert restored["source_ids"] == ["s0"]
    assert len(restored["kernel"]["sources"]) == 1
    assert restored["kernel"]["sources"][0]["levels"] == final["kernel"]["global_levels"]
    assert restored["kernel"]["sources"][0]["last_message"][4] == 0


def test_daily_book_continuous_stream_contract_changes_preserve_age_not_queue_progress(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data.daily_raw import unify_orderbook_day
    from models.exchange_book_replay import TardisExchangeBookTape

    paths = []
    for index in range(3):
        root = tmp_path / str(index)
        root.mkdir()
        day_start = UNION_START_US + index * 86400000000
        if index == 1:
            tape = _reconstructed_tape(root, [(1000, True, "carried_opening_snapshot", "tardis", 1000, UNION_LEVELS)])
            shift = day_start - BASE_MS * 1000
        else:
            tape = _observed_tape(root, [_union_message("cryptohft", 1000, snapshot=True, identifier=100),
                                          _union_message("tardis", 1000, snapshot=True)])
            shift = index * 86400000000
        table = pq.read_table(tape.paths[0])
        for name in ("timestamp", "local_timestamp", "source_observed_timestamp_us", "source_timestamp_us"):
            if name in table.schema.names:
                values = [value + shift if value is not None else None for value in table[name].to_pylist()]
                if name == "source_observed_timestamp_us" and index == 1:
                    values = [UNION_START_US + 1000] * len(table)
                table = table.set_column(table.schema.get_field_index(name), name, pa.array(values))
        for name in ("event_time", "transaction_time"):
            if name in table.schema.names:
                table = table.set_column(table.schema.get_field_index(name), name,
                    pa.array([value + shift // 1000 if value is not None else None for value in table[name].to_pylist()], type=pa.int64()))
        metadata = {key: value for key, value in pq.ParquetFile(tape.paths[0]).metadata.metadata.items() if key != b"ARROW:schema"}
        day = f"2026-01-0{index + 1}"
        metadata[b"narrowgate.day"] = day.encode()
        pq.write_table(table.replace_schema_metadata(metadata), tape.paths[0])
        output = root / "daily.parquet"
        unify_orderbook_day(tape.paths[0], output, day)
        paths.append(output)
    scheduler = HistoricalExchangeBookScheduler(TardisExchangeBookTape(paths, symbol="BTCUSDC", tick_size=1.),
                                                strict_sequence=False, union_minimum_levels=2)
    for index in range(3):
        stamp = (UNION_START_US + index * 86400000000 + 1000) * 1000
        assert scheduler.preview_at(stamp).snapshot_or_gap
        step = scheduler.advance_to(stamp)
        assert step.snapshot_reset and not step.level_changes
        assert scheduler.lookup("bid", 100).quantity == 1.
        assert scheduler.last_source_observed_ts_ns == (UNION_START_US + (0 if index == 1 else index) * 86400000000 + 1000) * 1000


@pytest.mark.parametrize("failure", ["gap", "crossed", "insufficient_depth", "snapshot_reset"])
def test_observed_union_retains_aged_view_without_source_rollback_or_fake_cancellation(tmp_path, failure):
    messages = [_union_message("tardis", 1000, snapshot=True),
                _union_message("cryptohft", 2000, snapshot=True, identifier=100)]
    if failure == "gap":
        bad = _union_message("cryptohft", 3000, (("bid", 100, 9.),), identifier=102, previous=999)
    elif failure == "crossed":
        bad = _union_message("cryptohft", 3000, (("bid", 105, 9.),))
    elif failure == "insufficient_depth":
        bad = _union_message("cryptohft", 3000, (("bid", 99, 0.),))
    else:
        bad = _union_message("cryptohft", 3000, (("bid", 100, 9.), ("ask", 101, 9.)), snapshot=True, identifier=102)
    messages += [bad, _union_message("tardis", 4000, (("bid", 100, 3.),))]
    tape = _observed_tape(tmp_path, messages)
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    scheduler.advance_to((UNION_START_US + 2000) * 1000)
    before = scheduler.top_levels(20)
    stale = scheduler.advance_to((UNION_START_US + 3000) * 1000)
    assert stale.invalidated and not stale.level_changes
    assert scheduler.top_levels(20) == before
    assert not scheduler.lookup("bid", 100).strict_usable
    assert scheduler.lookup("bid", 100).status == "unknown"
    assert scheduler.segment_id == 0
    if failure == "gap":
        assert scheduler.stats().sequence_gaps == 1
    assert scheduler.last_source_observed_ts_ns == (UNION_START_US + 2000) * 1000
    assert not scheduler._union_view_attached
    native = _native_union_state(messages[:3])
    expected = {(int(row[0]), row[1] // 100_000_000): row[2] / 1e8 for row in native["global_levels"]}
    actual = {(side, int(p)): q for side, levels in enumerate(scheduler.top_levels(20)) for p, q in levels}
    assert actual == expected
    assert scheduler.last_source_observed_ts_ns == native["global_observed_us"] * 1000
    recovered = scheduler.advance_to((UNION_START_US + 4000) * 1000)
    assert recovered.snapshot_reset and not recovered.level_changes
    assert scheduler.lookup("bid", 100).quantity == 3.
    assert scheduler.last_source_observed_ts_ns == (UNION_START_US + 4000) * 1000


def test_observed_union_true_selected_deltas_only_and_reset_keeps_unknown_holes(tmp_path):
    old = (("bid", 100, 1.), ("bid", 98, 1.), ("ask", 101, 1.), ("ask", 103, 1.))
    new = (("bid", 100, 4.), ("bid", 97, 1.), ("ask", 101, 1.), ("ask", 104, 1.))
    tape = _observed_tape(tmp_path, [
        _union_message("cryptohft", 1000, old, snapshot=True, identifier=100),
        _union_message("cryptohft", 2000, (("bid", 100, 2.),)),
        _union_message("cryptohft", 3000, new, snapshot=True, identifier=200),
    ])
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    scheduler.advance_to((UNION_START_US + 1000) * 1000)
    assert scheduler.lookup("bid", 99).status == "unknown"
    delta = scheduler.advance_to((UNION_START_US + 2000) * 1000)
    assert len(delta.level_changes) == 1 and not delta.snapshot_reset
    assert delta.level_changes[0].quantity_before == 1. and delta.level_changes[0].quantity_after == 2.
    reset = scheduler.advance_to((UNION_START_US + 3000) * 1000)
    assert reset.snapshot_reset and not reset.level_changes
    assert scheduler.lookup("bid", 98).status == "unknown"
    assert scheduler.lookup("bid", 100).quantity == 4.


def test_observed_union_initial_seed_not_terminal_and_checkpoint_preserves_source_states(tmp_path):
    source_messages = [_union_message("cryptohft", -2000, snapshot=True, identifier=100),
                       _union_message("tardis", -1000, snapshot=True)]
    seed = {"symbol": "BTCUSDC", "source_ids": ["cryptohft", "tardis"],
            "next_day_start_us": UNION_START_US, "kernel": _native_union_state(source_messages)}
    messages = [_union_message("cryptohft", 1000, (("bid", 100, 3.),)),
                _union_message("cryptohft", 2000, (("bid", 100, 4.),), identifier=102, previous=101)]
    tape = _observed_tape(tmp_path, messages, terminal=seed)
    unknown = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    unknown.advance_to((UNION_START_US + 1000) * 1000)
    assert unknown.lookup("bid", 100).status == "unknown"
    tape = _observed_tape(tmp_path, messages, seed=seed)
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    assert scheduler.last_source_observed_ts_ns == (UNION_START_US - 1000) * 1000
    assert scheduler.lookup("bid", 100).asof_exchange_ts_ns == UNION_START_US * 1000
    for t in (UNION_START_US - 10_000_000, UNION_START_US):
        assert not scheduler.lookup_strictly_before("bid", 100, t * 1000).strict_usable
    with pytest.raises(ValueError, match="time regressed"):
        scheduler.advance_to((UNION_START_US - 1) * 1000)
    scheduler.advance_to(UNION_START_US * 1000)
    assert scheduler.lookup_strictly_before("bid", 100, UNION_START_US * 1000 + 1).strict_usable
    scheduler.advance_to((UNION_START_US + 1000) * 1000)
    assert scheduler.lookup("bid", 100).quantity == 3.
    state = pickle.loads(pickle.dumps(scheduler.checkpoint()))
    restored = HistoricalExchangeBookScheduler.from_checkpoint(state, [])
    step = restored.advance_to((UNION_START_US + 2000) * 1000)
    assert len(step.level_changes) == 1 and restored.lookup("bid", 100).quantity == 4.
    assert restored.book.bid_levels is restored._union_sources["cryptohft"].scheduler.book.bid_levels


@pytest.mark.parametrize("defect", ["wrong_day", "future_source", "future_global", "wrong_symbol", "bad_view"])
def test_observed_union_initial_seed_cannot_contain_future_state(tmp_path, defect):
    messages = [_union_message("cryptohft", -1000, snapshot=True, identifier=100)]
    seed = {"symbol": "BTCUSDC", "source_ids": ["cryptohft", "tardis"],
            "next_day_start_us": UNION_START_US, "kernel": _native_union_state(messages)}
    if defect == "wrong_day":
        seed["next_day_start_us"] += 86_400_000_000
    elif defect == "future_source":
        seed["kernel"]["sources"][0]["observed_us"] = UNION_START_US + 1
    elif defect == "future_global":
        seed["kernel"]["global_observed_us"] = UNION_START_US + 1
    elif defect == "wrong_symbol":
        seed["symbol"] = "BTCUSDT"
    else:
        seed["kernel"]["view_source"] = 99
    tape = _observed_tape(tmp_path, [_union_message("cryptohft", 1000, (("bid", 100, 2.),))], seed=seed)
    with pytest.raises(ValueError, match="initial|future"):
        HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)


def test_observed_union_nonselected_gap_preserves_valid_selected_queue(tmp_path):
    tape = _observed_tape(tmp_path, [
        _union_message("cryptohft", 1000, snapshot=True, identifier=100),
        _union_message("tardis", 2000, snapshot=True),
        _union_message("cryptohft", 3000, (("bid", 100, 9.),), identifier=102, previous=999),
    ])
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    scheduler.advance_to((UNION_START_US + 2000) * 1000)
    segment = scheduler.segment_id
    step = scheduler.advance_to((UNION_START_US + 3000) * 1000)
    assert not step.invalidated and not step.snapshot_reset and not step.level_changes
    assert scheduler.segment_id == segment and scheduler.lookup("bid", 100).strict_usable
    assert scheduler._union_selected == "tardis"
    assert scheduler.stats().sequence_gaps == 1


def test_observed_union_scheduled_messages_match_iterator_state_and_no_delta_copy(tmp_path):
    tape = _observed_tape(tmp_path, [
        _union_message("cryptohft", 1000, snapshot=True, identifier=100),
        _union_message("tardis", 1000, snapshot=True),
        _union_message("cryptohft", 2000, (("bid", 100, 3.),)),
        _union_message("tardis", 2000, (("bid", 100, 4.),)),
    ])
    events = list(tape)
    direct = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    scheduled = HistoricalExchangeBookScheduler((), strict_sequence=False, union_minimum_levels=2)
    before_container = None
    for chunk in (events[:2], events[2:]):
        boundary = chunk[-1].exchange_ts_ns
        expected = direct.advance_to(boundary)
        actual = scheduled.apply_scheduled_events(chunk, boundary_ts_ns=boundary)
        assert actual.level_changes == expected.level_changes
        assert actual.snapshot_reset == expected.snapshot_reset
        assert scheduled.top_levels(20) == direct.top_levels(20)
        assert scheduled.last_source_observed_ts_ns == direct.last_source_observed_ts_ns
        if before_container is not None:
            assert before_container is scheduled.book.bid_levels
        before_container = scheduled.book.bid_levels


def test_observed_union_default_valid_depth_does_not_promote_two_levels(tmp_path):
    tape = _observed_tape(tmp_path, [_union_message("tardis", 1000, snapshot=True)])
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    scheduler.advance_to((UNION_START_US + 1000) * 1000)
    assert scheduler.lookup("bid", 100).status == "unknown"
    assert scheduler.top_levels(20) == ([], [])


def test_observed_union_aged_day_seed_is_not_queue_evidence_or_strict_native(tmp_path):
    messages = [_union_message("cryptohft", -3000, snapshot=True, identifier=100),
                _union_message("cryptohft", -2000, (("bid", 100, 9.),), identifier=102, previous=999)]
    seed = {"symbol": "BTCUSDC", "source_ids": ["cryptohft", "tardis"],
            "next_day_start_us": UNION_START_US, "kernel": _native_union_state(messages)}
    tape = _observed_tape(tmp_path, [], seed=seed)
    scheduler = HistoricalExchangeBookScheduler(tape, strict_sequence=False, union_minimum_levels=2)
    assert scheduler.top_levels(2) == ([(100., 1.), (99., 1.)], [(101., 1.), (102., 1.)])
    assert scheduler.lookup("bid", 100).status == "unknown"
    assert scheduler.last_source_observed_ts_ns == (UNION_START_US - 3000) * 1000
    with pytest.raises(ValueError, match="strict global exchange sequence"):
        HistoricalExchangeBookScheduler(tape, union_minimum_levels=2)


@pytest.mark.parametrize("bad_anchor", [False, True])
def test_recorder_snapshot_sequence_clock_keeps_original_time_and_checkpoint(tmp_path, monkeypatch, bad_anchor):
    from datetime import datetime, timezone
    from models.exchange_book_replay import CryptoHFTExchangeBookTape

    hour_ms = (BASE_MS // 3_600_000 + 1) * 3_600_000
    hour = datetime.fromtimestamp(hour_ms / 1000, timezone.utc)
    paths = [tmp_path / name for name in ("previous.parquet.zst", "current.parquet.zst")]
    for path in paths:
        path.touch()
    def event(kind, time_ms, identifier, previous=None):
        return HistoricalExchangeBookEvent("market", kind, time_ms * 1_000_000,
            event_time_ns=time_ms * 1_000_000,
            transaction_time_ns=0 if kind == "snapshot" else time_ms * 1_000_000,
            last_update_id=identifier if kind == "snapshot" else None,
            first_update_id=identifier, final_update_id=identifier,
            previous_final_update_id=previous,
            levels=(("bid", 1000, 2.), ("ask", 1010, 3.)))
    first = [event("snapshot", hour_ms - 2000, 10), event("update", hour_ms - 200, 11, 10)]
    second = [event("snapshot", hour_ms, 99 if bad_anchor else 11),
              event("update", hour_ms - 100, 12, 11), event("update", hour_ms + 100, 13, 12)]
    def tape(mode):
        out = CryptoHFTExchangeBookTape(raw_root=tmp_path, day=hour.strftime("%Y-%m-%d"),
            symbol="BTCUSDC", tick_size=.1, warmup_hours=0, strict_complete=False,
            cache_enabled=False, recorder_snapshot_clock=mode)
        out._expected = tuple((hour, path) for path in paths)
        monkeypatch.setattr(out, "_iter_hour", lambda p: iter(first if p == paths[0] else second))
        return out
    with pytest.raises(ValueError, match="exchange time regressed"):
        list(tape("original"))
    source = tape("preceding_update_id")
    if bad_anchor:
        with pytest.raises(ValueError, match="exchange time regressed"):
            list(source)
        return
    events = list(source)
    assert len(events) == 5  # No raw snapshot or delta was deleted.
    assert events[2].exchange_ts_ms == hour_ms - 200
    assert events[2].event_time_ns == hour_ms * 1_000_000
    assert events[2].exchange_ts_source == "preceding_update_sequence_anchor"
    assert source.cache_stats()["snapshot_sequence_anchors"] == 1
    assert events == list(source)
    full = HistoricalExchangeBookScheduler(source)
    full.advance_to((hour_ms + 200) * 1_000_000)
    assert full.stats().message_time_reversals == 0
    assert full.stats().sequence_anchored_snapshot_events == 1
    partial = HistoricalExchangeBookScheduler(source)
    partial.advance_to((hour_ms - 200) * 1_000_000)
    resumed = HistoricalExchangeBookScheduler.from_checkpoint(pickle.loads(pickle.dumps(partial.checkpoint())), ())
    resumed.resume_input_source(source)
    resumed.advance_to((hour_ms + 200) * 1_000_000)
    assert resumed.stats() == full.stats()
    assert resumed.top_levels(2) == full.top_levels(2)

    # After dropping an input prefix, the opening snapshot must retain exactly
    # the same anchor, using the preceding raw hour rather than a future delta.
    import data.build_active_order_queue_tape as raw_parser
    cropped = tape("preceding_update_id")
    cropped._expected = ((hour, paths[1]),)
    cropped._snapshot_anchor_context = paths[0]
    monkeypatch.setattr(raw_parser, "iter_cryptohft_logical_messages",
                        lambda *args, **kwargs: iter(first))
    assert list(cropped)[0].exchange_ts_ns == events[2].exchange_ts_ns
    identity = cropped.identity(include_sha256=False)
    assert identity["snapshot_anchor_context"]["path"] == str(paths[0])
    assert identity["recorder_snapshot_clock"] == "preceding_update_id"


def test_tardis_raw_messages_keep_full_depth_and_no_invented_sequence(tmp_path, monkeypatch):
    import pyarrow.csv as pacsv
    import data.normalize_tardis_orderbook as normalizer
    from models.exchange_book_replay import TardisExchangeBookTape

    path = tmp_path / "BTCUSDC.csv"
    header = "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
    def row(offset, snapshot, side, price, amount):
        return f"binance-futures,BTCUSDC,{BASE_MS * 1000 + offset},{BASE_MS * 1000 + offset + 5},{snapshot},{side},{price},{amount}\n"
    path.write_text(header + row(0, "true", "bid", 100., 3.)
                    + row(0, "true", "ask", 101., 4.)
                    + row(0, "true", "bid", 90., 8.)
                    + row(100, "false", "bid", 100., 2.)
                    + row(100, "false", "ask", 101., 0.)
                    + row(100, "false", "ask", 102., 5.)
                    + row(200, "false", "bid", 100., 1.))
    # The snapshot spans Arrow batches; it must still be one atomic message.
    monkeypatch.setattr(normalizer, "_open_csv", lambda path: pacsv.open_csv(
        path, read_options=pacsv.ReadOptions(block_size=190)))
    tape = TardisExchangeBookTape([path], symbol="BTCUSDC", tick_size=.1)
    events = list(tape)
    assert len(events) == 3
    assert len(events[0].levels) == 3 and len(events[1].levels) == 3
    assert events == list(tape)
    assert all(event.last_update_id is None and event.final_update_id is None for event in events)
    assert tape.identity()["exchange_sequence_available"] is False
    with pytest.raises(ValueError, match="cannot prove strict exchange sequence"):
        HistoricalExchangeBookScheduler(tape).advance_to(events[-1].exchange_ts_ns)
    full = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    full.advance_to(events[-1].exchange_ts_ns)
    assert full.top_levels(3) == ([(1000., 1.), (900., 8.)], [(1020., 5.)])
    assert full.stats().provider_ordered_events == 3
    assert "provider_ordered" in full.evidence_scope
    first = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    first.advance_to(events[1].exchange_ts_ns)
    saved = pickle.loads(pickle.dumps(first.checkpoint()))
    restored = HistoricalExchangeBookScheduler.from_checkpoint(saved, ())
    restored.resume_input_source(tape)
    restored.advance_to(events[-1].exchange_ts_ns)
    assert restored.top_levels(3) == full.top_levels(3)
    assert restored.stats() == full.stats()


@pytest.mark.parametrize("defect", ["exchange_clock", "receive_clock", "causality", "tick", "negative_qty"])
def test_tardis_raw_input_rejects_invalid_rows_without_reordering(tmp_path, defect):
    from models.exchange_book_replay import TardisExchangeBookTape
    path = tmp_path / "BTCUSDC.csv"
    exchange, receive, price, qty = 1001, 1006, 100., 1.
    if defect == "exchange_clock":
        exchange = 999
    elif defect == "receive_clock":
        receive = 1004
    elif defect == "causality":
        exchange, receive = 1007, 1006
    elif defect == "tick":
        price = 100.05
    else:
        qty = -1.
    path.write_text("exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        "binance-futures,BTCUSDC,1000,1005,true,bid,100,1\n"
        f"binance-futures,BTCUSDC,{exchange},{receive},false,bid,{price},{qty}\n")
    with pytest.raises(ValueError):
        list(TardisExchangeBookTape([path], symbol="BTCUSDC", tick_size=.1))


def test_provider_change_requires_snapshot_and_never_carries_old_book_levels():
    from dataclasses import replace
    start = BASE_MS * 1_000_000
    snapshot = HistoricalExchangeBookEvent("market", "snapshot", start,
        levels=(("bid", 100, 2.), ("ask", 102, 3.)), sequence_scope="provider_ordered")
    delta = replace(snapshot, event_type="delta", exchange_ts_ns=start + 100,
        sequence_scope="exchange_sequence", first_update_id=2, final_update_id=2,
        previous_final_update_id=1)
    with pytest.raises(ValueError, match="requires an actual source snapshot"):
        HistoricalExchangeBookScheduler([snapshot, delta], strict_sequence=False).advance_to(start + 100)
    native_snapshot = replace(delta, event_type="snapshot", last_update_id=1,
        levels=(("bid", 99, 4.), ("ask", 103, 5.)))
    scheduler = HistoricalExchangeBookScheduler([snapshot, native_snapshot], strict_sequence=False)
    scheduler.advance_to(start + 100)
    assert scheduler.top_levels(3) == ([(99., 4.)], [(103., 5.)])
    # Even after switching back, the run's evidence retains its provider-only prefix.
    assert "provider_ordered" in scheduler.evidence_scope
    with pytest.raises(ValueError, match="must not manufacture"):
        replace(snapshot, last_update_id=123)


def test_explicit_daily_source_plan_keeps_global_ordinals_and_source_identity(tmp_path):
    from models.exchange_book_replay import PlannedExchangeBookTape
    files = []
    for n in range(2):
        path = tmp_path / f"day{n}.csv"
        path.write_text("exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
            f"binance-futures,BTCUSDC,{1000+n},{1010+n},true,bid,100,1\n")
        files.append(path)
    plan = {"symbol": "BTCUSDC", "days": [
        {"day": f"2026-01-0{n+1}", "provider": "tardis", "raw_file": str(path)}
        for n, path in enumerate(files)]}
    tape = PlannedExchangeBookTape(plan, days=["2026-01-01", "2026-01-02"], symbol="BTCUSDC", tick_size=.1)
    events = list(tape)
    assert [event.source_ordinal for event in events] == [0, 1]
    assert [event.source for event in events] == list(map(str, files))
    assert tape.identity()["automatic_source_fallback"] is False
    with pytest.raises(ValueError, match="lacks requested context"):
        PlannedExchangeBookTape(plan, days=["2025-12-31"], symbol="BTCUSDC", tick_size=.1)
    with pytest.raises(ValueError, match="duplicate dates"):
        PlannedExchangeBookTape({**plan, "days": plan["days"] * 2}, days=["2026-01-01"],
                               symbol="BTCUSDC", tick_size=.1)


@pytest.mark.parametrize("opening_snapshot", [True, False])
@pytest.mark.parametrize("overlap", [True, False])
def test_daily_source_handover_uses_actual_snapshot_not_file_date(tmp_path, opening_snapshot, overlap):
    from models.exchange_book_replay import PlannedExchangeBookTape
    header = "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
    old, new = tmp_path / "old.csv", tmp_path / "new.csv"
    old.write_text(header + "binance-futures,BTCUSDC,1000,1001,true,bid,100,1\n"
                   "binance-futures,BTCUSDC,1994,1995,false,bid,100,99\n")
    opening_us = 1916 if overlap else 2000
    new.write_text(header + f"binance-futures,BTCUSDC,{opening_us},2001,{str(opening_snapshot).lower()},bid,99,2\n"
                   "binance-futures,BTCUSDC,2100,2101,false,bid,99,3\n")
    plan = {"symbol": "BTCUSDC", "days": [
        {"day": day, "provider": "tardis", "raw_file": str(path)}
        for day, path in (("2025-12-31", old), ("2026-01-01", new))]}
    tape = PlannedExchangeBookTape(plan, days=[r["day"] for r in plan["days"]],
                                   symbol="BTCUSDC", tick_size=.1)
    if not opening_snapshot and overlap:
        # Without a snapshot, keep the actual updates; the scheduler must
        # reject this overlapping delta clock rather than silently trim it.
        assert [e.exchange_ts_ns for e in tape] == [1_000_000, 1_994_000, 1_916_000, 2_100_000]
        with pytest.raises(ValueError, match="not exchange-time sorted"):
            HistoricalExchangeBookScheduler(tape, strict_sequence=False).advance_to(2_200_000)
        return
    events = list(tape)
    expected = [1_000_000] + ([] if overlap else [1_994_000]) + [opening_us * 1000, 2_100_000]
    assert [e.exchange_ts_ns for e in events] == expected
    assert [e.source_ordinal for e in events] == list(range(len(events)))
    assert events == list(tape)
    full = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    full.advance_to(2_200_000)
    partial = HistoricalExchangeBookScheduler(tape, strict_sequence=False)
    partial.advance_to(2_050_000)
    saved = pickle.loads(pickle.dumps(partial.checkpoint()))
    restored = HistoricalExchangeBookScheduler.from_checkpoint(saved, ())
    restored.resume_input_source(PlannedExchangeBookTape(plan, days=["2026-01-01"],
                                                       symbol="BTCUSDC", tick_size=.1))
    restored.advance_to(2_200_000)
    expected_bids = [(990., 3.)] if opening_snapshot else [(1000., 99.), (990., 3.)]
    assert full.top_levels(2) == restored.top_levels(2) == (expected_bids, [])
    assert full.stats() == restored.stats()


@pytest.mark.parametrize("cut_ms", [900, 916, 950])
def test_explicit_provider_gap_discards_old_book_and_survives_input_rotation(monkeypatch, tmp_path, cut_ms):
    from dataclasses import replace
    import models.exchange_book_replay as module

    start = BASE_MS * 1_000_000
    old = tmp_path / "old.csv"
    old.write_text("exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
                   f"binance-futures,BTCUSDC,{start//1000},{start//1000+1},true,bid,100,1\n"
                   f"binance-futures,BTCUSDC,{start//1000},{start//1000+1},true,ask,102,1\n"
                   f"binance-futures,BTCUSDC,{start//1000+994000},{start//1000+994001},false,bid,100,99\n")
    first = HistoricalExchangeBookEvent("market", "delta", start + 916_000_000,
        transaction_time_ns=start + 916_000_000, first_update_id=2, final_update_id=2,
        previous_final_update_id=1, source=str(tmp_path / "native"),
        levels=(("bid", 990, 2.), ("ask", 1030, 4.)))
    second = replace(first, exchange_ts_ns=start + 1_100_000_000,
        transaction_time_ns=start + 1_100_000_000, first_update_id=3, final_update_id=3,
        previous_final_update_id=2, levels=(("bid", 990, 3.),))
    class NativeTape:
        def __init__(self, **kwargs):
            pass
        def __iter__(self):
            yield from (first, second)
        def identity(self, **kwargs):
            return {"fixture": True}
    monkeypatch.setattr(module, "CryptoHFTExchangeBookTape", NativeTape)
    rows = [{"day": "2025-12-31", "provider": "tardis", "raw_file": str(old)},
            {"day": "2026-01-01", "provider": "cryptohft", "raw_root": str(tmp_path)}]
    plan = {"symbol": "BTCUSDC", "days": rows}
    def tape(days):
        return module.PlannedExchangeBookTape(plan, days=days, symbol="BTCUSDC", tick_size=.1)
    with pytest.raises(ValueError, match="actual opening snapshot"):
        list(tape([row["day"] for row in rows]))
    rows[1]["handover"] = "invalidate_then_delta_bootstrap"
    full_tape = tape([row["day"] for row in rows])
    events = list(full_tape)
    assert [event.event_type for event in events] == ["snapshot", "source_gap", "delta", "delta"]
    assert events[1].levels == () and events[1].last_update_id is None
    assert [event.source_ordinal for event in events] == [0, 1, 2, 3]
    assert full_tape.identity()["handovers"]["2026-01-01"] == "invalidate_then_delta_bootstrap"
    full = HistoricalExchangeBookScheduler(full_tape, strict_sequence=False, allow_delta_bootstrap=True)
    full.advance_to(start + 2_000_000_000)
    assert full.top_levels(2) == ([(990., 3.)], [(1030., 4.)])
    assert full.stats().source_gap_events == 1
    assert full.sequence.initialization_source == "delta"
    assert "provider_ordered" in full.evidence_scope
    partial = HistoricalExchangeBookScheduler(full_tape, strict_sequence=False, allow_delta_bootstrap=True)
    partial.advance_to(start + cut_ms * 1_000_000)
    restored = HistoricalExchangeBookScheduler.from_checkpoint(pickle.loads(pickle.dumps(partial.checkpoint())), ())
    restored.resume_input_source(tape(["2026-01-01"]))
    restored.advance_to(start + 2_000_000_000)
    assert restored.stats() == full.stats()
    assert restored.top_levels(2) == full.top_levels(2)
    with pytest.raises(ValueError, match="source gap"):
        HistoricalExchangeBookScheduler(tape(["2026-01-01"]), strict_sequence=True).advance_to(start + 2_000_000_000)


@pytest.mark.parametrize("defect", ["depth", "channel", "delivery", "flag"])
def test_configured_cooldown_refuses_missing_source_instead_of_static_baseline(defect):
    depth = SimpleNamespace(
        ts_ms=np.array([1]), bid_px=np.ones((1, 1)), ask_px=np.ones((1, 1)),
        bid_qty=np.ones((1, 1)), ask_qty=np.ones((1, 1)),
    )
    params = {"boolean_cooldown_policy_enabled": True}
    if defect == "depth":
        depth = None
    elif defect == "channel":
        depth.bid_qty = None
    elif defect == "flag":
        params["boolean_cooldown_policy_enabled"] = "false"
    with pytest.raises(ValueError, match="configured cooldown"):
        build_configured_cooldown_policy_adapter(window={"l2_data": depth}, params=params)
    assert build_configured_cooldown_policy_adapter(window={}, params={}) is None


def test_configured_sell_policy_reuses_live_windows_with_independent_arm_state(monkeypatch):
    from strategy.boolean_cooldown_live import (
        OWNER_POLICY_SELECTED_PREDICATES,
        LiveBooleanCooldownPolicy,
        RuntimeCooldownPolicyEvaluator,
    )

    def load_policy(**kwargs):
        assert kwargs["policy_path"].name == "sell.json"
        evaluator = RuntimeCooldownPolicyEvaluator(
            rules=(("FIXED_166S", (tuple(
                (name, False) for name in OWNER_POLICY_SELECTED_PREDICATES
            ),)),),
            policy_sha256="1" * 64, predicate_bundle_sha256="2" * 64,
        )
        return LiveBooleanCooldownPolicy(
            evaluator=evaluator, warmup_s=kwargs["warmup_s"],
            max_feature_age_s=kwargs["max_feature_age_s"], native_runtime=False,
        )

    monkeypatch.setattr(LiveBooleanCooldownPolicy, "from_files", load_policy)
    ts = np.arange(10, 1_010, 100, dtype=np.int64)
    depth = HistoricalL2Data(
        ts_ms=ts, bid_px=(100 + np.arange(len(ts)))[:, None],
        ask_px=(101 + np.arange(len(ts)))[:, None],
        bid_qty=np.ones((len(ts), 1)), ask_qty=np.ones((len(ts), 1)),
    )
    schedule = {key: ts * 1_000_000 + offset for key, offset in (
        ("exchange_ts_ns", 0), ("receive_ts_ns", 1), ("feature_ready_ts_ns", 2),
    )}
    params = {
        "boolean_cooldown_policy_enabled": True,
        "boolean_cooldown_policy_path": "sell.json",
        "boolean_cooldown_predicate_bundle_path": "sell-predicates.json",
        "boolean_cooldown_policy_sha256": "1" * 64,
        "boolean_cooldown_predicate_bundle_sha256": "2" * 64,
        "boolean_cooldown_ema_warmup_s": 0.2,
        "max_exec_book_visible_age_s": 5.0,
        "_exec_message_delivery": {"depth": schedule},
    }
    first, second = [
        build_configured_cooldown_policy_adapter(window={"l2_data": depth}, params=params)
        for _ in range(2)
    ]
    live = load_policy(policy_path=Path("sell.json"), warmup_s=0.2, max_feature_age_s=5.0)
    cutoff = 1_000_000_000
    for index in range(len(ts)):
        live.observe_depth(
            receive_ts_ns=int(schedule["receive_ts_ns"][index]),
            bids=list(zip(depth.bid_px[index], depth.bid_qty[index], strict=True)),
            asks=list(zip(depth.ask_px[index], depth.ask_qty[index], strict=True)),
            market_generation=index + 1, depth_generation=index + 1,
        )
    for baseline_ms in (85_000.0, 170_000.4, 255_000.5):
        snapshot = first.capture_exposure_fill(
            assignment_id=str(baseline_ms), fill_exchange_ts_ns=cutoff - 1,
            fill_visible_ts_ns=cutoff,
            m0_context={"side": "SELL", "fill_visible_ts_ns": cutoff,
                        "baseline_duration_ms": baseline_ms, "campaign_age_s": 300.0},
        )
        expected = live.evaluate(
            side="SELL", baseline_duration_ms=round(baseline_ms), campaign_age_s=300.0,
            decision_ts_ns=cutoff, snapshot_id=snapshot.snapshot_id,
        )
        actual = first.evaluate(snapshot, baseline_ms)
        assert (actual.action_id, actual.duration_ms, actual.fallback_reason) == (
            expected.action_id, expected.duration_ms, expected.fallback_reason,
        )
    assert first.audit()["depth_callbacks_consumed"] == len(ts)
    assert second.audit()["depth_callbacks_consumed"] == 0
    assert second.audit()["snapshots_emitted"] == 0
    disabled = first.capture_exposure_fill(
        assignment_id="buy-disabled", fill_exchange_ts_ns=cutoff - 1,
        fill_visible_ts_ns=cutoff,
        m0_context={"side": "BUY", "fill_visible_ts_ns": cutoff,
                    "baseline_duration_ms": 170_000.4, "campaign_age_s": 300.0},
    )
    assert disabled.decision.duration_ms == 170_000.4
    assert disabled.fallback_reason == "configured_policy_disabled_for_side"


def test_message_callback_serialization_preserves_measured_service_without_double_counting():
    exchange = np.array([100, 200, 300, 400], dtype=np.int64)
    receive = np.array([110, 210, 310, 600], dtype=np.int64)
    ready = np.array([350, 220, 320, 605], dtype=np.int64)
    legacy = HistoricalMessageDeliverySchedule(exchange, receive, ready)
    serial = HistoricalMessageDeliverySchedule(
        exchange, receive, ready, serialize_callback_service=True,
    )
    assert legacy.receive_ns_for_channel().tolist() == receive.tolist()
    assert legacy.ready_ns_for_channel().tolist() == [350, 350, 350, 605]
    assert serial.receive_ns_for_channel().tolist() == [110, 350, 360, 600]
    assert serial.ready_ns_for_channel().tolist() == [350, 360, 370, 605]
    np.testing.assert_array_equal(
        serial.ready_ns_for_channel() - serial.receive_ns_for_channel(), ready - receive,
    )
    assert serial.stats_dict()["callback_queued_events"] == 2
    assert serial.stats_dict()["max_callback_queue_delay_ns"] == 140
    assert serial.latest_visible_index(360) == 0
    assert serial.latest_visible_index(361) == 1
    np.testing.assert_array_equal(receive, [110, 210, 310, 600])


@pytest.mark.parametrize("shared_connection", [False, True])
def test_message_callback_serialization_respects_connection_not_channel(shared_connection):
    schedule = HistoricalMessageDeliverySchedule(
        [100, 200, 300, 400], [110, 210, 310, 410], [500, 220, 320, 420],
        channel_ids=["book", "trade", "book", "trade"],
        connection_ids=["public"] * 4 if shared_connection else None,
        serialize_callback_service=True,
    )
    assert schedule.receive_ns_for_channel("book").tolist() == [
        110, 510 if shared_connection else 500,
    ]
    assert schedule.receive_ns_for_channel("trade").tolist() == (
        [500, 520] if shared_connection else [210, 410]
    )


def test_message_callback_serialization_matches_scalar_recurrence_and_handles_empty():
    rng = np.random.default_rng(41)
    exchange = np.arange(100, dtype=np.int64)
    receive = exchange + rng.integers(0, 500, size=len(exchange))
    service = rng.integers(0, 100, size=len(exchange))
    schedule = HistoricalMessageDeliverySchedule(
        exchange, receive, receive + service, serialize_callback_service=True,
    )
    previous_finish = 0
    starts, finishes = [], []
    for raw_entry, duration in zip(receive, service, strict=True):
        entry = max(int(raw_entry), previous_finish)
        previous_finish = entry + int(duration)
        starts.append(entry)
        finishes.append(previous_finish)
    assert schedule.receive_ns_for_channel().tolist() == starts
    assert schedule.ready_ns_for_channel().tolist() == finishes
    empty = HistoricalMessageDeliverySchedule([], [], [], serialize_callback_service=True)
    assert empty.receive_ns_for_channel().size == 0
    assert empty.stats_dict()["callback_queued_events"] == 0


@pytest.mark.parametrize("cumulative_overflow", [False, True])
def test_message_callback_serialization_rejects_int64_overflow(cumulative_overflow):
    limit = np.iinfo(np.int64).max
    receive = [0, 0] if cumulative_overflow else [limit - 10, limit - 9]
    ready = [limit, limit] if cumulative_overflow else [limit, limit]
    with pytest.raises(ValueError, match="exceeds int64"):
        HistoricalMessageDeliverySchedule(
            [0, 0], receive, ready, serialize_callback_service=True,
        )


def _event(
    offset_ms: int,
    *,
    event_type: str,
    levels: tuple[tuple[str, int, float], ...],
    first_update_id: int | None = None,
    final_update_id: int | None = None,
    previous_final_update_id: int | None = None,
    last_update_id: int | None = None,
    ordinal: int = 0,
    receive_delay_ms: int = 1,
) -> HistoricalExchangeBookEvent:
    timestamp_ms = BASE_MS + int(offset_ms)
    return HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC",
        event_type=event_type,
        exchange_ts_ns=timestamp_ms * 1_000_000,
        local_receive_ts_ns=(timestamp_ms + int(receive_delay_ms)) * 1_000_000,
        event_time_ns=timestamp_ms * 1_000_000,
        transaction_time_ns=timestamp_ms * 1_000_000,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        last_update_id=last_update_id,
        levels=levels,
        source_ordinal=ordinal,
    )


def _events() -> list[HistoricalExchangeBookEvent]:
    return [
        _event(
            100,
            event_type="snapshot",
            levels=(
                ("bid", 990, 5.0),
                ("bid", 999, 2.0),
                ("ask", 1001, 2.0),
                ("ask", 1010, 4.0),
            ),
            last_update_id=100,
            ordinal=1,
        ),
        _event(
            500,
            event_type="delta",
            levels=(("bid", 990, 3.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
        _event(
            750,
            event_type="delta",
            levels=(("bid", 990, 4.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
        ),
    ]


def test_book_checkpoint_round_trip_preserves_prefetch_and_sequence(tmp_path):
    events = _events() + [_event(
        750, event_type="delta", levels=(("ask", 1001, 1.5),),
        first_update_id=103, final_update_id=103,
        previous_final_update_id=102, ordinal=4,
    )]
    scheduler = HistoricalExchangeBookScheduler(events, track_mid_changes=True)
    scheduler.advance_to((BASE_MS + 500) * 1_000_000)
    # Preview reads both same-time messages but does not apply them.
    assert scheduler.preview_at((BASE_MS + 750) * 1_000_000).event_count == 2
    assert scheduler.source_read_count == 4
    assert scheduler.stats().consumed_events == 2
    saved = scheduler.checkpoint()
    path = tmp_path / "book-state.pkl"
    with path.open("wb") as stream:
        pickle.dump(saved, stream, protocol=pickle.HIGHEST_PROTOCOL)
    with path.open("rb") as stream:
        restored = HistoricalExchangeBookScheduler.from_checkpoint(
            pickle.load(stream), events[scheduler.source_read_count:],
        )
    assert restored.sequence.book is restored.book
    assert restored.state_fingerprint() == scheduler.state_fingerprint()
    expected = scheduler.advance_to((BASE_MS + 800) * 1_000_000)
    actual = restored.advance_to((BASE_MS + 800) * 1_000_000)
    assert actual == expected
    assert restored.stats() == scheduler.stats()
    assert restored.mid_changes == scheduler.mid_changes
    assert restored.state_fingerprint() == scheduler.state_fingerprint()
    # Advancing either instance cannot mutate the saved checkpoint.
    again = HistoricalExchangeBookScheduler.from_checkpoint(saved, [])
    assert again.stats().consumed_events == 2
    assert again.lookup("bid", 990).quantity == 3.0


@pytest.mark.parametrize("cut", [1, 2])
def test_book_checkpoint_continues_exhausted_input_batch_without_reset(cut):
    events = _events()
    boundary = events[cut - 1].exchange_ts_ns
    expected = HistoricalExchangeBookScheduler(events, track_mid_changes=True)
    expected.advance_to(boundary)
    chunk = HistoricalExchangeBookScheduler(events[:cut], track_mid_changes=True)
    chunk.advance_to(boundary)
    assert chunk.next_exchange_ts_ns is None
    resumed = HistoricalExchangeBookScheduler.from_checkpoint(
        pickle.loads(pickle.dumps(chunk.checkpoint())), events[cut:],
    )
    assert resumed.sequence.book is resumed.book
    end = events[-1].exchange_ts_ns
    assert resumed.advance_to(end) == expected.advance_to(end)
    assert resumed.state_fingerprint() == expected.state_fingerprint()
    assert resumed.stats() == expected.stats()
    assert resumed.source_read_count == len(events)


def test_book_checkpoint_does_not_hide_sequence_gap_or_time_regression():
    scheduler = HistoricalExchangeBookScheduler(_events()[:1])
    scheduler.advance_to((BASE_MS + 100) * 1_000_000)
    saved = scheduler.checkpoint()
    wrong_sequence = _event(
        200, event_type="delta", levels=(("bid", 990, 1.0),),
        first_update_id=999, final_update_id=999, previous_final_update_id=998,
    )
    resumed = HistoricalExchangeBookScheduler.from_checkpoint(saved, [wrong_sequence])
    with pytest.raises(ValueError, match="sequence gap"):
        resumed.advance_to((BASE_MS + 200) * 1_000_000)
    with pytest.raises(ValueError, match="not exchange-time sorted"):
        HistoricalExchangeBookScheduler.from_checkpoint(saved, [_event(
            99, event_type="snapshot", levels=(("bid", 990, 1.0),), last_update_id=100,
        )])


def test_native_file_rotation_preserves_prefetch_and_global_source_ordinals():
    from dataclasses import replace

    events = [replace(event, source="hour-0.csv") for event in _events()[:1]]
    events += [replace(event, source="hour-1.csv") for event in _events()[1:]]
    events.append(replace(_event(
        900, event_type="delta", levels=(("ask", 1001, 1.5),),
        first_update_id=103, final_update_id=103, previous_final_update_id=102, ordinal=4,
    ), source="hour-2.csv"))
    original = HistoricalExchangeBookScheduler(events)
    original.advance_to((BASE_MS + 500) * 1_000_000)
    saved = HistoricalExchangeBookScheduler.from_checkpoint(original.checkpoint(), [])
    # Drop the first source file and re-number source ordinals as the raw tape
    # loader does for a different file window. The within-file cursor is stable.
    new_window = [replace(event, source_ordinal=i) for i, event in enumerate(events[1:], start=1)]
    saved.resume_input_source(new_window)
    assert saved.advance_to((BASE_MS + 1_000) * 1_000_000) == original.advance_to((BASE_MS + 1_000) * 1_000_000)
    assert saved.stats() == original.stats()
    assert saved.state_fingerprint() == original.state_fingerprint()
    assert saved._last_read_event == original._last_read_event


def test_native_file_rotation_rejects_changed_cursor_message():
    from dataclasses import replace

    events = [replace(event, source="hour-0.csv") for event in _events()]
    scheduler = HistoricalExchangeBookScheduler(events)
    marker = scheduler._last_read_event
    changed = [replace(marker, levels=(("bid", 990, 123.0),)), *events[1:]]
    with pytest.raises(ValueError, match="changed the saved source"):
        scheduler.resume_input_source(changed)


def test_exchange_book_event_is_not_visible_before_its_exchange_timestamp() -> None:
    scheduler = HistoricalExchangeBookScheduler(_events())
    assert scheduler.next_exchange_ts_ns == (
        BASE_MS + 100
    ) * 1_000_000

    scheduler.advance_to((BASE_MS + 500) * 1_000_000, inclusive=False)
    assert scheduler.next_exchange_ts_ns == (
        BASE_MS + 500
    ) * 1_000_000
    before = scheduler.lookup("BUY", 990)
    assert before.status == "exact"
    assert before.quantity == pytest.approx(5.0)

    advance = scheduler.advance_to(
        (BASE_MS + 500) * 1_000_000,
        inclusive=True,
    )
    after = scheduler.lookup("BUY", 990)
    assert after.quantity == pytest.approx(3.0)
    assert len(advance.level_changes) == 1
    assert advance.level_changes[0].delta_quantity == pytest.approx(-2.0)
    assert advance.source_events == (_events()[1],)
    assert advance.level_changes[0].receive_ts_ns == (
        BASE_MS + 501
    ) * 1_000_000


def test_advance_preserves_each_source_message_receive_timestamp() -> None:
    events = _events()[:1] + [
        _event(
            500,
            event_type="delta",
            levels=(("bid", 990, 3.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
            receive_delay_ms=7,
        ),
        _event(
            500,
            event_type="delta",
            levels=(("bid", 990, 2.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
            receive_delay_ms=19,
        ),
    ]
    scheduler = HistoricalExchangeBookScheduler(events)
    advance = scheduler.advance_to((BASE_MS + 500) * 1_000_000)

    assert advance.source_events == tuple(events)
    assert [change.receive_ts_ns for change in advance.level_changes] == [
        (BASE_MS + 507) * 1_000_000,
        (BASE_MS + 519) * 1_000_000,
    ]


def test_visibility_scheduler_head_of_line_clamps_receive_time_reordering() -> None:
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(("bid", 990, 5.0), ("ask", 1001, 2.0)),
            last_update_id=100,
            ordinal=1,
            receive_delay_ms=100,
        ),
        _event(
            110,
            event_type="delta",
            levels=(("bid", 990, 3.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
            receive_delay_ms=40,
        ),
    ]
    scheduler = HistoricalExchangeBookVisibilityScheduler()
    assigned = scheduler.enqueue_many(
        events,
        ready_timestamp=lambda event: event.local_receive_ts_ns,
    )

    assert assigned == (
        (BASE_MS + 200) * 1_000_000,
        (BASE_MS + 200) * 1_000_000,
    )
    scheduler.advance_to((BASE_MS + 200) * 1_000_000, inclusive=False)
    assert scheduler.lookup("BUY", 990).status == "unknown"

    advance = scheduler.advance_to(
        (BASE_MS + 200) * 1_000_000,
        inclusive=True,
    )
    assert [event.source_ordinal for event in advance.source_events] == [1, 2]
    assert [event.exchange_ts_ns for event in advance.source_events] == [
        (BASE_MS + 100) * 1_000_000,
        (BASE_MS + 110) * 1_000_000,
    ]
    assert [event.local_receive_ts_ns for event in advance.source_events] == [
        (BASE_MS + 200) * 1_000_000,
        (BASE_MS + 150) * 1_000_000,
    ]
    assert all(
        change.feature_ready_ts_ns == (BASE_MS + 200) * 1_000_000
        for change in advance.level_changes
    )
    assert scheduler.lookup("BUY", 990).quantity == pytest.approx(3.0)
    assert scheduler.stats().head_of_line_clamped_events == 1
    assert scheduler.stats().max_head_of_line_delay_ns == 50_000_000


def test_visibility_scheduler_clamps_feature_ready_before_exchange_truth() -> None:
    event = _event(
        100,
        event_type="snapshot",
        levels=(("bid", 990, 5.0), ("ask", 1001, 2.0)),
        last_update_id=100,
        ordinal=1,
    )
    scheduler = HistoricalExchangeBookVisibilityScheduler()

    assigned = scheduler.enqueue(
        event,
        feature_ready_ts_ns=(BASE_MS + 90) * 1_000_000,
    )
    scheduler.advance_to(assigned)

    assert assigned == event.exchange_ts_ns
    assert scheduler.stats().pre_exchange_clamped_events == 1
    assert scheduler.lookup("BUY", 990).quantity == pytest.approx(5.0)


def test_boundary_preview_is_read_only_and_collects_same_time_messages() -> None:
    events = _events()[:1] + [
        _event(
            500,
            event_type="delta",
            levels=(("bid", 990, 3.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
        _event(
            500,
            event_type="delta",
            levels=(("ask", 1010, 2.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
        ),
    ]
    scheduler = HistoricalExchangeBookScheduler(events)
    scheduler.advance_to((BASE_MS + 500) * 1_000_000, inclusive=False)
    fingerprint_before = scheduler.state_fingerprint()
    stats_before = scheduler.stats()

    preview = scheduler.preview_at((BASE_MS + 500) * 1_000_000)

    assert preview.event_count == 2
    assert preview.touched_levels == {
        ("bid", 990),
        ("ask", 1010),
    }
    assert not preview.snapshot_or_gap
    assert scheduler.state_fingerprint() == fingerprint_before
    assert scheduler.stats() == stats_before

    scheduler.advance_to((BASE_MS + 500) * 1_000_000, inclusive=True)
    assert scheduler.lookup("BUY", 990).quantity == pytest.approx(3.0)
    assert scheduler.lookup("SELL", 1010).quantity == pytest.approx(2.0)


def test_exchange_book_state_is_independent_of_strategy_query_trajectory() -> None:
    first = HistoricalExchangeBookScheduler(_events())
    second = HistoricalExchangeBookScheduler(_events())

    first.advance_to((BASE_MS + 500) * 1_000_000, inclusive=False)
    first.lookup("BUY", 990)
    first.lookup("SELL", 1010)
    first.advance_to((BASE_MS + 750) * 1_000_000, inclusive=True)

    second.advance_to((BASE_MS + 200) * 1_000_000, inclusive=True)
    for tick in (985, 990, 999, 1001, 1010, 1020):
        second.lookup("BUY" if tick < 1000 else "SELL", tick)
    second.advance_to((BASE_MS + 750) * 1_000_000, inclusive=True)

    assert first.state_fingerprint() == second.state_fingerprint()
    assert first.stats() == second.stats()


def test_level_filter_does_not_change_reconstructed_state() -> None:
    unfiltered = HistoricalExchangeBookScheduler(_events())
    filtered = HistoricalExchangeBookScheduler(_events())

    all_changes = unfiltered.advance_to(
        (BASE_MS + 750) * 1_000_000,
        inclusive=True,
    ).level_changes
    watched_changes = filtered.advance_to(
        (BASE_MS + 750) * 1_000_000,
        inclusive=True,
        emitted_levels={("ask", 1_001)},
    ).level_changes

    assert len(all_changes) == 2
    assert watched_changes == ()
    assert filtered.lookup("BUY", 990).quantity == pytest.approx(4.0)
    assert unfiltered.state_fingerprint() == filtered.state_fingerprint()


@pytest.mark.parametrize(
    ("side", "tick", "status", "quantity"),
    [
        ("BUY", 999, "exact", 2.0),
        ("SELL", 1_001, "exact", 2.0),
        ("BUY", 995, "known_zero", 0.0),
        ("SELL", 1_005, "known_zero", 0.0),
        ("BUY", 985, "unknown", None),
    ],
)
def test_strict_before_lookup_recovers_only_unchanged_level_state(
    side: str, tick: int, status: str, quantity: float | None,
) -> None:
    scheduler = HistoricalExchangeBookScheduler(_events())
    boundary = (BASE_MS + 500) * 1_000_000
    scheduler.advance_to(boundary, inclusive=False)
    prior = scheduler.lookup_strictly_before(side, tick, boundary)
    assert prior.status == status
    assert prior.quantity == quantity
    assert prior.asof_exchange_ts_ns == (BASE_MS + 100) * 1_000_000

    advance = scheduler.advance_to(boundary, emitted_levels=set())
    assert advance.level_changes == ()
    assert scheduler.lookup(side, tick).asof_exchange_ts_ns == boundary
    fingerprint = scheduler.state_fingerprint()
    stats = scheduler.stats()

    # Neither a lookup nor repeated inclusive/exclusive calls at t may erase
    # the true prior watermark or promote an unknown level to exact support.
    for inclusive in (True, False, True):
        scheduler.advance_to(boundary, inclusive=inclusive)
        assert scheduler.lookup_strictly_before(side, tick, boundary) == prior
    assert scheduler.state_fingerprint() == fingerprint
    assert scheduler.stats() == stats


@pytest.mark.parametrize("emitted_levels", [None, set()])
def test_strict_before_lookup_rejects_touched_level_even_when_not_emitted(
    emitted_levels: set[tuple[str, int]] | None,
) -> None:
    scheduler = HistoricalExchangeBookScheduler(_events())
    boundary = (BASE_MS + 500) * 1_000_000
    scheduler.advance_to(boundary, emitted_levels=emitted_levels)

    assert scheduler.lookup("BUY", 990).quantity == pytest.approx(3.0)
    prior = scheduler.lookup_strictly_before("BUY", 990, boundary)
    assert prior.status == "ambiguous"
    assert prior.reason == "same_timestamp_level_touched"
    assert prior.quantity is None
    assert not prior.strict_usable


def test_strict_before_lookup_retains_all_same_timestamp_scheduled_messages() -> None:
    scheduler = HistoricalExchangeBookScheduler([])
    snapshot, first_delta = _events()[:2]
    boundary = first_delta.exchange_ts_ns
    scheduler.apply_scheduled_events([snapshot], boundary_ts_ns=snapshot.exchange_ts_ns)
    scheduler.apply_scheduled_events([first_delta], boundary_ts_ns=boundary)
    second_delta = _event(
        500,
        event_type="delta",
        levels=(("ask", 1_010, 3.0),),
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
    )
    scheduler.apply_scheduled_events(
        [second_delta], boundary_ts_ns=boundary, emitted_levels=set(),
    )
    scheduler.apply_scheduled_events([], boundary_ts_ns=boundary)

    for side, tick in (("BUY", 990), ("SELL", 1_010)):
        lookup = scheduler.lookup_strictly_before(side, tick, boundary)
        assert lookup.reason == "same_timestamp_level_touched"
        assert not lookup.strict_usable
    unchanged = scheduler.lookup_strictly_before("BUY", 999, boundary)
    assert unchanged.quantity == pytest.approx(2.0)
    assert unchanged.asof_exchange_ts_ns == snapshot.exchange_ts_ns


@pytest.mark.parametrize("event_kind", ["snapshot", "source_gap", "sequence_gap"])
def test_strict_before_lookup_rejects_same_timestamp_book_discontinuities(
    event_kind: str,
) -> None:
    event = _event(
        500,
        event_type="delta" if event_kind == "sequence_gap" else event_kind,
        levels=() if event_kind == "source_gap" else (("bid", 990, 3.0),),
        first_update_id=101,
        final_update_id=101,
        previous_final_update_id=999,
        last_update_id=200 if event_kind == "snapshot" else None,
    )
    scheduler = HistoricalExchangeBookScheduler(
        [_events()[0], event], strict_sequence=False,
    )
    scheduler.advance_to(event.exchange_ts_ns, emitted_levels=set())

    lookup = scheduler.lookup_strictly_before("SELL", 1_001, event.exchange_ts_ns)
    assert lookup.status == "ambiguous"
    assert lookup.reason == "same_timestamp_book_discontinuity"
    assert lookup.quantity is None
    assert not lookup.strict_usable


def test_strict_before_lookup_does_not_rewind_past_retained_timestamp() -> None:
    scheduler = HistoricalExchangeBookScheduler(_events())
    scheduler.advance_to((BASE_MS + 750) * 1_000_000)

    lookup = scheduler.lookup_strictly_before("SELL", 1_001, (BASE_MS + 500) * 1_000_000)
    assert lookup.status == "unknown"
    assert lookup.reason == "strict_before_state_not_retained"
    assert lookup.quantity is None
    assert not lookup.strict_usable


def test_sequence_gap_invalidates_native_state_until_a_new_snapshot() -> None:
    broken = _events()[:2] + [
        _event(
            800,
            event_type="delta",
            levels=(("bid", 990, 1.0),),
            first_update_id=103,
            final_update_id=103,
            previous_final_update_id=999,
            ordinal=4,
        )
    ]
    diagnostic = HistoricalExchangeBookScheduler(
        broken,
        strict_sequence=False,
    )
    advance = diagnostic.advance_to(
        (BASE_MS + 800) * 1_000_000,
        inclusive=True,
    )
    assert advance.invalidated
    assert diagnostic.lookup("BUY", 990).status == "unknown"
    assert diagnostic.stats().sequence_gaps == 1

    strict = HistoricalExchangeBookScheduler(broken)
    with pytest.raises(ValueError, match="sequence gap"):
        strict.advance_to(
            (BASE_MS + 800) * 1_000_000,
            inclusive=True,
        )


def test_strict_sequence_begins_after_recoverable_warmup() -> None:
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(("bid", 990, 2.0), ("ask", 1001, 2.0)),
            last_update_id=100,
            ordinal=1,
        ),
        _event(
            200,
            event_type="delta",
            levels=(("bid", 990, 1.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=999,
            ordinal=2,
        ),
        _event(
            300,
            event_type="snapshot",
            levels=(("bid", 990, 3.0), ("ask", 1001, 2.0)),
            last_update_id=200,
            ordinal=3,
        ),
        _event(
            600,
            event_type="delta",
            levels=(("bid", 990, 1.0),),
            first_update_id=201,
            final_update_id=201,
            previous_final_update_id=999,
            ordinal=4,
        ),
    ]
    scheduler = HistoricalExchangeBookScheduler(
        events,
        strict_sequence=True,
        strict_after_ns=(BASE_MS + 400) * 1_000_000,
    )

    scheduler.advance_to((BASE_MS + 300) * 1_000_000)
    assert scheduler.lookup("BUY", 990).quantity == pytest.approx(3.0)
    with pytest.raises(ValueError, match="sequence gap"):
        scheduler.advance_to((BASE_MS + 600) * 1_000_000)


def test_delta_bootstrap_knows_only_explicitly_updated_levels() -> None:
    events = [
        _event(
            100,
            event_type="delta",
            levels=(("bid", 990, 3.0),),
            first_update_id=100,
            final_update_id=100,
            previous_final_update_id=99,
            ordinal=1,
        ),
        _event(
            200,
            event_type="delta",
            levels=(("ask", 1010, 4.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
    ]
    scheduler = HistoricalExchangeBookScheduler(
        events,
        allow_delta_bootstrap=True,
    )

    scheduler.advance_to(
        (BASE_MS + 200) * 1_000_000,
        inclusive=True,
    )

    assert scheduler.lookup("BUY", 990).status == "exact"
    assert scheduler.lookup("SELL", 1010).status == "exact"
    assert scheduler.lookup("BUY", 989).status == "unknown"
    assert scheduler.stats().delta_bootstrap_events == 1


def test_snapshot_proves_cross_side_structural_zero_half_lines() -> None:
    scheduler = HistoricalExchangeBookScheduler(_events()[:1])
    scheduler.advance_to((BASE_MS + 100) * 1_000_000, inclusive=True)

    high_bid = scheduler.lookup("BUY", 1_100)
    low_ask = scheduler.lookup("SELL", 900)

    assert high_bid.status == "known_zero"
    assert high_bid.reason == "opposite_top_structural_zero"
    assert low_ask.status == "known_zero"
    assert low_ask.reason == "opposite_top_structural_zero"
    assert scheduler.lookup("BUY", 900).status == "unknown"
    assert scheduler.lookup("SELL", 1_100).status == "unknown"


def test_new_snapshot_atomically_replaces_previous_book_segment() -> None:
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(("bid", 990, 2.0), ("ask", 1_001, 2.0)),
            last_update_id=100,
            ordinal=1,
        ),
        _event(
            300,
            event_type="snapshot",
            levels=(("bid", 1_090, 3.0), ("ask", 1_101, 4.0)),
            last_update_id=200,
            ordinal=2,
        ),
    ]
    scheduler = HistoricalExchangeBookScheduler(events)

    scheduler.advance_to((BASE_MS + 300) * 1_000_000, inclusive=True)

    bids, asks = scheduler.top_levels(2)
    assert bids == [(1_090.0, 3.0)]
    assert asks == [(1_101.0, 4.0)]
    assert scheduler.lookup("BUY", 990).status == "unknown"
    assert scheduler.lookup("SELL", 1_001).status == "known_zero"


def test_scheduler_consumes_all_native_events_before_next_replay_boundary() -> None:
    scheduler = HistoricalExchangeBookScheduler(_events())

    advance = scheduler.advance_to(
        (BASE_MS + 1_000) * 1_000_000,
        inclusive=True,
    )

    assert advance.accepted_events == 3
    assert [change.exchange_ts_ns for change in advance.level_changes] == [
        (BASE_MS + 500) * 1_000_000,
        (BASE_MS + 750) * 1_000_000,
    ]
    assert scheduler.lookup("BUY", 990).quantity == pytest.approx(4.0)


def test_tick_replay_seeds_queue_and_path_from_native_exchange_book() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200, BASE_MS + 2_200],
                dtype=np.int64,
            ),
            "price": np.full(3, 100.0),
            "quantity": np.zeros(3),
            "is_buyer_maker": np.ones(3, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 100.0,
        "rq_min": 100.0,
        "rq_max": 100.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "new_order_latency_ms": 0,
        "replace_min_price_change_ticks": 1_000.0,
        "replace_min_price_change_ticks_reducing": 1_000.0,
        "replace_min_interval_ms": 1_000_000.0,
        "replace_min_interval_ms_reducing": 1_000_000.0,
        "initial_live_state": {
            "active_orders": [
                {
                    "side": "BUY",
                    "price": 99.0,
                    "quantity": 0.001,
                    "remaining": 0.001,
                    "submit_ts_ms": BASE_MS + 100,
                    "event_ts_ms": BASE_MS + 300,
                    "status": "PENDING_NEW",
                    "mid_at_quote": 100.0,
                }
            ]
        },
        "trace_local_order_value_max": 20,
        "local_order_value_fill_horizon_ms": 500,
        "local_order_value_price_jump_ticks": 1.0,
        "exchange_book_queue_mode": "diagnostic",
        "exchange_book_queue_ambiguity_trace_max": 10,
    }
    bbo_ts = BASE_MS + np.asarray(
        [200, 400, 600, 800, 1_000, 1_200, 2_000],
        dtype=np.int64,
    )
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts,
        best_bid=np.full(bbo_ts.size, 99.9),
        best_ask=np.full(bbo_ts.size, 100.1),
        bid_qty=np.asarray([1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0]),
        ask_qty=np.asarray([2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0]),
    )

    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        bbo_data=bbo,
        exchange_book_event_tape=_events(),
    )

    assert result["exchange_book_events_consumed"] == 3
    assert result["exchange_book_queue_exact_count"] >= 1
    assert result["exchange_book_queue_scope"].startswith(
        "strategy_independent_native"
    )
    trace = result["_local_order_value_trace"]
    native_rows = [
        row
        for row in trace
        if row["simulator_queue_source"] == "native_exchange_book"
    ]
    assert native_rows
    assert native_rows[0]["queue_source"] == (
        "delayed_policy_topn_or_fitted"
    )
    assert native_rows[0]["exchange_book_queue_status"] == "exact"
    assert native_rows[0]["simulator_queue_init"] == pytest.approx(5.0)
    assert native_rows[0]["queue_init"] != pytest.approx(
        native_rows[0]["simulator_queue_init"]
    )
    assert native_rows[0]["exchange_book_event_count"] >= 1.0
    assert native_rows[0]["exchange_book_cancel_qty"] == pytest.approx(2.0)
    assert native_rows[0]["exchange_book_refill_qty"] == pytest.approx(1.0)
    assert native_rows[0]["cancel_count"] > 0.0
    assert native_rows[0]["refill_count"] > 0.0


def test_native_exchange_book_accepts_empirical_live_alignment_clock() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200], dtype=np.int64
            ),
            "price": np.full(2, 100.0),
            "quantity": np.zeros(2),
            "is_buyer_maker": np.ones(2, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 100.0,
        "rq_min": 100.0,
        "rq_max": 100.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "replay_event_clock": "empirical",
        "_empirical_requote_ts_ms": np.asarray(
            [BASE_MS + 200, BASE_MS + 1_200], dtype=np.int64
        ),
        "_empirical_requote_action": np.asarray([2, 2], dtype=np.int8),
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "new_order_latency_ms": 0,
        "exchange_book_queue_mode": "diagnostic",
    }

    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=_events(),
    )

    assert result["exchange_book_events_consumed"] == 3
    assert result["exchange_book_queue_mode"] == "diagnostic"


def test_order_activation_between_outer_events_uses_pre_activation_book() -> None:
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(
                ("bid", 900, 1.0),
                ("bid", 967, 5.0),
                ("bid", 999, 2.0),
                ("ask", 1001, 2.0),
                ("ask", 1034, 5.0),
                ("ask", 1100, 1.0),
            ),
            last_update_id=100,
            ordinal=1,
        ),
        _event(
            500,
            event_type="delta",
            levels=(("bid", 967, 3.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
        _event(
            750,
            event_type="delta",
            levels=(("bid", 967, 4.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
        ),
    ]
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200, BASE_MS + 1_200, BASE_MS + 2_200],
                dtype=np.int64,
            ),
            "price": np.full(3, 100.0),
            "quantity": np.zeros(3),
            "is_buyer_maker": np.ones(3, dtype=np.uint8),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 100.0,
        "rq_min": 100.0,
        "rq_max": 100.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "new_order_latency_ms": 100,
        "replace_min_price_change_ticks": 1_000.0,
        "replace_min_price_change_ticks_reducing": 1_000.0,
        "replace_min_interval_ms": 1_000_000.0,
        "replace_min_interval_ms_reducing": 1_000_000.0,
        "trace_local_order_value_max": 20,
        "local_order_value_fill_horizon_ms": 500,
        "local_order_value_price_jump_ticks": 1.0,
        "exchange_book_queue_mode": "diagnostic",
        "exchange_book_queue_ambiguity_trace_max": 10,
    }

    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=events,
    )

    buy_row = next(
        row
        for row in result["_local_order_value_trace"]
        if row["side"] == "BUY"
    )
    assert buy_row["decision_ts_ns"] == (BASE_MS + 300) * 1_000_000
    assert buy_row["simulator_queue_source"] == "native_exchange_book"
    assert buy_row["queue_source"] == "delayed_policy_topn_or_fitted"
    assert buy_row["simulator_queue_init"] == pytest.approx(5.0)
    assert buy_row["exchange_book_cancel_qty"] == pytest.approx(2.0)
    assert buy_row["exchange_book_refill_qty"] == pytest.approx(1.0)
    assert buy_row["exchange_book_queue_path_valid"] == 1


@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize(
    ("same_ms_trades", "ambiguous", "quantity_case"),
    [
        pytest.param([(100.0, 1)], False, "ordinary", id="different-price"),
        pytest.param([(96.7, 0)], False, "ordinary", id="opposite-aggressor"),
        pytest.param([(100.0, 1), (96.7, 0)], False, "ordinary", id="unrelated-batch"),
        pytest.param([(96.7, 1), (100.0, 1)], True, "ordinary", id="related-first-child"),
        pytest.param([(100.0, 1), (96.7, 1)], True, "ordinary", id="related-last-child"),
        pytest.param([(100.0, 1), (96.6, 1)], True, "ordinary", id="trade-through-last-child"),
        pytest.param([], False, "thin_level", id="one-lot-level-roundoff"),
        pytest.param([], False, "whole_lot_depletion", id="whole-lot-depletion"),
        pytest.param([], False, "partial_final_lot", id="partial-final-lot"),
        pytest.param([], False, "true_sub_lot", id="true-sub-lot"),
    ],
)
def test_native_queue_updates_preserve_fill_and_ambiguity(
    side: str, same_ms_trades: list[tuple[float, int]], ambiguous: bool, quantity_case: str,
) -> None:
    # Mirror the batch around the resting BUY/SELL price. Only a counterparty
    # taker reaching that level makes its update ambiguous, regardless of its
    # position in the batch. A later clear must still clear modeled ahead.
    buy = side == "BUY"
    order_price = 96.7 if buy else 103.4
    level_side = "bid" if buy else "ask"
    level_tick = 967 if buy else 1034
    thin_level = quantity_case == "thin_level"
    partial_fill = quantity_case == "partial_final_lot"
    sub_lot = quantity_case == "true_sub_lot"
    quantity_depletion = quantity_case == "whole_lot_depletion" or sub_lot
    initial_qty = 0.190 if thin_level else 5.0
    if quantity_depletion:
        initial_qty = 0.010
    elif partial_fill:
        initial_qty = 0.0
    order_qty = 0.011 if partial_fill else 0.001
    final_prices = [order_price]
    final_offsets = [1_000]
    final_quantities = [0.001]
    if quantity_depletion:
        # Public trade quantities need not be whole lots. Only representation
        # error in .011 - .010 may be corrected, not a real sub-lot remainder.
        final_quantities = [0.01099999 if sub_lot else 0.011]
    elif partial_fill:
        # The last lot remains active after filling .010 of an .011 order.
        final_quantities = [0.010]
    if thin_level or partial_fill:
        # In the thin-level case, .190 - .189 must not leave a residual above
        # .001 that shaves this next trade below the one-lot fill threshold.
        final_prices.append(order_price + (-0.1 if buy else 0.1))
        final_offsets.append(1_100 if partial_fill else 1_000)
        final_quantities.append(0.001)
    count = len(same_ms_trades)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200] + [BASE_MS + 500] * count
                + [BASE_MS + offset for offset in final_offsets] + [BASE_MS + 1_200],
                dtype=np.int64,
            ),
            "price": np.asarray(
                [100.0] + [
                    order_price + (price - 96.7) * (1 if buy else -1)
                    for price, _ in same_ms_trades
                ] + final_prices + [100.0]
            ),
            "quantity": np.asarray(
                [0.0] + [0.5] * count + final_quantities + [0.0]
            ),
            "is_buyer_maker": np.asarray(
                [1] + [flag if buy else 1 - flag for _, flag in same_ms_trades]
                + [int(buy)] * len(final_prices) + [1], dtype=np.uint8,
            ),
        }
    )
    params = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": order_qty,
        "max_inventory": 0.1 if partial_fill else 0.01,
        "requote_interval": 100.0,
        "rq_min": 100.0,
        "rq_max": 100.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "use_bar_pricing": True,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 1_000,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "new_order_latency_ms": 100,
        "replace_min_price_change_ticks": 1_000.0,
        "replace_min_price_change_ticks_reducing": 1_000.0,
        "replace_min_interval_ms": 1_000_000.0,
        "replace_min_interval_ms_reducing": 1_000_000.0,
        "trace_local_order_value_max": 20,
        "local_order_value_fill_horizon_ms": 500,
        "local_order_value_price_jump_ticks": 1.0,
        "exchange_book_queue_mode": "diagnostic",
        "trace_quotes_max": 20,
        "trace_fills_max": 20,
    }

    params["exchange_book_queue_ambiguity_trace_max"] = 10
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(("bid", 900, 1.0), ("bid", 967, initial_qty), ("bid", 999, 2.0),
                    ("ask", 1001, 2.0), ("ask", 1034, initial_qty), ("ask", 1100, 1.0)),
            last_update_id=100,
            ordinal=1,
        ),
        _event(
            500,
            event_type="delta",
            levels=((level_side, level_tick, initial_qty if quantity_case != "ordinary" else 3.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
        _event(
            750,
            event_type="delta",
            levels=((level_side, level_tick, initial_qty if quantity_case != "ordinary" else 4.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
        ),
        _event(
            900,
            event_type="delta",
            levels=((level_side, level_tick,
                     0.001 if thin_level else initial_qty if quantity_depletion else 0.0),),
            first_update_id=103,
            final_update_id=103,
            previous_final_update_id=102,
            ordinal=4,
        )
    ]
    result = simulate_tick(
        trades,
        np.asarray([BASE_MS], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        exchange_book_event_tape=events,
    )

    if ambiguous:
        assert result["exchange_book_queue_ambiguous_event_count"] >= 1
        assert result["exchange_book_queue_invalidated_order_count"] >= 1
        assert result["_exchange_book_queue_ambiguity_trace"][0]["reason"] == (
            "same_ms_exchange_book_ambiguity"
        )
        assert result["_exchange_book_queue_ambiguity_trace"][0]["ambiguous"] is True
    else:
        assert result["exchange_book_queue_ambiguous_event_count"] == 0
        assert result["exchange_book_queue_invalidated_order_count"] == 0
        assert result["_exchange_book_queue_ambiguity_trace"] == []
    native_rows = [
        row
        for row in result["_local_order_value_trace"]
        if row["simulator_queue_source"] == "native_exchange_book" and row["side"] == side
    ]
    assert native_rows
    assert native_rows[0]["exchange_book_queue_path_valid"] == int(not ambiguous)
    assert bool(native_rows[0]["exchange_book_ambiguous_event_count"]) is ambiguous
    target_order = next(
        row for row in result["_quote_trace"]
        if row["side"] == side and row["price"] == order_price
    )
    assert target_order["queue_init"] == pytest.approx(initial_qty)
    assert target_order["queue_left"] == pytest.approx(0.0)
    if sub_lot:
        assert target_order["outcome"] == "open_end"
        assert target_order["fill_qty"] == 0.0
        assert target_order["remaining"] == order_qty
        return
    if partial_fill:
        # Quote trace emits each fill delta, not one cumulative terminal row.
        # Both legs must belong to the original order, with one lot still live
        # after the first leg and an exact zero only after the second.
        order_rows = [
            row for row in result["_quote_trace"]
            if row["order_id"] == target_order["order_id"]
        ]
        assert target_order["quantity"] == order_qty
        assert [
            (row["outcome_ts"], row["fill_qty"], row["remaining"])
            for row in order_rows
        ] == [
            (BASE_MS + 1_000, 0.010, 0.001), (BASE_MS + 1_100, 0.001, 0.0),
        ]
        assert all(row["outcome"] == "fill" for row in order_rows)
        side_fills = [row for row in result["_fill_trace"] if row["side"] == side]
        assert [(row["fill_ts"], row["fill_qty"]) for row in side_fills] == [
            (BASE_MS + 1_000, 0.010), (BASE_MS + 1_100, 0.001),
        ]
        return
    assert target_order["outcome"] == "fill"
    assert target_order["fill_qty"] == order_qty
    assert target_order["remaining"] == 0.0
    assert target_order["outcome_ts"] == BASE_MS + final_offsets[-1]
