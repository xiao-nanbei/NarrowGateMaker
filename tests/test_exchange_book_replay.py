from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from models.backtest_tick import simulate_tick
from models.exchange_book_replay import (
    HistoricalExchangeBookScheduler,
    HistoricalExchangeBookVisibilityScheduler,
    HistoricalMessageDeliverySchedule,
)
from models.tick_data_types import (
    HistoricalBBOData,
    HistoricalExchangeBookEvent,
)

BASE_MS = 1_700_000_000_000


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
    ("same_ms_trades", "ambiguous"),
    [
        pytest.param([(100.0, 1)], False, id="different-price"),
        pytest.param([(96.7, 0)], False, id="opposite-aggressor"),
        pytest.param([(100.0, 1), (96.7, 0)], False, id="unrelated-batch"),
        pytest.param([(96.7, 1), (100.0, 1)], True, id="related-first-child"),
        pytest.param([(100.0, 1), (96.7, 1)], True, id="related-last-child"),
        pytest.param([(100.0, 1), (96.6, 1)], True, id="trade-through-last-child"),
    ],
)
def test_same_millisecond_ambiguity_does_not_freeze_modeled_queue(
    side: str, same_ms_trades: list[tuple[float, int]], ambiguous: bool,
) -> None:
    # Mirror the batch around the resting BUY/SELL price. Only a counterparty
    # taker reaching that level makes its update ambiguous, regardless of its
    # position in the batch. A later clear must still clear modeled ahead.
    buy = side == "BUY"
    order_price = 96.7 if buy else 103.4
    level_side = "bid" if buy else "ask"
    level_tick = 967 if buy else 1034
    count = len(same_ms_trades)
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS + 200] + [BASE_MS + 500] * count
                + [BASE_MS + 1_000, BASE_MS + 1_200],
                dtype=np.int64,
            ),
            "price": np.asarray(
                [100.0] + [
                    order_price + (price - 96.7) * (1 if buy else -1)
                    for price, _ in same_ms_trades
                ] + [order_price, 100.0]
            ),
            "quantity": np.asarray([0.0] + [0.5] * count + [0.001, 0.0]),
            "is_buyer_maker": np.asarray(
                [1] + [flag if buy else 1 - flag for _, flag in same_ms_trades]
                + [int(buy), 1], dtype=np.uint8,
            ),
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
        "trace_quotes_max": 20,
    }

    params["exchange_book_queue_ambiguity_trace_max"] = 10
    events = [
        _event(
            100,
            event_type="snapshot",
            levels=(("bid", 900, 1.0), ("bid", 967, 5.0), ("bid", 999, 2.0),
                    ("ask", 1001, 2.0), ("ask", 1034, 5.0), ("ask", 1100, 1.0)),
            last_update_id=100,
            ordinal=1,
        ),
        _event(
            500,
            event_type="delta",
            levels=((level_side, level_tick, 3.0),),
            first_update_id=101,
            final_update_id=101,
            previous_final_update_id=100,
            ordinal=2,
        ),
        _event(
            750,
            event_type="delta",
            levels=((level_side, level_tick, 4.0),),
            first_update_id=102,
            final_update_id=102,
            previous_final_update_id=101,
            ordinal=3,
        ),
        _event(
            900,
            event_type="delta",
            levels=((level_side, level_tick, 0.0),),
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
    assert target_order["queue_init"] == pytest.approx(5.0)
    assert target_order["queue_left"] == pytest.approx(0.0)
    assert target_order["outcome"] == "fill"
    assert target_order["outcome_ts"] == BASE_MS + 1_000
