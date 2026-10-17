"""Synthetic output parity for immutable per-message book reuse."""

from dataclasses import replace
from decimal import Decimal as D
import pickle

from data.runtime import ObservationProfile, PublicInputStream
from data.tardis_input import BookMessage, BookView, ObservableBook
from research.families.f08_side_taker_lifecycle.joint_trade_book_response import MarketPanels, day_of

SECOND = 1_000_000_000
MARKET = "binance_futures:perpetual:BTCUSDC"


def message(i, kind="delta", levels=None):
    return BookMessage(MARKET, "synthetic", i + 1, i + 1, i, str(i), kind,
                       i * 1000, "unknown", None, i * 1000, "complete", "unknown",
                       levels or (("bid", D(99), D(1 + i % 3)), ("ask", D(101), D(2))))


def expected(book, depth):
    bids = sorted(((p, q) for (s, p), q in book._levels.items() if s == "bid"), reverse=True)[:depth]
    asks = sorted((p, q) for (s, p), q in book._levels.items() if s == "ask")[:depth]
    valid = bool(book.initialized and bids and asks and bids[0][0] < asks[0][0])
    return BookView(book.version, tuple(bids), tuple(asks), book.source_asof_us,
                    book.state_change_us, book.state_time_certainty, valid)


def test_cached_depths_mutations_and_restore_are_exact_and_bounded():
    book = ObservableBook()
    for i in range(100):
        levels = (("bid", D(99 - i % 25), D(i % 4)), ("ask", D(101 + i % 25), D(2)))
        book.apply(message(i, "snapshot" if i % 31 == 0 else "delta", levels))
        old = book.view(20)
        for depth in (20, 1, 5, *range(1, 16)):
            assert book.view(depth) == expected(book, depth)
            assert len(book._view_cache) <= 8
        clone = pickle.loads(pickle.dumps(book))
        clone.apply(message(i + 100, levels=(("ask", D(98), D(1)),)))
        assert old == book.view(20)
        assert clone.view() == expected(clone, 20)
        assert clone._view_cache is not book._view_cache


def test_per_message_views_preserve_same_tick_intermediates_and_panels():
    profile = ObservationProfile("test", "source_timestamp_proxy", 10_000_000, 0, 0,
                                 SECOND, trade_coverage="observed",
                                 depth_connection_id="depth", trade_connection_id="trade")
    events = []
    for i in range(90):
        # Three complete messages per clock; resets and same-price versions.
        ts = (i // 3) * SECOND
        event = replace(message(i, "snapshot" if i % 30 == 0 else "delta"),
                        source_timestamp_us=ts // 1000)
        events.append((ts, event))

    def run(capture, mode="default", skip_book=False):
        stream = PublicInputStream([], profile=profile, start_ns=0, end_ns=35 * SECOND,
                                   market_id=MARKET, input_contract_id="test",
                                   capture_source_books=capture, consumer_mode=mode,
                                   scan_skip_book_view=skip_book)
        stream._channel = lambda channel: iter(events if channel == "incremental_book_L2" else [])
        panel = MarketPanels(start_ns=0, end_ns=35 * SECOND, profile=profile, limit=17)
        ticks = []
        for tick in stream:
            if capture and tick.exchange_events:
                assert len(tick.source_book_views) == 3
                assert len({v.version for v in tick.source_book_views}) == 3
            panel.advance(tick)
            ticks.append(replace(tick, source_book_views=None))
        panel.flush(35 * SECOND)
        return ticks, stream, panel

    old_ticks, old_stream, old = run(False)
    new_ticks, new_stream, new = run(True)
    assert old_ticks == new_ticks
    assert old_stream.stats == new_stream.stats
    assert old_stream.delivery_idle_ns == new_stream.delivery_idle_ns
    assert old.days == new.days
    assert old.samples["1970-01-01"].selected() == new.samples["1970-01-01"].selected()
    # Sharing immutable views changes pickle memoization, not economic state.
    for panel in old.states:
        assert vars(old.states[panel]) == vars(new.states[panel])
    scan_ticks, scan_stream, scan = run(True, "market_response_scan")
    assert scan_ticks == [replace(t, feature_frame=None) for t in new_ticks]
    assert scan_stream.stats == new_stream.stats
    assert scan_stream.delivery_idle_ns == new_stream.delivery_idle_ns
    assert scan.days == new.days
    assert scan.samples["1970-01-01"].selected() == new.samples["1970-01-01"].selected()
    light_ticks, light_stream, light = run(True, "market_response_scan", True)
    assert light_ticks == [replace(t, exchange_book=None) for t in scan_ticks]
    assert light_stream.stats == scan_stream.stats
    assert light.samples["1970-01-01"].selected() == scan.samples["1970-01-01"].selected()


def test_calendar_cache_preserves_negative_and_midnight_boundaries():
    assert day_of(-1) == "1969-12-31"
    assert day_of(0) == "1970-01-01"
    assert day_of(86400 * SECOND - 1) == "1970-01-01"
    assert day_of(86400 * SECOND) == "1970-01-02"


def test_incremental_top_one_hundred_thousand_mutations():
    import random
    rng = random.Random(78493)
    reference, fast = ObservableBook(), ObservableBook(incremental_top_cache=True)
    for i in range(100_000):
        side = rng.choice(("bid", "ask"))
        price = D(1000 + (1 if side == "ask" else -1) * rng.randint(1, 80))
        event = message(i, "snapshot" if i % 997 == 0 else "delta",
                        ((side, price, D(rng.randrange(5))),))
        assert reference.apply(event) == fast.apply(event)
        for depth in (1, 20, 37):
            assert reference.view(depth) == fast.view(depth)
        if i % 1000 == 0:
            fast = pickle.loads(pickle.dumps(fast))


def test_fast_response_random_resets_gaps_same_clock_and_restore():
    import random
    from features.trade_book_response import ResponseState
    rng = random.Random(632)
    states = [ResponseState(trade_coverage="observed", fast_response_state=flag)
              for flag in (False, True)]
    book = ObservableBook()
    now = 0
    for i in range(5000):
        now += rng.choice((0, 100_000_000, 1_500_000_000))
        levels = tuple((side, D(1000 + sign * p), D(rng.randrange(5)))
                       for side, sign in (("bid", -1), ("ask", 1))
                       for p in range(1, 30))
        book.apply(message(i, "snapshot" if i % 43 == 0 else "delta", levels))
        view = book.view(20)
        source = now - (2_000_000_000 if i % 41 == 0 else 0)
        frames = [s.observe_book(now, source, view, reset=i % 43 == 0) for s in states]
        assert frames[0] == frames[1]
        assert states[0].stats == states[1].stats
        assert states[0].recovery_outcomes == states[1].recovery_outcomes
        if i % 97 == 0:
            states = [pickle.loads(pickle.dumps(s)) for s in states]
        if i % 137 == 0:
            for s in states:
                s.invalidate(now, "synthetic")


def test_pre_optimization_serialized_states_restore_on_reference_path():
    from features.trade_book_response import ResponseState
    book = ObservableBook()
    book.apply(message(0, "snapshot"))
    expected_view = book.view()
    for key in ("incremental_top_cache", "_top_prices", "_top_members"):
        del book.__dict__[key]
    restored = pickle.loads(pickle.dumps(book))
    assert not restored.incremental_top_cache
    assert restored.view() == expected_view
    response = ResponseState(trade_coverage="observed")
    del response.__dict__["fast_response_state"]
    del response.__dict__["range_index"]
    restored_response = pickle.loads(pickle.dumps(response))
    assert not restored_response.fast_response_state
    assert restored_response.observe_book(0, 0, expected_view)
