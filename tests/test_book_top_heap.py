from decimal import Decimal as D
import random

from data.tardis_input import BookMessage, ObservableBook


def test_heap_top_matches_full_book_after_updates_deletes_and_rebases():
    import inspect
    fields = inspect.signature(BookMessage).parameters
    # Explicit synthetic message, not purchased rows.
    base = dict(market_id="m", source_file_id="synthetic", first_row=1, last_row=1,
        source_ordinal=0, provider_group_id="synthetic", kind="snapshot",
        source_timestamp_us=0, source_clock_kind="unknown", exchange_ts_ns=None,
        provider_receive_ts_us=0, boundary_status="complete", state_time_certainty="unknown")
    base = {k: v for k, v in base.items() if k in fields}
    book = ObservableBook()
    rng = random.Random(42)
    for i in range(500):
        levels = tuple((rng.choice(["bid", "ask"]), D(rng.randrange(1, 100)), D(rng.randrange(4))) for _ in range(20))
        book.apply(BookMessage(**{**base, "kind": "snapshot" if i % 90 == 0 else "delta",
                                  "source_ordinal": i, "source_timestamp_us": i}, levels=levels))
        for depth in (1, 5, 20, 100):
            view = book.view(depth)
            for side, actual in (("bid", view.bids), ("ask", view.asks)):
                expected = tuple(sorted(((p, q) for (s, p), q in book._levels.items() if s == side), reverse=side == "bid")[:depth])
                assert actual == expected
