"""Research depth cannot change or anticipate the original observation feed."""

from dataclasses import replace
from decimal import Decimal as D
import pickle

import pytest

from data.depth_observation import DepthObservationTap
from data.runtime import ObservationProfile, PublicInputStream
from data.tardis_input import BookMessage, ObservableBook


MARKET = "binance_futures:perpetual:BTCUSDC"


def message(ns=0, ordinal=0, kind="snapshot", levels=None):
    if levels is None:
        levels = tuple((side, D(price), D(1)) for side, prices in (
            ("bid", range(975, 1000)), ("ask", range(1001, 1026)),
        ) for price in prices)
    return BookMessage(MARKET, "synthetic", 1, 2, ordinal, "group", kind, ns // 1000,
                       "unknown", None, 0, "complete", "unknown", levels)


def stream(tap=None, *, measured=False):
    profile = ObservationProfile("synthetic", "source_timestamp_proxy", 0, 0, 0, 10**9)
    if measured:
        profile = replace(profile, measured_latency_path="synthetic",
                          measured_latency_sha256="synthetic",
                          measured_latency_market_id="binance:perp:BTCUSDC")
    result = PublicInputStream([], profile=profile, start_ns=0, end_ns=400_000_000,
                               market_id=MARKET, input_contract_id="synthetic",
                               incremental_top_cache=True, research_depth_tap=tap)
    rows = [(0, message()), (50_000_000, message(50_000_000, 1, "delta",
             (("bid", D(990), D(9)),))),
            (150_000_000, message(150_000_000, 2, "delta",
             (("bid", D(980), D(8)),)))]
    result._channel = lambda channel: iter(rows if channel == "incremental_book_L2" else [])
    return result


def test_opt_in_preserves_every_original_tick_frame_and_clock():
    old = stream()
    tap = DepthObservationTap(sample_period_ns=100_000_000)
    new = stream(tap)
    expected, observed = list(old), list(new)
    assert [replace(t, research_depth_observations=()) for t in observed] == expected
    assert new.stats == old.stats
    deep = [o for tick in observed for o in tick.research_depth_observations]
    assert len(deep) == 4
    assert all(len(o.original.payload.bids) == 20 and len(o.snapshot.book.bids) == 25 for o in deep)
    assert deep[0].snapshot.quantity("bid", D(990)) == 1
    assert deep[1].snapshot.quantity("bid", D(990)) == 9
    assert not tap.pending


def test_same_message_latency_draws_and_out_of_order_delivery(monkeypatch):
    import data.runtime as runtime
    draws = []

    class Delivery:
        def __init__(self, profile):
            pass

        def draw(self, channel, identity):
            draws.append((channel, identity))
            return dict(market_delay_ns=200_000_000 if identity == "depth:0" else 0,
                        processing_ns=0)

    monkeypatch.setattr(runtime, "MeasuredDelivery", Delivery)
    original = list(stream(measured=True))
    expected_draws = list(draws)
    draws.clear()
    tap = DepthObservationTap(sample_period_ns=100_000_000)
    observed = list(stream(tap, measured=True))
    assert draws == expected_draws
    assert [replace(t, research_depth_observations=()) for t in observed] == original
    deep = [o for tick in observed for o in tick.research_depth_observations]
    assert [o.original.event_id for o in deep][:2] == ["depth:100000000", "depth:0"]
    assert deep[1].original.ready_ns == 200_000_000
    assert deep[1].snapshot.quantity("bid", D(990)) == 1  # not latest source quantity


def test_external_delta_does_not_bridge_unknown_prices_and_reset_clears_range():
    book, tap = ObservableBook(), DepthObservationTap(sample_period_ns=100_000_000)
    for event in (message(), message(10_000, 1, "delta", (("bid", D(900), D(3)),))):
        book.apply(event)
        tap.source_applied(event, book)
    tap.publish("depth:0", book, 0, 0)
    snap = tap.pending["depth:0"]
    assert snap.quantity("bid", D(900)) == 3
    assert snap.quantity("bid", D(950)) is None
    assert not snap.covers("bid", D(900), D(999))
    assert snap.covers("bid", D(975), D(999))
    event = message(20_000, 2, "snapshot", (("bid", D(990), D(2)), ("ask", D(1010), D(2))))
    book.apply(event)
    tap.source_applied(event, book)
    tap.publish("depth:100000000", book, 100_000_000, 0)
    assert tap.pending["depth:100000000"].quantity("bid", D(900)) is None
    assert tap.pending["depth:0"] == snap
    assert pickle.loads(pickle.dumps(tap)).pending == tap.pending


def test_zero_is_known_only_inside_snapshot_and_projection():
    book, tap = ObservableBook(), DepthObservationTap(levels=20)
    event = message()
    book.apply(event)
    tap.source_applied(event, book)
    event = message(10_000, 1, "delta", (("bid", D(990), D(0)),))
    book.apply(event)
    tap.source_applied(event, book)
    tap.publish("depth:0", book, 0, 0)
    snap = tap.pending["depth:0"]
    assert snap.quantity("bid", D(990)) == 0
    assert snap.quantity("bid", D(975)) is None  # retained in source, not in this projection


@pytest.mark.parametrize("side,price,low,high", [
    ("bid", 1000, 975, 1000), ("ask", 1000, 1000, 1025)])
def test_better_touch_keeps_known_zero_inward_of_initial_snapshot(side, price, low, high):
    book, tap = ObservableBook(), DepthObservationTap()
    for event in (message(), message(10_000, 1, "delta", ((side, D(price), D(3)),))):
        book.apply(event)
        tap.source_applied(event, book)
    tap.publish("depth:0", book, 0, 0)
    snap = tap.pending["depth:0"]
    assert snap.covers(side, D(low), D(high))
    assert snap.quantity(side, D("999.5") if side == "bid" else D("1000.5")) == 0


def test_sampling_uses_publication_not_delivery_time():
    tap = DepthObservationTap(sample_period_ns=200_000_000)
    observed = [o for tick in stream(tap) for o in tick.research_depth_observations]
    assert [o.original.publish_ns for o in observed] == [0, 200_000_000]
    with pytest.raises(ValueError, match="existing depth"):
        stream(DepthObservationTap(sample_period_ns=111))


def test_engineering_comparison_retains_values_not_python_aliasing():
    from research.families.f08_side_taker_lifecycle.depth_information import canonical_tick
    point = (D("99.1"), D("2.0"))
    copied = (D("99.1"), D("2.0"))
    assert pickle.dumps((point, point)) != pickle.dumps((point, copied))
    assert canonical_tick((point, point)) == canonical_tick((point, copied))
    assert canonical_tick((point,)) != canonical_tick(((D("99.2"), D("2.0")),))
    assert canonical_tick((1,)) != canonical_tick((True,))
