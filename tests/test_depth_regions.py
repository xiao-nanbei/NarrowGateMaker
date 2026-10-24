from dataclasses import replace
from decimal import Decimal as D

import pytest

from data.depth_observation import DepthObservation, DepthObservationTap
from research.families.f08_side_taker_lifecycle.depth_regions import quote_regions


def observation():
    # Use the supported production stream to retain its Observation ABI.
    from test_depth_observation import stream
    return next(o for t in stream(DepthObservationTap(sample_period_ns=100_000_000))
                for o in t.research_depth_observations)


def query(obs=None, **changes):
    values = dict(side="BUY", read_ns=0, max_age_ns=10**9, old_price=995,
                  target_price=980, tick_size=1, neighbor_ticks=1)
    values.update(changes)
    return quote_regions(obs or observation(), **values)


def test_old_and_target_use_different_paths_and_original_projection():
    result = query()
    assert result["old"]["path"]["quantity"] == "5"
    assert result["old"]["path"]["original_region_covered"]
    assert result["target"]["path"]["quantity"] == "20"
    assert result["target"]["path"]["expanded_region_covered"]
    assert not result["target"]["path"]["original_region_covered"]
    assert result["target"]["price_rank"] == 20
    assert result["target"]["neighborhood"]["quantity"] == "3"
    assert result["complete_information_support"] == "not_evaluated"


def test_missing_and_unknown_are_not_zero_or_fake_old_orders():
    result = query(old_price=None, target_price=900)
    assert result["old"] == dict(status="not_applicable", price=None)
    assert result["target"]["point_quantity"] is None
    assert result["target"]["path"]["quantity"] is None
    assert not result["target"]["path"]["expanded_region_covered"]


def test_known_zero_distinct_from_unknown_and_neighborhood_boundary():
    obs = observation()
    snap = replace(obs.snapshot, book=replace(obs.snapshot.book,
                   bids=tuple((p, q) for p, q in obs.snapshot.book.bids if p != D(977))))
    original = replace(obs.original, payload=replace(obs.original.payload, bids=snap.book.bids[:20]))
    result = query(DepthObservation(original, snap), target_price=977)
    assert result["target"]["point_quantity"] == "0"
    assert result["target"]["price_rank"] is None
    assert result["target"]["neighborhood"]["quantity"] == "2"
    assert query(target_price=975)["target"]["neighborhood"]["quantity"] is None


def test_no_future_or_stale_book_as_usable_information():
    with pytest.raises(ValueError, match="not ready"):
        query(read_ns=-1)
    row = query(read_ns=10**9+1)["target"]
    assert row["point_quantity"] is None
    assert row["path"]["missing_reason"] == "stale_book"
    assert query(side="SELL", old_price=1005, target_price=1020)["target"]["path"]["quantity"] == "20"


@pytest.mark.parametrize("kwargs", [{"target_price":980.5}, {"tick_size":0},
                                   {"original_levels":21}, {"neighbor_ticks":-1}])
def test_invalid_definition_is_not_silently_adjusted(kwargs):
    with pytest.raises(ValueError):
        query(**kwargs)
