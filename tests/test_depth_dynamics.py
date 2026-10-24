from dataclasses import replace
from decimal import Decimal as D

import pytest

from data.depth_observation import DepthStateTap
from research.families.f08_side_taker_lifecycle.depth_dynamics import VisibleDepthHistory
from test_depth_observation import stream


def test_persistent_publications_preserve_old_state_and_original_stream():
    original = list(stream())
    observed = list(stream(DepthStateTap()))
    assert [replace(t, research_depth_observations=()) for t in observed] == original
    deep = [o for t in observed for o in t.research_depth_observations]
    assert deep[0].snapshot.quantity("bid", D(990)) == 1
    assert deep[1].snapshot.quantity("bid", D(990)) == 9
    assert deep[0].snapshot.quantity("bid", D(990)) == 1
    assert sum(a is b for a, b in zip(deep[0].snapshot.buckets, deep[1].snapshot.buckets, strict=True)) >= 63


def test_fixed_price_dynamics_and_warmup_are_not_order_cancels():
    history = VisibleDepthHistory(windows_ns=(100_000_000,), max_age_ns=10**9)
    deep = [o for t in stream(DepthStateTap()) for o in t.research_depth_observations]
    history.advance(deep[0])
    assert not history.query(side="bid", low=990, high=990, read_ns=0)["windows"]["100000000"]["available"]
    history.advance(deep[1])
    row = history.query(side="bid", low=990, high=990, read_ns=100_000_000)
    assert row["quantity"] == "9"
    assert row["windows"]["100000000"]["positive_qty"] == "8"
    assert row["windows"]["100000000"]["negative_qty"] == "0"
    assert not history.query(side="bid", low=950, high=950, read_ns=100_000_000)["available"]
    with pytest.raises(ValueError, match="future"):
        history.query(side="bid", low=990, high=990, read_ns=0)


def test_regressed_delivery_is_preserved_and_invalidates_history():
    deep = [o for t in stream(DepthStateTap()) for o in t.research_depth_observations]
    history = VisibleDepthHistory(windows_ns=(100_000_000,))
    history.advance(deep[1])
    older = replace(deep[0], original=replace(deep[0].original, ready_ns=200_000_000))
    history.advance(older)
    row = history.query(side="bid", low=990, high=990, read_ns=200_000_000)
    assert row["quantity"] == "1"
    assert history.regressions == 1
    assert not row["windows"]["100000000"]["available"]
