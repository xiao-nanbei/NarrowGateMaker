from dataclasses import replace
from decimal import Decimal as D
import pickle

import pytest

from data.depth_observation import DepthStateTap
from research.families.f08_side_taker_lifecycle.depth_price_runtime import AdaptiveDepthPricePolicy
from strategy.quote_core import QuoteState, DepthSnapshot, _compute_quote_core_py
from test_depth_observation import stream
from test_depth_quote_correction import config


def observations():
    base = next(o for t in stream(DepthStateTap()) for o in t.research_depth_observations)
    for t in range(0, 5_000_000_000, 100_000_000):
        yield replace(base, original=replace(base.original, ready_ns=t, source_asof_ns=t,
                                            publish_ns=t, receive_ns=t,
                                            event_id=f'depth:{t}'))


def policy(arm):
    return AdaptiveDepthPricePolicy(arm=arm, tick_size=1,
        reference_window_ns=2_000_000_000, sample_period_ns=1_000_000_000,
        dynamic_window_ns=1_000_000_000)


def query(p, t, *, bid=990, ask=1010):
    book = p.history.current.snapshot.book
    state = QuoteState(mid=1000, best_bid=999., best_ask=1001., inventory=0, sigma_sq=1)
    depth = DepthSnapshot(bids=tuple((float(p), float(q)) for p,q in book.bids),
                          asks=tuple((float(p), float(q)) for p,q in book.asks))
    return p.correction(bid=bid, ask=ask, state=state, depth=depth, read_ns=t)


@pytest.mark.parametrize('arm', sorted(AdaptiveDepthPricePolicy.ARMS))
def test_warmup_and_cold_restore_same_actual_correction(arm):
    p = policy(arm)
    for o in observations():
        p.advance(o)
        t = o.original.ready_ns
        result = query(p, t)
        if t < 2_000_000_000:
            assert not result['available']
            assert result['u'] == result['v'] == 0
        else:
            assert result['available']
            assert result['u'] == result['v'] == 0
            restored = pickle.loads(pickle.dumps(p))
            assert query(restored, t) == query(p, t)


def test_reset_at_query_time_invalidates_prior_window_even_before_sampling():
    p = policy('DEEP_DYNAMIC')
    rows = list(observations())
    for o in rows[:22]:
        p.advance(o)
    assert query(p, rows[21].original.ready_ns)['available']
    new = rows[22]
    new = replace(new, snapshot=replace(new.snapshot, snapshot_version=99))
    p.advance(new)
    assert query(p, new.original.ready_ns)['reason'] == 'reference_recent_discontinuity'


def test_near_cannot_see_deeper_past_or_current_points():
    p = policy('NEAR_STATIC')
    for o in observations():
        p.advance(o)
    # 975 is initialized by the full source but outside historical Top20.
    assert p.history.current.snapshot.quantity('bid', D(975)) is None
    row = p.reference.query(read_ns=p.history.clock, bid_width_ticks=24, ask_width_ticks=24)
    assert row['reason'] == 'reference_geometry_uncovered'
    assert query(p, p.history.clock, bid=975, ask=1025)['available']


def test_missing_deep_coverage_and_staleness_are_fallback_not_empty_book():
    p = policy('DEEP_STATIC')
    for o in observations():
        p.advance(o)
    assert not query(p, p.history.clock, bid=900, ask=1100)['available']
    assert not query(p, p.history.clock+2_000_000_000)['available']


def test_runtime_in_shared_quote_core_and_changed_quote():
    p = policy('DEEP_STATIC')
    rows = list(observations())
    for o in rows[:25]:
        p.advance(o)
    now = rows[25]
    buckets = tuple(dict(b) for b in now.snapshot.buckets)
    for bucket in buckets:
        for key in bucket:
            if key[0] == 'bid':
                bucket[key] *= 3
    book = replace(now.snapshot.book, bids=tuple((px, qty*3) for px,qty in now.snapshot.book.bids),
                   version=now.snapshot.book.version+1)
    now = replace(now, snapshot=replace(now.snapshot, buckets=buckets, book=book))
    p.advance(now)
    state = QuoteState(mid=1000, best_bid=999., best_ask=1001., inventory=0, sigma_sq=1)
    depth = DepthSnapshot(bids=tuple((float(x),float(y)) for x,y in book.bids),
                          asks=tuple((float(x),float(y)) for x,y in book.asks))
    cfg = config(tick_size=1., execution_intensity_slope=.3)
    baseline = _compute_quote_core_py(state, cfg, None, depth)
    changed = _compute_quote_core_py(state, cfg, None, depth,
        price_correction=lambda **kw: p.correction(**kw, read_ns=now.original.ready_ns))
    assert p.last_audit['available'], p.last_audit
    assert p.last_audit['u'] > 0 and p.last_audit['v'] < 0
    assert (changed.bid_price, changed.ask_price) != (baseline.bid_price, baseline.ask_price)
    assert changed.quote_context['BUY']['price_correction']['u'] == p.last_audit['u']
