import pickle

import pytest

from research.families.f08_side_taker_lifecycle.depth_price_policy import (
    GeometryMatchedReference, ReferenceSample, SideProfile,
    bounded_correction, corrected_raw_pair,
)


def profile(q=1, *, side='bid', outer=4):
    sign = -1 if side == 'bid' else 1
    return SideProfile.from_levels([(100+sign*i, q) for i in range(5)],
                                   side=side, tick_size=1, covered_ticks=outer)


def sample(t, q=1, epoch=1, outer=4):
    return ReferenceSample(t, profile(q, outer=outer),
                           profile(q, side='ask', outer=outer), epoch)


def reference():
    return GeometryMatchedReference(window_ns=30, maximum_gap_ns=10)


def test_warmup_future_and_current_sample_do_not_leak():
    r = reference()
    for t in (0, 10, 20):
        r.observe(sample(t))
    assert not r.query(read_ns=20, bid_width_ticks=2, ask_width_ticks=2)['available']
    before = r.query(read_ns=30, bid_width_ticks=2, ask_width_ticks=2)
    r.observe(sample(30, q=1000))
    assert r.query(read_ns=30, bid_width_ticks=2, ask_width_ticks=2) == before
    assert before['rho_ref'] == 1
    with pytest.raises(ValueError, match='future'):
        r.query(read_ns=29, bid_width_ticks=2, ask_width_ticks=2)


def test_time_weighting_not_message_count_and_geometry_is_exact():
    r = reference()
    # Unequal density by distance: 10 at BBO, 1 at distance 4, known empty inside.
    b = SideProfile.from_levels([(100, 10), (96, 1)], side='bid', tick_size=1, covered_ticks=4)
    a = SideProfile.from_levels([(101, 10), (105, 1)], side='ask', tick_size=1, covered_ticks=4)
    for t in (0, 10, 20):
        r.observe(ReferenceSample(t, b, a, 1))
    assert r.query(read_ns=30, bid_width_ticks=0, ask_width_ticks=0)['rho_ref'] == 10
    assert r.query(read_ns=30, bid_width_ticks=4, ask_width_ticks=4)['rho_ref'] == pytest.approx(2.2)
    assert not r.query(read_ns=30, bid_width_ticks=5, ask_width_ticks=4)['available']
    # Uneven publications must not reweight the same elapsed exposure.
    r = reference()
    for t, q in ((0, 1), (10, 3), (11, 3), (20, 3)):
        r.observe(sample(t, q))
    assert r.query(read_ns=30, bid_width_ticks=2, ask_width_ticks=2)['rho_ref'] == pytest.approx(7/3)


@pytest.mark.parametrize('kind', ['gap', 'unknown', 'rebase', 'coverage'])
def test_unknown_or_broken_history_never_becomes_zero(kind):
    r = reference()
    r.observe(sample(0))
    if kind == 'gap':
        r.observe(sample(11))
    elif kind == 'unknown':
        r.observe(ReferenceSample(10, None, None, 1))
    else:
        r.observe(sample(10, epoch=2 if kind == 'rebase' else 1,
                         outer=1 if kind == 'coverage' else 4))
    r.observe(sample(20))
    assert not r.query(read_ns=30, bid_width_ticks=2, ask_width_ticks=2)['available']


def test_known_holes_zero_not_unknown_and_cold_restore_exact():
    p = SideProfile.from_levels([(100, 2), (96, 3)], side='bid', tick_size=1, covered_ticks=4)
    assert p.quantity(3) == 2
    assert p.quantity(4) == 5
    assert p.quantity(5) is None
    r = reference()
    for t in range(0, 101, 10):
        r.observe(sample(t, q=t+1))
    restored = pickle.loads(pickle.dumps(r))
    assert len(r.samples) == 4
    assert restored.query(read_ns=100, bid_width_ticks=2, ask_width_ticks=2) == r.query(
        read_ns=100, bid_width_ticks=2, ask_width_ticks=2)


def test_correction_bounds_symmetry_and_dynamic_reduces_to_static():
    args = dict(qb=3., qa=1., grid_points=4, rho_ref=2.)
    static = bounded_correction(**args)
    dynamic = bounded_correction(**args, dynamic=True, previous_qb=3., previous_qa=1.)
    assert static == dynamic
    opposite = bounded_correction(qb=1., qa=3., grid_points=4, rho_ref=2.)
    assert opposite['u'] == -static['u']
    assert opposite['v'] == static['v']
    saturated = bounded_correction(qb=100., qa=0., grid_points=4, rho_ref=1.,
                                   dynamic=True, previous_qb=0., previous_qa=100.)
    assert saturated['u'] == .1
    assert -.1 <= saturated['v'] <= .1
    assert corrected_raw_pair(69970., 70030., u=.1, v=0) == (69973., 70033.)
    assert corrected_raw_pair(69970., 70030., u=0, v=-.1) == (69973., 70027.)


def test_missing_fallback_and_exact_noop():
    for qb, qa in ((None, 1), (0, 0)):
        row = bounded_correction(qb=qb, qa=qa, grid_points=2, rho_ref=1)
        assert row['u'] == row['v'] == 0 and not row['available']
    assert not bounded_correction(qb=1, qa=1, grid_points=2, rho_ref=1, dynamic=True)['available']
    pair = (0.10000000000000002, 0.30000000000000004)
    assert corrected_raw_pair(*pair, u=0, v=0) == pair
    with pytest.raises(ValueError):
        corrected_raw_pair(*pair, u=.1001, v=0)
