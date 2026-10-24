import copy
import json
import math

import pytest

from strategy.target_variance import (
    TargetVariancePair, TargetVarianceState, dynamic_outward_ticks,
)


def observe(state, t, p, version=None):
    return state.observe(price=p, ready_ns=round(t*1e9),
                         version=round(t*1000) if version is None else version)


def test_previous_mean_innovation_and_exact_weights():
    s = TargetVarianceState()
    observe(s, 0, 100)
    observe(s, 60, 106)
    assert s.mean_rate == pytest.approx(.05)
    assert s.variance_rate == pytest.approx(.3)
    observe(s, 120, 118)
    assert s.mean_rate == pytest.approx(.125)
    assert s.variance_rate == pytest.approx(.825)
    assert s.positive_dt_count == 2 and not s.ready


def test_warmup_zero_missing_and_long_gap_are_distinct():
    p = TargetVariancePair(2.0, 'synthetic-target-publication-v1')
    s = p.sides['BUY']
    assert p.outward_ticks('BUY', .1) == (15, 'uninitialized')
    for t in range(0, 301, 60):
        observe(s, t, 100)
    assert s.ready and s.variance_rate == 0
    assert p.outward_ticks('BUY', .1) == (1, None)
    s.invalidate('disconnected')
    assert p.outward_ticks('BUY', .1) == (15, 'disconnected')
    assert observe(s, 300, 100) == 'duplicate'
    assert not s.initialized
    observe(s, 360, 101)
    assert s.valid_span == 0 and not s.ready
    observe(s, 421, 102)
    assert s.last_invalid_reason == 'sample_gap' and s.valid_span == 0


def test_duplicate_and_same_timestamp_publication_order():
    s = TargetVarianceState()
    observe(s, 0, 100, 0)
    before = s.snapshot()
    assert observe(s, 0, 100, 0) == 'duplicate'
    assert s.snapshot() == before
    assert observe(s, 0, 105, 1) == 'same_time_replaced'
    assert s.positive_dt_count == 0
    observe(s, 1, 105, 2)
    assert s.variance_rate == 0 and s.mean_rate == 0
    before = s.snapshot()
    for kwargs in [dict(price=105,ready_ns=0,version=3),
                   dict(price=106,ready_ns=1_000_000_000,version=2),
                   dict(price=105,ready_ns=1_000_000_000,version=0)]:
        with pytest.raises(ValueError):s.observe(**kwargs)
        assert s.snapshot() == before


@pytest.mark.parametrize('bad', [math.nan, math.inf, -math.inf, 0, -1, True])
def test_invalid_price_does_not_mutate(bad):
    s = TargetVarianceState()
    observe(s, 0, 100)
    before = s.snapshot()
    with pytest.raises(ValueError):observe(s, 1, bad)
    assert s.snapshot() == before


@pytest.mark.parametrize('r,tick,v', [(0,.1,0),(-1,.1,2),(math.inf,.1,1),
                                    (1,0,1),(1,.1,-1),(1,.1,math.nan)])
def test_invalid_scale_is_not_warmup(r,tick,v):
    with pytest.raises(ValueError):
        dynamic_outward_ticks(variance_rate=v,r=r,tick_size=tick)


@pytest.mark.parametrize('n', [.001,.9,1,1.2,1.8,2.3,5,9,10,15,20,100,1000])
def test_adjacent_integer_cost_minimization(n):
    r = n**4 / 12
    got = dynamic_outward_ticks(variance_rate=1,r=r,tick_size=1)
    candidates = {max(1,math.floor(n)),max(1,math.ceil(n))}
    expected = min(candidates,key=lambda x:((x/n)**2+(n/x)**2,x))
    assert got == expected


def test_equal_cost_chooses_lower_integer():
    # n*=sqrt(2), so the two adjacent normalized costs both equal 2.5.
    assert dynamic_outward_ticks(variance_rate=1,r=1/3,tick_size=1) == 1


def test_scaling_no_hidden_tick_cap_and_numerical_range():
    base = dict(variance_rate=3,r=4,tick_size=.2)
    first = dynamic_outward_ticks(**base)
    for a in [.01,.5,2,100]:
        assert dynamic_outward_ticks(variance_rate=3*a*a,r=4*a*a,tick_size=.2*a)==first
    # These design points have integer continuous scales; each 16x multiplier doubles h.
    for v,r in [(1,1/12),(16,1/12),(1,16/12)]:
        assert dynamic_outward_ticks(variance_rate=v,r=r,tick_size=1)==(1 if v==1 and r==1/12 else 2)
    assert dynamic_outward_ticks(variance_rate=1,r=20**4/12,tick_size=1)==20
    assert dynamic_outward_ticks(variance_rate=0,r=1,tick_size=.1)==1
    with pytest.raises(ValueError):
        dynamic_outward_ticks(variance_rate=1e300,r=1e300,tick_size=1e-300)


def test_shared_algorithm_units_restore_cross_midnight_and_independent_sides():
    contract = 'synthetic-pre-throttle-target-v1'
    replay = TargetVariancePair(2,contract)
    online = TargetVariancePair(2,contract)
    for i in range(400):
        milliseconds = (86300+i)*1000
        p = 100 + math.sin(i/7)
        replay.sides['BUY'].observe(price=p,ready_ns=milliseconds*1_000_000,version=i)
        online.sides['BUY'].observe(price=p,ready_ns=int(milliseconds/1000)*1_000_000_000,version=i)
        if i in (60,100,200,300):
            replay = TargetVariancePair.restore(json.loads(json.dumps(replay.snapshot())),
                                                 r=2,observation_contract=contract)
        assert replay.snapshot() == online.snapshot()
        assert replay.outward_ticks('BUY',.1) == online.outward_ticks('BUY',.1)
    assert replay.sides['BUY'].ready
    assert not replay.sides['SELL'].initialized
    branch = TargetVariancePair.restore(replay.snapshot(),r=2,observation_contract=contract)
    branch.sides['BUY'].invalidate('branch_only')
    assert replay.sides['BUY'].ready
    for r,c in [(3,contract),(2,'different')]:
        with pytest.raises(ValueError):TargetVariancePair.restore(replay.snapshot(),r=r,observation_contract=c)
    bad = copy.deepcopy(replay.snapshot());bad['half_life_seconds']=30
    with pytest.raises(ValueError):TargetVariancePair.restore(bad,r=2,observation_contract=contract)


def test_corrupt_state_rejected_and_overflow_is_not_silently_clipped():
    p = TargetVariancePair(2,'synthetic-v1')
    v = p.snapshot();v['sides']['BUY']['variance_rate']=math.nan
    with pytest.raises(ValueError):TargetVariancePair.restore(v,r=2,observation_contract='synthetic-v1')
    s = TargetVarianceState();observe(s,0,1)
    before=s.snapshot()
    with pytest.raises(ValueError):observe(s,1,1e308)
    assert s.snapshot()==before
