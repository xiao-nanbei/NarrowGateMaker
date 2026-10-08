import json

import numpy as np
import pytest

from strategy.replace_threshold import ReplacePriceThreshold
from strategy.order_manager import Side, OrderState
from tests.test_live_replace_throttle import _engine, _order
from tests.test_python_planned_maintenance_replay import _inputs, _params, _async_fifo_params
from models.backtest_tick import simulate_tick
from models.tick_data_types import HistoricalBBOData


@pytest.mark.parametrize("side,sign", [(Side.BUY, 1), (Side.SELL, -1)])
@pytest.mark.parametrize("move", [-15, -14.999, -10, -9.999, 0, 9.999, 10, 14.999, 15])
@pytest.mark.parametrize("outward,inward", [(10, 15), (15, 10), (15, 15), (10, 10)])
@pytest.mark.parametrize("reducing", [False, True])
def test_actual_live_gate(side, sign, move, outward, inward, reducing):
    e = _engine()
    e.cfg.strategy.replace_min_price_change_ticks = 15
    e.cfg.strategy.replace_min_price_change_ticks_reducing = 15
    e.cfg.strategy.replace_price_threshold_mode = "directional"
    e.cfg.strategy.replace_min_price_change_ticks_outward = outward
    e.cfg.strategy.replace_min_price_change_ticks_inward = inward
    order = _order(side, 100, 2000, OrderState.OPEN)
    q = -sign if reducing else 0
    selected = 15 if reducing or move == 0 else inward if move > 0 else outward
    result = e._apply_replace_throttle(side=side, now_ts=order.create_time + 2,
        q=q, target_price=100 + sign * move * .1, order=order,
        needs_update=True, force_update=False)
    assert result == (abs(move) + 1e-9 >= selected)


@pytest.mark.parametrize("force,needs,age,has_order,expected", [
    (True, True, 0, True, True), (True, False, 0, True, False),
    (False, True, 0, True, False), (False, False, 2000, True, False),
    (False, True, 2000, False, True),
])
def test_original_branch_order(force, needs, age, has_order, expected):
    e = _engine()
    e.cfg.strategy.replace_price_threshold_mode = "directional"
    e.cfg.strategy.replace_min_price_change_ticks_outward = 10
    e.cfg.strategy.replace_min_price_change_ticks_inward = 15
    order = _order(Side.BUY, 100, age)
    assert e._apply_replace_throttle(side=Side.BUY, now_ts=order.create_time + age/1000,
        q=0, target_price=98.8, order=order if has_order else None,
        needs_update=needs, force_update=force) is expected


def replay(params, checkpoint=None, cut=None):
    trades, old = _inputs(crossing_fill_ts_ms=2200)
    # Both inward/outward changes, 12 ticks at each step, genuine async continuations.
    shift = np.where(old.ts_ms < 1000, 0, np.where(old.ts_ms < 2000, 1.2, -.0))
    bbo = HistoricalBBOData(ts_ms=old.ts_ms, best_bid=old.best_bid+shift,
        best_ask=old.best_ask+shift, bid_qty=old.bid_qty, ask_qty=old.ask_qty)
    return simulate_tick(trades, np.array([0]), np.array([1.]), params,
        bbo_data=bbo, checkpoint_at_ts_ms=cut, resume_checkpoint=checkpoint)


def clean(result):
    return normalize({k: v for k, v in result.items() if not k.startswith('directional_replace')
            and k not in ('replace_price_threshold_mode', 'replace_min_price_change_ticks_outward',
                         'replace_min_price_change_ticks_inward')})


def normalize(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    return value


@pytest.mark.parametrize("threshold", [10, 15])
@pytest.mark.parametrize("continuation", [False, True])
def test_replay_fixed_endpoints_and_restore(threshold, continuation):
    p = {**_params(), **_async_fifo_params(), 'planned_quote_stop_ts_ms': 0,
         'replace_min_price_change_ticks': threshold, 'replace_min_price_change_ticks_reducing': 15,
         'replace_min_interval_ms': 1000, 'replace_min_interval_ms_reducing': 125,
         'replace_terminal_continuation': continuation}
    fixed = replay(p)
    d = dict(p, replace_price_threshold_mode='directional',
        replace_min_price_change_ticks_outward=threshold,
        replace_min_price_change_ticks_inward=threshold)
    directional = replay(d)
    from tests.exact_replay_assertions import assert_exact_replay_value
    assert_exact_replay_value(clean(fixed), clean(directional))
    paused = replay(d, cut=1500)
    restored = replay(d, checkpoint=paused['_replay_checkpoint'])
    assert_exact_replay_value(normalize(directional), normalize(restored))


def test_selection_config_roundtrip_and_invalid():
    p = dict(replace_price_threshold_mode='directional',
        replace_min_price_change_ticks_outward=10, replace_min_price_change_ticks_inward=15)
    assert ReplacePriceThreshold.from_getter(json.loads(json.dumps(p)).get).outward == 10
    with pytest.raises(ValueError):
        ReplacePriceThreshold('directional')


@pytest.mark.parametrize("outward,inward,changed,blocked", [
    (10, 15, 'SELL:ordinary:outward', 'BUY:ordinary:inward'),
    (15, 10, 'BUY:ordinary:inward', 'SELL:ordinary:outward'),
])
def test_real_replay_direction_and_terminal_continuation(outward, inward, changed, blocked):
    p = {**_params(), **_async_fifo_params(), 'planned_quote_stop_ts_ms': 0,
         'replace_min_price_change_ticks': 15, 'replace_min_price_change_ticks_reducing': 15,
         'replace_price_threshold_mode': 'directional',
         'replace_min_price_change_ticks_outward': outward,
         'replace_min_price_change_ticks_inward': inward,
         'replace_terminal_continuation': True}
    result = replay(p)
    counts = result['directional_replace_counts']
    assert counts[changed]['decision_replace'] > 0
    assert counts[changed]['band_10_15'] > 0
    assert counts[blocked]['price_blocked'] > 0
    assert counts[blocked]['passed'] == 0
    assert result['replace_terminal_continuation_decision_count'] > 0
