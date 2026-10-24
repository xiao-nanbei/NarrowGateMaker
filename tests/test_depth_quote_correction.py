"""Research price correction uses original protections; default path is exact."""

from dataclasses import asdict
import pickle

import pytest

from strategy import quote_core as qc


def config(**changes):
    values = dict(tick_size=.1, lot_size=.001, maker_fee=0., order_size=.001,
                  max_inventory=.02, eta_inventory=.01, a_spread=.01,
                  risk_per_order=.01, execution_intensity_slope=1., risk_horizon_s=1.,
                  trade_intensity_acceleration_spread_mult=2.)
    values.update(changes)
    if values.get('p3_pair_spread_projection_enabled'):
        values.update(p3_event_type='touch', p3_horizon_s=10.,
                      p3_distance_origin='same_side_best_bid_or_ask_at_window_start',
                      p3_distance_unit='USDC_per_BTC', p3_side='pooled_buy_sell',
                      p3_queue_included=False)
    return qc.QuoteCoreConfig(**values)


def inputs(inventory=0):
    return (qc.QuoteState(mid=70000, inventory=inventory, sigma_sq=4,
                         best_bid=69999.9, best_ask=70000.1),
            qc.QuotePrediction(), qc.DepthSnapshot(
                bids=((69999.9, 3.),), asks=((70000.1, 1.),)))


@pytest.mark.parametrize('inventory', [-.02, 0, .02])
@pytest.mark.parametrize('changes', [{}, {'max_spread_bps': .1},
    {'regime_enabled': True, 'p3_pair_spread_projection_enabled': True,
     'p3_distance_touch_product_argmax': 30.},
    {'asym_strength': .3, 'inventory_asym_strength': .3}])
def test_zero_policy_preserves_all_result_and_context_bits(inventory, changes):
    state, prediction, depth = inputs(inventory)
    cfg = config(**changes)
    old = qc._compute_quote_core_py(state, cfg, prediction, depth)
    new = qc._compute_quote_core_py(state, cfg, prediction, depth,
                                   price_correction=lambda **_: {'u': 0., 'v': 0.})
    assert pickle.dumps(asdict(new)) == pickle.dumps(asdict(old))


def test_correction_context_protection_and_actual_target():
    state, prediction, depth = inputs()
    cfg = config(regime_enabled=True, p3_pair_spread_projection_enabled=True,
                 p3_distance_touch_product_argmax=30.)
    result = qc._compute_quote_core_py(state, cfg, prediction, depth,
                                      price_correction=lambda **_: {'u': .1, 'v': -.1})
    contexts = result.quote_context
    audit = contexts['BUY']['price_correction']
    assert audit['pair_floor_changed']
    assert contexts['BUY']['final_price'] == result.bid_price
    assert contexts['SELL']['final_price'] == result.ask_price
    assert result.ask_price-result.bid_price >= 60-.000001
    assert result.bid_price < state.best_ask
    assert result.ask_price > state.best_bid
    assert audit == contexts['SELL']['price_correction']


@pytest.mark.parametrize('u,v', [(float('nan'), 0), (0, float('inf')), (.10001, 0)])
def test_invalid_policy_fails_closed(u, v):
    state, prediction, depth = inputs()
    with pytest.raises(ValueError, match='envelope'):
        qc._compute_quote_core_py(state, config(), prediction, depth,
                                  price_correction=lambda **_: {'u': u, 'v': v})
