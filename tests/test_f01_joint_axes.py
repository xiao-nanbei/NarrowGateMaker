"""Bounded parameter admission and actual quote-consumer semantics, not economics."""
from dataclasses import asdict, replace

import pytest

from research.families.f01_fixed_parameter_racing.public_input import (
    _parameter_number, _validate_effective_quote_change,
)
from strategy.quote_core import (
    QuoteCoreConfig, QuotePrediction, QuoteState, _compute_quote_core_py,
    quote_core_config_from_params,
)


@pytest.mark.parametrize('value', [True, False, -0.1, float('nan'), float('inf'),
                                  -float('inf'), 'bad', None])
def test_asym_invalid_override_and_baseline(value):
    with pytest.raises(ValueError):
        _validate_effective_quote_change({'asym_strength': 0.}, {'asym_strength': value})
    with pytest.raises(ValueError):
        _validate_effective_quote_change({'asym_strength': value}, {'asym_strength': .1})


def test_explicit_baseline_and_numeric_parse():
    with pytest.raises(ValueError, match='explicit B0'):
        _validate_effective_quote_change({}, {'asym_strength': .1})
    assert _parameter_number('asym_strength', '0.1') == .1
    for name in ('eta_inventory', 'a_spread', 'risk_per_order'):
        with pytest.raises(ValueError, match='positive'):
            _validate_effective_quote_change({name: 0.}, {name: 0.})


def test_four_arms_have_only_declared_effective_differences():
    base = dict(eta_inventory=.046, a_spread=.046, risk_per_order=.046,
                inventory_reference_qty=1., execution_intensity_slope=1.,
                risk_horizon_s=1., trade_intensity_acceleration_spread_mult=2.,
                asym_strength=0., ml_enabled=True, order_size=.001, max_inventory=.026,
                maker_fee=0.)
    def effective(p):
        return quote_core_config_from_params(p, tick_size=.1, lot_size=.001,
            use_ml=True, use_depth_weighted_mid_proxy=False, use_depth_kappa=False)
    reference = asdict(effective(base))
    for g, a in ((.046, 0.), (.050, 0.), (.046, .1), (.050, .1)):
        change = dict(eta_inventory=g, a_spread=g, risk_per_order=g, asym_strength=a)
        _validate_effective_quote_change(base, change)
        actual = asdict(effective({**base, **change}))
        assert actual['ml_enabled'] is True
        assert all(actual[k] == v for k, v in change.items())
        assert {k for k in actual if actual[k] != reference[k]} <= set(change)


@pytest.mark.parametrize('p', [.8, .2, .5, .625, .375])
def test_ml_asym_actual_python_quote_sign_units_and_strict_threshold(p):
    cfg = QuoteCoreConfig(tick_size=.01, lot_size=.001, maker_fee=0., order_size=.001,
        max_inventory=.026, eta_inventory=.046, a_spread=.046, risk_per_order=.046,
        inventory_reference_qty=1., execution_intensity_slope=1., risk_horizon_s=1.,
        trade_intensity_acceleration_spread_mult=2., dir_threshold=.125, asym_strength=.1)
    state = QuoteState(mid=1000., inventory=0., sigma_sq=2.)
    prediction = QuotePrediction(touch_conditioned_up_probability_10000ms=p)
    actual = _compute_quote_core_py(state, cfg, prediction)
    zero = _compute_quote_core_py(state, replace(cfg, asym_strength=0.), prediction)
    expected = 2*.1*(p-.5) if abs(p-.5) > .125 else 0.
    assert actual.diagnostics['asym'] == expected
    assert zero.diagnostics['asym'] == 0.
    for side in ('BUY', 'SELL'):
        delta = actual.quote_context[side]['raw_price'] - zero.quote_context[side]['raw_price']
        assert delta == pytest.approx(zero.raw_half_spread*expected)
