from types import SimpleNamespace

import pytest

from live.config import MLConfig, RegimeConfig, _dataclass_from_dict
from strategy import native_runtime


@pytest.mark.parametrize('cls,field', [
    (MLConfig, 'gamma_dir_bonus'),
    (RegimeConfig, 'gamma_scale_min'),
    (RegimeConfig, 'gamma_scale_max'),
    (RegimeConfig, 'gamma_liq_scale_min'),
    (RegimeConfig, 'gamma_liq_scale_max'),
])
def test_old_dynamic_coefficient_names_are_rejected(cls, field):
    with pytest.raises(ValueError, match='unknown config key'):
        _dataclass_from_dict(cls, {field: 0.5}, path='config')


@pytest.mark.parametrize('version', [None, 1, 20260924])
def test_old_native_binary_is_rejected_without_fallback(monkeypatch, version):
    module = SimpleNamespace(APPLICATION_INTERFACE_VERSION=version)
    monkeypatch.setattr(native_runtime.importlib, 'import_module', lambda name: module)
    with pytest.raises(RuntimeError, match='application interface mismatch'):
        native_runtime.load_native_module(optional=True)


def test_current_dynamic_coefficients_are_explicit():
    ml = _dataclass_from_dict(MLConfig, {'inventory_direction_alignment_strength': 0.25}, path='ml')
    regime = _dataclass_from_dict(RegimeConfig, {
        'volatility_spread_scale_min': 0.4,
        'volatility_spread_scale_max': 2.1,
        'liquidity_spread_scale_min': 0.6,
        'liquidity_spread_scale_max': 3.2,
    }, path='regime')
    assert ml.inventory_direction_alignment_strength == 0.25
    assert regime.volatility_spread_scale_min == 0.4
    assert regime.liquidity_spread_scale_max == 3.2


def test_native_quote_has_no_gamma_and_requires_explicit_coefficients():
    native = native_runtime.load_native_module()
    config = native.QuoteCoreConfig()
    with pytest.raises(AttributeError):
        config.gamma = 0.046
    with pytest.raises(ValueError, match='eta_inventory must be positive and finite'):
        native.NativeLiveRuntimeCore(config)
