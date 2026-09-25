from types import SimpleNamespace

import pytest

from live.config import MLConfig, RegimeConfig, StrategyConfig, _dataclass_from_dict
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


def test_python_strategy_rejects_gamma():
    with pytest.raises(ValueError, match='unknown config key'):
        _dataclass_from_dict(StrategyConfig, {'gamma': 0.046}, path='strategy')


def test_python_quote_requires_explicit_coefficients():
    from strategy.quote_core import QuoteCoreConfig
    required = dict(execution_intensity_slope=0.05, risk_horizon_s=1., trade_intensity_acceleration_spread_mult=2., tick_size=0.1, lot_size=0.001,
                    maker_fee=0.0, order_size=0.001, max_inventory=0.01)
    with pytest.raises(TypeError):
        QuoteCoreConfig(**required, gamma=0.046)
    with pytest.raises(ValueError, match='eta_inventory'):
        QuoteCoreConfig(**required)
    config = QuoteCoreConfig(**required, eta_inventory=0.046,
                            a_spread=0.046, risk_per_order=0.046,
                            inventory_reference_qty=0.001)
    assert config.eta_inventory == 0.046
    assert not hasattr(config, 'gamma')


def test_current_configuration_exports_explicit_coefficients():
    from live.config import Config, to_backtest_params
    from models.backtest_config import build_backtest_base_params
    config = Config()
    config.strategy.eta_inventory = 0.002
    config.strategy.a_spread = 0.04
    config.strategy.risk_per_order = 0.05
    values = build_backtest_base_params(to_backtest_params(config))
    assert 'gamma' not in values
    assert values['eta_inventory'] == 0.002
    assert values['a_spread'] == 0.04
    assert values['risk_per_order'] == 0.05


def test_old_model_head_attributes_do_not_exist():
    from strategy.signal import Prediction
    prediction = Prediction()
    for field in ('dir_10s', 'vol_10s', 'ret_10s', 'tox_bid_5s'):
        assert not hasattr(prediction, field)
    assert hasattr(prediction, 'absolute_price_variance_rate_10000ms')


def test_retired_experiment_runner_is_not_importable():
    import importlib.util
    assert importlib.util.find_spec('models.experiment_runner') is None


def test_old_model_manifest_is_rejected(tmp_path):
    import json
    from strategy.public_model_contract import validate_public_bundle
    (tmp_path / 'public_input_model.json').write_text(json.dumps({
        'schema': 'retired_schema',
    }))
    with pytest.raises(ValueError):
        validate_public_bundle(tmp_path)
