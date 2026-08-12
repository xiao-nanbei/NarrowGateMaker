from live.config import Config, to_backtest_params
from models.backtest_config import build_backtest_base_params
from models.backtest_tick import replay_ml_enabled


def test_replay_ml_switch_cannot_be_reenabled_by_active_guards():
    assert replay_ml_enabled({"ml_enabled": False}, True, True) is False
    assert replay_ml_enabled({"ml_enabled": True}, True, True) is True
    assert replay_ml_enabled({"ml_enabled": True}, False, True) is False
    assert replay_ml_enabled({"ml_enabled": True}, True, False) is False


def test_live_ml_switch_survives_live_to_replay_abi():
    config = Config()
    config.ml.enabled = False

    live_params = to_backtest_params(config)
    replay_params = build_backtest_base_params(live_params)

    assert live_params["ml_enabled"] is False
    assert replay_params["ml_enabled"] is False
