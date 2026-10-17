from pathlib import Path

import pytest
import yaml

from live.config import StrategyConfig
from strategy import quote_core as qc

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")


ROOT = Path(__file__).resolve().parents[1]


def _release_config() -> qc.QuoteCoreConfig:
    return qc.QuoteCoreConfig(
        gamma=0.01,
        kappa=1.0,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.02,
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=2.0,
        spread_cap_mode=qc.SPREAD_CAP_COMPRESS,
        markout_spread_scale=0.4,
        markout_side_asymmetry_sign=1.0,
    )


def test_release_config_is_explicit_and_defaults_match_cpp() -> None:
    raw = yaml.safe_load((ROOT / "live/config.yaml").read_text())
    assert raw["strategy"]["markout_side_asymmetry_sign"] == 1.0
    assert StrategyConfig().markout_side_asymmetry_sign == 1.0
    field = qc.QuoteCoreConfig.__dataclass_fields__["markout_side_asymmetry_sign"]
    assert field.default == 1.0
    assert narrowgate_cpp.QuoteCoreConfig().markout_side_asymmetry_sign == 1.0


def test_corrected_sign_preserves_cpp_cap_and_gtx_parity(monkeypatch) -> None:
    cfg = _release_config()
    state = qc.QuoteState(
        mid=100.0,
        inventory=0.0,
        sigma_sq=25.0,
        trade_intensity=100.0,
        best_bid=99.9,
        best_ask=100.1,
        mo_ema_bid=10.0,
        mo_ema_ask=-10.0,
    )

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, qc.QuotePrediction())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, qc.QuotePrediction())

    assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
    assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
    assert cpp.spread == pytest.approx(py.spread, abs=cfg.tick_size * 1.01)

    maximum_spread = state.mid * cfg.max_spread_bps / 10_000.0
    assert py.spread <= maximum_spread + 2.0 * cfg.tick_size
    assert cpp.spread <= maximum_spread + 2.0 * cfg.tick_size
    assert py.bid_price < state.best_ask
    assert cpp.bid_price < state.best_ask
    assert py.ask_price > state.best_bid
    assert cpp.ask_price > state.best_bid
