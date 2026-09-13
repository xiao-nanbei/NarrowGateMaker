from types import SimpleNamespace

from live.config import Config, _validate_config
from strategy.maker_engine import MakerEngine, SidePolicyDecision
from strategy.post_fill_quote_response import (
    PostFillQuoteResponse,
    PostFillQuoteResponseConfig,
)
from strategy.signal import Prediction


def _engine(inventory: float) -> MakerEngine:
    cfg = Config()
    cfg.strategy.order_size = 0.001
    cfg.strategy.post_fill_quote_response_enabled = True
    cfg.strategy.post_fill_quote_response_mode = "inventory_shift"
    cfg.strategy.post_fill_inventory_ticks_per_order_unit = 0.25
    cfg.strategy.post_fill_inventory_max_ticks = 4.0
    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine.inventory = SimpleNamespace(net_position=inventory)
    engine._post_fill_quote_response = PostFillQuoteResponse(
        PostFillQuoteResponseConfig.from_params(vars(cfg.strategy))
    )
    engine._last_quote_diagnostics = {"max_spread": 10.0}
    engine._best_bid = 98.0
    engine._best_ask = 102.0
    engine._requote_count = 1
    return engine


def _policy(side: str) -> SidePolicyDecision:
    return SidePolicyDecision(side=side)


def test_live_q1_long_shifts_pair_without_widening() -> None:
    engine = _engine(0.004)
    pred = Prediction(vol_10s=3.0)
    bid, ask = engine._apply_post_fill_quote_response(
        q=0.004,
        bid_price=99.0,
        ask_price=101.0,
        pred=pred,
        bid_policy=_policy("BUY"),
        ask_policy=_policy("SELL"),
        best_bid=99.8,
        best_ask=100.2,
        now_ms=1_000,
    )
    assert bid == 98.9
    assert ask == 100.9
    assert abs((ask - bid) - 2.0) < 1e-12


def test_live_final_p3_floor_clamps_shifted_quote_and_flags_unsafe_keep() -> None:
    engine = _engine(0.004)
    engine._last_quote_diagnostics.update(
        {
            "p3_side_bbo_floor_enabled": True,
            "p3_touch_delta_star": 0.5,
        }
    )

    result = engine._apply_final_p3_side_bbo_floor(
        bid_price=99.7,
        ask_price=100.3,
        best_bid=99.9,
        best_ask=100.1,
        bid_order_price=99.7,
        ask_order_price=100.3,
        bid_order_active=True,
        ask_order_active=True,
    )

    assert result[:4] == (99.4, 100.60000000000001, 99.4, 100.60000000000001)
    assert result[4:] == (True, True, True, True)


def test_live_zero_p3_delta_is_inactive_for_existing_orders() -> None:
    engine = _engine(0.004)
    engine._last_quote_diagnostics.update(
        {
            "p3_side_bbo_floor_enabled": True,
            "p3_touch_delta_star": 0.0,
        }
    )

    result = engine._apply_final_p3_side_bbo_floor(
        bid_price=99.7,
        ask_price=100.3,
        best_bid=99.9,
        best_ask=100.1,
        bid_order_price=99.7,
        ask_order_price=100.3,
        bid_order_active=True,
        ask_order_active=True,
    )

    assert result[:2] == (99.7, 100.3)
    assert result[4:] == (False, False, False, False)


def test_live_rejects_flow_mode_without_causal_repair_bundle() -> None:
    cfg = Config()
    cfg.strategy.post_fill_quote_response_enabled = True
    cfg.strategy.post_fill_quote_response_mode = "flow_add_widen"
    try:
        _validate_config(cfg)
    except ValueError as exc:
        assert "causal campaign-repair" in str(exc)
    else:
        raise AssertionError("flow mode must fail closed until live repair wiring exists")
