import math

import pytest

from market_fusion import market_key
from strategy.global_flow import GlobalFlowEngine
from strategy.signal import SignalEngine


def test_l1_flow_tracks_aggressive_trade_and_bid_depletion():
    engine = GlobalFlowEngine(horizons_ms=(100,))
    market_id = "okx:perp:BTCUSDT"
    start = 1_700_000_000_000_000_000
    engine.on_book(
        market_id,
        receive_ts_ns=start,
        bid=100.0,
        bid_size=1.0,
        ask=101.0,
        ask_size=1.0,
    )
    engine.on_book(
        market_id,
        receive_ts_ns=start + 10_000_000,
        bid=100.0,
        bid_size=0.5,
        ask=101.0,
        ask_size=1.0,
    )
    engine.on_trade(
        market_id,
        receive_ts_ns=start + 15_000_000,
        price=100.0,
        size=0.4,
        aggressor_side="sell",
    )

    row = engine.market_window(
        market_id,
        now_ns=start + 20_000_000,
        horizon_ms=100,
    )
    assert row["aggressive_sell_volume"] == pytest.approx(0.4)
    assert row["trade_imbalance"] == pytest.approx(-1.0)
    assert row["bid_depletion"] == pytest.approx(0.5)
    assert row["l1_ofi"] == pytest.approx(-0.5)
    assert row["flow_pressure"] < 0.0


def test_stale_trade_is_counted_but_not_used_as_short_flow():
    engine = GlobalFlowEngine(horizons_ms=(100,), max_trade_event_age_ms=1_000)
    market_id = "bitget:spot:BTCUSDT"
    now_ns = 1_700_000_000_000_000_000

    accepted = engine.on_trade(
        market_id,
        receive_ts_ns=now_ns,
        exchange_ts_ns=now_ns - 2_000_000_000,
        price=100.0,
        size=1.0,
        aggressor_side="sell",
    )
    row = engine.market_window(market_id, now_ns=now_ns, horizon_ms=100)

    assert accepted is False
    assert row["trade_events"] == 0
    assert row["stale_trade_events"] == 1


def test_two_of_three_external_perp_flow_builds_pending_state():
    engine = GlobalFlowEngine(
        execution_symbol="BTCUSDC",
        reference_symbol="BTCUSDT",
        horizons_ms=(100,),
    )
    start = 1_700_000_000_000_000_000
    markets = [
        market_key("bitget", "perp", "BTCUSDT"),
        market_key("bybit", "perp", "BTCUSDT"),
    ]
    local_bridge = market_key("binance", "perp", "BTCUSDT")
    execution = market_key("binance", "perp", "BTCUSDC")
    for market_id in [*markets, local_bridge, execution]:
        engine.on_book(
            market_id,
            receive_ts_ns=start,
            bid=99.9,
            bid_size=1.0,
            ask=100.1,
            ask_size=1.0,
        )
    for index, market_id in enumerate(markets):
        engine.on_book(
            market_id,
            receive_ts_ns=start + 50_000_000 + index,
            bid=100.9,
            bid_size=1.2,
            ask=101.1,
            ask_size=0.8,
        )
        engine.on_trade(
            market_id,
            receive_ts_ns=start + 55_000_000 + index,
            price=101.0,
            size=0.2,
            aggressor_side="buy",
        )

    state = engine.snapshot(now_ns=start + 60_000_000)
    window = state.window(100)
    assert window["valid"] == 1
    assert window["perp"]["valid"] == 1
    assert window["perp"]["fresh_venues"] == 2
    assert window["perp"]["venue_agreement"] == pytest.approx(1.0)
    assert window["global_flow_pressure"] > 0.0
    assert window["global_minus_bridge_bps"] > 0.0
    assert window["execution"]["market_id"] == execution
    assert window["local_bridge"]["market_id"] == local_bridge
    assert state.to_dict()["schema_version"] == "global_flow.v1"


def test_invalid_single_venue_factor_does_not_enter_global_consensus():
    engine = GlobalFlowEngine(horizons_ms=(100,))
    start = 1_700_000_000_000_000_000
    perp_markets = [
        market_key("bitget", "perp", "BTCUSDT"),
        market_key("bybit", "perp", "BTCUSDT"),
    ]
    lone_spot = market_key("okx", "spot", "BTCUSDT")
    for market_id in [*perp_markets, lone_spot]:
        engine.on_book(
            market_id,
            receive_ts_ns=start,
            bid=99.9,
            bid_size=1.0,
            ask=100.1,
            ask_size=1.0,
        )
    for market_id in perp_markets:
        engine.on_book(
            market_id,
            receive_ts_ns=start + 50_000_000,
            bid=100.9,
            bid_size=1.0,
            ask=101.1,
            ask_size=1.0,
        )
    engine.on_book(
        lone_spot,
        receive_ts_ns=start + 50_000_001,
        bid=89.9,
        bid_size=1.0,
        ask=90.1,
        ask_size=1.0,
    )

    window = engine.snapshot(now_ns=start + 60_000_000).window(100)
    assert window["spot"]["valid"] == 0
    assert window["perp"]["valid"] == 1
    assert window["global_mid_move_bps"] == pytest.approx(
        window["perp"]["mid_move_bps"]
    )
    assert math.isnan(window["perp_minus_spot_move_bps"])


def test_signal_engine_routes_public_receive_time_events_into_flow_state():
    signal = SignalEngine(
        enable_ml=False,
        symbol="BTCUSDC",
        reference_symbol="BTCUSDT",
        global_flow_shadow_enabled=True,
    )
    start = 1_700_000_000_000_000_000
    for venue in ("bitget", "bybit"):
        signal.on_book_ticker(
            {"s": "BTCUSDT", "b": "99.9", "B": "1", "a": "100.1", "A": "1", "E": 1_700_000_000_000},
            market_type="perp",
            venue=venue,
            receive_ts_ns=start,
        )
        signal.on_cross_agg_trade(
            {"s": "BTCUSDT", "p": "100", "q": "0.1", "m": False, "T": 1_700_000_000_010},
            market_type="perp",
            venue=venue,
            receive_ts_ns=start + 10_000_000,
        )
    signal.on_book_ticker(
        {"s": "BTCUSDT", "b": "99.9", "B": "1", "a": "100.1", "A": "1", "E": 1_700_000_000_000},
        receive_ts_ns=start,
    )
    signal.on_book_ticker(
        {"s": "BTCUSDC", "b": "99.9", "B": "1", "a": "100.1", "A": "1", "E": 1_700_000_000_000},
        receive_ts_ns=start,
    )

    state = signal.global_flow_state(now_ns=start + 20_000_000).window(100)
    assert state["perp"]["fresh_venues"] == 2
    assert state["perp"]["aggressive_buy_volume"] == pytest.approx(0.2)
    assert state["global_flow_pressure"] > 0.0
