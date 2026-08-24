import math

import numpy as np
import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

from market_fusion import market_key
from strategy.global_flow import GlobalFlowEngine


def _assert_nested_close(actual, expected, path="root"):
    assert actual.keys() == expected.keys(), path
    for key, expected_value in expected.items():
        actual_value = actual[key]
        child = f"{path}.{key}"
        if isinstance(expected_value, dict):
            _assert_nested_close(actual_value, expected_value, child)
        elif isinstance(expected_value, list):
            assert len(actual_value) == len(expected_value), child
            for index, (actual_item, expected_item) in enumerate(
                zip(actual_value, expected_value)
            ):
                _assert_nested_close(
                    actual_item, expected_item, f"{child}[{index}]"
                )
        elif isinstance(expected_value, float):
            if math.isnan(expected_value):
                assert math.isnan(actual_value), child
            elif math.isinf(expected_value):
                assert actual_value == expected_value, child
            else:
                assert actual_value == pytest.approx(expected_value, abs=1e-12), child
        else:
            assert actual_value == expected_value, child


def _engines():
    common = {
        "execution_symbol": "BTCUSDC",
        "reference_symbol": "BTCUSDT",
        "horizons_ms": (50, 100, 250),
        "retention_ms": 2_000,
        "max_source_age_ms": 1_000.0,
        "max_trade_event_age_ms": 1_000.0,
    }
    python = GlobalFlowEngine(**common)
    native = GlobalFlowEngine(
        **common,
        native_backend=narrowgate_cpp.NativeGlobalFlowEngine(
            2_000, 1_000.0, 1_000.0
        ),
    )
    return python, native


def _feed_market(engine, market_id, start_ns, price_offset=0.0):
    engine.on_book(
        market_id,
        receive_ts_ns=start_ns,
        bid=100.0 + price_offset,
        bid_size=1.0,
        ask=101.0 + price_offset,
        ask_size=1.0,
        gap_flag=False,
    )
    engine.on_book(
        market_id,
        receive_ts_ns=start_ns + 10_000_000,
        bid=100.0 + price_offset,
        bid_size=0.7,
        ask=101.0 + price_offset,
        ask_size=1.2,
        gap_flag=True,
    )
    engine.on_book(
        market_id,
        receive_ts_ns=start_ns + 20_000_000,
        bid=100.1 + price_offset,
        bid_size=0.6,
        ask=101.1 + price_offset,
        ask_size=0.9,
    )
    receive_ns = start_ns + 25_000_000
    exchange_ns = np.asarray(
        [receive_ns - 3_000_000, receive_ns - 2_000_000, receive_ns - 1_000_000],
        dtype=np.int64,
    )
    prices = np.asarray(
        [100.2 + price_offset, 100.3 + price_offset, 100.1 + price_offset],
        dtype=np.float64,
    )
    sizes = np.asarray([0.2, 0.3, 0.4], dtype=np.float64)
    maker = np.asarray([0, 1, 1], dtype=np.uint8)
    assert engine.on_trade_batch(
        market_id,
        receive_ts_ns=receive_ns,
        exchange_ts_ns=exchange_ns,
        prices=prices,
        sizes=sizes,
        is_buyer_maker=maker,
    ) == 3

    assert not engine.on_trade(
        market_id,
        receive_ts_ns=start_ns + 35_000_000,
        exchange_ts_ns=start_ns - 2_000_000_000,
        price=100.0 + price_offset,
        size=0.5,
        aggressor_side="sell",
    )
    assert not engine.on_trade(
        market_id,
        receive_ts_ns=start_ns + 5_000_000,
        exchange_ts_ns=start_ns + 4_000_000,
        price=100.0 + price_offset,
        size=0.5,
        aggressor_side="buy",
    )


def test_native_global_flow_matches_python_windows_and_consensus():
    python, native = _engines()
    start_ns = 1_800_000_000_000_000_000
    markets = (
        (market_key("bitget", "perp", "BTCUSDT"), 0.0),
        (market_key("bybit", "perp", "BTCUSDT"), 0.2),
        (market_key("binance", "perp", "BTCUSDT"), -0.1),
        (market_key("binance", "perp", "BTCUSDC"), -0.15),
    )
    for engine in (python, native):
        for market_id, offset in markets:
            _feed_market(engine, market_id, start_ns, offset)

    now_ns = start_ns + 40_000_000
    for horizon_ms in (50, 100, 250):
        for market_id, _ in markets:
            _assert_nested_close(
                native.market_window(
                    market_id, now_ns=now_ns, horizon_ms=horizon_ms
                ),
                python.market_window(
                    market_id, now_ns=now_ns, horizon_ms=horizon_ms
                ),
                f"{market_id}.{horizon_ms}",
            )
    _assert_nested_close(
        native.snapshot(now_ns=now_ns).to_dict(),
        python.snapshot(now_ns=now_ns).to_dict(),
    )

    stats = native.backend_stats()
    assert stats["native"] == 1
    assert stats["market_count"] == len(markets)
    assert stats["trade_batches"] == 3 * len(markets)
    assert stats["trade_events_seen"] == 5 * len(markets)
    assert stats["trade_events_accepted"] == 3 * len(markets)
    assert stats["stale_trade_events"] == len(markets)
    assert stats["out_of_order_events"] == len(markets)
    assert stats["book_overflow_events"] == 0
    assert stats["trade_overflow_events"] == 0


def _bar_values(bar):
    return tuple(getattr(bar, name) for name in (
        "ts_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "buy_volume",
        "sell_volume",
        "trade_count",
        "buy_count",
        "sell_count",
        "quote_qty",
        "buy_quote_qty",
        "sell_quote_qty",
        "max_same_side_run",
        "max_buy_run",
        "max_sell_run",
        "buy_price_high",
        "buy_price_low",
        "sell_price_high",
        "sell_price_low",
    ))


def test_trade_bar_native_batch_matches_scalar_rollover_and_gap_fill():
    ts_ms = np.asarray([1_000, 1_100, 2_000, 4_000, 4_200], dtype=np.int64)
    prices = np.asarray([100.0, 100.1, 99.9, 100.2, 100.3], dtype=np.float64)
    sizes = np.asarray([0.1, 0.2, 0.3, 0.1, 0.1], dtype=np.float64)
    maker = np.asarray([0, 0, 1, 1, 0], dtype=np.uint8)

    scalar = narrowgate_cpp.TradeBarAggregator(True)
    scalar_completed = []
    for values in zip(ts_ms, prices, sizes, maker):
        scalar_completed.extend(
            scalar.update(int(values[0]), float(values[1]), float(values[2]), bool(values[3]))
        )

    batched = narrowgate_cpp.TradeBarAggregator(True)
    batch_completed = batched.update_batch(ts_ms, prices, sizes, maker)

    assert len(batch_completed) == len(scalar_completed) == 3
    for batch_bar, scalar_bar in zip(batch_completed, scalar_completed):
        assert _bar_values(batch_bar) == pytest.approx(_bar_values(scalar_bar))
    assert _bar_values(batched.current_bar()) == pytest.approx(
        _bar_values(scalar.current_bar())
    )


def test_signal_engine_native_flow_consumes_one_compact_frame(monkeypatch):
    monkeypatch.setenv("NARROWGATE_CPP_GLOBAL_FLOW", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")

    from strategy.signal import SignalEngine

    signal = SignalEngine(
        enable_ml=False,
        symbol="BTCUSDC",
        reference_symbol="BTCUSDT",
        global_flow_shadow_enabled=True,
    )
    ts_ms = np.asarray(
        [1_800_000_000_800, 1_800_000_000_900, 1_800_000_001_100],
        dtype=np.int64,
    )
    receive_ns = int(ts_ms[-1]) * 1_000_000 + 5_000_000
    signal.on_cross_trade_arrays(
        "BTCUSDT",
        ts_ms,
        np.asarray([60_000.0, 60_000.1, 60_000.2], dtype=np.float64),
        np.asarray([0.1, 0.2, 0.3], dtype=np.float64),
        np.asarray([1, 0, 0], dtype=np.uint8),
        market_type="perp",
        venue="bybit",
        receive_ts_ns=receive_ns,
        sequence_numbers=np.asarray([10, 11, 12], dtype=np.int64),
    )

    stats = signal.global_flow_backend_snapshot()
    source = signal.market_source_snapshot(now_ns=receive_ns + 1_000_000)[
        "bybit:perp:BTCUSDT"
    ]
    assert stats["native"] == 1
    assert stats["trade_batches"] == 1
    assert stats["trade_events_seen"] == 3
    assert stats["trade_events_accepted"] == 3
    assert source["last_trade_sequence"] == 12
    assert len(signal._cross_bar_buffers["bybit:perp:BTCUSDT"]) == 1
    assert signal._cpp_cross_aggregators["bybit:perp:BTCUSDT"].current_bucket_ms() == (
        1_800_000_001_000
    )


def test_native_global_flow_reports_fixed_ring_overflow():
    native = narrowgate_cpp.NativeGlobalFlowEngine(2_000, 1_000.0, 1_000.0)
    capacity = int(native.trade_capacity_per_market)
    count = capacity + 1
    receive_ns = 1_800_000_000_000_000_000
    accepted = native.on_trade_batch(
        "okx:perp:BTCUSDT",
        receive_ns,
        np.full(count, receive_ns - 1_000_000, dtype=np.int64),
        np.full(count, 60_000.0, dtype=np.float64),
        np.full(count, 0.001, dtype=np.float64),
        np.zeros(count, dtype=np.uint8),
    )

    stats = native.stats()
    window = native.market_window(
        "okx:perp:BTCUSDT", receive_ns + 1_000_000, 100
    )
    assert accepted == count
    assert stats["trade_events_accepted"] == count
    assert stats["trade_overflow_events"] == 1
    assert window["trade_events"] == capacity
    assert window["trade_overflow_events"] == 1
