import gzip
import json

import pytest

from research.families.f05_fill_quality_quote_ev.audit.fill_toxicity import (
    build_fill_toxicity_rows,
    iter_causal_rows,
    load_fills,
)
from research.system_engineering.audit.market_data_latency import MarketDataLatencySimulator


def _book(market_id, ts_ns, bid, ask, bid_size=1.0, ask_size=1.0):
    return {
        "market_id": market_id,
        "event_type": "book",
        "exchange_event_ts_ns": ts_ns,
        "local_receive_ts_ns": ts_ns,
        "feature_ready_ts_ns": ts_ns,
        "bid": bid,
        "bid_size": bid_size,
        "ask": ask,
        "ask_size": ask_size,
    }


def _trade(market_id, ts_ns, price, size, side):
    return {
        "market_id": market_id,
        "event_type": "trade",
        "exchange_event_ts_ns": ts_ns,
        "local_receive_ts_ns": ts_ns,
        "feature_ready_ts_ns": ts_ns,
        "price": price,
        "size": size,
        "aggressor_side": side,
    }


def test_receive_time_flow_is_sampled_at_fill_and_labels_maker_signed_markout(tmp_path):
    start = 1_700_000_000_000_000_000
    execution = "binance:perp:BTCUSDC"
    bridge = "binance:perp:BTCUSDT"
    bitget = "bitget:perp:BTCUSDT"
    bybit = "bybit:perp:BTCUSDT"
    rows = [
        _book(execution, start, 99.9, 100.1),
        _book(bridge, start + 1, 99.9, 100.1),
        _book(bitget, start + 2, 99.9, 100.1),
        _book(bybit, start + 3, 99.9, 100.1),
        _book(bitget, start + 10_000_000, 99.9, 100.1, bid_size=0.4),
        _trade(bitget, start + 11_000_000, 99.9, 0.5, "sell"),
        _book(bybit, start + 12_000_000, 99.9, 100.1, bid_size=0.5),
        _trade(bybit, start + 13_000_000, 99.9, 0.4, "sell"),
        # This local move is visible exactly at fill+50ms and labels the fill.
        _book(execution, start + 70_000_000, 98.9, 99.1),
        # Future external buying must not leak into the fill-time flow state.
        _trade(bitget, start + 80_000_000, 99.1, 5.0, "buy"),
    ]
    path = tmp_path / "tape.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.writelines(json.dumps(row) + "\n" for row in rows)
    fill_ts_ns = start + 20_000_000
    fills = [
        {
            "fill_id": 1,
            "fill_ts": fill_ts_ns / 1_000_000_000,
            "fill_ts_ns": fill_ts_ns,
            "side": "BUY",
            "trade_type": "OPEN",
            "price": 100.0,
            "qty": 0.001,
        },
        {
            "fill_id": 2,
            "fill_ts": fill_ts_ns / 1_000_000_000,
            "fill_ts_ns": fill_ts_ns,
            "side": "SELL",
            "trade_type": "OPEN",
            "price": 100.0,
            "qty": 0.001,
        },
    ]

    result = build_fill_toxicity_rows(
        tape_paths=[path],
        fills=fills,
        flow_horizons_ms=(100,),
        markout_horizons_ms=(10, 50),
        max_future_book_age_ms=100,
    )

    assert result[0]["global_flow_valid_100ms"] == 1
    assert result[0]["global_flow_pressure_100ms"] < 0.0
    assert result[0]["execution_book_fresh_100ms"] == 1
    assert result[0]["bitget_perp_book_fresh_100ms"] == 1
    assert result[0]["bybit_perp_book_fresh_100ms"] == 1
    assert result[0]["markout_10ms_bps"] == pytest.approx(0.0)
    assert result[0]["markout_50ms_bps"] == pytest.approx(-100.0)
    assert result[0]["toxic_50ms"] == 1
    assert result[1]["markout_50ms_bps"] == pytest.approx(100.0)
    assert result[1]["toxic_50ms"] == 0


def test_profile_latency_can_make_external_flow_unavailable_at_fill(tmp_path):
    start = 1_700_000_000_000_000_000
    execution = "binance:perp:BTCUSDC"
    bridge = "binance:perp:BTCUSDT"
    bitget = "bitget:perp:BTCUSDT"
    bybit = "bybit:perp:BTCUSDT"
    rows = [
        _book(execution, start, 99.9, 100.1),
        _book(bridge, start + 1, 99.9, 100.1),
        _book(bitget, start + 2, 99.9, 100.1),
        _book(bybit, start + 3, 99.9, 100.1),
        _trade(bitget, start + 5_000_000, 99.9, 0.5, "sell"),
        _trade(bybit, start + 6_000_000, 99.9, 0.4, "sell"),
        _book(execution, start + 70_000_000, 98.9, 99.1),
    ]
    for row in rows:
        row["transport"] = "websocket"
    path = tmp_path / "tape.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.writelines(json.dumps(row) + "\n" for row in rows)
    groups = []
    for market_id, event_type in sorted(
        {(row["market_id"], row["event_type"]) for row in rows}
    ):
        groups.append(
            {
                "market_id": market_id,
                "event_type": event_type,
                "transport": "websocket",
                "visibility_lag_ms_p50": 30.0,
                "visibility_lag_ms_p95": 30.0,
                "visibility_lag_ms_p99": 30.0,
                "visibility_lag_ms_p999": 30.0,
                "visibility_lag_ms_max": 30.0,
                "simulation_quantile_probabilities": [0.0, 1.0],
                "simulation_visibility_lag_ms_quantiles": [30.0, 30.0],
            }
        )
    simulator = MarketDataLatencySimulator(
        {
            "schema": "market_data_latency_profile.v1",
            "profile_id": "aws_tokyo_test",
            "groups": groups,
        }
    )
    fill_ts_ns = start + 20_000_000
    fills = [
        {
            "fill_id": 1,
            "fill_ts": fill_ts_ns / 1_000_000_000,
            "fill_ts_ns": fill_ts_ns,
            "side": "BUY",
            "trade_type": "OPEN",
            "price": 100.0,
            "qty": 0.001,
        }
    ]

    captured = build_fill_toxicity_rows(
        tape_paths=[path],
        fills=fills,
        flow_horizons_ms=(100,),
        markout_horizons_ms=(50,),
    )
    delayed = build_fill_toxicity_rows(
        tape_paths=[path],
        fills=fills,
        flow_horizons_ms=(100,),
        markout_horizons_ms=(50,),
        latency_simulator=simulator,
        latency_mode="profile_p50",
    )

    assert captured[0]["global_flow_valid_100ms"] == 1
    assert delayed[0]["global_flow_valid_100ms"] == 0
    assert delayed[0]["market_data_latency_profile_id"] == "aws_tokyo_test"


def test_captured_tape_reorders_cross_callback_feature_timestamps(tmp_path):
    start = 1_700_000_000_000_000_000
    rows = [
        _book("binance:perp:BTCUSDT", start + 20_000_000, 99.9, 100.1),
        _book("binance:perp:BTCUSDC", start + 10_000_000, 99.9, 100.1),
    ]
    path = tmp_path / "interleaved.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.writelines(json.dumps(row) + "\n" for row in rows)

    replayed = list(iter_causal_rows([path]))

    assert [row["feature_ready_ts_ns"] for row in replayed] == [
        start + 10_000_000,
        start + 20_000_000,
    ]


def test_fill_loader_excludes_inventory_sync_rows(tmp_path):
    path = tmp_path / "trades.csv"
    path.write_text(
        "timestamp,side,trade_type,qty,price\n"
        "1700000000.0,BUY,OPEN,0.001,100\n"
        "1700000001.0,BUY,SYNC_ADJUST,0.004,100\n",
        encoding="utf-8",
    )

    fills = load_fills(path)

    assert len(fills) == 1
    assert fills[0]["trade_type"] == "OPEN"


def test_fill_loader_accepts_gzip_csv(tmp_path):
    path = tmp_path / "trades.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        handle.write(
            "timestamp,side,trade_type,qty,price\n"
            "1700000000.0,SELL,CLOSE,0.001,100\n"
        )

    fills = load_fills(path)

    assert len(fills) == 1
    assert fills[0]["side"] == "SELL"
