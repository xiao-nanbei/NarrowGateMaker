import pandas as pd

from research.families.f10_live_replay_attribution.audit.live_order_fill_closure import (
    build_live_order_fill_closure,
    summarize_live_order_fill_closure,
)


def test_passive_trade_through_closes_live_orders() -> None:
    outcomes = pd.DataFrame(
        [
            {
                "timestamp": 1.000,
                "event_type": "placed",
                "client_order_id": "buy",
                "side": "BUY",
                "price": 99.0,
            },
            {
                "timestamp": 2.000,
                "event_type": "filled",
                "client_order_id": "buy",
                "side": "BUY",
                "price": 99.0,
            },
            {
                "timestamp": 1.000,
                "event_type": "placed",
                "client_order_id": "sell",
                "side": "SELL",
                "price": 101.0,
            },
            {
                "timestamp": 2.000,
                "event_type": "canceled",
                "client_order_id": "sell",
                "side": "SELL",
                "price": 101.0,
            },
            {
                "timestamp": 1.500,
                "event_type": "placed_close",
                "client_order_id": "ioc",
                "side": "BUY",
                "price": 102.0,
            },
            {
                "timestamp": 1.501,
                "event_type": "filled",
                "client_order_id": "ioc",
                "side": "BUY",
                "price": 102.0,
            },
        ]
    )
    trades = pd.DataFrame(
        [
            {
                "time": 1500,
                "price": 99.0,
                "qty": 0.001,
                "is_buyer_maker": True,
            },
            {
                "time": 1600,
                "price": 100.5,
                "qty": 0.001,
                "is_buyer_maker": False,
            },
        ]
    )

    panel = build_live_order_fill_closure(outcomes, trades, day_end_ms=2001)
    summary = summarize_live_order_fill_closure(panel)

    assert panel["client_order_id"].tolist() == ["buy", "sell"]
    assert panel["predicted_price_through_fill"].tolist() == [True, False]
    assert summary["actual_fills"] == 1
    assert summary["predicted_fills"] == 1
    assert summary["recall"] == 1.0
    assert summary["precision"] == 1.0
