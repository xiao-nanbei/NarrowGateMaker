"""Exchange admission must not use the strategy's delayed delivered book."""
import numpy as np
import pandas as pd
import pytest

from models.backtest_tick import simulate_tick
from models.tick_data_types import HistoricalBBOData, HistoricalExchangeBookEvent

BASE = 1_700_000_000_000


def replay_activation(side, delivered_cross, exchange_cross, *, inputs=None, book_offset=100, trades_override=None):
    def prices(cross):
        return ((99.8, 100.0 if cross else 100.2) if side == "BUY"
                else (100.2 if cross else 100.0, 100.4))
    bid, ask = prices(delivered_cross)
    ebid, eask = prices(exchange_cross)
    bbo = HistoricalBBOData(
        ts_ms=np.array([BASE + 100, BASE + 2000]),
        best_bid=np.array([bid, bid]), best_ask=np.array([ask, ask]),
        bid_qty=np.ones(2), ask_qty=np.ones(2), source="public_delivered_depth")
    events = [HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC", event_type="snapshot",
        exchange_ts_ns=(BASE + book_offset) * 1_000_000, local_receive_ts_ns=0,
        levels=(("bid", round(ebid * 10), 1.), ("ask", round(eask * 10), 1.)),
        sequence_scope="provider_ordered", source_ordinal=1)]
    params = dict(
        eta_inventory=.01, a_spread=.01, risk_per_order=.01,
        inventory_reference_qty=1., execution_intensity_slope=1., risk_horizon_s=1.,
        trade_intensity_acceleration_spread_mult=2., order_size=.001,
        max_inventory=.01, maker_fee=0., taker_fee=0., tick_size=.1, lot_size=.001,
        use_bar_pricing=True, replay_event_clock="merged", replay_clock_interval_ms=1000,
        requote_interval=100., rq_min=100., rq_max=100., collect_curves=False,
        position_timeout=0., markout_ema_span_fills=0, max_exec_book_age_s=0.,
        new_order_latency_ms=100, exchange_book_queue_mode="diagnostic",
        trace_quotes_max=100, trace_fills_max=100, trace_local_order_lifecycle_max=100,
        initial_live_state={"active_orders": [dict(
            side=side, price=100.1, quantity=.001, remaining=.001,
            submit_ts_ms=BASE + 100, event_ts_ms=BASE + 300,
            status="PENDING_NEW", mid_at_quote=100.1)]})
    trades = pd.DataFrame(dict(
        transact_time=np.array([BASE + 200, BASE + 1200, BASE + 2200]),
        price=np.full(3, 100.1), quantity=np.zeros(3), is_buyer_maker=np.ones(3)))
    if inputs is not None:
        bbo, events = inputs["bbo"], inputs["exchange_book_event_tape"]
    if trades_override is not None:
        trades = trades_override
    return simulate_tick(trades, np.array([BASE]), np.array([1.]), params,
                         bbo_data=bbo, exchange_book_event_tape=events)


@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("delivered_cross,exchange_cross", [(False, True), (True, False), (False, False), (True, True)])
def test_activation_uses_execution_book_not_delivered_book(side, delivered_cross, exchange_cross):
    result = replay_activation(side, delivered_cross, exchange_cross)
    assert result["gtx_rejects"] == int(exchange_cross)
    restored = [r for r in result["_quote_trace"] if r["order_id"] == 0]
    assert bool(restored) == (not exchange_cross)


@pytest.mark.parametrize("offset", [300, 400])
def test_unknown_or_equal_time_initial_book_does_not_admit(offset):
    result = replay_activation("BUY", False, False, book_offset=offset)
    row = next(r for r in result["_quote_trace"] if r["order_id"] == 0)
    assert result["gtx_rejects"] == 0
    assert row["activation_book_status"] == "UNKNOWN"
    assert not row["exchange_accepted"]


def test_public_input_adapter_delayed_depth_does_not_control_admission(tmp_path):
    from dataclasses import asdict
    from data.facts import materialize
    from data.runtime import ObservationProfile, derive_inputs
    from models.replay.public_input import load_public_replay_inputs

    book, trade = tmp_path / "book.csv", tmp_path / "trade.csv"
    book.write_text(
        "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        f"binance-futures,BTCUSDC,{(BASE+100)*1000},0,true,bid,99.8,1\n"
        f"binance-futures,BTCUSDC,{(BASE+100)*1000},0,true,ask,100.2,1\n"
        f"binance-futures,BTCUSDC,{(BASE+250)*1000},0,false,ask,100.0,1\n")
    trade.write_text(
        "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
        f"binance-futures,BTCUSDC,{(BASE+200)*1000},0,1,buy,100.1,0.001\n")
    materialize({"source_profile": "tardis_only", "files": [
        {"path": str(book), "symbol": "BTCUSDC", "channel": "incremental_book_L2"},
        {"path": str(trade), "symbol": "BTCUSDC", "channel": "trades"},
    ]}, tmp_path / "facts")
    profile = ObservationProfile("controlled", "source_timestamp_proxy", 100_000_000, 0,
                                 100_000_000, 1_000_000_000, trade_coverage="observed")
    derive_inputs({"facts_root": str(tmp_path / "facts"), "observation_profile": asdict(profile),
                   "start_ns": BASE*1_000_000, "end_ns": (BASE+2300)*1_000_000,
                   "market_id": "binance_futures:perpetual:BTCUSDC"}, tmp_path / "consumer")
    inputs = load_public_replay_inputs(tmp_path / "consumer", tick_size=.1)
    bbo = inputs["bbo"]
    assert bbo.best_ask[np.searchsorted(bbo.ts_ms, BASE+300, side="right")-1] == 100.2
    result = replay_activation("BUY", False, True, inputs=inputs)
    assert result["gtx_rejects"] == 1
    assert not any(r["order_id"] == 0 for r in result["_quote_trace"])


@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("residual_cancel", [False, True])
def test_public_trade_is_not_consumed_again_as_book_cancellation(side, residual_cancel):
    buy = side == "BUY"
    level_side = "bid" if buy else "ask"
    remaining = .004 if residual_cancel else .006
    events = [HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC", event_type="snapshot",
        exchange_ts_ns=(BASE + 100) * 1_000_000, local_receive_ts_ns=0,
        levels=(("bid", 1001 if buy else 999, .010),
                ("ask", 1003 if buy else 1001, .010)),
        sequence_scope="provider_ordered", source_ordinal=1),
        HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC", event_type="delta",
        exchange_ts_ns=(BASE + 600) * 1_000_000, local_receive_ts_ns=0,
        levels=((level_side, 1001, remaining),),
        sequence_scope="provider_ordered", source_ordinal=2)]
    # Delivered mid intentionally differs from execution mid, with both
    # books allowing the restored GTX order. This also exercises the scope
    # boundary between admission and existing quote-distance diagnostics.
    delivered_bid, delivered_ask = ((99.9, 100.9) if buy else (99.3, 100.3))
    bbo = HistoricalBBOData(
        ts_ms=np.array([BASE + 100, BASE + 2000]),
        best_bid=np.full(2, delivered_bid), best_ask=np.full(2, delivered_ask),
        bid_qty=np.ones(2), ask_qty=np.ones(2), source="public_delivered_depth")
    trades = pd.DataFrame(dict(
        transact_time=np.array([BASE + x for x in (200, 400, 800, 900, 2200)]),
        price=np.full(5, 100.1), quantity=np.array([0., .004, remaining, .001, 0.]),
        is_buyer_maker=np.full(5, int(buy), dtype=np.uint8)))
    result = replay_activation(side, False, False,
        inputs={"bbo": bbo, "exchange_book_event_tape": events}, trades_override=trades)
    fills = [r for r in result["_fill_trace"] if r["order_id"] == 0]
    assert len(fills) == 1
    assert fills[0]["fill_ts"] == BASE + 900
    assert fills[0]["fill_qty"] == .001
    assert result["exchange_book_queue_cancel_ahead_qty"] == pytest.approx(.002 if residual_cancel else 0.)
    expected_mid = (delivered_bid + delivered_ask) / 2
    assert fills[0]["quote_mid"] == expected_mid
    assert fills[0]["quote_dist"] == ((expected_mid - 100.1) if buy else (100.1 - expected_mid))
    assert fills[0]["move_from_quote_mid_to_fill"] == abs(100.1 - expected_mid)
