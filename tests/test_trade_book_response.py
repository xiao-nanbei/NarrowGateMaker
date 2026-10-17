"""Synthetic clock diagnostics; no private evidence or strategy executions."""

from dataclasses import asdict
from decimal import Decimal as D
import json

import pytest

from data.facts import materialize
from data.observation import Observation, TradeContribution
from data.runtime import InputTick, ObservationProfile, derive_inputs
from data.tardis_input import BookView, CONTRACT
from research.families.f08_side_taker_lifecycle.trade_book_response import ClockAdmission, audit_bundle


def observation(*, ready=5, source=1, version=1, trade=False):
    payload = (TradeContribution("trade", source, D(100), D(1), "buy", 1, None, "source", 0)
               if trade else BookView(version, ((D(99), D(1)),), ((D(101), D(1)),),
                                      0, 0, "unknown", True))
    return Observation("event", "market", CONTRACT, "synthetic", source, source, ready,
                       ready, payload, coverage="unknown")


def tick(now, *observations):
    return InputTick(now, (), observations, (), None, None)


def test_clock_order_and_missing_coverage_stay_explicit():
    audit = ClockAdmission()
    audit.advance(tick(7, observation(), observation(trade=True)))
    result = audit.report()
    assert result["counts"]["trade_coverage:unknown"] == 1
    assert result["delays"]["trade_ready_to_read"]["sum_ns"] == 2
    assert result["delays"]["trade_publish_to_receive"]["sum_ns"] == 4
    assert result["counts"]["visible_book"] == 1


def test_future_and_regressing_delivery_rejected_without_sorting():
    audit = ClockAdmission()
    with pytest.raises(ValueError, match="before ready"):
        audit.advance(tick(4, observation()))
    audit = ClockAdmission()
    audit.advance(tick(7, observation(ready=7)))
    with pytest.raises(ValueError, match="delivery order"):
        audit.advance(tick(8, observation(ready=6)))
    with pytest.raises(ValueError, match="scheduler time"):
        audit.advance(tick(6))


def test_late_replacement_is_reported_not_used_to_lower_watermark():
    audit = ClockAdmission()
    audit.advance(tick(10, observation(source=5, ready=10, version=3)))
    audit.advance(tick(11, observation(source=4, ready=11, version=2)))
    assert audit.book_watermark == (5, 3)
    assert audit.report()["counts"]["late_replacement_book_observations"] == 1


def test_original_bundle_producer_restored_without_strategy_or_output_overwrite(tmp_path):
    book, trade = tmp_path / "book.csv", tmp_path / "trade.csv"
    book.write_text("exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
                    "binance-futures,BTCUSDC,1000000,8000000,true,bid,100,1\n"
                    "binance-futures,BTCUSDC,1000000,8000000,true,ask,102,1\n")
    trade.write_text("exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
                     "binance-futures,BTCUSDC,1200000,8000000,1,buy,101,2\n")
    materialize({"source_profile": "tardis_only", "files": [
        {"path": str(book), "symbol": "BTCUSDC", "channel": "incremental_book_L2"},
        {"path": str(trade), "symbol": "BTCUSDC", "channel": "trades"},
    ]}, tmp_path / "facts")
    profile = ObservationProfile("test", "source_timestamp_proxy", 200_000_000, 0,
                                 100_000_000, 1_000_000_000, trade_coverage="observed")
    bundle = tmp_path / "consumer"
    derive_inputs({"facts_root": str(tmp_path / "facts"), "observation_profile": asdict(profile),
                   "start_ns": 1_000_000_000, "end_ns": 4_000_000_000,
                   "market_id": "binance_futures:perpetual:BTCUSDC"}, bundle)
    output = tmp_path / "audit.json"
    report = audit_bundle(bundle, start_ns=1_000_000_000, end_ns=3_000_000_000, output=output)
    assert report["strategy_replays"] == 0
    assert report["strategy_speed_multiple"] is None
    assert report["counts"]["visible_trade"] == 1
    assert report["counts"]["trade_native_exchange_missing"] == 1
    assert report["counts"]["source_book"] == 1
    assert report["counts"]["source_book_levels"] == 2
    assert json.loads(output.read_text())["status"] == "complete"
    with pytest.raises(FileExistsError):
        audit_bundle(bundle, start_ns=1_000_000_000, end_ns=3_000_000_000, output=output)
    # Starting diagnostics later does not restart delivery or silently drop
    # the original producer warmup. The already-delivered trade is not new.
    later = audit_bundle(bundle, start_ns=2_000_000_000, end_ns=3_000_000_000,
                         output=tmp_path / "later.json")
    assert later["warmup_ticks_excluded"] > 0
    assert later["counts"].get("visible_trade", 0) == 0
