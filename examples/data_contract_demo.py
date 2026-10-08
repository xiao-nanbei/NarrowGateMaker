"""Synthetic Tardis-format input walkthrough; no purchased or live records.

Run from the repository root: python -m examples.data_contract_demo
All generated files are temporary. This is an interface example, not validation
of a real market interval, a trained model, or economic replay.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory

from data.facts import materialize, read_facts, validate_bundle
from data.runtime import ConsumerBundle, ObservationProfile, derive_inputs
from data.tardis_input import TradeExecution


def run_demo(root: Path) -> dict:
    """Build controlled raw -> facts -> observations/Bars/features examples."""
    raw = root / "raw"
    raw.mkdir()
    book = raw / "book.csv"
    book.write_text(
        "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        "binance-futures,BTCUSDC,1000000,8000000,true,bid,100,1\n"
        "binance-futures,BTCUSDC,1000000,8000000,true,ask,102,1\n"
        "binance-futures,BTCUSDC,2100000,9000000,false,bid,101,2\n"
    )
    trades = raw / "trades.csv"
    trades.write_text(
        "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
        "binance-futures,BTCUSDC,1200000,8000000,1,buy,101,2\n"
        "binance-futures,BTCUSDC,1200000,8100000,1,buy,101,2\n"
        "binance-futures,BTCUSDC,1200000,8200000,2,sell,101,1\n"
    )
    facts = root / "facts"
    materialize({"source_profile": "tardis_only", "files": [
        {"path": str(book), "symbol": "BTCUSDC", "channel": "incremental_book_L2"},
        {"path": str(trades), "symbol": "BTCUSDC", "channel": "trades"},
    ]}, facts, row_group_size=1)
    quality = validate_bundle(facts)
    executions = [event for event in read_facts(facts) if isinstance(event, TradeExecution)]
    # This explicit proxy is a modeled scenario, NOT historical clock evidence.
    profile = ObservationProfile(
        "synthetic_demo", "source_timestamp_proxy", 200_000_000, 0,
        300_000_000, 1_000_000_000, trade_coverage="observed",
    )
    consumers = root / "consumers"
    receipt = derive_inputs({
        "facts_root": str(facts), "observation_profile": asdict(profile),
        "start_ns": 1_000_000_000, "end_ns": 4_000_000_000,
        "market_id": "binance_futures:perpetual:BTCUSDC",
        "include_outcome_bars": True,
    }, consumers)
    frames = list(ConsumerBundle(consumers).frames())
    assert [event.trade_id for event in executions] == [1, 2]
    assert dict(frames[0].values)["mid"] is None
    assert dict(frames[1].values)["mid"] == 101
    assert frames[0].max_dependency_ready_ns is None  # no delivered dependency yet
    assert all(
        frame.max_dependency_ready_ns <= frame.cutoff_ns
        for frame in frames if frame.max_dependency_ready_ns is not None
    )
    assert receipt["stats"]["future_fill_violations"] == 0
    return {
        "fixture": "synthetic_not_market_evidence",
        "unique_trade_ids": [event.trade_id for event in executions],
        "observed_quantity": str(sum(event.quantity for event in executions)),
        "fact_clock_evidence": "unknown",
        "observation_scenario": asdict(profile),
        "quality_files": quality["files"],
        "consumer_stats": receipt["stats"],
        "feature_frames": [asdict(frame) for frame in frames],
        "training": "not_run", "economic_replay": "not_run", "live": "not_run",
    }


def main() -> None:
    with TemporaryDirectory(prefix="narrowgate-synthetic-") as temporary:
        result = run_demo(Path(temporary).resolve())
        print(json.dumps(result, default=str, indent=2))


if __name__ == "__main__":
    main()
