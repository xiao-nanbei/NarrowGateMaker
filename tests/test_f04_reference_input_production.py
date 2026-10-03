"""Synthetic, market-distinct production checks; no purchased records."""

import json
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import pytest

from data.facts import _digest, materialize
from data.runtime import ConsumerBundle, ObservationProfile, derive_inputs
from research.families.f04_external_market_alpha.reference_input_production import (
    derive_reference_input, reference_input_plan,
)


DAY = "2025-08-01"
START_NS = 1_754_006_400_000_000_000


def _facts(root, symbol):
    root.mkdir()
    book, trade = root / "book.csv", root / "trades.csv"
    source_us = START_NS // 1_000 + 1_000_000
    book.write_text(
        "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
        f"binance-futures,{symbol},{source_us},{source_us + 999999},true,bid,100,1\n"
        f"binance-futures,{symbol},{source_us},{source_us + 999999},true,ask,102,1\n"
    )
    trade.write_text(
        "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
        f"binance-futures,{symbol},{source_us + 200000},{source_us + 999999},1,buy,101,2\n"
    )
    day = root / DAY
    materialize({"source_profile": "tardis_only", "files": [
        {"path": str(book), "symbol": symbol, "channel": "incremental_book_L2"},
        {"path": str(trade), "symbol": symbol, "channel": "trades"},
    ]}, day)
    return day


def _spec(tmp_path):
    execution_facts = _facts(tmp_path / "execution-facts", "BTCUSDC")
    reference_root = tmp_path / "reference-facts"
    reference = _facts(reference_root, "BTCUSDT")
    (reference_root / "manifest.json").write_text(json.dumps({
        "schema": "data.full_calendar.v1", "source_profile": "tardis_only",
        "start": DAY, "end": DAY, "symbols": ["BTCUSDT"],
        "days": [{"calendar_date": DAY, "status": "content_scanned", "bundle": str(reference),
                  "manifest_sha256": _digest(reference / "manifest.json")}],
    }))
    profile = ObservationProfile("execution_fixture", "source_timestamp_proxy", 200_000_000, 0,
                                 100_000_000, 1_000_000_000, trade_coverage="observed")
    execution = tmp_path / "execution"
    derive_inputs({"source_bundles": [{"path": str(execution_facts),
                                       "sha256": _digest(execution_facts / "manifest.json")}],
                   "market_id": "binance_futures:perpetual:BTCUSDC",
                   "observation_profile": asdict(profile), "start_ns": START_NS,
                   "end_ns": START_NS + 4_000_000_000}, execution)
    contract = tmp_path / "frozen.json"
    contract.write_text(json.dumps({
        "schema": "narrowgate.f04.tardis_two_market_first_batch.v1",
        "markets": {"execution": "binance_futures:perpetual:BTCUSDC",
                    "reference": "binance_futures:perpetual:BTCUSDT",
                    "reference_currency_conversion": "none; only currency-invariant reference features admitted"},
        "fit_days_utc": [DAY],
        "evaluation": {"shard": "unused-evaluation"},
        "bound_existing_fit_inputs": {DAY: {"execution_consumer_manifest_sha256":
                                            _digest(execution / "manifest.json")}},
        "observation": {"reference": {"profile_id": "f04_btcusdt_source_proxy_simulated_250ms_plus_1ms_v1",
                                      "clock_policy": "source_timestamp_proxy", "market_delay_ns": 250_000_000,
                                      "processing_ns": 1_000_000, "measured_latency_path": None,
                                      "measured_latency_sha256": None, "measured_latency_market_id": None,
                                      "origin": "frozen conservative scenario before outcomes"}},
    }))
    return {"schema": "f04.reference_input_production.v1", "contract_path": str(contract),
            "execution_root": str(execution), "reference_facts_root": str(reference_root), "unit": DAY}


def test_f04_reference_producer_binds_market_and_reuses_exact_existing(tmp_path):
    spec = _spec(tmp_path)
    plan = reference_input_plan(spec)
    assert plan["market_id"] == "binance_futures:perpetual:BTCUSDT"
    assert plan["observation_scenario"]["classification"] == "simulated_not_measured"
    assert plan["observation_profile"]["market_delay_ns"] == 250_000_000
    assert plan["observation_profile"]["processing_ns"] == 1_000_000
    assert plan["start_ns"] == START_NS
    output = tmp_path / "reference"
    assert derive_reference_input(spec, output)["status"] == "completed"
    reference = ConsumerBundle(output)
    assert reference.manifest["plan"] == plan
    assert reference.manifest["stats"]["future_fill_violations"] == 0
    assert derive_reference_input(spec, output)["status"] == "verified_existing"
    contract_path = Path(spec["contract_path"])
    contract = json.loads(contract_path.read_text())
    contract["observation"]["reference"]["origin"] = "different frozen scenario"
    contract_path.write_text(json.dumps(contract))
    with pytest.raises(ValueError, match="another plan"):
        derive_reference_input(spec, output)


def test_f04_reference_producer_rejects_retargeted_execution(tmp_path):
    spec = _spec(tmp_path)
    contract_path = Path(spec["contract_path"])
    contract = json.loads(contract_path.read_text())
    contract["bound_existing_fit_inputs"][DAY]["execution_consumer_manifest_sha256"] = "0" * 64
    contract_path.write_text(json.dumps(contract))
    with pytest.raises(ValueError, match="identity mismatch"):
        reference_input_plan(spec)


def test_f04_reference_plan_reuses_mirrored_execution_without_origin_facts(tmp_path):
    spec = _spec(tmp_path)
    # The already-produced execution artifacts are portable even when the
    # original fact locator in their frozen manifest is not mounted remotely.
    with patch.object(ConsumerBundle, "source_paths", side_effect=AssertionError("origin facts not required")):
        plan = reference_input_plan(spec)
    assert plan["source_start_day"] == DAY
    assert plan["execution_parent_manifest_sha256"] == _digest(Path(spec["execution_root"]) / "manifest.json")
