"""Produce one market-bound F04 reference input with the maintained data runtime.

The execution ConsumerBundle fixes the support and warmup. This module only
selects the same dated BTCUSDT facts and declares their separate delivery
scenario; it does not read outcomes, train, or implement another clock/reader.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from data.facts import _digest
from data.runtime import ConsumerBundle, ObservationProfile, bundle_paths, derive_inputs
from models.replay.settlement_recovery import verify_manifest_relocation


EXECUTION_MARKET = "binance_futures:perpetual:BTCUSDC"
REFERENCE_MARKET = "binance_futures:perpetual:BTCUSDT"
_ARTIFACTS = ("features", "bars", "depth")


def _utc_ns(value: str) -> int:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset().total_seconds() != 0:
        raise ValueError("frozen account boundary must be UTC")
    delta = timestamp - datetime(1970, 1, 1, tzinfo=UTC)
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000_000
            + delta.microseconds * 1_000)


def reference_input_plan(spec: dict) -> dict:
    """Bind a BTCUSDT source/day sequence to one existing execution interval."""
    if spec.get("schema") != "f04.reference_input_production.v1":
        raise ValueError("unknown F04 reference production specification")
    contract = json.loads(Path(spec["contract_path"]).read_text())
    if contract.get("schema") != "narrowgate.f04.tardis_two_market_first_batch.v1":
        raise ValueError("unknown F04 frozen research contract")
    if contract["markets"] != {"execution": EXECUTION_MARKET, "reference": REFERENCE_MARKET,
                               "reference_currency_conversion": "none; only currency-invariant reference features admitted"}:
        raise ValueError("F04 frozen market/currency mapping changed")
    execution = ConsumerBundle(spec["execution_root"])
    execution_plan = execution.manifest["plan"]
    if execution_plan["market_id"] != EXECUTION_MARKET:
        raise ValueError("F04 execution bundle must be BTCUSDC perpetual")
    unit = spec["unit"]
    if unit in contract["fit_days_utc"]:
        expected_parent = contract["bound_existing_fit_inputs"][unit]["execution_consumer_manifest_sha256"]
    elif unit == contract["evaluation"]["shard"]:
        expected_parent = contract["evaluation"]["execution_consumer_manifest_sha256"]
        account_start_ns = _utc_ns(contract["evaluation"]["account_start_utc"])
        account_end_ns = _utc_ns(contract["evaluation"]["account_end_exclusive_utc"])
        if not execution_plan["start_ns"] <= account_start_ns < execution_plan["end_ns"] == account_end_ns:
            raise ValueError("F04 evaluation account interval mismatch")
    else:
        raise ValueError("unit outside frozen F04 input budget")
    parent_id = _digest(execution.root / "manifest.json")
    if parent_id != expected_parent:
        relocated_manifest = spec.get("bound_execution_relocation_manifest")
        if not relocated_manifest:
            raise ValueError("F04 execution ConsumerBundle identity mismatch")
        verify_manifest_relocation(execution.root / "manifest.json", relocated_manifest,
                                   original_sha256=parent_id, relocated_sha256=expected_parent)
    # The frozen execution ConsumerBundle may be mirrored to another host while
    # its source-fact paths retain their original local locators. We only need
    # the bound source dates here; no execution facts are replayed or rebuilt.
    execution_days = [Path(row["path"]).name for row in execution.manifest["source_bundles"]]
    if not execution_days or execution_days != sorted(set(execution_days)):
        raise ValueError("execution source days must be unique and ordered")
    if unit in contract["fit_days_utc"] and execution_days[-1] != unit:
        raise ValueError("F04 fit unit source-day mismatch")
    if unit == contract["evaluation"]["shard"] and execution_days[0] != contract["evaluation"]["reference_warmup_day_utc"]:
        raise ValueError("F04 evaluation warmup day mismatch")
    reference = bundle_paths(spec["reference_facts_root"], start=execution_days[0],
                             end=execution_days[-1], relocated=True)
    if [path.name for path in reference] != execution_days:
        raise ValueError("reference facts do not match execution support days")
    reference_clock = contract["observation"]["reference"]
    profile = dict(execution_plan["observation_profile"])
    for name in ("profile_id", "clock_policy", "market_delay_ns", "processing_ns",
                 "measured_latency_path", "measured_latency_sha256", "measured_latency_market_id"):
        profile[name] = reference_clock[name]
    ObservationProfile(**profile).require_market(REFERENCE_MARKET)
    scenario = {"schema": "data.observation_scenario.v1", "market_id": REFERENCE_MARKET,
                "classification": "simulated_not_measured",
                "parameter_basis": reference_clock["origin"],
                "provider_local_timestamp_as_receive": False,
                "native_observation_parity": "not_proven"}
    return {
        "source_bundles": [{"path": str(path), "sha256": _digest(path / "manifest.json")}
                           for path in reference],
        "market_id": REFERENCE_MARKET,
        "observation_profile": profile,
        "observation_scenario": scenario,
        "start_ns": execution_plan["start_ns"],
        "end_ns": execution_plan["end_ns"],
        "source_start_day": execution_days[0],
        "source_end_day": execution_days[-1],
        "include_outcome_bars": False,
        "execution_parent_manifest_sha256": parent_id,
        "bound_execution_manifest_sha256": expected_parent,
        "frozen_contract_sha256": _digest(Path(spec["contract_path"])),
        "frozen_unit": unit,
    }


def derive_reference_input(spec: dict, output: str | Path) -> dict:
    """Create a reference bundle once, or verify the exact existing product."""
    plan = reference_input_plan(spec)
    output = Path(output)
    if output.exists():
        bundle = ConsumerBundle(output)
        if bundle.manifest["plan"] != plan:
            raise ValueError("existing F04 reference bundle belongs to another plan")
        bundle.source_paths()
        for name in _ARTIFACTS:
            next(bundle.batches(name, batch_size=1), None)
        if set(bundle.manifest["files"]) != set(_ARTIFACTS):
            raise ValueError("unexpected F04 reference artifact set")
        return {"status": "verified_existing", "manifest_sha256": _digest(output / "manifest.json"),
                "rows": {name: bundle.manifest["files"][name]["rows"] for name in _ARTIFACTS}}
    result = derive_inputs(plan, output)
    return {"status": "completed", "manifest_sha256": _digest(output / "manifest.json"),
            "rows": {name: result["files"][name]["rows"] for name in _ARTIFACTS}}


def main() -> None:
    import argparse
    from research.families.f04_external_market_alpha.daylight_stage import (
        admit_worker, run_daylight,
    )

    parser = argparse.ArgumentParser(description="Create/verify one F04 BTCUSDT reference ConsumerBundle")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.worker:
        raise SystemExit(run_daylight(
            [sys.executable, "-m",
             "research.families.f04_external_market_alpha.reference_input_production",
             "--worker", *sys.argv[1:]],
            minimum_seconds=7_200))
    admit_worker(minimum_seconds=7_200)
    result = derive_reference_input(json.loads(args.spec.read_text()), args.output)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
