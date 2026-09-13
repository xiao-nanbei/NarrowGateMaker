"""Create-only, daylight-bounded economic replay of the frozen F04 first pair.

This is an explicit research runner. It never selects a winner, changes live,
or treats a reference price in USDT as an execution price in USDC.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
import math
import os
import signal
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pandas as pd

from data.facts import save_private_json
from data.runtime import ConsumerBundle
from models.replay.settlement_recovery import verify_manifest_relocation
from research.families.f04_external_market_alpha.daylight_stage import remaining_seconds
from research.families.f04_external_market_alpha.tardis_direction import (
    DirectionModel,
    TardisDirectionSignalEngine,
    _plan,
    _sha,
)

MARKET = "binance_futures:perpetual:BTCUSDC"
MODULE = "research.families.f04_external_market_alpha.tardis_economic"


def daylight_budget_seconds(now=None) -> float:
    """Never admit a new full account without two daylight hours remaining."""
    try:
        return remaining_seconds(now, minimum_seconds=7140)
    except RuntimeError as exc:
        raise RuntimeError("F04 account replay is not admitted within the Shanghai daylight budget") from exc


def _evaluation_bundle(root: Path, plan: dict, relocation_manifest: Path | None) -> ConsumerBundle:
    bundle = ConsumerBundle(root)
    expected = plan["evaluation"]["execution_consumer_manifest_sha256"]
    actual = _sha(root / "manifest.json")
    if actual != expected:
        if relocation_manifest is None or _sha(relocation_manifest) != expected:
            raise ValueError("F04 account input differs from its frozen parent")
        verify_manifest_relocation(relocation_manifest, root / "manifest.json",
                                   original_sha256=expected, relocated_sha256=actual)
    if bundle.manifest["plan"]["market_id"] != MARKET:
        raise ValueError("F04 account execution market is not BTCUSDC")
    start = pd.Timestamp(plan["evaluation"]["account_start_utc"]).value
    end = pd.Timestamp(plan["evaluation"]["account_end_exclusive_utc"]).value
    source = bundle.manifest["plan"]
    if not source["start_ns"] < start < end == source["end_ns"]:
        raise ValueError("F04 account or warmup boundary changed")
    bundle.source_paths()
    return bundle


def relocate_evaluation_bundle(*, plan_path: Path, original_manifest: Path,
                               frozen_root: Path, fact_paths_by_sha: dict[str, str],
                               measured_latency_path: Path | None, output: Path) -> dict:
    """Rebind only locators of already verified eval Parquet to existing facts.

    The original local manifest and its frozen first relocation are an exact
    named pair.  A second create-only relocation points at the matching fact
    bytes already on the research host; no source, observation or market value
    is synthesized, and no large execution Parquet is copied.
    """
    daylight_budget_seconds()
    plan, _ = _plan(plan_path)
    original_manifest, frozen_root, output = map(Path, (original_manifest, frozen_root, output))
    frozen_manifest = frozen_root / "manifest.json"
    expected = plan["evaluation"]["execution_consumer_manifest_sha256"]
    original_sha = _sha(original_manifest)
    if _sha(frozen_manifest) != expected:
        raise ValueError("F04 frozen first relocation identity changed")
    verify_manifest_relocation(original_manifest, frozen_manifest,
                               original_sha256=original_sha, relocated_sha256=expected)
    frozen_bundle = ConsumerBundle(frozen_root)
    if frozen_bundle.manifest["plan"]["market_id"] != MARKET:
        raise ValueError("F04 original execution input is not BTCUSDC")
    for name in frozen_bundle.manifest["files"]:
        next(frozen_bundle.batches(name, batch_size=1), None)
    frozen = json.loads(frozen_manifest.read_text())
    source_specs = frozen["source_bundles"]
    required = [row["sha256"] for row in source_specs]
    if len(required) != len(set(required)) or set(fact_paths_by_sha) != set(required):
        raise ValueError("F04 relocated facts need one exact path per parent identity")
    roots = []
    for sha in required:
        path = Path(fact_paths_by_sha[sha]).resolve(strict=True)
        if not (path / "manifest.json").is_file() or _sha(path / "manifest.json") != sha:
            raise ValueError("F04 relocated fact manifest identity changed")
        roots.append(path)
    if len({path.parent for path in roots}) != 1:
        raise ValueError("F04 relocated fact days must share one declared facts root")
    profile = frozen["plan"]["observation_profile"]
    expected_latency_sha = profile["measured_latency_sha256"]
    if expected_latency_sha is None:
        if measured_latency_path is not None:
            raise ValueError("F04 cannot add a latency profile to the frozen input")
        latency = None
    else:
        if measured_latency_path is None:
            raise ValueError("F04 execution-market measured latency file is required")
        latency = Path(measured_latency_path).resolve(strict=True)
        if _sha(latency) != expected_latency_sha:
            raise ValueError("F04 execution-market measured latency identity changed")
    if output.exists() or list(output.parent.glob(output.name + ".*.part")):
        raise FileExistsError("F04 relocated eval input already exists or has an incomplete stage")
    manifest = deepcopy(frozen)
    for row, path in zip(manifest["source_bundles"], roots, strict=True):
        row["path"] = str(path)
    manifest["plan"]["facts_root"] = str(roots[0].parent)
    manifest["plan"]["observation_profile"]["measured_latency_path"] = (
        str(latency) if latency is not None else None)
    manifest["relocated_from_manifest_sha256"] = expected
    manifest["relocation_previous_parent_sha256"] = original_sha
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.with_name(output.name + "." + uuid4().hex + ".part")
    stage.mkdir(mode=0o700)
    try:
        for spec in frozen_bundle.manifest["files"].values():
            source = frozen_root / spec["file"]
            os.link(source, stage / spec["file"])
        save_private_json(stage / "manifest.json", manifest)
        actual = _sha(stage / "manifest.json")
        verify_manifest_relocation(frozen_manifest, stage / "manifest.json",
                                   original_sha256=expected, relocated_sha256=actual)
        rebound = ConsumerBundle(stage)
        rebound.source_paths()
        for name in rebound.manifest["files"]:
            next(rebound.batches(name, batch_size=1), None)
        os.rename(stage, output)
        return {"schema": "narrowgate.f04.eval_locator_only_relocation.v1",
                "original_manifest_sha256": original_sha,
                "frozen_manifest_sha256": expected,
                "research_host_manifest_sha256": actual,
                "source_fact_manifest_sha256": required,
                "measured_latency_sha256": expected_latency_sha,
                "execution_parquet_hardlinks": len(rebound.manifest["files"])}
    except BaseException as exc:
        save_private_json(stage / "failure.json", {"type": type(exc).__name__, "message": str(exc)})
        raise


def verified_funding(plan: dict, funding_root: Path) -> dict:
    """Keep the original observed millisecond settlement clock, never slot-round it."""
    start = pd.Timestamp(plan["evaluation"]["account_start_utc"]).value
    end = pd.Timestamp(plan["evaluation"]["account_end_exclusive_utc"]).value
    expected_files = plan["economic_environment"]["funding_day_sha256"]
    events, identities = [], []
    for day, expected_sha in expected_files.items():
        path = funding_root / f"{day}.parquet"
        if _sha(path) != expected_sha:
            raise ValueError(f"F04 funding identity changed: {day}")
        identities.append(expected_sha)
        for row in pd.read_parquet(path).to_dict("records"):
            if row.get("symbol") != "BTCUSDC":
                raise ValueError("F04 funding symbol mismatch")
            settlement = int(row["fundingTime"]) * 1_000_000
            if start < settlement <= end:
                rate, mark = float(row["fundingRate"]), float(row["markPrice"])
                if not math.isfinite(rate) or not math.isfinite(mark) or mark <= 0:
                    raise ValueError("nonfinite F04 funding event")
                events.append({"settlement_ns": settlement, "rate": rate, "mark_price": mark})
    events.sort(key=lambda row: row["settlement_ns"])
    clocks = [row["settlement_ns"] for row in events]
    slots = [value // (8 * 3600 * 10**9) * (8 * 3600 * 10**9) for value in clocks]
    if (not clocks or len(clocks) != len(set(clocks)) or len(slots) != len(set(slots))
            or not set(range(start + 8 * 3600 * 10**9, end, 8 * 3600 * 10**9)) <= set(slots)
            or not set(slots) <= set(range(start, end + 1, 8 * 3600 * 10**9))):
        raise ValueError("F04 observed funding schedule is incomplete")
    return {"market_id": MARKET, "coverage_start_ns": start, "coverage_end_ns": end,
            "source_identity": identities, "events": events,
            "expected_settlements_ns": clocks}


def _common_params(plan: dict, config_path: Path, timing_path: Path) -> dict:
    if (_sha(config_path) != plan["economic_environment"]["config_sha256"]
            or _sha(timing_path) != plan["economic_environment"]["runtime_timing_sha256"]):
        raise ValueError("F04 economic configuration or runtime timing identity changed")
    from models.backtest_config import load_tick_base_params
    from models.backtest_tick import _configured_cooldown_evaluator
    from research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit import (
        _apply_runtime_timing_samples,
    )

    params = load_tick_base_params(symbol="BTCUSDC", config_path=config_path,
                                   include_queue_calibration=False)
    config = json.loads(config_path.read_text())
    for key in ("tick_size", "lot_size"):
        value = float(config[key])
        if not 0 < value < float("inf"):
            raise ValueError(f"invalid {key}")
        params[key] = value
    for key in ("boolean_cooldown_policy_enabled", "buy_e3_cooldown_policy_enabled"):
        params[key] = False
    _apply_runtime_timing_samples(params, timing_path,
        effective_time_assumption="exchange_event_proxy",
        bulk_cancel_model="matched_risk_case", private_fill_model="observed_callback")
    params.update(public_fill_volume_policy="all_public_volume_eligible",
                  exchange_book_queue_mode="diagnostic", replay_event_clock="merged",
                  replay_clock_interval_ms=100, replay_purpose="diagnostic",
                  account_start_ns=pd.Timestamp(plan["evaluation"]["account_start_utc"]).value,
                  cross_market_enabled=False, collect_curves=False,
                  trace_fills_max=10_000_000, trace_decisions_max=10_000_000,
                  trace_quotes_max=10_000_000,
                  replay_initial_state_mode="fresh_start", cold_flat_clock_start=True,
                  record_utc_accounting=True,
                  rng_seed=plan["economic_environment"]["rng_seed"],
                  latency_seed=plan["economic_environment"]["latency_seed"])
    _configured_cooldown_evaluator(params)
    return params


def run_one_arm(*, arm: str, plan_path: Path, execution_root: Path,
                relocation_manifest: Path | None, reference_root: Path | None,
                original_execution_manifest: Path | None,
                pair_model_root: Path, frozen_f03_root: Path, config_path: Path,
                timing_path: Path, funding_root: Path, output: Path,
                source_archive: Path | None = None) -> dict:
    """Run one independently initialized full account and publish only complete output."""
    daylight_budget_seconds()
    plan, plan_sha = _plan(plan_path)
    if arm not in {"M0", "M1"}:
        raise ValueError("one F04 M0 or M1 arm required")
    if output.exists():
        raise FileExistsError("F04 economic arm is create-only; verify its existing receipt")
    if list(output.parent.glob(output.name + ".*.part")):
        raise FileExistsError("incomplete F04 economic arm exists; inspect before retry")
    source_sha = _sha(source_archive) if source_archive is not None else None
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.with_name(output.name + "." + uuid4().hex + ".part")
    stage.mkdir(mode=0o700)
    try:
        bundle = _evaluation_bundle(execution_root, plan, relocation_manifest)
        model = DirectionModel(pair_model_root, arm, plan_path=plan_path)
        if model.manifest.get("source_archive_sha256") != source_sha:
            raise ValueError("F04 economic source snapshot differs from fitted model")
        if arm == "M1" and reference_root is None:
            raise ValueError("M1 requires its independently accepted reference input")
        if arm == "M0" and reference_root is not None:
            raise ValueError("M0 must not consume a reference input")
        engine = TardisDirectionSignalEngine(bundle.root, reference_root, frozen_f03_root,
                                             model, relocation_manifest=relocation_manifest,
                                             original_execution_manifest=original_execution_manifest)
        funding = verified_funding(plan, funding_root)
        params = _common_params(plan, config_path, timing_path)
        from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs
        from models.replay.public_accounting import settle_public_replay

        prepared = prepare_public_inputs(bundle.root, tick_size=params["tick_size"])
        result = simulate_prepared_inputs(prepared, params, signal_engine=engine)
        account = settle_public_replay(bundle.root, result,
            initial_capital=plan["economic_environment"]["initial_capital_usdc"],
            max_mark_age_ns=plan["economic_environment"]["max_terminal_mark_age_ns"],
            funding=funding)
        if not account["economic_complete"] or account["all_in_net_pnl"] is None:
            raise ValueError("F04 arm lacks a complete cash/funding/MTM account")
        for name in ("fills", "order_outcomes", "routed_decisions"):
            if result["_trace_coverage"][name]["unrecorded"]:
                raise ValueError(f"F04 {name} trace was truncated")
        traces = {"fills.parquet": result["_fill_trace"],
                  "orders.parquet": result["_quote_trace"],
                  "decisions.parquet": result["_decision_trace"]}
        for name, rows in traces.items():
            pd.DataFrame(rows).to_parquet(stage / name, compression="zstd", index=False)
        save_private_json(stage / "utc-equity-marks.json", result["_utc_accounting_marks"])
        receipt = {"schema": "narrowgate.f04.first_pair_economic_arm.v1", "arm": arm,
                   "plan_sha256": plan_sha, "model_manifest_sha256": _sha(pair_model_root / "manifest.json"),
                   "source_archive_sha256": source_sha,
                   "execution_manifest_sha256": _sha(bundle.root / "manifest.json"),
                   "reference_manifest_sha256": _sha(reference_root / "manifest.json")
                   if reference_root is not None else None,
                   "frozen_f03_manifest_sha256": _sha(frozen_f03_root / "public_input_model.json"),
                   "config_sha256": _sha(config_path), "timing_sha256": _sha(timing_path),
                   "funding_source_identity": funding["source_identity"],
                   "input": result["public_input_contract"], "accounting": account,
                   "signal": engine.report(), "trace_coverage": result["_trace_coverage"],
                   "trace_files": {name: {"sha256": _sha(stage / name), "rows": len(rows)}
                                   for name, rows in traces.items()},
                   "utc_equity_marks_sha256": _sha(stage / "utc-equity-marks.json"),
                   "interpretation": "modeled_delivery_and_public_volume_eligibility_not_live_parity"}
        save_private_json(stage / "accounting.json", receipt)
        os.rename(stage, output)
        return receipt
    except BaseException as exc:
        # An expensive failed run is evidence, not disposable scratch space.
        save_private_json(stage / "failure.json", {"type": type(exc).__name__, "message": str(exc)})
        raise


def main() -> None:
    if sys.argv[1:2] == ["relocate"]:
        seconds = daylight_budget_seconds()
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(
            TimeoutError("F04 locator verification reached Shanghai compute cutoff")))
        signal.setitimer(signal.ITIMER_REAL, max(1, seconds - 60))
        locator = argparse.ArgumentParser(description="Verify create-only F04 eval locator relocation")
        for name in ("plan", "original-manifest", "frozen-root", "mapping", "output"):
            locator.add_argument("--" + name, type=Path, required=True)
        locator.add_argument("--measured-latency", type=Path)
        paths = locator.parse_args(sys.argv[2:])
        receipt = relocate_evaluation_bundle(
            plan_path=paths.plan, original_manifest=paths.original_manifest,
            frozen_root=paths.frozen_root,
            fact_paths_by_sha=json.loads(paths.mapping.read_text()),
            measured_latency_path=paths.measured_latency, output=paths.output)
        print(json.dumps(receipt, sort_keys=True))
        return
    parser = argparse.ArgumentParser(description="Bounded F04 one-arm independent economic replay")
    for name in ("plan", "execution-root", "pair-model-root", "frozen-f03-root",
                 "config", "timing", "funding-root", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--reference-root", type=Path)
    parser.add_argument("--relocation-manifest", type=Path)
    parser.add_argument("--original-execution-manifest", type=Path)
    parser.add_argument("--arm", choices=("M0", "M1"), required=True)
    parser.add_argument("--source-archive", type=Path, required=True)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not args.worker:
        seconds = daylight_budget_seconds()
        command = [sys.executable, "-m", MODULE, *sys.argv[1:], "--worker"]
        try:
            subprocess.run(command, check=True, timeout=max(1, seconds - 60))
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError("F04 worker stopped before the Shanghai 22:00 compute boundary") from exc
        return
    seconds = daylight_budget_seconds()
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(
        TimeoutError("F04 worker reached Shanghai compute cutoff")))
    signal.setitimer(signal.ITIMER_REAL, max(1, seconds - 60))
    receipt = run_one_arm(arm=args.arm, plan_path=args.plan, execution_root=args.execution_root,
                          relocation_manifest=args.relocation_manifest,
                          reference_root=args.reference_root,
                          original_execution_manifest=args.original_execution_manifest,
                          pair_model_root=args.pair_model_root,
                          frozen_f03_root=args.frozen_f03_root, config_path=args.config,
                          timing_path=args.timing, funding_root=args.funding_root,
                          output=args.output, source_archive=args.source_archive)
    print(json.dumps({"arm": args.arm, "output": str(args.output),
                      "net_pnl": receipt["accounting"]["all_in_net_pnl"]}, sort_keys=True))


if __name__ == "__main__":
    main()
