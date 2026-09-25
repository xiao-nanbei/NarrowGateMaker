"""Training-only P3 from bound new-source facts, never legacy model metadata."""

import argparse
from dataclasses import asdict, replace
from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np

from data.facts import _digest, save_private_json
from data.runtime import ObservationProfile, PublicInputStream, bundle_paths
from data.tardis_input import CONTRACT
from research.families.f02_empirical_p3_touch.audit.p3_touch_calibration import (
    public_window_reaches, survival_curve,
)
from research.families.f02_empirical_p3_touch.touch_probability import TouchProbabilityModel
from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS


def validate_plan(plan):
    if plan.get("source_profile") != "tardis_only" or plan.get("symbol") != "BTCUSDC":
        raise ValueError("new BTCUSDC-only source contract required")
    if plan.get("fit_days") != list(TRAIN_DAYS):
        raise ValueError("P3 must use exactly the predeclared training days, not B/F")
    if plan.get("tick_size", 0.1) != 0.1:
        raise ValueError("P3 distance grid requires the declared 0.1 USDC quote tick")
    profile = ObservationProfile(**plan["observation_profile"])
    if profile.trade_coverage != "observed":
        raise ValueError("explicit observed-trade interpretation required")
    return profile


def collect_day(plan, day, output, *, facts_root=None, latency_path=None):
    profile = validate_plan(plan)
    if latency_path is not None:
        profile = replace(profile, measured_latency_path=str(latency_path))
    if day not in TRAIN_DAYS:
        raise ValueError("day outside training support")
    output = Path(output)
    if output.exists():
        raise FileExistsError("P3 day already exists; verify its identity before reuse")
    root = Path(facts_root or plan["facts_root"])
    previous = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    first = max(previous, TRAIN_DAYS[0])
    bundles = bundle_paths(root, start=first, end=day, relocated=facts_root is not None)
    # Prior file establishes source state; minute warmup establishes delivery.
    import pandas as pd
    start = int(pd.Timestamp(day, tz="UTC").value)
    end = start + 86_400_000_000_000
    warmup = start - 60_000_000_000 if first < day else start
    stream = PublicInputStream(bundles, profile=profile, start_ns=warmup, end_ns=end,
        market_id="binance_futures:perpetual:BTCUSDC", input_contract_id=CONTRACT)
    reach = public_window_reaches(stream, start_ns=start, end_ns=end)
    # JSON null represents no observed touch, not unknown coverage.
    result = {"schema": "p3.public_input_day.v1", "visibility": "local_only_do_not_publish",
              "day": day, "plan": plan, "profile": asdict(profile),
              "source_bundles": [{"day": p.name, "manifest_sha256": _digest(p / "manifest.json")}
                                 for p in bundles],
              "BUY": [float(x) if np.isfinite(x) else None for x in reach["BUY"]],
              "SELL": [float(x) if np.isfinite(x) else None for x in reach["SELL"]],
              "decision_ns": reach["decision_ns"].tolist(),
              "actual_outcome_end_ns": reach["actual_outcome_end_ns"].tolist(),
              "calendar_windows": reach["calendar_windows"],
              "unsupported_or_boundary_windows": reach["unsupported_or_boundary_windows"],
              "stats": stream.stats, "interpretation": reach["interpretation"]}
    save_private_json(output, result)
    return result


def fit(plan, daily_root, output):
    validate_plan(plan)
    output = Path(output)
    if output.exists():
        raise FileExistsError("frozen calibration cannot be overwritten")
    values, inputs = [], []
    for day in TRAIN_DAYS:
        path = Path(daily_root) / (day + ".json")
        row = json.loads(path.read_text())
        if row.get("schema") != "p3.public_input_day.v1" or row.get("day") != day or row.get("plan") != plan:
            raise ValueError("P3 input day/plan identity mismatch")
        if len(row["BUY"]) != len(row["SELL"]) or len(row["BUY"]) != len(row["decision_ns"]):
            raise ValueError("P3 opportunity denominator mismatch")
        import pandas as pd
        boundary = int(pd.Timestamp(day, tz="UTC").value) + 86_400_000_000_000
        if len(row["actual_outcome_end_ns"]) != len(row["decision_ns"]) or any(
            end != start + 10_000_000_000 or end >= boundary
            for start, end in zip(row["decision_ns"], row["actual_outcome_end_ns"], strict=True)
        ):
            raise ValueError("P3 outcome crosses training day boundary")
        for side in ("BUY", "SELL"):
            values.extend(-np.inf if x is None else float(x) for x in row[side])
        from research.families.f03_causal_13_head.public_input_panel import delivery_identity
        if delivery_identity(row["profile"]) != delivery_identity(plan["observation_profile"]):
            raise ValueError("P3 observed delivery profile differs from plan")
        expected = bundle_paths(Path(plan["facts_root"]),
            start=max(TRAIN_DAYS[0], (date.fromisoformat(day)-timedelta(days=1)).isoformat()), end=day)
        sources = [{"day": p.name, "manifest_sha256": _digest(p / "manifest.json")} for p in expected]
        if row.get("source_bundles") != sources:
            raise ValueError("P3 daily source binding mismatch")
        inputs.append({"day": day, "sha256": _digest(path), "windows_per_side": len(row["BUY"]),
                       "source_bundles": sources})
    if not values:
        raise ValueError("no supported training opportunities")
    grid = np.arange(0.1, 120.05, 0.1)
    curve = survival_curve(np.asarray(values), grid)
    metadata = {"event_type": "touch", "horizon_s": 10.0,
                "distance_unit": "USDC_per_BTC", "queue_included": False, "quote_tick_size": 0.1,
                "distance_origin": "same_side_best_bid_or_ask_at_window_start",
                "touch_source": "new_source_trades_against_delivered_BBO",
                "fit_days": list(TRAIN_DAYS), "validation_days": [], "test_days": [],
                "plan": plan, "daily_inputs": inputs,
                "native_observation_parity": "not_proven"}
    model = TouchProbabilityModel(model_type="empirical_survival", delta_grid=grid.tolist(),
        probability_grid=curve.tolist(), schema_version="narrowgate_p3_touch_calibration.v3",
        metadata=metadata)
    metadata["delta_star"] = model.distance_touch_product_argmax(delta_max=120.0)
    metadata["kappa_eff"] = model.touch_log_probability_distance_slope(metadata["delta_star"])
    output.parent.mkdir(parents=True, exist_ok=True)
    model.save(output)
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--day")
    mode.add_argument("--days", nargs="+")
    mode.add_argument("--fit-daily-root", type=Path)
    parser.add_argument("--facts-root", type=Path, help="Explicit byte-identical relocated fact calendar")
    parser.add_argument("--latency-profile", type=Path, help="Relocated measured profile; frozen checksum still applies")
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    if args.day:
        collect_day(plan, args.day, args.output, facts_root=args.facts_root, latency_path=args.latency_profile)
    elif args.days:
        from concurrent.futures import ProcessPoolExecutor
        if args.workers < 1 or len(set(args.days)) != len(args.days):
            parser.error("positive workers and unique assigned days required")
        if any(day not in TRAIN_DAYS for day in args.days):
            parser.error("assignment includes a non-training day")
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            jobs = {day: pool.submit(collect_day, plan, day, args.output/(day+".json"),
                facts_root=args.facts_root, latency_path=args.latency_profile) for day in args.days}
            for day, job in jobs.items():
                row = job.result()
                print(json.dumps({"day": day, "status": "completed", "windows_per_side": len(row["BUY"])}), flush=True)
    else:
        fit(plan, args.fit_daily_root, args.output)


if __name__ == "__main__":
    main()
