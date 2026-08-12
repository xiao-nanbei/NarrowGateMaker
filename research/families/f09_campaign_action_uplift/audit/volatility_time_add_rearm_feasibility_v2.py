#!/usr/bin/env python3
"""BBO-clock feasibility audit for volatility-time add rearm.

This Development-only identity replaces the sparse individual-trade clock
used by v1 with completed one-second executable-mid buckets built from the
frozen normalized 100ms BBO. It still forbids reward and action outcomes.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models.audit.experiment_manifest import git_workspace_identity
from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_feasibility as v1_contract,
)
from research.families.f09_campaign_action_uplift.audit.volatility_time_add_rearm_feasibility import (
    _previous_day,
    _samples_for_episode,
    _utc_day_bounds,
    build_fill_unit_episodes,
    canonical_sha256,
    freeze_reference_rates,
    load_fill_events,
    sha256_file,
    summarize_feasibility,
)
from strategy.fill_cooldown import (
    integrate_variance_time_episode,
    price_variance_to_bps2_rate,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "volatility_time_add_rearm_feasibility.v2_1"
FAMILY_ID = "volatility_time_add_rearm_feasibility_v2_1"
SIDES = ("BUY", "SELL")


def _bbo_path(root: Path, day: str) -> Path:
    return root / "bbo" / f"BTCUSDC-bbo-{day}.parquet"


def load_causal_bbo_variance_samples(
    normalized_l2_root: Path,
    day: str,
    *,
    rolling_window_s: int,
    max_bbo_source_age_ms: int,
    max_abs_return_bps_1s: float,
    ready_delay_ms: int,
) -> pd.DataFrame:
    """Build a strict completed-bucket variance clock from normalized BBO.

    A BBO event stamped exactly at a bucket end belongs to the next bucket.
    This prevents the event at ``t + 1s`` from entering the feature that first
    becomes available at ``t + 1s``.
    """

    start_ms, end_ms = _utc_day_bounds(day)
    warmup_ms = max(120_000, int(rolling_window_s) * 2_000)
    frames: list[pd.DataFrame] = []
    for source_day in (_previous_day(day), day):
        path = _bbo_path(normalized_l2_root, source_day)
        if not path.is_file():
            continue
        frame = pd.read_parquet(
            path,
            columns=["timestamp", "best_bid", "best_ask"],
        )
        frames.append(frame)
    if not frames:
        raise FileNotFoundError(f"no normalized BBO source for {day}")

    raw = pd.concat(frames, ignore_index=True)
    ts = pd.to_numeric(raw["timestamp"], errors="coerce").to_numpy(dtype=float)
    bid = pd.to_numeric(raw["best_bid"], errors="coerce").to_numpy(dtype=float)
    ask = pd.to_numeric(raw["best_ask"], errors="coerce").to_numpy(dtype=float)
    valid = (
        np.isfinite(ts)
        & np.isfinite(bid)
        & np.isfinite(ask)
        & (bid > 0.0)
        & (ask > bid)
    )
    source = pd.DataFrame(
        {
            "timestamp": ts[valid].astype(np.int64),
            "mid": 0.5 * (bid[valid] + ask[valid]),
        }
    )
    source = (
        source.sort_values("timestamp", kind="stable")
        .drop_duplicates("timestamp", keep="last")
    )
    source = source[
        (source["timestamp"] >= start_ms - warmup_ms)
        & (source["timestamp"] < end_ms)
    ]
    if source.empty:
        raise ValueError(f"normalized BBO does not overlap {day}")

    bucket_end_ms = np.arange(
        start_ms - warmup_ms + 1_000,
        end_ms + 1,
        1_000,
        dtype=np.int64,
    )
    source_ts = source["timestamp"].to_numpy(dtype=np.int64)
    source_mid = source["mid"].to_numpy(dtype=float)
    source_index = np.searchsorted(source_ts, bucket_end_ms, side="left") - 1
    has_source = source_index >= 0
    mid = np.full(bucket_end_ms.size, np.nan, dtype=float)
    selected_ts = np.full(
        bucket_end_ms.size,
        np.iinfo(np.int64).min,
        dtype=np.int64,
    )
    mid[has_source] = source_mid[source_index[has_source]]
    selected_ts[has_source] = source_ts[source_index[has_source]]
    source_age_ms = np.where(
        has_source,
        bucket_end_ms - selected_ts,
        np.iinfo(np.int64).max,
    )

    previous_mid = np.roll(mid, 1)
    previous_mid[0] = np.nan
    delta = mid - previous_mid
    abs_return_bps = np.abs(delta / previous_mid) * 10_000.0
    bucket_valid = (
        has_source
        & np.isfinite(mid)
        & np.isfinite(delta)
        & (source_age_ms >= 0)
        & (source_age_ms <= int(max_bbo_source_age_ms))
        & np.isfinite(abs_return_bps)
        & (abs_return_bps <= float(max_abs_return_bps_1s))
    )
    sigma_sq = (
        pd.Series(delta)
        .rolling(int(rolling_window_s), min_periods=int(rolling_window_s))
        .var(ddof=0)
        .to_numpy(dtype=float)
    )
    invalid_count = (
        pd.Series((~bucket_valid).astype(np.int16))
        .rolling(int(rolling_window_s), min_periods=int(rolling_window_s))
        .sum()
        .to_numpy(dtype=float)
    )
    sample_valid = (
        np.isfinite(sigma_sq)
        & np.isfinite(invalid_count)
        & (invalid_count == 0.0)
        & np.isfinite(mid)
        & (mid > 0.0)
    )
    output = pd.DataFrame(
        {
            "feature_ready_ts_ms": bucket_end_ms + int(ready_delay_ms),
            "price": mid,
            "sigma_sq_price_per_s": sigma_sq,
            "valid": sample_valid,
            "source_age_ms": source_age_ms,
            "bucket_end_ts_ms": bucket_end_ms,
        }
    )
    return output[
        (output["bucket_end_ts_ms"] >= start_ms)
        & (output["bucket_end_ts_ms"] <= end_ms)
    ].reset_index(drop=True)


def validate_bbo_identity(
    normalized_l2_root: Path,
    daily_quality_path: Path,
    days: list[str],
) -> list[dict[str, Any]]:
    quality = pd.read_csv(daily_quality_path, dtype={"day": str})
    required_columns = {
        "day",
        "formal_eligible",
        "bbo_sha256",
        "bbo_coverage",
        "bbo_p99_gap_s",
    }
    missing = sorted(required_columns - set(quality.columns))
    if missing:
        raise ValueError(f"daily quality is missing columns: {missing}")
    quality = quality.set_index("day", drop=False)
    identities: list[dict[str, Any]] = []
    for day in days:
        if day not in quality.index:
            raise ValueError(f"daily quality has no row for {day}")
        row = quality.loc[day]
        if not bool(row["formal_eligible"]):
            raise ValueError(f"normalized BBO day is not formal-eligible: {day}")
        path = _bbo_path(normalized_l2_root, day)
        expected = str(row["bbo_sha256"])
        actual = sha256_file(path)
        if actual != expected:
            raise ValueError(f"normalized BBO hash mismatch for {day}")
        identities.append(
            {
                "day": day,
                "path": str(path),
                "sha256": actual,
                "coverage": float(row["bbo_coverage"]),
                "p99_gap_s": float(row["bbo_p99_gap_s"]),
            }
        )
    return identities


def attach_reference_start_rates(
    episodes: pd.DataFrame,
    normalized_l2_root: Path,
    *,
    rolling_window_s: int,
    max_bbo_source_age_ms: int,
    max_abs_return_bps_1s: float,
    max_feature_age_ms: int,
    ready_delay_ms: int,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for day, daily in episodes.groupby("day", sort=True):
        variance = load_causal_bbo_variance_samples(
            normalized_l2_root,
            str(day),
            rolling_window_s=rolling_window_s,
            max_bbo_source_age_ms=max_bbo_source_age_ms,
            max_abs_return_bps_1s=max_abs_return_bps_1s,
            ready_delay_ms=ready_delay_ms,
        )
        ready = variance["feature_ready_ts_ms"].to_numpy(dtype=np.int64)
        rows = daily.copy()
        rates: list[float] = []
        valid_flags: list[bool] = []
        ready_values: list[int] = []
        for start in rows["episode_start_ts_ms"].astype(np.int64):
            index = int(np.searchsorted(ready, start, side="right")) - 1
            valid = False
            rate = np.nan
            ready_ts = -1
            if index >= 0:
                sample = variance.iloc[index]
                ready_ts = int(sample["feature_ready_ts_ms"])
                valid = bool(sample["valid"]) and start - ready_ts <= max_feature_age_ms
                if valid:
                    rate = price_variance_to_bps2_rate(
                        float(sample["sigma_sq_price_per_s"]),
                        float(sample["price"]),
                    )
                    valid = math.isfinite(rate) and rate >= 0.0
            rates.append(float(rate))
            valid_flags.append(bool(valid))
            ready_values.append(ready_ts)
        rows["start_variance_rate_bps2_per_s"] = rates
        rows["start_variance_valid"] = valid_flags
        rows["start_feature_ready_ts_ms"] = ready_values
        parts.append(rows)
    return pd.concat(parts, ignore_index=True) if parts else episodes.copy()


def measure_clock_coverage(
    samples: list[Any],
    *,
    start_ms: int,
    stop_ms: int,
    max_feature_age_ms: int,
) -> tuple[float, float]:
    """Measure valid and frozen clock time, clipped to the actual release."""

    start = int(start_ms)
    stop = max(start, int(stop_ms))
    max_age = max(0, int(max_feature_age_ms))
    ordered = sorted(samples, key=lambda row: int(row.feature_ready_ts_ms))
    valid_ms = 0.0
    stale_ms = 0.0
    covered_until = start
    for index, sample in enumerate(ordered):
        ready = int(sample.feature_ready_ts_ms)
        if ready >= stop:
            break
        interval_start = max(start, ready)
        next_ready = (
            int(ordered[index + 1].feature_ready_ts_ms)
            if index + 1 < len(ordered)
            else stop
        )
        interval_end = min(stop, next_ready)
        if interval_end <= interval_start:
            continue
        if interval_start > covered_until:
            stale_ms += float(interval_start - covered_until)
        fresh_end = min(interval_end, ready + max_age)
        valid_end = max(interval_start, fresh_end)
        interval_valid = (
            bool(sample.valid)
            and ready <= interval_start
            and math.isfinite(float(sample.mid_price))
            and float(sample.mid_price) > 0.0
            and math.isfinite(float(sample.sigma_sq_price_per_s))
            and float(sample.sigma_sq_price_per_s) >= 0.0
            and valid_end > interval_start
        )
        if interval_valid:
            valid_ms += float(valid_end - interval_start)
            stale_ms += float(interval_end - valid_end)
        else:
            stale_ms += float(interval_end - interval_start)
        covered_until = max(covered_until, interval_end)
    if covered_until < stop:
        stale_ms += float(stop - covered_until)
    exposure_ms = float(stop - start)
    if not math.isclose(valid_ms + stale_ms, exposure_ms, abs_tol=1e-6):
        raise AssertionError("variance-clock coverage does not partition exposure")
    return valid_ms, stale_ms


def evaluate_delay_scenario(
    episodes: pd.DataFrame,
    normalized_l2_root: Path,
    *,
    evaluation_days: list[str],
    reference_rates: dict[str, float],
    base_cooldown_s: float,
    minimum_wall_time_ms: int,
    maximum_wall_time_ms: int,
    max_feature_age_ms: int,
    rolling_window_s: int,
    max_bbo_source_age_ms: int,
    max_abs_return_bps_1s: float,
    ready_delay_ms: int,
    cpp_module: Any | None,
) -> pd.DataFrame:
    selected = episodes[episodes["day"].astype(str).isin(set(evaluation_days))]
    output: list[dict[str, Any]] = []
    for day, daily in selected.groupby("day", sort=True):
        variance = load_causal_bbo_variance_samples(
            normalized_l2_root,
            str(day),
            rolling_window_s=rolling_window_s,
            max_bbo_source_age_ms=max_bbo_source_age_ms,
            max_abs_return_bps_1s=max_abs_return_bps_1s,
            ready_delay_ms=ready_delay_ms,
        )
        ready = variance["feature_ready_ts_ms"].to_numpy(dtype=np.int64)
        for episode in daily.to_dict("records"):
            start = int(episode["episode_start_ts_ms"])
            censor = int(episode["censor_ts_ms"])
            units = max(1.0, float(episode["consecutive_same_side_fill_units"]))
            baseline_deadline = start + int(math.ceil(base_cooldown_s * units * 1_000.0))
            budget = float(reference_rates[str(episode["side"])]) * base_cooldown_s * units
            stop = min(censor, start + maximum_wall_time_ms)
            samples = _samples_for_episode(variance, start_ms=start, stop_ms=stop)
            result = integrate_variance_time_episode(
                samples,
                episode_start_ts_ms=start,
                budget_bps2=budget,
                minimum_wall_time_ms=minimum_wall_time_ms,
                maximum_wall_time_ms=maximum_wall_time_ms,
                max_feature_age_ms=max_feature_age_ms,
                censor_ts_ms=censor,
            )
            start_index = int(np.searchsorted(ready, start, side="right")) - 1
            start_valid = False
            start_ready_ts = -1
            if start_index >= 0:
                sample = variance.iloc[start_index]
                start_ready_ts = int(sample["feature_ready_ts_ms"])
                start_valid = bool(sample["valid"]) and start - start_ready_ts <= max_feature_age_ms

            candidate_end = int(result.rearm_ts_ms) if result.rearm_ts_ms is not None else censor
            baseline_end = min(baseline_deadline, censor)
            coverage_valid_ms, coverage_stale_ms = measure_clock_coverage(
                samples,
                start_ms=start,
                stop_ms=candidate_end,
                max_feature_age_ms=max_feature_age_ms,
            )
            cpp_match = True
            if cpp_module is not None:
                native = cpp_module.integrate_variance_time_episode(
                    np.asarray([row.feature_ready_ts_ms for row in samples], dtype=np.int64),
                    np.asarray([row.mid_price for row in samples], dtype=np.float64),
                    np.asarray([row.sigma_sq_price_per_s for row in samples], dtype=np.float64),
                    np.asarray([row.valid for row in samples], dtype=np.uint8),
                    start,
                    budget,
                    minimum_wall_time_ms,
                    maximum_wall_time_ms,
                    max_feature_age_ms,
                    censor,
                )
                cpp_match = bool(
                    native["reason"] == result.reason
                    and native["rearm_ts_ms"] == result.rearm_ts_ms
                    and math.isclose(
                        float(native["accumulated_qv_bps2"]),
                        result.accumulated_qv_bps2,
                        rel_tol=1e-10,
                        abs_tol=1e-10,
                    )
                    and math.isclose(
                        float(native["stale_frozen_ms"]),
                        result.stale_frozen_ms,
                        rel_tol=1e-10,
                        abs_tol=1e-10,
                    )
                )
            observed_ms = max(0.0, float(candidate_end - start))
            row = dict(episode)
            row.update(
                {
                    "ready_delay_ms": int(ready_delay_ms),
                    "start_variance_valid": bool(start_valid),
                    "start_feature_ready_ts_ms": int(start_ready_ts),
                    "reference_variance_rate_bps2_per_s": reference_rates[str(episode["side"])],
                    "variance_budget_bps2": budget,
                    "baseline_rearm_ts_ms": baseline_deadline,
                    "baseline_effective_end_ts_ms": baseline_end,
                    "candidate_rearm_ts_ms": result.rearm_ts_ms,
                    "candidate_effective_end_ts_ms": candidate_end,
                    "candidate_reason": result.reason,
                    "candidate_accumulated_qv_bps2": result.accumulated_qv_bps2,
                    "candidate_stale_frozen_ms": coverage_stale_ms,
                    "candidate_valid_interval_ms": coverage_valid_ms,
                    "integrator_raw_stale_frozen_ms": result.stale_frozen_ms,
                    "integrator_raw_valid_interval_ms": result.valid_interval_ms,
                    "candidate_observed_ms": observed_ms,
                    "candidate_valid_time_rate": coverage_valid_ms / max(observed_ms, 1.0),
                    "timing_delta_s": (candidate_end - baseline_end) / 1_000.0,
                    "cpp_variance_clock_match": cpp_match,
                }
            )
            output.append(row)
    return pd.DataFrame(output)


def summarize_v2(
    evaluated: pd.DataFrame,
    *,
    material_delta_s: float,
    gates: dict[str, float],
    blocker_contract: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    cells: list[pd.DataFrame] = []
    daily: list[pd.DataFrame] = []
    for delay, scenario in evaluated.groupby("ready_delay_ms", sort=True):
        scenario_cells, scenario_daily, _ = summarize_feasibility(
            scenario,
            material_delta_s=material_delta_s,
            gates=gates,
        )
        scenario_cells.insert(0, "ready_delay_ms", int(delay))
        scenario_daily.insert(0, "ready_delay_ms", int(delay))
        cells.append(scenario_cells)
        daily.append(scenario_daily)
    cells_frame = pd.concat(cells, ignore_index=True)
    daily_frame = pd.concat(daily, ignore_index=True)
    clock_passed = bool(
        len(cells_frame) == 2 * evaluated["ready_delay_ms"].nunique()
        and cells_frame["side_feasibility_pass"].all()
    )
    blocker_passed = bool(
        blocker_contract.get("buy_q90_replayed", False)
        and blocker_contract.get("consecutive_loss_cooldown_replayed", False)
        and blocker_contract.get("sync_degrade_event_semantics_frozen", False)
    )
    feasibility_passed = bool(clock_passed and blocker_passed)
    if not clock_passed:
        decision = "close_bbo_variance_clock_on_development_mechanics"
    elif not blocker_passed:
        decision = "clock_mechanics_supported_hold_action_for_blocker_parity"
    else:
        decision = "feasibility_pass_register_action_experiment_spec_only"
    return cells_frame, daily_frame, {
        "variance_clock_mechanics_passed": clock_passed,
        "current_live_blocker_parity_passed": blocker_passed,
        "feasibility_passed": feasibility_passed,
        "decision": decision,
        "reward_or_pnl_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_created": False,
        "action_or_live_authorization": False,
    }


def _load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected v2.1 feasibility spec schema")
    if payload.get("outcome_access", {}).get("reward_or_pnl_allowed") is not False:
        raise ValueError("v2.1 feasibility spec must forbid reward/PnL access")
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--fill-trace", type=Path, required=True)
    parser.add_argument("--normalized-l2-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--require-cpp", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    fill_path = args.fill_trace.expanduser().resolve()
    normalized_root = args.normalized_l2_root.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    implementation = spec["implementation_identity"]
    if sha256_file(Path(__file__).resolve()) != implementation["feasibility_runner_v2_sha256"]:
        raise ValueError("v2 feasibility implementation hash mismatch")
    if sha256_file(Path(v1_contract.__file__).resolve()) != implementation["frozen_v1_helper_sha256"]:
        raise ValueError("frozen v1 helper hash mismatch")
    implementation_paths = {
        "fill_cooldown_contract_sha256": ROOT / "strategy" / "fill_cooldown.py",
        "cpp_bindings_sha256": ROOT / "cpp" / "narrowgate_cpp" / "bindings.cpp",
        "cpp_tick_replay_sha256": ROOT / "cpp" / "narrowgate_cpp" / "tick_replay.cpp",
        "cpp_tick_replay_header_sha256": ROOT / "cpp" / "narrowgate_cpp" / "tick_replay.hpp",
    }
    for identity_key, identity_path in implementation_paths.items():
        if sha256_file(identity_path) != implementation[identity_key]:
            raise ValueError(f"implementation hash mismatch: {identity_path}")
    panels = spec["panels"]
    split_path = Path(panels["source_split_path"]).resolve()
    if sha256_file(split_path) != panels["source_split_sha256"]:
        raise ValueError("source split hash mismatch")
    development_days = list(panels["development_days"])
    reference_days = list(panels["reference_days"])
    evaluation_days = list(panels["evaluation_days"])
    if set(reference_days) & set(evaluation_days):
        raise ValueError("reference and evaluation days must be disjoint")
    if sorted(reference_days + evaluation_days) != sorted(development_days):
        raise ValueError("reference/evaluation days must partition Development")

    source = spec["source_identity"]
    if str(fill_path) != str(Path(source["expected_fill_trace_path"]).resolve()):
        raise ValueError("fill trace path differs from frozen v2.1 spec")
    if sha256_file(fill_path) != source["expected_fill_trace_sha256"]:
        raise ValueError("fill trace hash differs from frozen v2.1 spec")
    if str(normalized_root) != str(Path(source["normalized_l2_root"]).resolve()):
        raise ValueError("normalized L2 root differs from frozen v2.1 spec")
    quality_path = Path(source["normalized_l2_quality_path"]).resolve()
    if sha256_file(quality_path) != source["normalized_l2_quality_sha256"]:
        raise ValueError("normalized L2 daily-quality hash mismatch")
    blocker = spec["blocker_parity_contract"]
    blocker_path = Path(blocker["observational_diagnostic_path"]).resolve()
    if sha256_file(blocker_path) != blocker["observational_diagnostic_sha256"]:
        raise ValueError("blocker-attribution diagnostic hash mismatch")
    bbo_identities = validate_bbo_identity(
        normalized_root,
        quality_path,
        development_days,
    )

    events = load_fill_events(fill_path, development_days)
    mechanics = spec["mechanics"]
    episodes = build_fill_unit_episodes(
        events,
        order_size_btc=float(mechanics["order_size_btc"]),
        lot_size_btc=float(mechanics["lot_size_btc"]),
    )
    clock = spec["variance_clock"]
    reference_delay_ms = int(clock["reference_ready_delay_ms"])
    reference_episodes = attach_reference_start_rates(
        episodes,
        normalized_root,
        rolling_window_s=int(clock["rolling_window_s"]),
        max_bbo_source_age_ms=int(clock["max_bbo_source_age_ms"]),
        max_abs_return_bps_1s=float(clock["max_abs_return_bps_1s"]),
        max_feature_age_ms=int(clock["max_feature_age_ms"]),
        ready_delay_ms=reference_delay_ms,
    )
    reference_rates = freeze_reference_rates(reference_episodes, reference_days)

    cpp_module = None
    try:
        import narrowgate_cpp as cpp_module  # type: ignore
    except Exception:
        if args.require_cpp:
            raise
    if args.require_cpp and not hasattr(cpp_module, "integrate_variance_time_episode"):
        raise RuntimeError("narrowgate_cpp lacks the variance-time ABI")

    evaluated_parts = [
        evaluate_delay_scenario(
            episodes,
            normalized_root,
            evaluation_days=evaluation_days,
            reference_rates=reference_rates,
            base_cooldown_s=float(mechanics["base_cooldown_s"]),
            minimum_wall_time_ms=int(mechanics["minimum_wall_time_ms"]),
            maximum_wall_time_ms=int(mechanics["maximum_wall_time_ms"]),
            max_feature_age_ms=int(clock["max_feature_age_ms"]),
            rolling_window_s=int(clock["rolling_window_s"]),
            max_bbo_source_age_ms=int(clock["max_bbo_source_age_ms"]),
            max_abs_return_bps_1s=float(clock["max_abs_return_bps_1s"]),
            ready_delay_ms=int(delay),
            cpp_module=cpp_module,
        )
        for delay in clock["ready_delay_scenarios_ms"]
    ]
    evaluated = pd.concat(evaluated_parts, ignore_index=True)
    cells, daily, summary = summarize_v2(
        evaluated,
        material_delta_s=float(spec["gates"]["material_timing_delta_s"]),
        gates=spec["gates"],
        blocker_contract=spec["blocker_parity_contract"],
    )

    output.mkdir(parents=True, exist_ok=False)
    episodes_path = output / "development_episode_mechanics.parquet"
    evaluated_path = output / "development_evaluation_episodes.parquet"
    cells_path = output / "side_delay_gate_cells.csv"
    daily_path = output / "daily_timing_metrics.csv"
    report_path = output / "report.json"
    markdown_path = output / "report.md"
    reference_episodes.to_parquet(episodes_path, index=False, compression="zstd")
    evaluated.to_parquet(evaluated_path, index=False, compression="zstd")
    cells.to_csv(cells_path, index=False)
    daily.to_csv(daily_path, index=False)
    report = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "family_id": FAMILY_ID,
        **summary,
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "source_fill_trace": {
            "path": str(fill_path),
            "sha256": sha256_file(fill_path),
            "rows_loaded_without_outcomes": int(len(events)),
        },
        "panels": {
            "development_days": development_days,
            "reference_days": reference_days,
            "evaluation_days": evaluation_days,
            "validation_days_read": [],
            "sealed_holdout_days_read": [],
        },
        "reference_variance_rate_bps2_per_s": reference_rates,
        "ready_delay_scenarios_ms": list(clock["ready_delay_scenarios_ms"]),
        "blocker_parity_contract": spec["blocker_parity_contract"],
        "gates": spec["gates"],
        "side_delay_results": cells.to_dict("records"),
        "bbo_identities": bbo_identities,
        "artifacts": {
            "episode_mechanics": str(episodes_path),
            "evaluation_episodes": str(evaluated_path),
            "side_delay_gate_cells": str(cells_path),
            "daily_timing_metrics": str(daily_path),
        },
        "implementation": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
            "cpp_module_path": str(getattr(cpp_module, "__file__", "")),
            "cpp_module_sha256": (
                sha256_file(Path(cpp_module.__file__))
                if cpp_module is not None and getattr(cpp_module, "__file__", "")
                else ""
            ),
        },
        "workspace": git_workspace_identity(ROOT),
    }
    report["report_payload_sha256"] = canonical_sha256(report)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Volatility-Time Add Rearm Feasibility v2.1",
        "",
        "Development-only BBO-clock mechanics audit. No reward, PnL, markout, Validation, or holdout was read.",
        "",
        f"- decision: `{summary['decision']}`",
        f"- variance clock mechanics passed: `{summary['variance_clock_mechanics_passed']}`",
        f"- current-live blocker parity passed: `{summary['current_live_blocker_parity_passed']}`",
        f"- full feasibility passed: `{summary['feasibility_passed']}`",
        f"- reference variance: `{reference_rates}` bps^2/s",
        "",
        "## Side / Delay Gates",
        "",
        "```text",
        cells.to_string(index=False),
        "```",
        "",
        "Clock support alone does not authorize randomized replay or live deployment.",
    ]
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    manifest = {
        "schema_version": "volatility_time_add_rearm_feasibility_manifest.v2_1",
        "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        "artifacts": [
            {"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size}
            for path in (episodes_path, evaluated_path, cells_path, daily_path, markdown_path)
        ],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output_dir": str(output), **summary}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
