#!/usr/bin/env python3
"""Run the 50-day live-held BER baseline with strict native queue and latency."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import shutil
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import data_root, native_exchange_book_cache_root, resolve_portable_path
from models import backtest_tick as bt
from models.backtest_config import (
    add_queue_calibration_params,
    validate_formal_replay_calibration,
)
from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as parent,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "btc_usdc_current_live_held_ber_strict_native_latency_baseline_50d_v1_20260810"
SPEC_LOCATOR = (
    "${NARROWGATE_PRIVATE_RESEARCH_ROOT}/"
    "current_live_held_ber_strict_native_latency_baseline_50d_v1_spec_20260810.json"
)
DEFAULT_OUTPUT = (
    data_root(ROOT)
    / "reports/current_live_held_ber_strict_native_latency_baseline_50d_v1_20260810"
)
DEFAULT_NATIVE_CACHE = native_exchange_book_cache_root(ROOT)
DAY_SUCCESS = "_SUCCESS"
PANEL_SUCCESS = "_PANEL_SUCCESS"

NATIVE_COUNTER_FIELDS = (
    "exchange_book_queue_lookup_count",
    "exchange_book_queue_exact_count",
    "exchange_book_queue_known_zero_count",
    "exchange_book_queue_missing_count",
    "exchange_book_queue_invalidated_order_count",
    "exchange_book_queue_cancel_ahead_event_count",
    "exchange_book_queue_cancel_ahead_qty",
    "exchange_book_queue_ambiguous_event_count",
    "exchange_book_cancel_trade_ambiguous_order_count",
    "exchange_book_cancel_book_ambiguous_order_count",
    "exchange_book_events_consumed",
    "exchange_book_events_accepted",
    "exchange_book_events_rejected",
    "exchange_book_source_gap_events",
    "exchange_book_invalid_sequence_messages",
    "exchange_book_snapshot_events",
    "exchange_book_delta_events",
    "exchange_book_delta_bootstrap_events",
    "exchange_book_sequence_gaps",
    "exchange_book_message_time_reversals",
    "exchange_book_transaction_timestamp_events",
    "exchange_book_event_timestamp_fallback_events",
    "exchange_book_receive_timestamp_fallback_events",
    "exchange_book_unknown_timestamp_source_events",
)

LATENCY_FIELDS = (
    "new_order_latency_ms",
    "cancel_order_latency_ms",
    "latency_jitter_ms",
    "new_order_latency_sample_count",
    "cancel_order_latency_sample_count",
    "exec_book_visibility_delay_enabled",
    "exec_book_visibility_delay_sample_count",
    "exec_book_visibility_delay_applied_count",
    "exec_book_visibility_delay_applied_avg_ms",
    "exec_book_visibility_delay_applied_max_ms",
    "exec_depth_visibility_delay_applied_avg_ms",
    "exec_depth_visibility_delay_applied_max_ms",
    "exec_trade_visibility_delay_applied_avg_ms",
    "exec_trade_visibility_delay_applied_max_ms",
    "exec_book_visibility_paired_hit_count",
    "exec_book_visibility_paired_miss_count",
    "latency_sampler_version",
    "latency_profile_id",
    "latency_scenario",
    "latency_seed",
)


class StrictNativeLatencyError(RuntimeError):
    """Raised when strict queue or latency evidence is incomplete."""


def _spec_path() -> Path:
    try:
        return resolve_portable_path(SPEC_LOCATOR, root=ROOT)
    except (RuntimeError, ValueError) as exc:
        raise StrictNativeLatencyError(
            "strict-native latency spec requires NARROWGATE_PRIVATE_RESEARCH_ROOT"
        ) from exc


def _spec() -> dict[str, Any]:
    payload = parent._load_json(_spec_path(), role="strict-native latency spec")
    if payload.get("identity") != IDENTITY:
        raise StrictNativeLatencyError("strict-native latency identity drifted")
    parent_path = parent._resolve_repo_path(payload["parent_diagnostic"]["path"])
    parent._validate_file(
        parent_path,
        payload["parent_diagnostic"]["sha256"],
        role="parent 50-day diagnostic",
    )
    parent_spec = parent._spec()
    if len(parent.ordered_days(parent_spec)) != int(payload["parent_diagnostic"]["days"]):
        raise StrictNativeLatencyError("parent 50-day denominator drifted")
    queue = payload["queue_calibration"]
    parent._validate_file(
        parent._resolve_repo_path(str(queue["path"])),
        str(queue["sha256"]),
        role="queue calibration v3",
    )
    return payload


def _profile_path(spec: Mapping[str, Any]) -> Path:
    visibility = spec["strategy_visibility"]
    gateway = spec["gateway_latency"]
    if visibility["profile_path"] != gateway["profile_path"]:
        raise StrictNativeLatencyError("visibility and gateway profiles diverged")
    if visibility["profile_sha256"] != gateway["profile_sha256"]:
        raise StrictNativeLatencyError("visibility and gateway profile hashes diverged")
    return parent._validate_file(
        parent._resolve_repo_path(str(visibility["profile_path"])),
        str(visibility["profile_sha256"]),
        role="AWS Tokyo latency profile",
    )


def _latency_inputs(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    path = _profile_path(spec)
    rest = bt._load_live_perf_latency_samples(
        path,
        mode=str(spec["gateway_latency"]["sample_mode"]),
    )
    visibility = bt._load_exec_book_visibility_profile(path)
    new_samples = np.ascontiguousarray(
        rest["new_order_latency_samples_ms"], dtype=np.float64
    )
    cancel_samples = np.ascontiguousarray(
        rest["cancel_order_latency_samples_ms"], dtype=np.float64
    )
    book_samples = np.ascontiguousarray(
        visibility["exec_book_visibility_delay_samples_ms"], dtype=np.float64
    )
    expected = spec["gateway_latency"]
    if len(new_samples) != int(expected["new_order_samples"]):
        raise StrictNativeLatencyError("new-order latency sample count drifted")
    if len(cancel_samples) != int(expected["cancel_order_samples"]):
        raise StrictNativeLatencyError("cancel latency sample count drifted")
    if len(book_samples) != int(spec["strategy_visibility"]["observed_visibility_rows"]):
        raise StrictNativeLatencyError("book visibility sample count drifted")
    params = {
        "_new_order_latency_samples_ms": new_samples,
        "_cancel_order_latency_samples_ms": cancel_samples,
        "_exec_book_visibility_delay_samples_ms": book_samples,
        "live_perf_latency_mode": str(expected["sample_mode"]),
        "latency_profile_id": "aws_tokyo_live_perf_20260715_full_avg_v1",
        "latency_environment": str(spec["strategy_visibility"]["environment"]),
        "latency_scenario": "baseline",
        "latency_seed": int(expected["latency_seed"]),
        "exec_book_visibility_mode": str(spec["strategy_visibility"]["mode"]),
        "exec_book_visibility_delay_profile_id": (
            "aws_tokyo_live_perf_20260715_full_visibility_age_v1"
        ),
        "exec_book_visibility_delay_profile_path": str(path),
        "exec_book_visibility_delay_seed": int(
            spec["strategy_visibility"]["visibility_seed"]
        ),
    }
    audit = {
        "profile_path": str(path),
        "profile_sha256": parent._sha256_file(path),
        "new_order_samples": len(new_samples),
        "cancel_order_samples": len(cancel_samples),
        "visibility_samples": len(book_samples),
        "visibility_mean_ms": float(np.mean(book_samples)),
        "visibility_p50_ms": float(np.quantile(book_samples, 0.50)),
        "visibility_p90_ms": float(np.quantile(book_samples, 0.90)),
        "visibility_p99_ms": float(np.quantile(book_samples, 0.99)),
    }
    return params, audit


def _strict_base(spec: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    parent_spec = parent._spec()
    params, projection_audit = parent._base_params(parent_spec)
    latency_params, latency_audit = _latency_inputs(spec)
    params.update(latency_params)
    params.update(
        {
            "replay_purpose": "formal",
            "replay_event_clock": str(spec["policy_clock"]["mode"]),
            "replay_clock_interval_ms": int(
                spec["policy_clock"]["nominal_interval_ms"]
            ),
            "execution_trade_source": "individual_trades",
            "exchange_book_queue_mode": "strict",
            "queue_l2_cancel_ahead_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
        }
    )
    queue = spec["queue_calibration"]
    add_queue_calibration_params(
        params,
        symbol="BTCUSDC",
        strict=True,
        path=parent._resolve_repo_path(str(queue["path"])),
    )
    if params.get("queue_calibration_schema_version") != queue["schema_version"]:
        raise StrictNativeLatencyError("queue calibration schema drifted")
    if params.get("queue_calibration_apply_mode") != queue["apply_mode"]:
        raise StrictNativeLatencyError("queue calibration apply mode drifted")
    if params.get("queue_calibration_fit_days") != queue["fit_days"]:
        raise StrictNativeLatencyError("queue calibration fit days drifted")
    if params.get("queue_calibration_replay_params") != queue["replay_params"]:
        raise StrictNativeLatencyError("queue calibration replay parameters drifted")
    validate_formal_replay_calibration(params, require_latency=True)
    return params, {
        "offline_projection": projection_audit,
        "latency": latency_audit,
    }


def _native_tape(
    spec: Mapping[str, Any],
    *,
    day: str,
    cache_dir: Path,
) -> CryptoHFTExchangeBookTape:
    truth = spec["exchange_truth"]
    return CryptoHFTExchangeBookTape(
        raw_root=parent._resolve_repo_path(str(truth["raw_root"])),
        day=day,
        symbol=str(truth["symbol"]),
        exchange=str(truth["exchange"]),
        tick_size=0.1,
        warmup_hours=int(truth["warmup_hours"]),
        strict_complete=True,
        cache_dir=cache_dir,
        cache_enabled=True,
    )


def _execution_fields(result: Mapping[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "exchange_book_queue_mode": str(result.get("exchange_book_queue_mode", "")),
        "exchange_book_queue_scope": str(result.get("exchange_book_queue_scope", "")),
        "book_source_authority": str(result.get("book_source_authority", "")),
        "book_exact_queue_policy_eligible": bool(
            result.get("book_exact_queue_policy_eligible", False)
        ),
    }
    for key in NATIVE_COUNTER_FIELDS:
        value = result.get(key, 0)
        fields[key] = float(value) if key.endswith("_qty") else int(value or 0)
    for key in LATENCY_FIELDS:
        value = result.get(key)
        if isinstance(value, (bool, np.bool_)):
            fields[key] = bool(value)
        elif isinstance(value, (int, np.integer)):
            fields[key] = int(value)
        elif isinstance(value, (float, np.floating)):
            fields[key] = float(value)
        else:
            fields[key] = value
    return fields


def _validate_execution(summary: Mapping[str, Any]) -> None:
    if summary.get("exchange_book_queue_mode") != "strict":
        raise StrictNativeLatencyError("exchange-book queue mode is not strict")
    if summary.get("exchange_book_queue_scope") != (
        "strategy_independent_native_snapshot_delta_exchange_time_v1"
    ):
        raise StrictNativeLatencyError("exchange-book queue scope drifted")
    if int(summary.get("exchange_book_events_consumed", 0)) <= 0:
        raise StrictNativeLatencyError("no native exchange-book events were consumed")
    if int(summary.get("exchange_book_events_accepted", 0)) <= 0:
        raise StrictNativeLatencyError("no native exchange-book events were accepted")
    lookups = int(summary.get("exchange_book_queue_lookup_count", 0))
    accounted = sum(
        int(summary.get(key, 0))
        for key in (
            "exchange_book_queue_exact_count",
            "exchange_book_queue_known_zero_count",
            "exchange_book_queue_missing_count",
        )
    )
    if lookups <= 0 or lookups != accounted:
        raise StrictNativeLatencyError("native queue lookup accounting failed")
    zero_fields = (
        "exchange_book_queue_missing_count",
        "exchange_book_source_gap_events",
        "exchange_book_invalid_sequence_messages",
        "exchange_book_sequence_gaps",
        "exchange_book_message_time_reversals",
    )
    for key in zero_fields:
        if int(summary.get(key, -1)) != 0:
            raise StrictNativeLatencyError(f"strict native gate failed: {key}")
    for key in (
        "new_order_latency_sample_count",
        "cancel_order_latency_sample_count",
        "exec_book_visibility_delay_sample_count",
        "exec_book_visibility_delay_applied_count",
    ):
        if int(summary.get(key, 0)) <= 0:
            raise StrictNativeLatencyError(f"latency gate failed: {key}")
    if not bool(summary.get("exec_book_visibility_delay_enabled", False)):
        raise StrictNativeLatencyError("execution-book visibility delay is disabled")


def preflight(*, native_cache: Path = DEFAULT_NATIVE_CACHE) -> dict[str, Any]:
    spec = _spec()
    parent_spec = parent._spec()
    days = parent.ordered_days(parent_spec)
    _, latency_audit = _latency_inputs(spec)
    source_hours = 0
    for day in days:
        tape = _native_tape(spec, day=day, cache_dir=native_cache)
        source_hours += len(tape.source_paths)
    return {
        "identity": IDENTITY,
        "passed": True,
        "days": len(days),
        "strict_complete_days": len(days),
        "target_plus_warmup_source_hours": source_hours,
        "native_cache": str(native_cache),
        "latency": latency_audit,
        "exact_historical_receive_time_authority": False,
    }


def _day_dir(output: Path, day: str) -> Path:
    return output / "days" / day


def _load_day(output: Path, day: str) -> dict[str, Any] | None:
    directory = _day_dir(output, day)
    manifest_path = directory / "manifest.json"
    marker = directory / DAY_SUCCESS
    if not manifest_path.is_file() or not marker.is_file():
        return None
    if marker.read_text(encoding="ascii").strip() != parent._sha256_file(manifest_path):
        raise StrictNativeLatencyError(f"{day} admission marker drifted")
    manifest = parent._load_json(manifest_path, role=f"{day} strict-native manifest")
    for name in ("summary", "campaigns", "fills"):
        row = manifest[name]
        parent._validate_file(Path(row["path"]), row["sha256"], role=f"{day} {name}")
    return manifest


def execute_day(
    day: str,
    *,
    output: Path = DEFAULT_OUTPUT,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
) -> dict[str, Any]:
    existing = _load_day(output, day)
    if existing is not None:
        return {"day": day, "reused": True}
    spec = _spec()
    parent_spec = parent._spec()
    days = parent.ordered_days(parent_spec)
    if day not in days:
        raise StrictNativeLatencyError(f"day is outside frozen 50-day panel: {day}")
    prepared = parent.prepare(parent.DEFAULT_CACHE)
    prefix = parent._prefix_plan(parent_spec)
    window, ml_data, binding = parent._load_day_inputs(
        day,
        spec=parent_spec,
        prefix_plan=prefix,
        prepared_plan=prepared,
        cache_root=parent.DEFAULT_CACHE,
    )
    params, audit = _strict_base(spec)
    tape = _native_tape(spec, day=day, cache_dir=native_cache)
    tape_identity = tape.identity(include_sha256=True)
    result = bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        params,
        ml_data=ml_data,
        bbo_data=window.bbo_data,
        l2_data=window.l2_data,
        var_ti=window.var_ti,
        var_retsq=window.var_retsq,
        exchange_book_event_tape=tape,
    )
    summary, campaigns, fills = parent.native_runner._project_arm(
        day=day,
        arm="current_live_held_global_ber_control",
        result=result,
        order_size=float(params["order_size"]),
        campaign_mae_trace_max=int(params["trace_campaign_repair_max"]),
    )
    summary.update(_execution_fields(result))
    summary.update(
        {
            "engine_evidence": "python_strict_native_latency",
            "exact_historical_receive_time_authority": False,
            "latency_profile_sha256": audit["latency"]["profile_sha256"],
        }
    )
    _validate_execution(summary)

    final = _day_dir(output, day)
    staging = final.parent / f".{day}.{uuid.uuid4().hex}.partial"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        summary_path = staging / "summary.json"
        campaigns_path = staging / "campaigns.parquet"
        fills_path = staging / "fills.parquet"
        parent._atomic_json(summary_path, summary)
        campaigns.to_parquet(campaigns_path, index=False, compression="zstd")
        fills.to_parquet(fills_path, index=False, compression="zstd")
        manifest = {
            "schema_version": f"{IDENTITY}.day.v1",
            "identity": IDENTITY,
            "day": day,
            "spec_sha256": parent._sha256_file(_spec_path()),
            "input_binding": binding,
            "native_tape_identity": tape_identity,
            "latency_audit": audit["latency"],
            "summary": {
                "path": str(final / "summary.json"),
                "sha256": parent._sha256_file(summary_path),
            },
            "campaigns": {
                "path": str(final / "campaigns.parquet"),
                "sha256": parent._sha256_file(campaigns_path),
            },
            "fills": {
                "path": str(final / "fills.parquet"),
                "sha256": parent._sha256_file(fills_path),
            },
        }
        parent._atomic_json(staging / "manifest.json", manifest)
        parent._atomic_text(
            staging / DAY_SUCCESS,
            parent._sha256_file(staging / "manifest.json") + "\n",
        )
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staging, final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return {"day": day, "reused": False}


def run(
    *,
    days: Sequence[str] | None,
    workers: int,
    output: Path = DEFAULT_OUTPUT,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
) -> list[dict[str, Any]]:
    spec = parent._spec()
    selected = list(days) if days else parent.ordered_days(spec)
    if workers <= 0:
        raise StrictNativeLatencyError("workers must be positive")
    started = time.monotonic()
    results: list[dict[str, Any]] = []

    def progress(result: Mapping[str, Any]) -> None:
        results.append(dict(result))
        count = len(results)
        elapsed = time.monotonic() - started
        eta = elapsed / count * (len(selected) - count) if count else math.nan
        print(
            f"strict-native-latency completed={count}/{len(selected)} "
            f"day={result['day']} elapsed_s={elapsed:.1f} eta_s={eta:.1f}",
            flush=True,
        )

    if workers == 1:
        for day in selected:
            progress(execute_day(day, output=output, native_cache=native_cache))
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    execute_day,
                    day,
                    output=output,
                    native_cache=native_cache,
                ): day
                for day in selected
            }
            for future in concurrent.futures.as_completed(futures):
                progress(future.result())
    return sorted(results, key=lambda row: str(row["day"]))


def finalize(*, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    spec = parent._spec()
    days = parent.ordered_days(spec)
    summaries: list[dict[str, Any]] = []
    campaigns: list[pd.DataFrame] = []
    fills: list[pd.DataFrame] = []
    for day in days:
        manifest = _load_day(output, day)
        if manifest is None:
            raise StrictNativeLatencyError(f"strict-native day is missing: {day}")
        summary = parent._load_json(Path(manifest["summary"]["path"]), role=f"{day} summary")
        _validate_execution(summary)
        summaries.append(summary)
        campaigns.append(pd.read_parquet(manifest["campaigns"]["path"]))
        fills.append(pd.read_parquet(manifest["fills"]["path"]))
    daily = pd.DataFrame(summaries).sort_values("day").reset_index(drop=True)
    campaign_frame = pd.concat(campaigns, ignore_index=True)
    fill_frame = pd.concat(fills, ignore_index=True)
    metrics = parent._section_metrics(days, daily, campaign_frame)
    report = {
        "schema_version": f"{IDENTITY}.report.v1",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "strict_native_latency_development_complete",
        "panel": {
            "days": len(days),
            "ordered_utc_days": days,
            "daily_fresh_start": True,
            "independent_confirmation": False,
        },
        "economics": metrics,
        "execution": {
            "exchange_book_queue_mode": "strict",
            "native_events_consumed": int(daily["exchange_book_events_consumed"].sum()),
            "queue_lookups": int(daily["exchange_book_queue_lookup_count"].sum()),
            "queue_missing": int(daily["exchange_book_queue_missing_count"].sum()),
            "new_order_latency_samples": int(
                daily["new_order_latency_sample_count"].min()
            ),
            "cancel_order_latency_samples": int(
                daily["cancel_order_latency_sample_count"].min()
            ),
            "visibility_samples": int(
                daily["exec_book_visibility_delay_sample_count"].min()
            ),
            "visibility_applied": int(
                daily["exec_book_visibility_delay_applied_count"].sum()
            ),
            "exact_historical_receive_time_authority": False,
        },
        "permissions": {
            "development_economics_read": True,
            "exact_live_transport_authority": False,
            "action_authority": False,
            "live_action_authority": False,
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    daily_path = output / "daily.parquet"
    campaigns_path = output / "campaigns.parquet"
    fills_path = output / "fills.parquet"
    report_path = output / "report.json"
    daily.to_parquet(daily_path, index=False, compression="zstd")
    campaign_frame.to_parquet(campaigns_path, index=False, compression="zstd")
    fill_frame.to_parquet(fills_path, index=False, compression="zstd")
    parent._atomic_json(report_path, report)
    manifest = {
        "schema_version": f"{IDENTITY}.manifest.v1",
        "identity": IDENTITY,
        "spec": {
            "path": SPEC_LOCATOR,
            "sha256": parent._sha256_file(_spec_path()),
        },
        "report": {"path": str(report_path), "sha256": parent._sha256_file(report_path)},
        "daily": {"path": str(daily_path), "sha256": parent._sha256_file(daily_path)},
        "campaigns": {
            "path": str(campaigns_path),
            "sha256": parent._sha256_file(campaigns_path),
        },
        "fills": {"path": str(fills_path), "sha256": parent._sha256_file(fills_path)},
        "runner": {
            "path": str(Path(__file__).resolve()),
            "sha256": parent._sha256_file(Path(__file__).resolve()),
        },
    }
    manifest_path = output / "manifest.json"
    parent._atomic_json(manifest_path, manifest)
    parent._atomic_text(
        output / PANEL_SUCCESS,
        parent._sha256_file(manifest_path) + "\n",
    )
    return report


def status(*, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    days = parent.ordered_days(parent._spec())
    completed = [day for day in days if _load_day(output, day) is not None]
    return {
        "identity": IDENTITY,
        "completed": len(completed),
        "total": len(days),
        "remaining": len(days) - len(completed),
        "completed_days": completed,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    preflight_parser = sub.add_parser("preflight")
    preflight_parser.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--days", nargs="*")
    run_parser.add_argument("--workers", type=int, default=1)
    run_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    run_parser.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    final_parser = sub.add_parser("finalize")
    final_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    status_parser = sub.add_parser("status")
    status_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.command == "preflight":
        payload = preflight(native_cache=args.native_cache)
    elif args.command == "run":
        payload = run(
            days=args.days,
            workers=args.workers,
            output=args.output,
            native_cache=args.native_cache,
        )
    elif args.command == "finalize":
        payload = finalize(output=args.output)
    elif args.command == "status":
        payload = status(output=args.output)
    else:
        raise AssertionError(args.command)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
