#!/usr/bin/env python3
"""Summarize historical REST row aggregates, not exchange-effective delays.

The exported avg/max/sum distributions preserve their original aggregation
semantics. They cannot recover individual request tails, private visibility,
or the execution environment of an asynchronous order gateway.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "narrowgate_rest_latency_calibration.v1"


def load_runtime_timing_samples(
    path: Path, *, effective_time_assumption: str, bulk_cancel_model: str = "unmodeled",
    private_fill_model: str = "unmodeled",
) -> dict[str, Any]:
    """Load observed HTTP/private pairs with an explicit effective-time model.

    This does not identify matching-engine effective time, model gateway
    failures, or turn decision-weighted snapshots into message-delay samples.
    No observations are clipped, independently resampled, or silently dropped.
    """
    assumptions = {
        "dispatch": "dispatch-time lower bound (zero), not observed effectiveness",
        "exchange_event_proxy": "private exchange-event timestamp proxy, not exact effectiveness",
        "observable_upper_bound": "minimum of observed private visibility and HTTP return",
    }
    if effective_time_assumption not in assumptions:
        raise ValueError(f"effective_time_assumption must be one of {tuple(assumptions)}")
    if bulk_cancel_model not in {"unmodeled", "matched_risk_case"}:
        raise ValueError("bulk_cancel_model must be unmodeled or matched_risk_case")
    if private_fill_model not in {"unmodeled", "observed_callback"}:
        raise ValueError("private_fill_model must be unmodeled or observed_callback")
    source = Path(path).expanduser().resolve()
    raw = source.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.get("schema") != (
        "narrowgate_private_empirical_timing_samples.v1"
    ):
        raise ValueError("unsupported runtime timing sample schema")
    gateway = payload.get("gateway")
    if not isinstance(gateway, dict):
        raise ValueError("runtime timing samples require a gateway object")
    columns = gateway.get("columns")
    required = {
        "outcome", "execution_status", "dispatch_to_http_response_ms",
        "dispatch_to_private_ack_ms", "private_exchange_event_minus_dispatch_ms",
    }
    if (
        not isinstance(columns, list)
        or not all(isinstance(name, str) for name in columns)
        or len(columns) != len(set(columns))
        or not required.issubset(columns)
    ):
        raise ValueError("runtime timing gateway columns are missing or duplicated")
    operations = gateway.get("operations")
    coverage = gateway.get("coverage")
    if not isinstance(operations, dict) or set(operations) != {"order.place", "order.cancel"}:
        raise ValueError("runtime timing samples require exactly order.place and order.cancel")
    if not isinstance(coverage, dict):
        raise ValueError("runtime timing samples require gateway coverage")

    def observed(value: Any, *, field: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise ValueError(f"{field} must be an observed finite nonnegative number")
        return float(value)

    samples: dict[str, np.ndarray] = {}
    for method, operation in (("order.place", "new"), ("order.cancel", "cancel")):
        rows = operations[method]
        counts = coverage.get(method)
        if not isinstance(rows, list) or not rows:
            raise ValueError(f"{method} requires nonempty observed request rows")
        if not isinstance(counts, dict) or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counts.values()
        ):
            raise ValueError(f"{method} coverage must contain nonnegative integer counts")
        if any(
            value and (
                (name.startswith("outcome:") and name != "outcome:successes")
                or (name.startswith("execution_status:") and name != (
                    "execution_status:authoritative_success"
                ))
            )
            for name, value in counts.items()
        ):
            raise ValueError(f"{method} coverage contains unsupported failure/reject/UNKNOWN")
        if any(counts.get(name) != len(rows) for name in (
            "unique_completed_attempts", "matched_private_ack",
        )) or any(counts.get(name, 0) != 0 for name in (
            "duplicate_completion_rows_excluded", "unmatched_private_ack",
            "excluded_non_rest", "excluded_no_dispatch_in_window",
            "http_interval_missing_or_invalid",
        )):
            raise ValueError(f"{method} has incomplete or excluded same-request timing pairs")
        triples = []
        for index, row in enumerate(rows):
            if not isinstance(row, list) or len(row) != len(columns):
                raise ValueError(f"{method} row {index} does not match gateway columns")
            fields = dict(zip(columns, row, strict=True))
            if fields["outcome"] != "successes" or fields["execution_status"] != (
                "authoritative_success"
            ):
                raise ValueError(
                    f"{method} row {index} contains failure/reject/UNKNOWN; "
                    "a successful-request latency model cannot simulate that outcome"
                )
            http = observed(fields["dispatch_to_http_response_ms"], field=f"{method} HTTP")
            private = observed(fields["dispatch_to_private_ack_ms"], field=f"{method} private ACK")
            if effective_time_assumption == "dispatch":
                effective = 0.0
            elif effective_time_assumption == "observable_upper_bound":
                effective = min(private, http)
            else:
                effective = observed(
                    fields["private_exchange_event_minus_dispatch_ms"],
                    field=f"{method} exchange_event_proxy row {index}",
                )
            if effective > min(private, http):
                raise ValueError(
                    f"{method} row {index} effective-time assumption exceeds observed "
                    "HTTP/private bound; no clipping or row exclusion is permitted"
                )
            triples.append((effective, private, http))
        samples[operation] = np.ascontiguousarray(triples, dtype=np.float64)

    # Completion success alone does not identify the status carried by HTTP.
    # Only a complete status count over these same requests supports an early
    # local ACK when a validated RESULT arrives before the private callback.
    http_statuses: dict[str, str] = {}
    status_counts = payload.get("http_result_status_counts")
    if status_counts is not None:
        if not isinstance(status_counts, dict) or not status_counts.get("source"):
            raise ValueError("HTTP RESULT status counts require their source")
        for method, operation, expected in (
            ("order.place", "new", "NEW"), ("order.cancel", "cancel", "CANCELED"),
        ):
            counts = status_counts.get(method)
            if (
                not isinstance(counts, dict) or set(counts) != {expected}
                or isinstance(counts[expected], bool)
                or not isinstance(counts[expected], int)
                or counts[expected] != len(samples[operation])
            ):
                raise ValueError("HTTP RESULT status counts must cover every paired request")
            http_statuses[operation] = expected

    bulk_params: dict[str, Any] = {}
    bulk_metadata: dict[str, Any] = {
        "mode": bulk_cancel_model,
        "consumed_by_replay": False,
        "observed_sample_count": 0,
        "limitations": [
            "Bulk safety cancellation is unmodeled; unmatched shutdown HTTP "
            "is not a terminal sample."
        ],
    }
    if bulk_cancel_model == "matched_risk_case":
        case = payload.get("bulk_cancel_matched_risk_case")
        if (
            not isinstance(case, dict)
            or not isinstance(case.get("source"), str) or not case["source"].strip()
            or type(case.get("sample_count")) is not int or case["sample_count"] != 1
            or type(case.get("target_count")) is not int or case["target_count"] != 1
        ):
            raise ValueError("matched_risk_case requires one identified single-target risk case")
        bulk_http = observed(case.get("http_return_ms"), field="bulk cancel HTTP")
        bulk_private = observed(case.get("private_visibility_ms"), field="bulk cancel private ACK")
        if effective_time_assumption == "dispatch":
            bulk_effective = 0.0
        elif effective_time_assumption == "observable_upper_bound":
            bulk_effective = min(bulk_private, bulk_http)
        else:
            bulk_effective = observed(
                case.get("exchange_event_proxy_ms"), field="bulk cancel exchange_event_proxy"
            )
        if bulk_effective > min(bulk_private, bulk_http):
            raise ValueError(
                "bulk cancel effective-time assumption exceeds observed HTTP/private bound"
            )
        bulk_semantics = (
            "one matched non-shutdown single-target risk case (n=1), not a stable distribution; "
            f"effective_time_assumption={effective_time_assumption}; "
            "shared effective/private/HTTP phases for all targets in each simulated batch "
            "are a modeling assumption, not observed multi-target timing; "
            "unmatched shutdown HTTP samples are not used"
        )
        bulk_params = {
            "_bulk_cancel_timing_samples_ms": np.ascontiguousarray(
                [[bulk_effective, bulk_private, bulk_http]], dtype=np.float64
            ),
            "_bulk_cancel_timing_sample_semantics": bulk_semantics,
        }
        bulk_metadata = {
            "mode": bulk_cancel_model,
            "consumed_by_replay": True,
            "source": case["source"],
            "observed_sample_count": 1,
            "observed_target_count": 1,
            "shared_phases_for_all_targets": "modeling_assumption",
            "effective_time_assumption": effective_time_assumption,
            "limitations": [bulk_semantics],
        }

    compute = payload.get("compute", {})
    if not isinstance(compute, dict):
        raise ValueError("runtime compute metadata must be an object")
    compute_columns = compute.get("columns", [])
    compute_rows = compute.get("rows", [])
    if (
        not isinstance(compute_columns, list)
        or not all(isinstance(name, str) for name in compute_columns)
        or len(compute_columns) != len(set(compute_columns))
        or "signal_path" not in compute_columns
        or not isinstance(compute_rows, list)
    ):
        raise ValueError("runtime compute metadata requires columns including signal_path")
    path_index = compute_columns.index("signal_path")
    by_path: dict[str, list[list[Any]]] = {}
    for row in compute_rows:
        if not isinstance(row, list) or len(row) != len(compute_columns):
            raise ValueError("runtime compute metadata row does not match its columns")
        if not isinstance(row[path_index], str):
            raise ValueError("runtime compute metadata signal_path must be explicit")
        by_path.setdefault(row[path_index], []).append(row)
    semantics = (
        "same-request observed dispatch-to-private-ACK and dispatch-to-HTTP-return; "
        f"effective_time_assumption={effective_time_assumption}: "
        f"{assumptions[effective_time_assumption]}; no tail clipping"
    )
    private_fill_params: dict[str, Any] = {}
    private_fill_metadata: dict[str, Any] = {
        "mode": private_fill_model,
        "consumed_by_replay": False,
        "observed_sample_count": 0,
        "limitations": [
            "Private-fill callback visibility is unmodeled; no zero-delay observation is implied."
        ],
    }
    if private_fill_model == "observed_callback":
        values = gateway.get("private_fill_exchange_event_to_callback_ms")
        if not isinstance(values, list) or not values:
            raise ValueError("observed_callback requires nonempty private-fill callback samples")
        fill_samples = np.ascontiguousarray([
            observed(value, field=f"private-fill callback sample {index}")
            for index, value in enumerate(values)
        ], dtype=np.float64)
        private_fill_params = {"_private_fill_visibility_latency_samples_ms": fill_samples}
        private_fill_metadata = {
            "mode": private_fill_model,
            "consumed_by_replay": True,
            "observed_sample_count": int(fill_samples.size),
            "population": "observed exchange-event-to-private-callback intervals",
            "limitations": [
                "Private-fill visibility uses observed exchange-event-to-callback intervals; "
                "the exchange timestamp is a match-time proxy, not an independently measured "
                "matching-engine clock. No tail clipping or missing-value substitution.",
                "Observed callback samples do not prove complete fill coverage, a stable "
                "long-run tail, or correlation with individual NEW/CANCEL request samples.",
            ],
        }
    return {
        "params": {
            "replay_purpose": "diagnostic",
            "replay_event_clock": "merged",
            "replay_main_loop_sleep_ms": 100,
            "order_transport": "rest",
            "async_order_lanes_enabled": True,
            "cross_side_order_lanes_enabled": False,
            "rest_gateway_timing_mode": "sampled_async_fifo",
            "rest_gateway_timing_profile_path": "",
            "latency_baseline_clip_quantile": 1.0,
            "latency_jitter_ms": 0.0,
            "_serial_rest_return_samples_by_operation": samples,
            "_serial_rest_return_sample_semantics": semantics,
            "_serial_rest_http_result_status_by_operation": http_statuses,
            **bulk_params,
            **private_fill_params,
        },
        "calibration": {
            "source": {"path": str(source), "sha256": hashlib.sha256(raw).hexdigest()},
            "source_release": payload.get("source_release"),
            "source_commit": payload.get("source_commit"),
            "window": payload.get("window"),
            "sample_counts": {name: len(rows) for name, rows in samples.items()},
            "effective_time_assumption": effective_time_assumption,
            "semantics": semantics,
            "gateway_coverage": coverage,
            "http_result_status_counts": status_counts,
            "bulk_cancel_observations": payload.get("bulk_cancel_http_observations"),
            "bulk_cancel_model": bulk_metadata,
            "private_fill_model": private_fill_metadata,
            "compute": {
                "columns": compute_columns,
                "by_signal_path": by_path,
                "consumed_by_replay": False,
            },
            "snapshot_population": payload.get("snapshots", {}).get("population"),
            "limitations": [
                *payload.get("limitations", []),
                *bulk_metadata["limitations"],
                *private_fill_metadata["limitations"],
                "Compute paths remain metadata; no measured compute samples are injected.",
                "Decision-to-dispatch includes FIFO waiting and is not a compute sample.",
                "Snapshot source lag/total age is not injected as per-message delay.",
                "This loader does not establish full live/replay or economic equivalence.",
            ],
        },
    }


def runtime_compute_overrides(
    calibration: dict[str, Any], *, initial_bucket_end_ms: int, clock: str,
    bucket_ms: int = 10_000,
) -> dict[str, Any]:
    """Adapt already-read stage rows without re-reading the evidence file.

    Signal/sync precede snapshot; quote math precedes enqueue. The remaining
    local work is placed after enqueue as an explicit approximation because
    the telemetry does not identify individual admission offsets.
    """
    compute = calibration["compute"]
    columns = compute["columns"]
    required = {"sync_check_ms", "signal_compute_ms", "compute_quotes_ms", "requote_total_ms"}
    if not required.issubset(columns):
        raise ValueError(
            "phase-conditioned compute requires measured total/sync/signal/quote stages"
        )
    paths = compute["by_signal_path"]
    if set(paths) != {"cached_no_new_bucket", "new_bucket", "catch_up"}:
        raise ValueError("phase-conditioned compute requires all three explicit signal paths")
    if type(initial_bucket_end_ms) is not int or type(bucket_ms) is not int or bucket_ms <= 0:
        raise ValueError("compute bucket duration and initial watermark must be explicit integers")
    if initial_bucket_end_ms % bucket_ms:
        raise ValueError("compute initial watermark must align with the bucket grid")
    if clock not in {"prediction_delivery", "source_time_assumption"}:
        raise ValueError(
            "compute clock must explicitly identify delivery or source-time assumption"
        )
    samples = {}
    for path, rows in paths.items():
        if not rows:
            raise ValueError("phase-conditioned compute cannot use an empty path")
        triples = []
        for row in rows:
            fields = dict(zip(columns, row, strict=True))
            values = [fields[key] for key in sorted(required)]
            if any(
                isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0 for value in values
            ):
                raise ValueError("compute stages must be observed finite nonnegative numbers")
            pre = fields["sync_check_ms"] + fields["signal_compute_ms"]
            enqueue = pre + fields["compute_quotes_ms"]
            tail = fields["requote_total_ms"] - enqueue
            if tail < 0:
                raise ValueError("measured compute stages exceed total; cannot clip the residual")
            triples.append((pre, enqueue, tail))
        samples[path] = np.ascontiguousarray(triples, dtype=np.float64)
    return {
        "_runtime_compute_samples_by_path": samples,
        "runtime_compute_bucket_ms": bucket_ms,
        "runtime_compute_initial_bucket_end_ms": initial_bucket_end_ms,
        "runtime_compute_clock": clock,
        "_runtime_compute_sample_semantics": (
            "same-requote paired sync+signal before snapshot, then quote math before enqueue; "
            "remaining measured local time placed after enqueue, not exact admission gaps; "
            f"stratum follows {clock} bucket progress, not empirical path frequencies; "
            "catchup pools observed catchup counts; successful normal requotes only, "
            "not safety/closing calls; no tail clipping; short-pilot distribution"
        ),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _samples(frame: pd.DataFrame, prefix: str, mode: str = "avg") -> np.ndarray:
    counts = pd.to_numeric(frame[f"{prefix}_count"], errors="coerce")
    sums = pd.to_numeric(frame[f"{prefix}_sum_us"], errors="coerce")
    maxima = pd.to_numeric(frame[f"{prefix}_max_us"], errors="coerce")
    if mode == "max":
        values = maxima
    elif mode == "sum":
        values = sums
    else:
        values = sums / counts.replace(0, np.nan)
    out = values.replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=np.float64)
    return out[out > 0.0] / 1000.0


def _summary(values: np.ndarray) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64)
    if not values.size:
        return {"count": 0}
    return {
        "count": int(values.size),
        "mean_ms": float(np.mean(values)),
        "p50_ms": float(np.quantile(values, 0.50)),
        "p95_ms": float(np.quantile(values, 0.95)),
        "p99_ms": float(np.quantile(values, 0.99)),
        "p999_ms": float(np.quantile(values, 0.999)),
        "max_ms": float(np.max(values)),
    }


def calibrate(args: argparse.Namespace) -> dict[str, Any]:
    source = args.telemetry.expanduser().resolve()
    frame = pd.read_csv(source)
    frame["timestamp"] = pd.to_datetime(
        pd.to_numeric(frame["timestamp"], errors="coerce"), unit="s", utc=True
    )
    frame = frame.dropna(subset=["timestamp"])
    complete = frame.loc[
        (frame["timestamp"] >= pd.Timestamp(args.start_day, tz="UTC"))
        & (frame["timestamp"] < pd.Timestamp(args.end_day, tz="UTC") + pd.Timedelta(days=1))
    ].copy()
    if complete.empty:
        raise ValueError("latency fit interval has no telemetry rows")
    latest = frame["timestamp"].max()
    recent = frame.loc[frame["timestamp"] >= latest - pd.Timedelta(hours=args.recent_hours)]

    args.replay_telemetry.parent.mkdir(parents=True, exist_ok=True)
    serial = complete.copy()
    serial["timestamp"] = serial["timestamp"].astype("int64") / 1e9
    serial.to_csv(
        args.replay_telemetry,
        index=False,
        compression={"method": "gzip", "compresslevel": 6, "mtime": 0},
    )
    modes: dict[str, Any] = {}
    recent_modes: dict[str, Any] = {}
    for mode in ("avg", "max", "sum"):
        modes[mode] = {
            "new": _summary(_samples(complete, "rest_new", mode)),
            "cancel": _summary(_samples(complete, "rest_cancel", mode)),
        }
        recent_modes[mode] = {
            "new": _summary(_samples(recent, "rest_new", mode)),
            "cancel": _summary(_samples(recent, "rest_cancel", mode)),
        }
    report = {
        "schema_version": SCHEMA_VERSION,
        "profile_id": args.profile_id,
        "environment": {
            "region": args.region,
            "instance": args.instance,
            "os": args.os_label,
            "cpu": args.cpu_label,
            "memory": args.memory_label,
            "config_sha256": args.config_sha256,
        },
        "fit_interval": {
            "start_utc": complete["timestamp"].min().isoformat(),
            "end_utc": complete["timestamp"].max().isoformat(),
            "rows": int(len(complete)),
            "start_day": args.start_day,
            "end_day": args.end_day,
        },
        "recent_diagnostic": {
            "hours": float(args.recent_hours),
            "start_utc": recent["timestamp"].min().isoformat(),
            "end_utc": recent["timestamp"].max().isoformat(),
            "rows": int(len(recent)),
        },
        "sample_semantics": (
            "per-telemetry-row REST sum/count average; "
            "max and replace-round sum retained as stress modes"
        ),
        "fit_distributions": modes,
        "recent_distributions": recent_modes,
        "source": {"path": str(source), "sha256": _sha256(source)},
        "replay_telemetry": {
            "path": str(args.replay_telemetry.resolve()),
            "sha256": _sha256(args.replay_telemetry),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--telemetry", type=Path, required=True)
    parser.add_argument("--replay-telemetry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day", required=True)
    parser.add_argument("--recent-hours", type=float, default=3.0)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--os-label", required=True)
    parser.add_argument("--cpu-label", required=True)
    parser.add_argument("--memory-label", required=True)
    parser.add_argument("--config-sha256", required=True)
    args = parser.parse_args()
    report = calibrate(args)
    print(json.dumps({
        "profile_id": report["profile_id"],
        "fit_interval": report["fit_interval"],
        "avg": report["fit_distributions"]["avg"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
