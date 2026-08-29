#!/usr/bin/env python3
"""Freeze empirical REST new/cancel latency for formal tick replay."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SCHEMA_VERSION = "narrowgate_rest_latency_calibration.v1"


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
        "sample_semantics": "per-telemetry-row REST sum/count average; max and replace-round sum retained as stress modes",
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
