#!/usr/bin/env python3
"""Build the frozen 40-day split-source feature panel with visible progress."""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_finalizer as finalizer,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel as panel,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "owner_modeled_queue_feature_panel_batch.v1"
)
PROGRESS_HEARTBEAT_S = 30.0
DEFAULT_M0_ROOT = panel.DEFAULT_DATA_ROOT / (
    "reports/causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "owner_modeled_queue_m0_panel_v1"
)


class ModeledFeaturePanelBatchError(RuntimeError):
    """Raised when a bounded feature-panel batch cannot complete."""


def _write_progress(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}"
    with temporary.open("w", encoding="ascii") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _validate_or_missing(output_root: Path, day: str) -> bool:
    destination = output_root / day
    if not destination.exists():
        return False
    finalizer._read_day(output_root, day)
    return True


def _staging_telemetry(output_root: Path, days: Sequence[str]) -> dict[str, Any]:
    telemetry: dict[str, Any] = {}
    for day in days:
        matches = tuple(output_root.glob(f".{day}.staging-*"))
        if len(matches) > 1:
            raise ModeledFeaturePanelBatchError(
                f"multiple staging directories exist for {day}: {matches}"
            )
        if not matches:
            telemetry[day] = {"staging_present": False, "staging_bytes": 0}
            continue
        staging = matches[0]
        telemetry[day] = {
            "staging_present": True,
            "staging_bytes": int(
                sum(
                    path.stat().st_size
                    for path in staging.rglob("*")
                    if path.is_file()
                )
            ),
            "staging_name": staging.name,
        }
    return telemetry


def _build_one(
    day: str,
    output_root: Path,
    m0_root: Path,
    normalized_execution_plan: Path,
    native_observation_cache_root: Path,
) -> dict[str, Any]:
    enrichment, manifest = panel.resolve_m0_enrichment_day(m0_root, day)
    return panel.build_day_from_native_sources(
        day,
        output_root=output_root,
        normalized_execution_plan=normalized_execution_plan,
        native_observation_cache_root=native_observation_cache_root,
        m0_enrichment_path=enrichment,
        m0_enrichment_manifest=manifest,
    )


def run_batch(
    *,
    days: Sequence[str],
    workers: int,
    output_root: Path,
    m0_root: Path,
    normalized_execution_plan: Path,
    native_observation_cache_root: Path,
    progress_path: Path,
) -> dict[str, Any]:
    if workers < 1:
        raise ModeledFeaturePanelBatchError("workers must be positive")
    ordered_days = tuple(dict.fromkeys(str(day) for day in days))
    unknown = sorted(set(ordered_days) - set(panel.PREFIX40_DAYS))
    if unknown:
        raise ModeledFeaturePanelBatchError(f"days outside frozen prefix40: {unknown}")
    output_root = output_root.expanduser().resolve()
    completed = [day for day in ordered_days if _validate_or_missing(output_root, day)]
    pending = [day for day in ordered_days if day not in completed]
    started = time.monotonic()
    state: dict[str, Any] = {
        "identity": IDENTITY,
        "status": "running" if pending else "complete",
        "workers": int(workers),
        "ordered_days": list(ordered_days),
        "pending": list(pending),
        "running": [],
        "completed": list(completed),
        "failed": {},
        "updated_at_utc": datetime.now(UTC).isoformat(),
    }
    _write_progress(progress_path, state)
    if not pending:
        return state

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures: dict[Any, str] = {}
        queue = list(pending)
        while queue or futures:
            while queue and len(futures) < workers:
                day = queue.pop(0)
                if day in panel.M2_COMMON_SUPPORT_DAYS:
                    cache_day = native_observation_cache_root / day
                    if not cache_day.is_dir():
                        raise ModeledFeaturePanelBatchError(
                            f"raw-native observation cache is not admitted for {day}"
                        )
                futures[
                    executor.submit(
                        _build_one,
                        day,
                        output_root,
                        m0_root,
                        normalized_execution_plan,
                        native_observation_cache_root,
                    )
                ] = day
            state["pending"] = list(queue)
            state["running"] = sorted(futures.values())
            state["running_telemetry"] = _staging_telemetry(
                output_root, state["running"]
            )
            state["elapsed_s"] = time.monotonic() - started
            state["updated_at_utc"] = datetime.now(UTC).isoformat()
            _write_progress(progress_path, state)
            done, _ = wait(
                tuple(futures),
                timeout=PROGRESS_HEARTBEAT_S,
                return_when=FIRST_COMPLETED,
            )
            if not done:
                continue
            for future in done:
                day = futures.pop(future)
                try:
                    manifest = future.result()
                    finalizer._read_day(output_root, day)
                except BaseException as exc:
                    state["failed"][day] = f"{type(exc).__name__}: {exc}"
                    state["status"] = "failed"
                    state["running"] = sorted(futures.values())
                    state["pending"] = list(queue)
                    state["elapsed_s"] = time.monotonic() - started
                    state["updated_at_utc"] = datetime.now(UTC).isoformat()
                    _write_progress(progress_path, state)
                    for active in futures:
                        active.cancel()
                    raise ModeledFeaturePanelBatchError(
                        f"feature-panel build failed for {day}"
                    ) from exc
                state["completed"].append(day)
                print(
                    json.dumps(
                        {
                            "utc_day": day,
                            "opportunity_count": manifest["opportunity_count"],
                            "manifest_sha256": manifest["canonical_manifest_sha256"],
                            "completed": len(state["completed"]),
                            "total": len(ordered_days),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

    state["status"] = "complete"
    state["pending"] = []
    state["running"] = []
    state["running_telemetry"] = {}
    state["completed"] = [day for day in ordered_days if day in state["completed"]]
    state["elapsed_s"] = time.monotonic() - started
    state["updated_at_utc"] = datetime.now(UTC).isoformat()
    _write_progress(progress_path, state)
    return state


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", nargs="*", default=panel.PREFIX40_DAYS)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--output-root", type=Path, default=panel.DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--m0-root", type=Path, default=DEFAULT_M0_ROOT)
    parser.add_argument(
        "--normalized-execution-plan",
        type=Path,
        default=panel.DEFAULT_NORMALIZED_EXECUTION_PLAN,
    )
    parser.add_argument(
        "--native-observation-cache-root",
        type=Path,
        default=panel.DEFAULT_NATIVE_OBSERVATION_CACHE,
    )
    parser.add_argument("--progress-path", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    progress_path = args.progress_path or (args.output_root / "_batch_progress.json")
    result = run_batch(
        days=tuple(args.days),
        workers=args.workers,
        output_root=args.output_root,
        m0_root=args.m0_root,
        normalized_execution_plan=args.normalized_execution_plan,
        native_observation_cache_root=args.native_observation_cache_root,
        progress_path=progress_path,
    )
    print(json.dumps(result, sort_keys=True, allow_nan=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
