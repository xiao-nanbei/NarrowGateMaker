#!/usr/bin/env python3
"""Run exact daily native-observation cache builds with bounded parallelism."""

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
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel as panel,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_observation_cache as cache,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "raw_native_observation_cache_batch.v1"
)
PROGRESS_HEARTBEAT_S = 30.0


class NativeObservationBatchError(RuntimeError):
    """Raised when a bounded cache batch cannot finish atomically."""


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
    cache.validate_admitted_cache(output_root, day, deep=False)
    return True


def _staging_telemetry(output_root: Path, days: Sequence[str]) -> dict[str, Any]:
    """Report bounded write progress without treating staging as admission."""

    telemetry: dict[str, Any] = {}
    for day in days:
        matches = tuple(output_root.glob(f".{day}.staging-*"))
        if len(matches) > 1:
            raise NativeObservationBatchError(
                f"multiple staging directories exist for {day}: {matches}"
            )
        if not matches:
            telemetry[day] = {"staging_present": False, "staging_bytes": 0}
            continue
        staging = matches[0]
        size = sum(
            path.stat().st_size
            for path in staging.rglob("*")
            if path.is_file()
        )
        telemetry[day] = {
            "staging_present": True,
            "staging_bytes": int(size),
            "staging_name": staging.name,
        }
    return telemetry


def _build_one(
    day: str,
    output_root: Path,
    raw_native_root: Path,
    native_book_cache: Path,
    individual_trade_root: Path,
) -> dict[str, Any]:
    return cache.build_real_day_cache(
        day=day,
        output_root=output_root,
        raw_native_root=raw_native_root,
        native_book_cache=native_book_cache,
        individual_trade_root=individual_trade_root,
    )


def run_batch(
    *,
    days: Sequence[str],
    workers: int,
    output_root: Path,
    raw_native_root: Path,
    native_book_cache: Path,
    individual_trade_root: Path,
    progress_path: Path,
) -> dict[str, Any]:
    if workers < 1:
        raise NativeObservationBatchError("workers must be positive")
    ordered_days = tuple(dict.fromkeys(str(day) for day in days))
    unknown = sorted(set(ordered_days) - set(panel.M2_COMMON_SUPPORT_DAYS))
    if unknown:
        raise NativeObservationBatchError(f"days outside frozen M2 support: {unknown}")
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
                future = executor.submit(
                    _build_one,
                    day,
                    output_root,
                    raw_native_root,
                    native_book_cache,
                    individual_trade_root,
                )
                futures[future] = day
            state["pending"] = list(queue)
            state["running"] = sorted(futures.values())
            state["running_telemetry"] = _staging_telemetry(
                output_root, state["running"]
            )
            state["updated_at_utc"] = datetime.now(UTC).isoformat()
            state["elapsed_s"] = time.monotonic() - started
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
                    cache.validate_admitted_cache(output_root, day, deep=False)
                except BaseException as exc:
                    state["failed"][day] = f"{type(exc).__name__}: {exc}"
                    state["status"] = "failed"
                    state["running"] = sorted(futures.values())
                    state["pending"] = list(queue)
                    state["updated_at_utc"] = datetime.now(UTC).isoformat()
                    state["elapsed_s"] = time.monotonic() - started
                    _write_progress(progress_path, state)
                    for active in futures:
                        active.cancel()
                    raise NativeObservationBatchError(
                        f"raw-native cache build failed for {day}"
                    ) from exc
                state["completed"].append(day)
                print(
                    json.dumps(
                        {
                            "utc_day": day,
                            "observation_count": manifest["observation_count"],
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
    state["updated_at_utc"] = datetime.now(UTC).isoformat()
    state["elapsed_s"] = time.monotonic() - started
    _write_progress(progress_path, state)
    return state


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", nargs="*", default=panel.M2_COMMON_SUPPORT_DAYS)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output-root", type=Path, default=cache.DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--raw-native-root", type=Path, default=cache.DEFAULT_RAW_NATIVE_ROOT)
    parser.add_argument("--native-book-cache", type=Path, default=cache.DEFAULT_NATIVE_BOOK_CACHE)
    parser.add_argument(
        "--individual-trade-root",
        type=Path,
        default=cache.DEFAULT_INDIVIDUAL_TRADE_ROOT,
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
        raw_native_root=args.raw_native_root,
        native_book_cache=args.native_book_cache,
        individual_trade_root=args.individual_trade_root,
        progress_path=progress_path,
    )
    print(json.dumps(result, sort_keys=True, allow_nan=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
