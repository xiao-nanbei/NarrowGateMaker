#!/usr/bin/env python3
"""Build the offline F05 native-observation cache from one canonical source manifest.

The batch has no day-list input. It revalidates the offline source manifest and
its receipts, derives exactly the selected 30 target days plus their required
D+1 continuation context, and binds every admitted observation cache back to
canonical source bytes. Continuation-only days never become target assignments.
The batch never loads outcomes, labels, candidate actions, or policy rewards.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_native_observation_cache as cache,
)

IDENTITY = f"{offline.IDENTITY}.native_observation_batch_v1"
SCHEMA_VERSION = f"{IDENTITY}.manifest.v2"
PROGRESS_SCHEMA_VERSION = f"{IDENTITY}.progress.v2"
MAX_WORKERS = 8
PROGRESS_HEARTBEAT_S = 30.0
DEFAULT_OUTPUT_ROOT = offline.default_layout().project_data_root / (
    "cache/replay_dag/f05_full_multiscale_offline_native_observation_v1"
)
PROGRESS_NAME = "_offline_native_observation_batch_progress.json"
MANIFEST_NAME = "_offline_native_observation_batch_manifest.json"


class OfflineNativeObservationBatchError(RuntimeError):
    """Raised when the source-bound observation batch fails closed."""


def _file_sha256(path: Path) -> str:
    return offline.file_sha256(path)


def _canonical_document_sha256(payload: Mapping[str, Any], field: str) -> str:
    return offline.canonical_document_sha256(payload, field)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="ascii",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def _selection_sha256(source: Mapping[str, Any]) -> str:
    receipts = source.get("target_day_receipts")
    if not isinstance(receipts, list):
        raise OfflineNativeObservationBatchError("source target-day receipts are missing")
    by_day: dict[str, Mapping[str, Any]] = {}
    for receipt in receipts:
        if not isinstance(receipt, Mapping):
            raise OfflineNativeObservationBatchError("source target-day receipt is malformed")
        day = str(receipt.get("utc_day"))
        if day in by_day:
            raise OfflineNativeObservationBatchError(f"source target day is duplicated: {day}")
        if receipt.get("day_receipt_sha256") != offline.canonical_document_sha256(
            receipt, "day_receipt_sha256"
        ):
            raise OfflineNativeObservationBatchError(
                f"source target-day receipt hash drifted: {day}"
            )
        by_day[day] = receipt
    selected = tuple(str(day) for day in source.get("selected_days", ()))
    if len(selected) != offline.REQUIRED_DAYS or len(set(selected)) != offline.REQUIRED_DAYS:
        raise OfflineNativeObservationBatchError("source manifest must select exactly 30 unique days")
    if any(day not in by_day or by_day[day].get("source_gate_eligible") is not True for day in selected):
        raise OfflineNativeObservationBatchError("selected source day lacks an eligible receipt")
    selection_body = {
        "identity": offline.IDENTITY,
        "panel_role": offline.PANEL_ROLE,
        "required_days": offline.REQUIRED_DAYS,
        "candidate_order": list(offline.CANDIDATE_TARGET_DAYS),
        "consumed_exclusions": list(offline.CONSUMED_TARGET_DAYS),
        "selected_days": list(selected),
        "selected_day_receipts": [by_day[day]["day_receipt_sha256"] for day in selected],
    }
    return offline.canonical_sha256(selection_body)


def load_source_contract(
    source_manifest_path: Path,
    *,
    layout: offline.OfflineSourceLayout,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    """Revalidate the complete source chain and return its sole legal day panel."""

    path = source_manifest_path.expanduser().resolve()
    source = offline.validate_canonical_manifest(
        path,
        rehash_sources=True,
        layout=layout,
    )
    if source.get("status") != "offline_canonical_source_gate_passed_panel_mechanics_required":
        raise OfflineNativeObservationBatchError("source manifest has not passed the canonical gate")
    expected_selection = _selection_sha256(source)
    if source.get("selection_sha256") != expected_selection:
        raise OfflineNativeObservationBatchError("source selection hash drifted")
    folds = source.get("fold_manifest")
    if not isinstance(folds, Mapping) or folds.get("selection_sha256") != expected_selection:
        raise OfflineNativeObservationBatchError("source fold/selection binding drifted")
    days = tuple(str(day) for day in source["selected_days"])
    return source, days


def _observation_context_days(
    source: Mapping[str, Any],
    selected_target_days: Sequence[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return target D plus required D+1 days without expanding the target panel."""

    selected = tuple(str(day) for day in selected_target_days)
    selected_set = set(selected)
    target_rows = {
        str(row.get("utc_day")): row
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping)
    }
    continuation: set[str] = set()
    for day in selected:
        target = target_rows.get(day)
        context = target.get("context_days") if isinstance(target, Mapping) else None
        if not isinstance(context, Mapping) or str(context.get("D")) != day:
            raise OfflineNativeObservationBatchError(
                f"selected target context is missing or malformed: {day}"
            )
        d_plus_1 = str(context.get("D_plus_1", ""))
        if not d_plus_1:
            raise OfflineNativeObservationBatchError(
                f"selected target lacks D+1 continuation: {day}"
            )
        continuation.add(d_plus_1)
    observation_days = tuple(sorted(selected_set | continuation))
    continuation_only = tuple(day for day in observation_days if day not in selected_set)
    return observation_days, continuation_only


def _source_receipt(
    source: Mapping[str, Any],
    day: str,
    *,
    layout: offline.OfflineSourceLayout,
) -> Mapping[str, Any]:
    bindings = source.get("source_day_receipt_files")
    if not isinstance(bindings, Mapping) or day not in bindings:
        raise OfflineNativeObservationBatchError(f"source-day receipt binding is missing: {day}")
    binding = bindings[day]
    if not isinstance(binding, Mapping):
        raise OfflineNativeObservationBatchError(f"source-day receipt binding is malformed: {day}")
    path = offline._resolve_portable(str(binding.get("path")), layout=layout)
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineNativeObservationBatchError(f"cannot load source-day receipt: {day}") from exc
    if not isinstance(receipt, Mapping):
        raise OfflineNativeObservationBatchError(f"source-day receipt root is malformed: {day}")
    if _file_sha256(path) != binding.get("sha256"):
        raise OfflineNativeObservationBatchError(f"source-day receipt file hash drifted: {day}")
    if receipt.get("source_day_receipt_sha256") != binding.get("canonical_sha256"):
        raise OfflineNativeObservationBatchError(
            f"source-day receipt canonical binding drifted: {day}"
        )
    return receipt


def _expected_day_sources(
    source: Mapping[str, Any],
    day: str,
    *,
    selected_target_days: Sequence[str],
    layout: offline.OfflineSourceLayout,
) -> tuple[tuple[str, ...], tuple[str, ...], str, str]:
    target_rows = {
        str(row.get("utc_day")): row
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping)
    }
    selected = set(str(value) for value in selected_target_days)
    if day in selected:
        target = target_rows.get(day)
        context = target.get("context_days") if isinstance(target, Mapping) else None
        if not isinstance(context, Mapping):
            raise OfflineNativeObservationBatchError(
                f"selected target context is missing: {day}"
            )
        observed_days = (str(context.get("D_minus_1")), str(context.get("D")))
        if observed_days[1] != day:
            raise OfflineNativeObservationBatchError(
                f"selected target D identity drifted: {day}"
            )
        observation_role = "selected_target"
        observation_receipt_sha = str(target["day_receipt_sha256"])
    else:
        parents = [
            row
            for target_day, row in target_rows.items()
            if target_day in selected
            and isinstance(row.get("context_days"), Mapping)
            and str(row["context_days"].get("D_plus_1")) == day
        ]
        if not parents:
            raise OfflineNativeObservationBatchError(
                f"observation day is neither target nor required D+1: {day}"
            )
        parent_context = parents[0]["context_days"]
        observed_days = (str(parent_context.get("D")), day)
        observation_role = "continuation_only"
        receipt_hashes = {
            source_day: str(
                _source_receipt(source, source_day, layout=layout).get(
                    "source_day_receipt_sha256"
                )
            )
            for source_day in observed_days
        }
        observation_receipt_sha = offline.canonical_sha256(
            {
                "identity": IDENTITY,
                "observation_role": observation_role,
                "utc_day": day,
                "parent_target_days": sorted(str(row["utc_day"]) for row in parents),
                "source_manifest_canonical_sha256": source["canonical_manifest_sha256"],
                "source_selection_sha256": source["selection_sha256"],
                "source_day_receipts": receipt_hashes,
            }
        )
    raw_hashes: list[str] = []
    trade_hashes: list[str] = []
    for source_day in observed_days:
        receipt = _source_receipt(source, source_day, layout=layout)
        raw = receipt.get("raw_orderbook")
        hours = raw.get("hours") if isinstance(raw, Mapping) else None
        if not isinstance(hours, list) or len(hours) != 24:
            raise OfflineNativeObservationBatchError(
                f"canonical raw-hour receipt is incomplete: {source_day}"
            )
        raw_hashes.extend(str(row.get("sha256")) for row in hours if isinstance(row, Mapping))
        trades = receipt.get("individual_trades")
        if not isinstance(trades, Mapping):
            raise OfflineNativeObservationBatchError(
                f"canonical individual-trade receipt is missing: {source_day}"
            )
        trade_hashes.append(str(trades.get("sha256")))
    if len(raw_hashes) != 48 or len(trade_hashes) != 2:
        raise OfflineNativeObservationBatchError(f"canonical observation inputs drifted: {day}")
    return (
        tuple(raw_hashes),
        tuple(trade_hashes),
        observation_receipt_sha,
        observation_role,
    )


def _assert_day_source_binding(
    source: Mapping[str, Any],
    day: str,
    cache_manifest: Mapping[str, Any],
    *,
    selected_target_days: Sequence[str],
    layout: offline.OfflineSourceLayout,
) -> tuple[str, str]:
    raw_hashes, trade_hashes, observation_receipt_sha, observation_role = (
        _expected_day_sources(
            source,
            day,
            selected_target_days=selected_target_days,
            layout=layout,
        )
    )
    binding = cache_manifest.get("source_binding")
    if not isinstance(binding, Mapping):
        raise OfflineNativeObservationBatchError(f"cache source binding is missing: {day}")
    tape = binding.get("raw_native_tape_identity")
    files = tape.get("files") if isinstance(tape, Mapping) else None
    if not isinstance(files, list):
        raise OfflineNativeObservationBatchError(f"cache raw-tape binding is malformed: {day}")
    actual_raw = tuple(str(row.get("sha256")) for row in files if isinstance(row, Mapping))
    trades = binding.get("official_individual_trades")
    if not isinstance(trades, list):
        raise OfflineNativeObservationBatchError(f"cache trade binding is malformed: {day}")
    actual_trades = tuple(str(row.get("sha256")) for row in trades if isinstance(row, Mapping))
    if actual_raw != raw_hashes or actual_trades != trade_hashes:
        raise OfflineNativeObservationBatchError(
            f"cache inputs do not match canonical D-1/D receipts: {day}"
        )
    if (
        tape.get("day") != day
        or tape.get("warmup_hours") != 24
        or tape.get("continuation_hours") != 0
        or tape.get("strict_complete") is not True
        or binding.get("symbol") != offline.SYMBOL
        or binding.get("receive_time_transport_authority") is not False
    ):
        raise OfflineNativeObservationBatchError(f"cache source semantics drifted: {day}")
    return observation_receipt_sha, observation_role


def _validated_day_row(
    source: Mapping[str, Any],
    day: str,
    *,
    output_root: Path,
    selected_target_days: Sequence[str],
    layout: offline.OfflineSourceLayout,
    deep: bool,
) -> dict[str, Any]:
    validation = cache.validate_admitted_cache(output_root, day, deep=deep)
    manifest = validation.manifest
    observation_receipt_sha, observation_role = _assert_day_source_binding(
        source,
        day,
        manifest,
        selected_target_days=selected_target_days,
        layout=layout,
    )
    manifest_path = validation.day_root / cache.MANIFEST_NAME
    return {
        "utc_day": day,
        "observation_role": observation_role,
        "target_assignment_eligible": observation_role == "selected_target",
        "observation_receipt_sha256": observation_receipt_sha,
        "cache_canonical_manifest_sha256": manifest["canonical_manifest_sha256"],
        "cache_manifest_file_sha256": _file_sha256(manifest_path),
        # validate_admitted_cache already re-hashes the complete Parquet file.
        # Reuse that verified manifest binding instead of reading multi-GB bytes twice.
        "cache_parquet_sha256": str(manifest["parquet"]["sha256"]),
        "cache_observation_sha256": validation.observation_sha256,
        "observation_count": validation.observation_count,
        "source_binding_sha256": cache.canonical_sha256(manifest["source_binding"]),
    }


def _build_one(
    arguments: tuple[str, str, str, str, str],
) -> dict[str, Any]:
    day, output_root, raw_native_root, native_book_cache, individual_trade_root = arguments
    return cache.build_real_day_cache(
        day=day,
        output_root=Path(output_root),
        raw_native_root=Path(raw_native_root),
        native_book_cache=Path(native_book_cache),
        individual_trade_root=Path(individual_trade_root),
        symbol=offline.SYMBOL,
    )


def _validate_one(
    arguments: tuple[
        Mapping[str, Any],
        str,
        str,
        tuple[str, ...],
        offline.OfflineSourceLayout,
        bool,
    ],
) -> dict[str, Any]:
    source, day, output_root, selected_target_days, layout, deep = arguments
    return _validated_day_row(
        source,
        day,
        output_root=Path(output_root),
        selected_target_days=selected_target_days,
        layout=layout,
        deep=deep,
    )


def _progress_payload(
    *,
    source: Mapping[str, Any],
    selected_target_days: Sequence[str],
    observation_context_days: Sequence[str],
    continuation_only_days: Sequence[str],
    workers: int,
    status: str,
    completed: Sequence[str],
    pending: Sequence[str],
    running: Sequence[str],
    failed: Mapping[str, str],
    started: float,
) -> dict[str, Any]:
    return {
        "schema_version": PROGRESS_SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": status,
        "source_canonical_manifest_sha256": source["canonical_manifest_sha256"],
        "source_selection_sha256": source["selection_sha256"],
        "workers": workers,
        "selected_target_days": list(selected_target_days),
        "observation_context_days": list(observation_context_days),
        "continuation_only_days": list(continuation_only_days),
        "completed": list(completed),
        "pending": list(pending),
        "running": list(running),
        "failed": dict(failed),
        "elapsed_s": time.monotonic() - started,
        "updated_at_utc": datetime.now(UTC).isoformat(),
        "economic_outcomes_read": False,
        "labels_read": False,
        "actions_read": False,
    }


def run_batch(
    *,
    source_manifest_path: Path,
    workers: int,
    output_root: Path,
    native_book_cache: Path,
    progress_path: Path,
    manifest_path: Path,
    layout: offline.OfflineSourceLayout,
) -> dict[str, Any]:
    """Build target-day observations plus the minimum required D+1 context."""

    if workers < 1 or workers > MAX_WORKERS:
        raise OfflineNativeObservationBatchError(
            f"workers must be between 1 and {MAX_WORKERS}"
        )
    source, selected_target_days = load_source_contract(source_manifest_path, layout=layout)
    observation_days, continuation_only_days = _observation_context_days(
        source, selected_target_days
    )
    root = output_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    raw_native_root = layout.raw_orderbook_root.expanduser().resolve().parent
    individual_trade_root = layout.individual_trades_root.expanduser().resolve().parent
    native_cache = native_book_cache.expanduser().resolve()
    completed: list[str] = []
    pending: list[str] = []
    for day in observation_days:
        if (root / day).exists():
            _validated_day_row(
                source,
                day,
                output_root=root,
                selected_target_days=selected_target_days,
                layout=layout,
                deep=False,
            )
            completed.append(day)
        else:
            pending.append(day)
    started = time.monotonic()
    failed: dict[str, str] = {}

    def write_progress(status: str, running: Sequence[str], queue: Sequence[str]) -> None:
        _atomic_json(
            progress_path.expanduser().resolve(),
            _progress_payload(
                source=source,
                selected_target_days=selected_target_days,
                observation_context_days=observation_days,
                continuation_only_days=continuation_only_days,
                workers=workers,
                status=status,
                completed=tuple(day for day in observation_days if day in completed),
                pending=queue,
                running=running,
                failed=failed,
                started=started,
            ),
        )

    write_progress("running" if pending else "validating", (), pending)
    arguments = {
        day: (
            day,
            str(root),
            str(raw_native_root),
            str(native_cache),
            str(individual_trade_root),
        )
        for day in pending
    }
    try:
        if workers == 1:
            for index, day in enumerate(tuple(pending)):
                write_progress("running", (day,), tuple(pending[index + 1 :]))
                _build_one(arguments[day])
                _validated_day_row(
                    source,
                    day,
                    output_root=root,
                    selected_target_days=selected_target_days,
                    layout=layout,
                    deep=False,
                )
                completed.append(day)
        elif pending:
            queue = list(pending)
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures: dict[Any, str] = {}
                while queue or futures:
                    while queue and len(futures) < workers:
                        day = queue.pop(0)
                        futures[executor.submit(_build_one, arguments[day])] = day
                    write_progress("running", sorted(futures.values()), tuple(queue))
                    done, _ = wait(
                        tuple(futures),
                        timeout=PROGRESS_HEARTBEAT_S,
                        return_when=FIRST_COMPLETED,
                    )
                    for future in done:
                        day = futures.pop(future)
                        future.result()
                        _validated_day_row(
                            source,
                            day,
                            output_root=root,
                            selected_target_days=selected_target_days,
                            layout=layout,
                            deep=False,
                        )
                        completed.append(day)
    except BaseException as exc:
        day = next(
            (value for value in observation_days if value not in completed), "batch"
        )
        failed[day] = f"{type(exc).__name__}: {exc}"
        write_progress(
            "failed",
            (),
            tuple(value for value in observation_days if value not in completed),
        )
        raise OfflineNativeObservationBatchError(
            f"source-bound native observation cache build failed: {day}"
        ) from exc

    write_progress("validating", (), ())
    validation_arguments = [
        (source, day, str(root), selected_target_days, layout, True)
        for day in observation_days
    ]
    if workers == 1:
        day_rows = [_validate_one(arguments) for arguments in validation_arguments]
    else:
        with ProcessPoolExecutor(max_workers=min(workers, len(observation_days))) as executor:
            day_rows = list(executor.map(_validate_one, validation_arguments))
    source_path = source_manifest_path.expanduser().resolve()
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "outcome_blind_native_observation_cache_admitted",
        "source_manifest": {
            "path": offline._portable_path(
                source_path,
                project_data=layout.project_data_root,
                market_data=layout.marketdata_root,
            ),
            "file_sha256": _file_sha256(source_path),
            "canonical_manifest_sha256": source["canonical_manifest_sha256"],
            "selection_sha256": source["selection_sha256"],
        },
        "selected_target_days": list(selected_target_days),
        "selected_target_day_count": len(selected_target_days),
        "observation_context_days": list(observation_days),
        "observation_context_day_count": len(observation_days),
        "continuation_only_days": list(continuation_only_days),
        "continuation_days_create_target_assignments": False,
        "workers": workers,
        "cache_contract": cache.cache_contract(),
        "days": day_rows,
        "permissions": {
            "economic_outcomes_read": False,
            "labels_read": False,
            "actions_read": False,
            "model_trained": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    result["canonical_manifest_sha256"] = _canonical_document_sha256(
        result, "canonical_manifest_sha256"
    )
    _atomic_json(manifest_path.expanduser().resolve(), result)
    write_progress("complete", (), ())
    return result


def validate_batch_manifest(
    path: Path,
    *,
    output_root: Path,
    layout: offline.OfflineSourceLayout,
    workers: int = 6,
    deep: bool = True,
) -> dict[str, Any]:
    manifest_path = path.expanduser().resolve()
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineNativeObservationBatchError("cannot load batch manifest") from exc
    if not isinstance(payload, dict):
        raise OfflineNativeObservationBatchError("batch manifest root must be an object")
    if payload.get("identity") != IDENTITY or payload.get("schema_version") != SCHEMA_VERSION:
        raise OfflineNativeObservationBatchError("batch manifest identity drifted")
    if payload.get("canonical_manifest_sha256") != _canonical_document_sha256(
        payload, "canonical_manifest_sha256"
    ):
        raise OfflineNativeObservationBatchError("batch manifest hash drifted")
    source_binding = payload.get("source_manifest")
    if not isinstance(source_binding, Mapping):
        raise OfflineNativeObservationBatchError("batch source binding is missing")
    source_path = offline._resolve_portable(str(source_binding.get("path")), layout=layout)
    source, selected_target_days = load_source_contract(source_path, layout=layout)
    observation_days, continuation_only_days = _observation_context_days(
        source, selected_target_days
    )
    if (
        source_binding.get("file_sha256") != _file_sha256(source_path)
        or source_binding.get("canonical_manifest_sha256") != source["canonical_manifest_sha256"]
        or source_binding.get("selection_sha256") != source["selection_sha256"]
        or tuple(payload.get("selected_target_days", ())) != selected_target_days
        or payload.get("selected_target_day_count") != offline.REQUIRED_DAYS
        or tuple(payload.get("observation_context_days", ())) != observation_days
        or payload.get("observation_context_day_count") != len(observation_days)
        or tuple(payload.get("continuation_only_days", ())) != continuation_only_days
        or payload.get("continuation_days_create_target_assignments") is not False
    ):
        raise OfflineNativeObservationBatchError("batch source/day binding drifted")
    if workers < 1:
        raise OfflineNativeObservationBatchError("validation workers must be positive")
    validation_arguments = [
        (
            source,
            day,
            str(output_root.expanduser().resolve()),
            selected_target_days,
            layout,
            bool(deep),
        )
        for day in observation_days
    ]
    if workers == 1:
        expected_rows = [_validate_one(arguments) for arguments in validation_arguments]
    else:
        with ProcessPoolExecutor(max_workers=min(workers, len(observation_days))) as executor:
            expected_rows = list(executor.map(_validate_one, validation_arguments))
    if payload.get("days") != expected_rows:
        raise OfflineNativeObservationBatchError("batch per-day cache bindings drifted")
    permissions = payload.get("permissions")
    if not isinstance(permissions, Mapping) or any(value is not False for value in permissions.values()):
        raise OfflineNativeObservationBatchError("batch permission boundary drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--source-manifest", type=Path, required=True)
    build.add_argument("--workers", type=int, default=4)
    build.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    build.add_argument("--native-book-cache", type=Path, default=cache.DEFAULT_NATIVE_BOOK_CACHE)
    build.add_argument("--progress-path", type=Path)
    build.add_argument("--manifest-path", type=Path)
    validate = subparsers.add_parser("validate")
    validate.add_argument("manifest", type=Path)
    validate.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    validate.add_argument("--workers", type=int, default=6)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    layout = offline.default_layout()
    if args.command == "build":
        progress_path = args.progress_path or (args.output_root / PROGRESS_NAME)
        manifest_path = args.manifest_path or (args.output_root / MANIFEST_NAME)
        result = run_batch(
            source_manifest_path=args.source_manifest,
            workers=args.workers,
            output_root=args.output_root,
            native_book_cache=args.native_book_cache,
            progress_path=progress_path,
            manifest_path=manifest_path,
            layout=layout,
        )
    else:
        result = validate_batch_manifest(
            args.manifest,
            output_root=args.output_root,
            layout=layout,
            workers=args.workers,
        )
    print(
        json.dumps(
            {
                "identity": result["identity"],
                "status": result["status"],
                "selected_target_day_count": result["selected_target_day_count"],
                "observation_context_day_count": result[
                    "observation_context_day_count"
                ],
                "canonical_manifest_sha256": result["canonical_manifest_sha256"],
                "economic_outcomes_read": False,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
