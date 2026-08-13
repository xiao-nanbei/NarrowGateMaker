#!/usr/bin/env python3
"""Materialize and validate the outcome-blind native-book cache for F05.

The formal replay forks are cache-read-only.  This module is the single-owner
stage that parses each unique D-1/D/D+1 source day once, writes the reusable
hour cache, and binds every admitted hour into one immutable receipt.  It never
loads strategy outcomes, labels, assignments, or candidate actions.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from models.exchange_book_replay import CryptoHFTExchangeBookTape
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

IDENTITY = f"{offline.IDENTITY}.native_book_cache_v1"
SCHEMA_VERSION = f"{IDENTITY}.manifest.v1"
SYMBOL = offline.SYMBOL
EXCHANGE = "binance_futures"
MARKET_ID = f"{EXCHANGE}:perpetual:{SYMBOL}"
TICK_SIZE = 0.1
MIN_FREE_BYTES = 50 * 1024**3
SAFETY_RESERVE_BYTES = 60 * 1024**3
OUTPUT_MULTIPLIER = 2.5
FALLBACK_HOUR_BYTES = 12 * 1024**2


class OfflineNativeCacheError(RuntimeError):
    """Raised when native cache construction or admission fails closed."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return _canonical_sha256(payload)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _context_days(source: Mapping[str, Any]) -> tuple[str, ...]:
    selected = tuple(str(day) for day in source.get("selected_days", ()))
    rows = {
        str(row.get("utc_day")): row
        for row in source.get("target_day_receipts", ())
        if isinstance(row, Mapping)
    }
    context: set[str] = set()
    for day in selected:
        row = rows.get(day)
        if not isinstance(row, Mapping) or row.get("source_gate_eligible") is not True:
            raise OfflineNativeCacheError(f"selected source day is not eligible: {day}")
        values = row.get("context_days")
        if not isinstance(values, Mapping) or set(values) != {
            "D_minus_1",
            "D",
            "D_plus_1",
        }:
            raise OfflineNativeCacheError(f"source context contract drifted: {day}")
        context.update(str(value) for value in values.values())
    if len(selected) != offline.REQUIRED_DAYS:
        raise OfflineNativeCacheError("source manifest lacks the frozen 30-day panel")
    return tuple(sorted(context))


def _existing_hour_sizes(cache_root: Path) -> tuple[int, ...]:
    if not cache_root.is_dir():
        return ()
    return tuple(
        int(path.stat().st_size)
        for path in cache_root.rglob("logical_messages_*.parquet")
        if path.is_file()
    )


def storage_preflight(cache_root: Path, context_days: Sequence[str]) -> dict[str, Any]:
    root = cache_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    existing_hours = 0
    for day in context_days:
        directory = root / EXCHANGE / SYMBOL / day
        if directory.is_dir():
            existing_hours += sum(
                1 for path in directory.glob("*/*.manifest.json") if path.is_file()
            )
    required_hours = len(context_days) * 24
    missing_hours = max(0, required_hours - existing_hours)
    sizes = _existing_hour_sizes(root)
    average_hour_bytes = (
        int(sum(sizes) / len(sizes)) if sizes else FALLBACK_HOUR_BYTES
    )
    estimated_final_bytes = missing_hours * average_hour_bytes
    required_free_bytes = int(
        SAFETY_RESERVE_BYTES + OUTPUT_MULTIPLIER * estimated_final_bytes
    )
    free_bytes = int(shutil.disk_usage(root).free)
    passed = free_bytes >= MIN_FREE_BYTES and free_bytes >= required_free_bytes
    result = {
        "required_hours": required_hours,
        "existing_hours": existing_hours,
        "missing_hours": missing_hours,
        "average_existing_hour_bytes": average_hour_bytes,
        "estimated_new_final_bytes": estimated_final_bytes,
        "free_bytes": free_bytes,
        "minimum_free_bytes": MIN_FREE_BYTES,
        "required_free_bytes": required_free_bytes,
        "passed": passed,
    }
    if not passed:
        raise OfflineNativeCacheError(
            "storage safety gate failed before native cache materialization"
        )
    return result


def _tape(*, raw_root: Path, cache_root: Path, day: str, read_only: bool) -> CryptoHFTExchangeBookTape:
    return CryptoHFTExchangeBookTape(
        raw_root=raw_root,
        day=day,
        symbol=SYMBOL,
        exchange=EXCHANGE,
        tick_size=TICK_SIZE,
        warmup_hours=0,
        continuation_hours=0,
        strict_complete=True,
        cache_dir=cache_root,
        cache_enabled=True,
        cache_read_only=read_only,
    )


def _materialize_context_day(arguments: tuple[str, str, str]) -> dict[str, Any]:
    day, raw_root_value, cache_root_value = arguments
    tape = _tape(
        raw_root=Path(raw_root_value),
        cache_root=Path(cache_root_value),
        day=day,
        read_only=False,
    )
    completeness = tape.materialize_cache(verify_sha256=True)
    return {
        "day": day,
        "cache_stats": tape.cache_stats(),
        "completeness": completeness,
    }


def _portable_hour(row: Mapping[str, Any], *, layout: offline.OfflineSourceLayout) -> dict[str, Any]:
    output = dict(row)
    for key in ("source_path", "data_path", "manifest_path"):
        output[key] = offline._portable_path(
            Path(str(row[key])),
            project_data=layout.project_data_root,
            market_data=layout.marketdata_root,
        )
    return output


def _resolve_portable(value: Any, *, layout: offline.OfflineSourceLayout) -> Path:
    if not isinstance(value, str) or not value:
        raise OfflineNativeCacheError("portable path binding is missing")
    roots = (
        ("${NARROWGATE_MARKETDATA_ROOT}", layout.marketdata_root.resolve()),
        ("${NARROWGATE_DATA_ROOT}", layout.project_data_root.resolve()),
    )
    for marker, root in roots:
        if value == marker:
            return root
        prefix = marker + "/"
        if value.startswith(prefix):
            return (root / value[len(prefix) :]).resolve()
    raise OfflineNativeCacheError(f"unsupported portable path: {value}")


def build_manifest(
    *,
    source_manifest_path: Path,
    cache_root: Path,
    output_path: Path,
    workers: int,
    layout: offline.OfflineSourceLayout,
) -> dict[str, Any]:
    if workers < 1:
        raise OfflineNativeCacheError("workers must be positive")
    source = offline.validate_canonical_manifest(
        source_manifest_path.expanduser().resolve(),
        rehash_sources=True,
        layout=layout,
    )
    if source.get("permissions", {}).get("economic_outcomes_read") is not False:
        raise OfflineNativeCacheError("source manifest is not outcome-blind")
    context_days = _context_days(source)
    cache = cache_root.expanduser().resolve()
    storage = storage_preflight(cache, context_days)
    # OfflineSourceLayout ends at the exchange directory, while the native
    # tape appends its exchange identity itself.
    raw_root = layout.raw_orderbook_root.expanduser().resolve().parent
    arguments = tuple((day, str(raw_root), str(cache)) for day in context_days)
    rows: list[dict[str, Any]] = []
    if workers == 1:
        for argument in arguments:
            rows.append(_materialize_context_day(argument))
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_materialize_context_day, argument): argument[0]
                for argument in arguments
            }
            for future in as_completed(futures):
                day = futures[future]
                try:
                    rows.append(future.result())
                except Exception as exc:
                    raise OfflineNativeCacheError(
                        f"native cache materialization failed for {day}"
                    ) from exc
    rows.sort(key=lambda row: str(row["day"]))
    day_rows: list[dict[str, Any]] = []
    total_events = 0
    total_levels = 0
    for row in rows:
        completeness = row["completeness"]
        if (
            completeness.get("expected_hour_count") != 24
            or completeness.get("complete_hour_count") != 24
            or completeness.get("verify_sha256") is not True
        ):
            raise OfflineNativeCacheError(
                f"native cache day is incomplete: {row['day']}"
            )
        hours = [
            _portable_hour(hour, layout=layout)
            for hour in completeness.get("hours", ())
        ]
        if len(hours) != 24:
            raise OfflineNativeCacheError(f"native cache hour census drifted: {row['day']}")
        total_events += sum(int(hour["event_count"]) for hour in hours)
        total_levels += sum(int(hour["level_count"]) for hour in hours)
        day_rows.append(
            {
                "day": row["day"],
                "expected_hour_count": 24,
                "complete_hour_count": 24,
                "canonical_identity_sha256": completeness["canonical_identity_sha256"],
                "hours": hours,
            }
        )
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "outcome_blind_native_hour_cache_materialized",
        "source_manifest": {
            "path": offline._portable_path(
                source_manifest_path.expanduser().resolve(),
                project_data=layout.project_data_root,
                market_data=layout.marketdata_root,
            ),
            "sha256": _file_sha256(source_manifest_path.expanduser().resolve()),
            "canonical_sha256": source["canonical_manifest_sha256"],
        },
        "source_selection_sha256": source["selection_sha256"],
        "context_days": list(context_days),
        "context_day_count": len(context_days),
        "expected_hour_count": len(context_days) * 24,
        "complete_hour_count": len(context_days) * 24,
        "cache_root": offline._portable_path(
            cache,
            project_data=layout.project_data_root,
            market_data=layout.marketdata_root,
        ),
        "parser_contract": {
            "exchange": EXCHANGE,
            "market_id": MARKET_ID,
            "symbol": SYMBOL,
            "tick_size": TICK_SIZE,
            "source_clock": "transaction_with_event_then_receive_fallback",
            "single_owner_materialization": True,
            "formal_forks_cache_read_only": True,
        },
        "storage_preflight": storage,
        "totals": {
            "event_count": total_events,
            "level_count": total_levels,
        },
        "days": day_rows,
        "permissions": {
            "economic_outcomes_read": False,
            "labels_generated": False,
            "candidate_actions_evaluated": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest["manifest_sha256"] = _canonical_document_sha256(
        manifest, "manifest_sha256"
    )
    _atomic_json(output_path.expanduser().resolve(), manifest)
    return manifest


def validate_manifest(
    path: Path,
    *,
    layout: offline.OfflineSourceLayout,
) -> dict[str, Any]:
    manifest_path = path.expanduser().resolve()
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineNativeCacheError("cannot load native cache manifest") from exc
    if not isinstance(payload, dict):
        raise OfflineNativeCacheError("native cache manifest root must be an object")
    if payload.get("schema_version") != SCHEMA_VERSION or payload.get("identity") != IDENTITY:
        raise OfflineNativeCacheError("native cache identity drifted")
    if payload.get("manifest_sha256") != _canonical_document_sha256(
        payload, "manifest_sha256"
    ):
        raise OfflineNativeCacheError("native cache manifest hash drifted")
    source_binding = payload.get("source_manifest")
    if not isinstance(source_binding, Mapping):
        raise OfflineNativeCacheError("source manifest binding is missing")
    source_path = _resolve_portable(source_binding.get("path"), layout=layout)
    if _file_sha256(source_path) != source_binding.get("sha256"):
        raise OfflineNativeCacheError("source manifest file hash drifted")
    source = offline.validate_canonical_manifest(
        source_path,
        rehash_sources=True,
        layout=layout,
    )
    if source.get("canonical_manifest_sha256") != source_binding.get("canonical_sha256"):
        raise OfflineNativeCacheError("source manifest canonical identity drifted")
    expected_days = _context_days(source)
    if payload.get("context_days") != list(expected_days):
        raise OfflineNativeCacheError("native cache context-day order drifted")
    cache_root = _resolve_portable(payload.get("cache_root"), layout=layout)
    rows = payload.get("days")
    if not isinstance(rows, list) or len(rows) != len(expected_days):
        raise OfflineNativeCacheError("native cache day census drifted")
    observed_days: list[str] = []
    total_events = 0
    total_levels = 0
    for row in rows:
        if not isinstance(row, Mapping):
            raise OfflineNativeCacheError("native cache day row is malformed")
        day = str(row.get("day"))
        observed_days.append(day)
        tape = _tape(raw_root=layout.raw_orderbook_root.parent, cache_root=cache_root, day=day, read_only=True)
        current = tape.cache_completeness(verify_sha256=True)
        expected_hours = row.get("hours")
        if not isinstance(expected_hours, list) or len(expected_hours) != 24:
            raise OfflineNativeCacheError(f"native cache hour receipt drifted: {day}")
        portable_current = [
            _portable_hour(hour, layout=layout) for hour in current.get("hours", ())
        ]
        if portable_current != expected_hours:
            raise OfflineNativeCacheError(f"native cache bytes drifted: {day}")
        if current.get("canonical_identity_sha256") != row.get("canonical_identity_sha256"):
            raise OfflineNativeCacheError(f"native cache day identity drifted: {day}")
        total_events += sum(int(hour["event_count"]) for hour in portable_current)
        total_levels += sum(int(hour["level_count"]) for hour in portable_current)
    if observed_days != list(expected_days):
        raise OfflineNativeCacheError("native cache day order drifted")
    if payload.get("complete_hour_count") != len(expected_days) * 24:
        raise OfflineNativeCacheError("native cache complete-hour count drifted")
    if payload.get("totals") != {
        "event_count": total_events,
        "level_count": total_levels,
    }:
        raise OfflineNativeCacheError("native cache event totals drifted")
    if payload.get("permissions") != {
        "economic_outcomes_read": False,
        "labels_generated": False,
        "candidate_actions_evaluated": False,
        "action_authorized": False,
        "live_authorized": False,
    }:
        raise OfflineNativeCacheError("native cache permissions drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--source-manifest", type=Path, required=True)
    build.add_argument("--cache-root", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--workers", type=int, default=4)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--manifest", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    layout = offline.default_layout()
    if args.command == "build":
        payload = build_manifest(
            source_manifest_path=args.source_manifest,
            cache_root=args.cache_root,
            output_path=args.output,
            workers=args.workers,
            layout=layout,
        )
    else:
        payload = validate_manifest(args.manifest, layout=layout)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
