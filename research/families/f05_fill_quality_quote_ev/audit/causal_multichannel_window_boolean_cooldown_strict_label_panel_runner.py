#!/usr/bin/env python3
"""Run the frozen cooldown-v2 strict-label panel with bounded resumption."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_labels as strict_labels,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
RUNNER_IDENTITY = f"{IDENTITY}.strict_label_panel_runner.v1"
PROGRESS_SCHEMA_VERSION = f"{RUNNER_IDENTITY}.progress.v2"
PANEL_SCHEMA_VERSION = f"{RUNNER_IDENTITY}.panel.v2"
FORMAL_LABEL_PANEL_SCHEMA_VERSION = f"{IDENTITY}.strict_native_label_panel.v3"
FORMAL_RUN_DIRECTORY_NAME = "formal_full_support_41d_v9"
FEATURE_BLOCK = "M2"
MAX_OPPORTUNITIES = None
DEFAULT_MAX_WORKERS = 1
DEFAULT_PREBUILD_WORKERS = 2
MAX_DAY_WORKERS_CAP = 2
MAX_PREBUILD_WORKERS_CAP = 4
V2_SPEC = strict_labels.V2_SPEC
DEFAULT_OUTPUT = strict_labels.DEFAULT_OUTPUT
DEFAULT_CACHE_ROOT = strict_labels.panel.DEFAULT_CACHE
DEFAULT_NATIVE_CACHE = strict_labels.DEFAULT_NATIVE_CACHE


class StrictLabelPanelRunnerError(RuntimeError):
    """Raised when a panel run or its durable identity is invalid."""


@dataclass(frozen=True)
class DayTask:
    day: str
    output: str
    cache_root: str
    native_cache: str
    native_cache_receipt: str


@dataclass(frozen=True)
class SourceSegment:
    segment_id: str
    start_day: str
    end_day: str
    source_days: tuple[str, ...]
    target_days: tuple[str, ...]

    @property
    def hour_count(self) -> int:
        return 24 * len(self.source_days)


@dataclass(frozen=True)
class SegmentTask:
    segment: SourceSegment
    native_cache: str
    receipt_path: str
    result_path: str


@dataclass(frozen=True)
class SourceUnionPlan:
    target_days: tuple[str, ...]
    unique_source_days: tuple[str, ...]
    segments: tuple[SourceSegment, ...]
    naive_target_hour_scans: int
    unique_source_hours: int
    hours_saved: int


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StrictLabelPanelRunnerError(f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise StrictLabelPanelRunnerError(f"JSON root is not an object: {path}")
    return payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}")
    with temporary.open("x", encoding="ascii") as handle:
        handle.write(
            json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
        )
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _formal_day_universe(
    spec: Mapping[str, Any],
) -> tuple[tuple[str, ...], frozenset[str], tuple[str, ...], tuple[str, ...]]:
    ordered = spec.get("ordered_utc_days")
    if not isinstance(ordered, Mapping):
        raise StrictLabelPanelRunnerError("v2 Spec lacks ordered_utc_days")
    prefix40 = tuple(str(day) for day in ordered.get("prefix40", ()))
    added10 = tuple(str(day) for day in ordered.get("added10", ()))
    frozen50 = (*prefix40, *added10)
    strict_source = (
        spec.get("source_separation", {}).get("strict_native_2026", {})
    )
    if not isinstance(strict_source, Mapping):
        raise StrictLabelPanelRunnerError("v2 Spec lacks strict-native identity")
    reduced = frozenset(str(day) for day in strict_source.get("reduced_support_days", ()))
    expected_count = strict_source.get("full_D_minus_1_D_D_plus_1_feature_support_days")
    expected_identity = strict_source.get("full_support_identity")
    if len(prefix40) != 40 or len(added10) != 10:
        raise StrictLabelPanelRunnerError("v2 Spec 40+10 day order drifted")
    if len(frozen50) != 50 or len(set(frozen50)) != 50:
        raise StrictLabelPanelRunnerError("v2 Spec frozen 50-day denominator drifted")
    if len(reduced) != 9 or not reduced.issubset(frozen50):
        raise StrictLabelPanelRunnerError("v2 Spec reduced-support identity drifted")
    if expected_count != 41:
        raise StrictLabelPanelRunnerError("v2 Spec full-support count is not 41")
    if expected_identity != strict_labels.FULL_SUPPORT_IDENTITY:
        raise StrictLabelPanelRunnerError("v2 Spec full-support name drifted")
    formal = tuple(day for day in frozen50 if day not in reduced)
    if len(formal) != 41:
        raise StrictLabelPanelRunnerError("derived full-support denominator is not 41")
    return formal, reduced, prefix40, added10


def _selected_days(
    formal_days: Sequence[str],
    reduced_days: frozenset[str],
    engineering_days: Sequence[str] | None,
) -> tuple[tuple[str, ...], bool]:
    if engineering_days is None:
        return tuple(formal_days), True
    requested = tuple(str(day) for day in engineering_days)
    if not requested or len(set(requested)) != len(requested):
        raise StrictLabelPanelRunnerError(
            "engineering subset must contain unique explicit days"
        )
    reduced_requested = tuple(day for day in requested if day in reduced_days)
    if reduced_requested:
        raise StrictLabelPanelRunnerError(
            "engineering subset includes reduced-support days: "
            f"{reduced_requested}"
        )
    outside = tuple(day for day in requested if day not in formal_days)
    if outside:
        raise StrictLabelPanelRunnerError(
            f"engineering subset is outside the frozen 41 days: {outside}"
        )
    requested_set = set(requested)
    ordered_subset = tuple(day for day in formal_days if day in requested_set)
    return ordered_subset, ordered_subset == tuple(formal_days)


def _validate_workers(
    workers: int,
    *,
    name: str = "max_workers",
    cap: int = MAX_DAY_WORKERS_CAP,
) -> int:
    if isinstance(workers, bool) or not isinstance(workers, int):
        raise StrictLabelPanelRunnerError(f"{name} must be an integer")
    if workers < 1 or workers > cap:
        raise StrictLabelPanelRunnerError(
            f"{name} must be within [1, {cap}]"
        )
    return workers


def _parse_utc_day(value: str) -> date:
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError as exc:
        raise StrictLabelPanelRunnerError(
            f"invalid UTC day in source union: {value!r}"
        ) from exc
    if parsed.isoformat() != str(value):
        raise StrictLabelPanelRunnerError(
            f"non-canonical UTC day in source union: {value!r}"
        )
    return parsed


def _source_union_plan(
    days: Sequence[str],
    *,
    formal: bool,
) -> SourceUnionPlan:
    target_dates = tuple(_parse_utc_day(str(day)) for day in days)
    if not target_dates or len(set(target_dates)) != len(target_dates):
        raise StrictLabelPanelRunnerError(
            "source union requires non-empty unique target days"
        )
    if tuple(sorted(target_dates)) != target_dates:
        raise StrictLabelPanelRunnerError("target days are not chronological")

    source_dates = sorted(
        {
            target + timedelta(days=offset)
            for target in target_dates
            for offset in (-1, 0, 1)
        }
    )

    # Coalesce only overlapping D-1/D/D+1 intervals. Merely adjacent source
    # days can have different strictness ownership: the last day of one
    # interval may be absent while the first day of the next interval is only
    # warmup and may recover from a sequence break before its target starts.
    grouped: list[tuple[date, date, list[date]]] = []
    for target in target_dates:
        start = target - timedelta(days=1)
        end = target + timedelta(days=1)
        if not grouped or start > grouped[-1][1]:
            grouped.append((start, end, [target]))
            continue
        previous_start, previous_end, group_targets = grouped[-1]
        grouped[-1] = (
            previous_start,
            max(previous_end, end),
            [*group_targets, target],
        )

    segments: list[SourceSegment] = []
    assigned_targets: list[str] = []
    for index, (group_start, group_end, group_targets) in enumerate(
        grouped, start=1
    ):
        source_group = tuple(
            group_start + timedelta(days=offset)
            for offset in range((group_end - group_start).days + 1)
        )
        segment_targets = tuple(target.isoformat() for target in group_targets)
        if not segment_targets or len(source_group) < 3:
            raise StrictLabelPanelRunnerError(
                "source union produced a segment without a complete target window"
            )
        start_day = source_group[0].isoformat()
        end_day = source_group[-1].isoformat()
        segments.append(
            SourceSegment(
                segment_id=(
                    f"segment-{index:03d}-{start_day.replace('-', '')}-"
                    f"{end_day.replace('-', '')}"
                ),
                start_day=start_day,
                end_day=end_day,
                source_days=tuple(day.isoformat() for day in source_group),
                target_days=segment_targets,
            )
        )
        assigned_targets.extend(segment_targets)

    target_days = tuple(target.isoformat() for target in target_dates)
    if tuple(assigned_targets) != target_days:
        raise StrictLabelPanelRunnerError(
            "each target day must map to exactly one contiguous source segment"
        )
    unique_source_days = tuple(day.isoformat() for day in source_dates)
    if formal:
        if len(target_days) != 41:
            raise StrictLabelPanelRunnerError(
                "formal source union target count is not 41"
            )
        if len(unique_source_days) != 57:
            raise StrictLabelPanelRunnerError(
                "formal D-1/D/D+1 union is not 57 unique UTC source days"
            )
        if len(segments) != 8:
            raise StrictLabelPanelRunnerError(
                "formal D-1/D/D+1 union is not eight overlapping-window segments"
            )

    naive_hours = len(target_days) * 72
    unique_hours = len(unique_source_days) * 24
    return SourceUnionPlan(
        target_days=target_days,
        unique_source_days=unique_source_days,
        segments=tuple(segments),
        naive_target_hour_scans=naive_hours,
        unique_source_hours=unique_hours,
        hours_saved=naive_hours - unique_hours,
    )


def _receipt_identity(payload: Mapping[str, Any]) -> str:
    body = dict(payload)
    body.pop("canonical_identity_sha256", None)
    return _canonical_sha256(body)


def _seal_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(payload)
    row["canonical_identity_sha256"] = _receipt_identity(row)
    return row


def _expected_segment_hours(segment: SourceSegment) -> tuple[str, ...]:
    hours: list[str] = []
    for source_day in segment.source_days:
        hours.extend(f"{source_day}T{hour:02d}:00:00Z" for hour in range(24))
    return tuple(hours)


def _validate_segment_receipt(
    receipt: Mapping[str, Any],
    *,
    segment: SourceSegment,
    native_cache: Path,
) -> None:
    if receipt.get("schema_version") != (
        f"{RUNNER_IDENTITY}.native_cache_segment_receipt.v3"
    ):
        raise StrictLabelPanelRunnerError(
            f"segment receipt schema drifted for {segment.segment_id}"
        )
    expected = {
        "segment_id": segment.segment_id,
        "start_day": segment.start_day,
        "end_day": segment.end_day,
        "source_days": list(segment.source_days),
        "target_days": list(segment.target_days),
        "anchor_day": segment.target_days[0],
        "strict_start_day": segment.target_days[0],
        "hour_count": segment.hour_count,
        "native_cache_root": str(native_cache.expanduser().resolve()),
        "economic_outcomes_read": False,
        "arms_run": False,
        "scheduler_replay_count": 1,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise StrictLabelPanelRunnerError(
                f"segment receipt {key} drifted for {segment.segment_id}"
            )
    if receipt.get("canonical_identity_sha256") != _receipt_identity(receipt):
        raise StrictLabelPanelRunnerError(
            f"segment receipt identity drifted for {segment.segment_id}"
        )
    hours = receipt.get("hours")
    if not isinstance(hours, list) or len(hours) != segment.hour_count:
        raise StrictLabelPanelRunnerError(
            f"segment receipt hours drifted for {segment.segment_id}"
        )
    observed_hours = tuple(str(row.get("utc_hour")) for row in hours)
    if observed_hours != _expected_segment_hours(segment):
        raise StrictLabelPanelRunnerError(
            f"segment receipt hour order drifted for {segment.segment_id}"
        )
    zero_counters = receipt.get("strict_zero_counters")
    if not isinstance(zero_counters, Mapping) or set(zero_counters) != set(
        strict_labels._STRICT_SOURCE_ZERO_FIELDS
    ):
        raise StrictLabelPanelRunnerError(
            f"segment strict zero-counter schema drifted for {segment.segment_id}"
        )
    if any(int(value) != 0 for value in zero_counters.values()):
        raise StrictLabelPanelRunnerError(
            f"segment strict source audit failed for {segment.segment_id}"
        )
    baseline = receipt.get("strict_counter_baseline")
    if not isinstance(baseline, Mapping) or set(baseline) != set(
        strict_labels._STRICT_SOURCE_ZERO_FIELDS
    ):
        raise StrictLabelPanelRunnerError(
            f"segment strict counter baseline drifted for {segment.segment_id}"
        )
    target_starts = receipt.get("target_start_states")
    if not isinstance(target_starts, list) or len(target_starts) != len(
        segment.target_days
    ):
        raise StrictLabelPanelRunnerError(
            f"segment target-start audit drifted for {segment.segment_id}"
        )
    for day, state in zip(segment.target_days, target_starts, strict=True):
        if not isinstance(state, Mapping) or state.get("target_day") != day:
            raise StrictLabelPanelRunnerError(
                f"segment target-start order drifted for {segment.segment_id}"
            )
        if state.get("initialized") is not True:
            raise StrictLabelPanelRunnerError(
                f"segment target start is uninitialized for {day}"
            )
        if state.get("initialization_source") != "snapshot":
            raise StrictLabelPanelRunnerError(
                f"segment target start is not snapshot-seeded for {day}"
            )


def _prebuild_native_segment(
    segment: SourceSegment,
    *,
    native_cache: Path,
) -> dict[str, Any]:
    """Materialize and strict-sequence replay one deduplicated source segment."""

    strict_spec = strict_labels.strict_baseline._spec()
    anchor_day = (_parse_utc_day(segment.start_day) + timedelta(days=1)).isoformat()
    base_tape = strict_labels.strict_baseline._native_tape(
        strict_spec,
        day=anchor_day,
        cache_dir=Path(native_cache),
    )
    continuation_hours = (len(segment.source_days) - 2) * 24
    tape = type(base_tape)(
        raw_root=base_tape.raw_root,
        day=anchor_day,
        symbol=base_tape.symbol,
        tick_size=base_tape.tick_size,
        exchange=base_tape.exchange,
        warmup_hours=24,
        continuation_hours=continuation_hours,
        strict_complete=True,
        cache_dir=Path(native_cache),
        cache_enabled=True,
        refresh_cache=False,
        cache_read_only=False,
    )
    cache_contract = tape.materialize_cache(verify_sha256=True)
    if int(cache_contract["expected_hour_count"]) != segment.hour_count:
        raise StrictLabelPanelRunnerError(
            f"segment expected-hour count drifted for {segment.segment_id}"
        )
    if int(cache_contract["complete_hour_count"]) != segment.hour_count:
        raise StrictLabelPanelRunnerError(
            f"segment cache is incomplete for {segment.segment_id}"
        )
    expected_hours = _expected_segment_hours(segment)
    observed_hours = tuple(
        str(row["utc_hour"]) for row in cache_contract["hours"]
    )
    if observed_hours != expected_hours:
        raise StrictLabelPanelRunnerError(
            f"segment cache hour order drifted for {segment.segment_id}"
        )

    validation_tape = strict_labels._clone_native_tape(
        tape, cache_read_only=True
    )
    scheduler = strict_labels.HistoricalExchangeBookScheduler(
        validation_tape,
        strict_sequence=True,
        strict_after_ns=int(validation_tape.day_start_ns),
        allow_delta_bootstrap=False,
    )
    target_start_states: list[dict[str, Any]] = []
    strict_counter_baseline: dict[str, int] | None = None
    for target_day in segment.target_days:
        target_start_ns = int(
            datetime.combine(
                _parse_utc_day(target_day),
                datetime.min.time(),
                tzinfo=UTC,
            ).timestamp()
            * 1_000_000_000
        )
        scheduler.advance_to(target_start_ns, inclusive=False)
        start_stats = scheduler.stats_dict()
        start_state = {
            "target_day": target_day,
            "target_start_ts_ns": target_start_ns,
            "initialized": bool(scheduler.sequence.initialized),
            "initialization_source": str(
                scheduler.sequence.initialization_source or ""
            ),
            "segment_id": int(scheduler.segment_id),
        }
        if start_state["initialized"] is not True:
            raise StrictLabelPanelRunnerError(
                f"segment target start is uninitialized: {target_day}"
            )
        if start_state["initialization_source"] != "snapshot":
            raise StrictLabelPanelRunnerError(
                f"segment target start is not snapshot-seeded: {target_day}"
            )
        target_start_states.append(start_state)
        if strict_counter_baseline is None:
            strict_counter_baseline = {
                field: int(start_stats[field])
                for field in strict_labels._STRICT_SOURCE_ZERO_FIELDS
            }
    scheduler.advance_to(2**63 - 1)
    source_stats = scheduler.stats_dict()
    if int(source_stats["consumed_events"]) <= 0:
        raise StrictLabelPanelRunnerError(
            f"segment contains no source events: {segment.segment_id}"
        )
    if source_stats["initialized"] is not True:
        raise StrictLabelPanelRunnerError(
            f"segment did not finish initialized: {segment.segment_id}"
        )
    if strict_counter_baseline is None:
        raise StrictLabelPanelRunnerError(
            f"segment has no strict target boundary: {segment.segment_id}"
        )
    zero_counters = {
        field: int(source_stats[field]) - int(strict_counter_baseline[field])
        for field in strict_labels._STRICT_SOURCE_ZERO_FIELDS
    }
    if any(zero_counters.values()):
        raise StrictLabelPanelRunnerError(
            f"segment strict source audit failed: {zero_counters}"
        )
    validation_cache_stats = validation_tape.cache_stats()
    if int(validation_cache_stats["hour_failures_fallback_to_source"]) != 0:
        raise StrictLabelPanelRunnerError(
            f"segment cache validation fell back to source: {segment.segment_id}"
        )
    if int(validation_cache_stats["hour_hits"]) != segment.hour_count:
        raise StrictLabelPanelRunnerError(
            f"segment cache validation did not consume each hour once: {segment.segment_id}"
        )

    return {
        "schema_version": f"{RUNNER_IDENTITY}.native_cache_segment_receipt.v3",
        "identity": IDENTITY,
        "segment_id": segment.segment_id,
        "start_day": segment.start_day,
        "end_day": segment.end_day,
        "source_days": list(segment.source_days),
        "target_days": list(segment.target_days),
        "anchor_day": anchor_day,
        "strict_start_day": segment.target_days[0],
        "hour_count": segment.hour_count,
        "native_cache_root": str(Path(native_cache).expanduser().resolve()),
        "cache_contract_sha256": str(
            cache_contract["canonical_identity_sha256"]
        ),
        "hours": list(cache_contract["hours"]),
        "source_scheduler_stats": source_stats,
        "strict_counter_baseline": strict_counter_baseline,
        "strict_zero_counters": zero_counters,
        "target_start_states": target_start_states,
        "materialization_cache_stats": tape.cache_stats(),
        "validation_cache_stats": validation_cache_stats,
        "scheduler_replay_count": 1,
        "target_day_scheduler_replay_count": 0,
        "economic_outcomes_read": False,
        "arms_run": False,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def _ensure_segment_receipt(
    segment: SourceSegment,
    *,
    native_cache: Path,
    receipt_path: Path,
) -> dict[str, Any]:
    if receipt_path.is_file():
        receipt = _load_json(receipt_path)
        _validate_segment_receipt(
            receipt, segment=segment, native_cache=native_cache
        )
        return receipt
    started = time.monotonic()
    receipt = _prebuild_native_segment(segment, native_cache=native_cache)
    receipt["elapsed_seconds"] = round(time.monotonic() - started, 6)
    sealed = _seal_receipt(receipt)
    _atomic_json(receipt_path, sealed)
    return sealed


def _fork_segment_task(task: SegmentTask) -> int:
    pid = os.fork()
    if pid != 0:
        return pid
    try:
        try:
            receipt = _ensure_segment_receipt(
                task.segment,
                native_cache=Path(task.native_cache),
                receipt_path=Path(task.receipt_path),
            )
        except BaseException as exc:  # noqa: BLE001 - child persists failure
            _atomic_json(
                Path(task.result_path),
                {"ok": False, "error": f"{type(exc).__name__}: {exc}"},
            )
            os._exit(1)
        _atomic_json(
            Path(task.result_path),
            {
                "ok": True,
                "segment_id": task.segment.segment_id,
                "receipt_sha256": receipt["canonical_identity_sha256"],
            },
        )
        os._exit(0)
    except BaseException:
        os._exit(2)


def _run_segment_prebuilds(
    segments: Sequence[SourceSegment],
    *,
    native_cache: Path,
    segment_directory: Path,
    result_directory: Path,
    max_workers: int,
) -> list[dict[str, Any]]:
    receipts: dict[str, dict[str, Any]] = {}
    pending: list[SourceSegment] = []
    for segment in segments:
        receipt_path = segment_directory / f"{segment.segment_id}.json"
        if receipt_path.is_file():
            receipts[segment.segment_id] = _ensure_segment_receipt(
                segment,
                native_cache=native_cache,
                receipt_path=receipt_path,
            )
        else:
            pending.append(segment)

    if max_workers == 1:
        for index, segment in enumerate(pending, start=1):
            receipt_path = segment_directory / f"{segment.segment_id}.json"
            receipts[segment.segment_id] = _ensure_segment_receipt(
                segment,
                native_cache=native_cache,
                receipt_path=receipt_path,
            )
            print(
                f"[strict-label-cache] segment={segment.segment_id} "
                f"status=complete progress={index}/{len(pending)}",
                file=sys.stderr,
                flush=True,
            )
    else:
        iterator = iter(pending)
        running: dict[int, tuple[SourceSegment, Path]] = {}

        def submit_next() -> bool:
            try:
                segment = next(iterator)
            except StopIteration:
                return False
            result_path = result_directory / (
                f"{segment.segment_id}.{uuid.uuid4().hex}.json"
            )
            task = SegmentTask(
                segment=segment,
                native_cache=str(native_cache),
                receipt_path=str(
                    segment_directory / f"{segment.segment_id}.json"
                ),
                result_path=str(result_path),
            )
            running[_fork_segment_task(task)] = (segment, result_path)
            return True

        for _ in range(min(max_workers, len(pending))):
            submit_next()
        while running:
            pid, wait_status = os.waitpid(-1, 0)
            if pid not in running:
                raise StrictLabelPanelRunnerError(
                    f"reaped unknown segment child pid={pid}"
                )
            segment, result_path = running.pop(pid)
            result = _load_json(result_path) if result_path.is_file() else {}
            if not (
                os.WIFEXITED(wait_status)
                and os.WEXITSTATUS(wait_status) == 0
                and result.get("ok") is True
            ):
                raise StrictLabelPanelRunnerError(
                    f"segment prebuild failed for {segment.segment_id}: "
                    f"{result.get('error', wait_status)}"
                )
            receipt_path = segment_directory / f"{segment.segment_id}.json"
            receipts[segment.segment_id] = _ensure_segment_receipt(
                segment,
                native_cache=native_cache,
                receipt_path=receipt_path,
            )
            result_path.unlink(missing_ok=True)
            print(
                f"[strict-label-cache] segment={segment.segment_id} "
                "status=complete",
                file=sys.stderr,
                flush=True,
            )
            submit_next()

    return [receipts[segment.segment_id] for segment in segments]


def _target_hour_strings(day: str) -> tuple[str, ...]:
    target = _parse_utc_day(day)
    return tuple(
        f"{(target + timedelta(days=offset)).isoformat()}T{hour:02d}:00:00Z"
        for offset in (-1, 0, 1)
        for hour in range(24)
    )


def _validate_target_receipt(
    receipt: Mapping[str, Any],
    *,
    day: str,
    native_cache: Path,
) -> None:
    expected = {
        "schema_version": f"{RUNNER_IDENTITY}.native_cache_target_72h_receipt.v3",
        "identity": IDENTITY,
        "day": day,
        "complete_hour_count": 72,
        "native_cache_root": str(native_cache.expanduser().resolve()),
        "derived_from_validated_segment_hours": True,
        "scheduler_replay_count": 0,
        "economic_outcomes_read": False,
        "arms_run": False,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise StrictLabelPanelRunnerError(
                f"target 72h receipt {key} drifted for {day}"
            )
    if receipt.get("canonical_identity_sha256") != _receipt_identity(receipt):
        raise StrictLabelPanelRunnerError(
            f"target 72h receipt identity drifted for {day}"
        )
    hours = receipt.get("hours")
    if not isinstance(hours, list) or len(hours) != 72:
        raise StrictLabelPanelRunnerError(
            f"target 72h receipt hour count drifted for {day}"
        )
    if tuple(str(row.get("utc_hour")) for row in hours) != _target_hour_strings(
        day
    ):
        raise StrictLabelPanelRunnerError(
            f"target 72h receipt hour order drifted for {day}"
        )


def _derive_target_receipts(
    plan: SourceUnionPlan,
    *,
    segment_receipts: Sequence[Mapping[str, Any]],
    segment_directory: Path,
    target_directory: Path,
) -> list[dict[str, Any]]:
    hour_index: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for segment_receipt in segment_receipts:
        segment_id = str(segment_receipt["segment_id"])
        receipt_path = segment_directory / f"{segment_id}.json"
        receipt_sha256 = _sha256(receipt_path)
        for hour_row in segment_receipt["hours"]:
            utc_hour = str(hour_row["utc_hour"])
            if utc_hour in hour_index:
                raise StrictLabelPanelRunnerError(
                    f"deduplicated segment hours overlap at {utc_hour}"
                )
            hour_index[utc_hour] = (
                hour_row,
                {
                    "segment_id": segment_id,
                    "segment_receipt_path": str(receipt_path.resolve()),
                    "segment_receipt_sha256": receipt_sha256,
                },
            )

    target_rows: list[dict[str, Any]] = []
    for day in plan.target_days:
        expected_hours = _target_hour_strings(day)
        missing = tuple(hour for hour in expected_hours if hour not in hour_index)
        if missing:
            raise StrictLabelPanelRunnerError(
                f"target receipt lacks verified source hours for {day}: {missing[:3]}"
            )
        selected = [hour_index[hour] for hour in expected_hours]
        segment_bindings = {
            _canonical_sha256(binding): binding for _, binding in selected
        }
        if len(segment_bindings) != 1:
            raise StrictLabelPanelRunnerError(
                f"target 72h receipt crosses source segments for {day}"
            )
        binding = next(iter(segment_bindings.values()))
        receipt = _seal_receipt(
            {
                "schema_version": (
                    f"{RUNNER_IDENTITY}.native_cache_target_72h_receipt.v3"
                ),
                "identity": IDENTITY,
                "day": day,
                "source_days": [
                    (_parse_utc_day(day) + timedelta(days=offset)).isoformat()
                    for offset in (-1, 0, 1)
                ],
                "complete_hour_count": 72,
                "hours": [dict(hour_row) for hour_row, _ in selected],
                **binding,
                "native_cache_root": str(
                    segment_receipts[0]["native_cache_root"]
                ),
                "derived_from_validated_segment_hours": True,
                "scheduler_replay_count": 0,
                "economic_outcomes_read": False,
                "arms_run": False,
                "nested_oof_run": False,
                "action_authorized": False,
                "live_authorized": False,
            }
        )
        target_path = target_directory / f"{day}.json"
        if target_path.is_file():
            existing = _load_json(target_path)
            if existing != receipt:
                raise StrictLabelPanelRunnerError(
                    f"target 72h receipt drifted for {day}"
                )
        else:
            _atomic_json(target_path, receipt)
        _validate_target_receipt(
            receipt,
            day=day,
            native_cache=Path(str(receipt["native_cache_root"])),
        )
        target_rows.append(
            {
                "day": day,
                "complete_hour_count": 72,
                "receipt_path": str(target_path.resolve()),
                "receipt_sha256": _sha256(target_path),
                "canonical_identity_sha256": receipt[
                    "canonical_identity_sha256"
                ],
                "scheduler_replay_count": 0,
                "economic_outcomes_read": False,
                "arms_run": False,
            }
        )
    return target_rows


def _validate_prebuild_manifest(
    manifest: Mapping[str, Any],
    *,
    plan: SourceUnionPlan,
    formal: bool,
    native_cache: Path,
) -> None:
    expected = {
        "schema_version": f"{RUNNER_IDENTITY}.native_cache_prebuild_union.v3",
        "identity": IDENTITY,
        "formal_full_support_run": bool(formal),
        "ordered_days": list(plan.target_days),
        "unique_source_days": list(plan.unique_source_days),
        "unique_source_day_count": len(plan.unique_source_days),
        "segment_count": len(plan.segments),
        "naive_target_hour_scans": plan.naive_target_hour_scans,
        "unique_source_hours": plan.unique_source_hours,
        "hours_saved": plan.hours_saved,
        "native_cache_root": str(native_cache.expanduser().resolve()),
        "economic_outcomes_read": False,
        "arms_run": False,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise StrictLabelPanelRunnerError(
                f"native cache union manifest {key} drifted"
            )
    if len(manifest.get("segments", ())) != len(plan.segments):
        raise StrictLabelPanelRunnerError("native cache segment receipts drifted")
    if len(manifest.get("days", ())) != len(plan.target_days):
        raise StrictLabelPanelRunnerError("native cache target receipts drifted")
    zero_counters = manifest.get("strict_zero_counters")
    if not isinstance(zero_counters, Mapping) or any(
        int(value) != 0 for value in zero_counters.values()
    ):
        raise StrictLabelPanelRunnerError(
            "native cache union manifest strict counters drifted"
        )
    for segment, row in zip(
        plan.segments, manifest["segments"], strict=True
    ):
        if row.get("segment_id") != segment.segment_id:
            raise StrictLabelPanelRunnerError(
                "native cache segment manifest order drifted"
            )
        receipt_path = Path(str(row.get("receipt_path", "")))
        if not receipt_path.is_file():
            raise StrictLabelPanelRunnerError(
                f"native cache segment receipt is missing: {segment.segment_id}"
            )
        if row.get("receipt_sha256") != _sha256(receipt_path):
            raise StrictLabelPanelRunnerError(
                f"native cache segment receipt hash drifted: {segment.segment_id}"
            )
        receipt = _load_json(receipt_path)
        _validate_segment_receipt(
            receipt, segment=segment, native_cache=native_cache
        )
        if row.get("canonical_identity_sha256") != receipt.get(
            "canonical_identity_sha256"
        ):
            raise StrictLabelPanelRunnerError(
                f"native cache segment identity drifted: {segment.segment_id}"
            )
    for day, row in zip(plan.target_days, manifest["days"], strict=True):
        if row.get("day") != day:
            raise StrictLabelPanelRunnerError(
                "native cache target manifest order drifted"
            )
        receipt_path = Path(str(row.get("receipt_path", "")))
        if not receipt_path.is_file():
            raise StrictLabelPanelRunnerError(
                f"native cache target receipt is missing: {day}"
            )
        if row.get("receipt_sha256") != _sha256(receipt_path):
            raise StrictLabelPanelRunnerError(
                f"native cache target receipt hash drifted: {day}"
            )
        receipt = _load_json(receipt_path)
        _validate_target_receipt(receipt, day=day, native_cache=native_cache)
        if row.get("canonical_identity_sha256") != receipt.get(
            "canonical_identity_sha256"
        ):
            raise StrictLabelPanelRunnerError(
                f"native cache target identity drifted: {day}"
            )


def prebuild_native_panel_cache(
    *,
    output: Path = DEFAULT_OUTPUT,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
    prebuild_workers: int = DEFAULT_PREBUILD_WORKERS,
    engineering_days: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Prebuild the outcome-blind deduplicated native source-day union."""

    started = time.monotonic()
    workers = _validate_workers(
        prebuild_workers,
        name="prebuild_workers",
        cap=MAX_PREBUILD_WORKERS_CAP,
    )
    spec = _load_json(Path(V2_SPEC))
    formal_days, reduced_days, prefix40, added10 = _formal_day_universe(spec)
    days, formal = _selected_days(formal_days, reduced_days, engineering_days)
    plan = _source_union_plan(days, formal=formal)
    destination = _native_prebuild_directory(Path(output), days, formal)
    segment_directory = destination / "segments"
    target_directory = destination / "targets"
    result_directory = destination / "segment_results"
    segment_directory.mkdir(parents=True, exist_ok=True)
    target_directory.mkdir(parents=True, exist_ok=True)
    result_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = destination / "manifest.json"
    native_cache = Path(native_cache)
    lock = _acquire_lock(destination / ".prebuild.lock")
    try:
        if manifest_path.is_file():
            manifest = _load_json(manifest_path)
            _validate_prebuild_manifest(
                manifest,
                plan=plan,
                formal=formal,
                native_cache=native_cache,
            )
            return manifest

        segment_receipts = _run_segment_prebuilds(
            plan.segments,
            native_cache=native_cache,
            segment_directory=segment_directory,
            result_directory=result_directory,
            max_workers=workers,
        )
        target_receipts = _derive_target_receipts(
            plan,
            segment_receipts=segment_receipts,
            segment_directory=segment_directory,
            target_directory=target_directory,
        )
        strict_zero_counters = {
            field: sum(
                int(receipt["strict_zero_counters"][field])
                for receipt in segment_receipts
            )
            for field in strict_labels._STRICT_SOURCE_ZERO_FIELDS
        }
        if any(strict_zero_counters.values()):
            raise StrictLabelPanelRunnerError(
                "native cache union strict source counters are nonzero"
            )
        segment_rows = []
        for segment, receipt in zip(
            plan.segments, segment_receipts, strict=True
        ):
            receipt_path = segment_directory / f"{segment.segment_id}.json"
            segment_rows.append(
                {
                    "segment_id": segment.segment_id,
                    "start_day": segment.start_day,
                    "end_day": segment.end_day,
                    "source_day_count": len(segment.source_days),
                    "hour_count": segment.hour_count,
                    "target_days": list(segment.target_days),
                    "receipt_path": str(receipt_path.resolve()),
                    "receipt_sha256": _sha256(receipt_path),
                    "canonical_identity_sha256": receipt[
                        "canonical_identity_sha256"
                    ],
                    "elapsed_seconds": float(receipt["elapsed_seconds"]),
                    "scheduler_replay_count": 1,
                    "economic_outcomes_read": False,
                    "arms_run": False,
                }
            )
        manifest = {
            "schema_version": f"{RUNNER_IDENTITY}.native_cache_prebuild_union.v3",
            "identity": IDENTITY,
            "formal_full_support_run": bool(formal),
            "ordered_days": list(days),
            "day_count": len(days),
            "prefix40_full_support_count": sum(day in prefix40 for day in days),
            "added10_full_support_count": sum(day in added10 for day in days),
            "unique_source_days": list(plan.unique_source_days),
            "unique_source_day_count": len(plan.unique_source_days),
            "segment_count": len(plan.segments),
            "segments": segment_rows,
            "naive_target_hour_scans": plan.naive_target_hour_scans,
            "unique_source_hours": plan.unique_source_hours,
            "hours_saved": plan.hours_saved,
            "segment_worker_limit": workers,
            "segment_scheduler_replay_count": len(plan.segments),
            "target_day_scheduler_replay_count": 0,
            "target_receipts_derived_without_scheduler_replay": True,
            "native_cache_root": str(native_cache.expanduser().resolve()),
            "days": target_receipts,
            "strict_zero_counters": strict_zero_counters,
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "economic_outcomes_read": False,
            "arms_run": False,
            "nested_oof_run": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        _atomic_json(manifest_path, manifest)
        return manifest
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def _identity_binding(spec_path: Path) -> dict[str, str]:
    runner_path = Path(__file__).resolve()
    strict_path = Path(strict_labels.__file__).resolve()
    execution = strict_labels._execution_identity_hashes()
    return {
        "spec_path": str(spec_path.resolve()),
        "spec_sha256": _sha256(spec_path),
        "runner_code_path": str(runner_path),
        "runner_code_sha256": _sha256(runner_path),
        "strict_label_code_path": str(strict_path),
        "strict_label_code_sha256": _sha256(strict_path),
        **{
            f"execution_{key}": str(value)
            for key, value in sorted(execution.items())
        },
    }


def _run_directory(output: Path, days: Sequence[str], formal: bool) -> Path:
    if formal:
        name = FORMAL_RUN_DIRECTORY_NAME
    else:
        name = f"engineering_subset_v9_{_canonical_sha256(list(days))[:16]}"
    return output / "panel_runner" / name


def _native_prebuild_directory(
    output: Path,
    days: Sequence[str],
    formal: bool,
) -> Path:
    """Keep source-union v3 reusable while isolating label execution roots."""

    if formal:
        name = "formal_full_support_41d"
    else:
        name = f"engineering_subset_{_canonical_sha256(list(days))[:16]}"
    return output / "panel_runner" / name / "native_cache_prebuild_union_v3"


def _day_manifest_path(output: Path, day: str) -> Path:
    return (
        output
        / f"support_identity={strict_labels.FULL_SUPPORT_IDENTITY}"
        / f"feature_block={FEATURE_BLOCK}"
        / f"execution_identity={strict_labels.FORMAL_EXECUTION_IDENTITY}"
        / "days"
        / day
        / "manifest.json"
    )


def _run_one_day(task: DayTask) -> dict[str, Any]:
    output = Path(task.output)
    payload = strict_labels.run_day(
        task.day,
        feature_block=FEATURE_BLOCK,
        support_identity=strict_labels.FULL_SUPPORT_IDENTITY,
        max_opportunities=MAX_OPPORTUNITIES,
        output=output,
        cache_root=Path(task.cache_root),
        native_cache=Path(task.native_cache),
        native_cache_receipt=Path(task.native_cache_receipt),
    )
    if payload.get("target_day") != task.day:
        raise StrictLabelPanelRunnerError("run_day returned a different target day")
    if payload.get("feature_block") != FEATURE_BLOCK:
        raise StrictLabelPanelRunnerError("run_day returned a non-M2 admission")
    if payload.get("source_support_identity") != strict_labels.FULL_SUPPORT_IDENTITY:
        raise StrictLabelPanelRunnerError("run_day returned reduced-support admission")
    if payload.get("max_opportunities") is not MAX_OPPORTUNITIES:
        raise StrictLabelPanelRunnerError("run_day returned a bounded opportunity run")
    if payload.get("schema_version") != strict_labels.DAY_SCHEMA_VERSION:
        raise StrictLabelPanelRunnerError("run_day returned an obsolete day schema")
    manifest_path = _day_manifest_path(output, task.day)
    if not manifest_path.is_file():
        raise StrictLabelPanelRunnerError(
            f"run_day admission lacks manifest: {manifest_path}"
        )
    return {
        "day": task.day,
        "manifest_path": str(manifest_path.resolve()),
        "manifest_size_bytes": int(manifest_path.stat().st_size),
        "manifest_sha256": _sha256(manifest_path),
    }


def _fork_day_task(task: DayTask, result_path: Path) -> int:
    """Fork one day without multiprocessing semaphores or pickle transport."""

    pid = os.fork()
    if pid != 0:
        return pid
    try:
        try:
            admission = _run_one_day(task)
        except BaseException as exc:  # noqa: BLE001 - child must persist failure
            _atomic_json(
                result_path,
                {
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
            os._exit(1)
        _atomic_json(result_path, {"ok": True, "admission": admission})
        os._exit(0)
    except BaseException:
        os._exit(2)


def _initial_progress(
    *,
    days: Sequence[str],
    formal: bool,
    binding: Mapping[str, str],
    output: Path,
    cache_root: Path,
    native_cache: Path,
    native_cache_prebuild: Mapping[str, Any],
) -> dict[str, Any]:
    created_at = _utc_now()
    return {
        "schema_version": PROGRESS_SCHEMA_VERSION,
        "identity": RUNNER_IDENTITY,
        "run_kind": "formal_full_support" if formal else "engineering_subset",
        "ordered_days": list(days),
        "feature_block": FEATURE_BLOCK,
        "max_opportunities": MAX_OPPORTUNITIES,
        "source_support_identity": strict_labels.FULL_SUPPORT_IDENTITY,
        "identity_binding": dict(binding),
        "execution_paths": {
            "output": str(output.resolve()),
            "cache_root": str(cache_root.resolve()),
            "native_cache": str(native_cache.resolve()),
        },
        "native_cache_prebuild": dict(native_cache_prebuild),
        "created_at": created_at,
        "updated_at": created_at,
        "state": "running",
        "days": {
            day: {
                "status": "queued",
                "queued_at": created_at,
                "started_at": None,
                "completed_at": None,
                "failed_at": None,
                "elapsed_seconds": None,
                "attempts": 0,
                "manifest_path": None,
                "manifest_sha256": None,
                "manifest_size_bytes": None,
                "error": None,
            }
            for day in days
        },
        "final_panel_manifest": None,
    }


def _validate_progress(
    progress: Mapping[str, Any],
    *,
    days: Sequence[str],
    formal: bool,
    binding: Mapping[str, str],
    output: Path,
    cache_root: Path,
    native_cache: Path,
    native_cache_prebuild: Mapping[str, Any],
) -> None:
    if progress.get("schema_version") != PROGRESS_SCHEMA_VERSION:
        raise StrictLabelPanelRunnerError("progress schema drifted")
    if progress.get("identity") != RUNNER_IDENTITY:
        raise StrictLabelPanelRunnerError("progress identity drifted")
    if progress.get("identity_binding") != dict(binding):
        raise StrictLabelPanelRunnerError(
            "refusing resume after v2 Spec or execution code identity drift"
        )
    expected_kind = "formal_full_support" if formal else "engineering_subset"
    if progress.get("run_kind") != expected_kind:
        raise StrictLabelPanelRunnerError("progress run kind drifted")
    if progress.get("ordered_days") != list(days):
        raise StrictLabelPanelRunnerError("progress day denominator drifted")
    if progress.get("feature_block") != FEATURE_BLOCK:
        raise StrictLabelPanelRunnerError("progress feature block drifted")
    if progress.get("max_opportunities") is not MAX_OPPORTUNITIES:
        raise StrictLabelPanelRunnerError("progress opportunity limit drifted")
    if progress.get("source_support_identity") != strict_labels.FULL_SUPPORT_IDENTITY:
        raise StrictLabelPanelRunnerError("progress support identity drifted")
    expected_paths = {
        "output": str(output.resolve()),
        "cache_root": str(cache_root.resolve()),
        "native_cache": str(native_cache.resolve()),
    }
    if progress.get("execution_paths") != expected_paths:
        raise StrictLabelPanelRunnerError("progress execution paths drifted")
    if progress.get("native_cache_prebuild") != dict(native_cache_prebuild):
        raise StrictLabelPanelRunnerError(
            "progress native cache prebuild binding drifted"
        )
    rows = progress.get("days")
    if (
        not isinstance(rows, Mapping)
        or len(rows) != len(days)
        or set(rows) != set(days)
    ):
        raise StrictLabelPanelRunnerError("progress day rows drifted")


def _persist_progress(path: Path, progress: dict[str, Any]) -> None:
    progress["updated_at"] = _utc_now()
    _atomic_json(path, progress)


def _validate_completed_days(
    progress: dict[str, Any],
    *,
    output: Path,
    cache_root: Path,
    native_cache: Path,
    target_receipts: Mapping[str, Path],
) -> None:
    for day in progress["ordered_days"]:
        row = progress["days"][day]
        if row["status"] != "completed":
            continue
        validated = _run_one_day(
            DayTask(
                day=day,
                output=str(output),
                cache_root=str(cache_root),
                native_cache=str(native_cache),
                native_cache_receipt=str(target_receipts[day]),
            )
        )
        if row.get("manifest_path") != validated["manifest_path"]:
            raise StrictLabelPanelRunnerError(
                f"completed admission path drifted for {day}"
            )
        if row.get("manifest_sha256") != validated["manifest_sha256"]:
            raise StrictLabelPanelRunnerError(
                f"completed admission hash drifted for {day}"
            )
        if row.get("manifest_size_bytes") != validated["manifest_size_bytes"]:
            raise StrictLabelPanelRunnerError(
                f"completed admission size drifted for {day}"
            )


def _queue_incomplete(progress: dict[str, Any]) -> None:
    now = _utc_now()
    for day in progress["ordered_days"]:
        row = progress["days"][day]
        if row["status"] == "completed":
            continue
        if row.get("error"):
            row["last_error"] = row["error"]
        row.update(
            {
                "status": "queued",
                "queued_at": now,
                "started_at": None,
                "completed_at": None,
                "failed_at": None,
                "elapsed_seconds": None,
                "manifest_path": None,
                "manifest_sha256": None,
                "manifest_size_bytes": None,
                "error": None,
            }
        )
    progress["state"] = "running"
    progress["final_panel_manifest"] = None


def _final_manifest(
    progress: Mapping[str, Any],
    *,
    binding: Mapping[str, str],
    formal_days: Sequence[str],
    prefix40: Sequence[str],
    added10: Sequence[str],
    native_cache_prebuild: Mapping[str, Any],
) -> dict[str, Any]:
    ordered_days = tuple(progress["ordered_days"])
    rows = progress["days"]
    if any(rows[day]["status"] != "completed" for day in ordered_days):
        raise StrictLabelPanelRunnerError("cannot finalize an incomplete panel")
    manifests = [
        {
            "day": day,
            "manifest_path": rows[day]["manifest_path"],
            "manifest_size_bytes": rows[day]["manifest_size_bytes"],
            "manifest_sha256": rows[day]["manifest_sha256"],
        }
        for day in ordered_days
    ]
    prefix_set = set(prefix40)
    added_set = set(added10)
    formal = tuple(ordered_days) == tuple(formal_days)
    return {
        "schema_version": PANEL_SCHEMA_VERSION,
        "identity": RUNNER_IDENTITY,
        "created_at": _utc_now(),
        "run_kind": "formal_full_support" if formal else "engineering_subset",
        "formal_full_support_run": formal,
        "ordered_days": list(ordered_days),
        "day_count": len(ordered_days),
        "prefix40_full_support_count": sum(day in prefix_set for day in ordered_days),
        "added10_full_support_count": sum(day in added_set for day in ordered_days),
        "feature_block": FEATURE_BLOCK,
        "max_opportunities": MAX_OPPORTUNITIES,
        "source_support_identity": strict_labels.FULL_SUPPORT_IDENTITY,
        "formal_schema_chain": {
            "formal_day_schema": strict_labels.DAY_SCHEMA_VERSION,
            "formal_label_panel_schema": FORMAL_LABEL_PANEL_SCHEMA_VERSION,
            "panel_runner_panel_schema": PANEL_SCHEMA_VERSION,
            "panel_runner_progress_schema": PROGRESS_SCHEMA_VERSION,
        },
        "spec_path": binding["spec_path"],
        "spec_sha256": binding["spec_sha256"],
        "runner_code_path": binding["runner_code_path"],
        "runner_code_sha256": binding["runner_code_sha256"],
        "strict_label_code_path": binding["strict_label_code_path"],
        "strict_label_code_sha256": binding["strict_label_code_sha256"],
        "execution_identity_hashes": {
            key.removeprefix("execution_"): value
            for key, value in binding.items()
            if key.startswith("execution_")
        },
        "day_manifests": manifests,
        "native_cache_prebuild": dict(native_cache_prebuild),
        "permissions": {
            "economic_outcomes_read_by_orchestrator": False,
            "economic_outcomes_aggregated": False,
            "nested_oof_run": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }


def _acquire_lock(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="ascii")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise StrictLabelPanelRunnerError(
            f"another panel runner owns the orchestration lock: {path}"
        ) from exc
    return handle


def run_panel(
    *,
    output: Path = DEFAULT_OUTPUT,
    cache_root: Path = DEFAULT_CACHE_ROOT,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
    max_workers: int = DEFAULT_MAX_WORKERS,
    prebuild_workers: int = DEFAULT_PREBUILD_WORKERS,
    engineering_days: Sequence[str] | None = None,
) -> dict[str, Any]:
    workers = _validate_workers(max_workers)
    cache_workers = _validate_workers(
        prebuild_workers,
        name="prebuild_workers",
        cap=MAX_PREBUILD_WORKERS_CAP,
    )
    spec_path = Path(V2_SPEC)
    spec = _load_json(spec_path)
    formal_days, reduced_days, prefix40, added10 = _formal_day_universe(spec)
    days, formal = _selected_days(formal_days, reduced_days, engineering_days)
    output = Path(output)
    cache_root = Path(cache_root)
    native_cache = Path(native_cache)
    prebuild_manifest = prebuild_native_panel_cache(
        output=output,
        native_cache=native_cache,
        prebuild_workers=cache_workers,
        engineering_days=engineering_days,
    )
    prebuild_directory = _native_prebuild_directory(output, days, formal)
    prebuild_manifest_path = prebuild_directory / "manifest.json"
    if not prebuild_manifest_path.is_file():
        raise StrictLabelPanelRunnerError("native cache prebuild manifest is missing")
    native_cache_prebuild = {
        "path": str(prebuild_manifest_path.resolve()),
        "sha256": _sha256(prebuild_manifest_path),
        "unique_source_day_count": int(
            prebuild_manifest["unique_source_day_count"]
        ),
        "segment_count": int(prebuild_manifest["segment_count"]),
        "target_day_scheduler_replay_count": int(
            prebuild_manifest["target_day_scheduler_replay_count"]
        ),
        "economic_outcomes_read": False,
        "arms_run": False,
    }
    target_receipts = {
        str(row["day"]): Path(str(row["receipt_path"]))
        for row in prebuild_manifest["days"]
    }
    if set(target_receipts) != set(days):
        raise StrictLabelPanelRunnerError(
            "native cache prebuild target denominator drifted"
        )
    binding = _identity_binding(spec_path)
    run_directory = _run_directory(output, days, formal)
    progress_path = run_directory / "progress.json"
    final_path = run_directory / "panel_manifest.json"
    lock = _acquire_lock(run_directory / ".runner.lock")
    try:
        if progress_path.exists():
            progress = _load_json(progress_path)
            _validate_progress(
                progress,
                days=days,
                formal=formal,
                binding=binding,
                output=output,
                cache_root=cache_root,
                native_cache=native_cache,
                native_cache_prebuild=native_cache_prebuild,
            )
            _validate_completed_days(
                progress,
                output=output,
                cache_root=cache_root,
                native_cache=native_cache,
                target_receipts=target_receipts,
            )
            final_row = progress.get("final_panel_manifest")
            if progress.get("state") == "completed" and isinstance(final_row, Mapping):
                if not final_path.is_file():
                    raise StrictLabelPanelRunnerError("completed panel manifest is missing")
                if final_row.get("path") != str(final_path.resolve()):
                    raise StrictLabelPanelRunnerError("final panel path drifted")
                if final_row.get("sha256") != _sha256(final_path):
                    raise StrictLabelPanelRunnerError("final panel hash drifted")
                return _load_json(final_path)
            _queue_incomplete(progress)
        else:
            progress = _initial_progress(
                days=days,
                formal=formal,
                binding=binding,
                output=output,
                cache_root=cache_root,
                native_cache=native_cache,
                native_cache_prebuild=native_cache_prebuild,
            )
        _persist_progress(progress_path, progress)

        pending = [
            day for day in days if progress["days"][day]["status"] != "completed"
        ]
        day_iterator = iter(pending)
        running: dict[int, tuple[str, float, Path]] = {}
        child_results = run_directory / "child_results"
        child_results.mkdir(parents=True, exist_ok=True)

        def submit_next() -> bool:
            try:
                day = next(day_iterator)
            except StopIteration:
                return False
            row = progress["days"][day]
            attempt = int(row.get("attempts", 0)) + 1
            row.update(
                {
                    "status": "running",
                    "started_at": _utc_now(),
                    "attempts": attempt,
                    "error": None,
                }
            )
            _persist_progress(progress_path, progress)
            task = DayTask(
                day=day,
                output=str(output),
                cache_root=str(cache_root),
                native_cache=str(native_cache),
                native_cache_receipt=str(target_receipts[day]),
            )
            result_path = child_results / f"{day}.attempt-{attempt}.json"
            if result_path.exists():
                raise StrictLabelPanelRunnerError(
                    f"child result path already exists: {result_path}"
                )
            started = time.monotonic()
            pid = _fork_day_task(task, result_path)
            running[pid] = (day, started, result_path)
            return True

        for _ in range(min(workers, len(pending))):
            submit_next()
        while running:
            pid, wait_status = os.waitpid(-1, 0)
            if pid not in running:
                raise StrictLabelPanelRunnerError(
                    f"reaped unknown day child pid={pid}"
                )
            day, started, result_path = running.pop(pid)
            elapsed = round(time.monotonic() - started, 6)
            row = progress["days"][day]
            child = _load_json(result_path) if result_path.is_file() else {}
            child_ok = bool(
                os.WIFEXITED(wait_status)
                and os.WEXITSTATUS(wait_status) == 0
                and child.get("ok") is True
                and isinstance(child.get("admission"), Mapping)
            )
            if not child_ok:
                error = str(child.get("error") or f"child_wait_status={wait_status}")
                row.update(
                    {
                        "status": "failed",
                        "failed_at": _utc_now(),
                        "elapsed_seconds": elapsed,
                        "error": error,
                    }
                )
                print(
                    f"[strict-label-panel] day={day} status=failed "
                    f"elapsed_s={elapsed:.3f} error={error}",
                    file=sys.stderr,
                    flush=True,
                )
            else:
                admission = dict(child["admission"])
                row.update(
                    {
                        "status": "completed",
                        "completed_at": _utc_now(),
                        "elapsed_seconds": elapsed,
                        "manifest_path": admission["manifest_path"],
                        "manifest_sha256": admission["manifest_sha256"],
                        "manifest_size_bytes": admission["manifest_size_bytes"],
                        "error": None,
                    }
                )
                print(
                    f"[strict-label-panel] day={day} status=completed "
                    f"elapsed_s={elapsed:.3f} "
                    f"manifest_sha256={admission['manifest_sha256']}",
                    file=sys.stderr,
                    flush=True,
                )
            _persist_progress(progress_path, progress)
            submit_next()

        failed = [day for day in days if progress["days"][day]["status"] == "failed"]
        if failed:
            progress["state"] = "failed"
            _persist_progress(progress_path, progress)
            raise StrictLabelPanelRunnerError(
                f"strict-label panel has failed days: {tuple(failed)}"
            )
        panel_manifest = _final_manifest(
            progress,
            binding=binding,
            formal_days=formal_days,
            prefix40=prefix40,
            added10=added10,
            native_cache_prebuild=native_cache_prebuild,
        )
        _atomic_json(final_path, panel_manifest)
        progress["state"] = "completed"
        progress["final_panel_manifest"] = {
            "path": str(final_path.resolve()),
            "sha256": _sha256(final_path),
            "size_bytes": int(final_path.stat().st_size),
        }
        _persist_progress(progress_path, progress)
        return panel_manifest
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    parser.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Formal day-runner workers; keep at 1 for the formal panel.",
    )
    parser.add_argument(
        "--prebuild-workers",
        type=int,
        default=DEFAULT_PREBUILD_WORKERS,
        help="Outcome-blind native-cache prebuild workers.",
    )
    parser.add_argument(
        "--prebuild-only",
        action="store_true",
        help="Materialize and verify D-1/D/D+1 native caches without running arms.",
    )
    parser.add_argument(
        "--engineering-days",
        nargs="+",
        help="Explicit full-support subset; omission runs the formal 41-day panel.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.prebuild_only:
        manifest = prebuild_native_panel_cache(
            output=args.output,
            native_cache=args.native_cache,
            prebuild_workers=args.prebuild_workers,
            engineering_days=args.engineering_days,
        )
    else:
        manifest = run_panel(
            output=args.output,
            cache_root=args.cache_root,
            native_cache=args.native_cache,
            max_workers=args.max_workers,
            prebuild_workers=args.prebuild_workers,
            engineering_days=args.engineering_days,
        )
    print(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
