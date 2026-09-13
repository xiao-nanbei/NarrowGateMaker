#!/usr/bin/env python3
"""Generate strict-native cooldown-v2 one-shot labels from shared prefixes."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import sys
import tempfile
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, fields
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_paths import data_root
from models import backtest_tick as bt
from models import data_windows
from models.exchange_book_replay import HistoricalExchangeBookScheduler
from models.native_exchange_book_cache import native_book_parser_identity
from models.replay.f05_ema_add_wait_two_day_window import (
    F05ReplayDay,
    stitch_two_days,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_native_features import (
    EXCHANGE_WINDOW_READY_CLOCK,
    NativeM2BookFeatureAccumulator,
    NativeM2BookWindowContract,
    NativeM2TradeMergeAccumulator,
    RawNativeM2BookFeatureStream,
    native_m2_book_feature_schema,
    stream_native_m2_causal_observations,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_replay_emitter import (
    CooldownV2ReplayEmitter,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    ARM_RESULT_SCHEMA_VERSION,
    OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
    PosixCooldownSharedPrefixExecutor,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    SCHEMA_VERSION as SHARED_PREFIX_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    CooldownAssignmentSnapshotV2,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_strict_checkpoint import (
    build_strict_native_source_contract_from_single_tape,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_windows import (
    STRICT_EXCHANGE_TIME_PROFILE,
    WindowExtractionAccumulator,
    WindowExtractionContract,
    stream_causal_windows,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as panel,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_strict_native_latency_baseline_50d as strict_baseline,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
RUNNER_IDENTITY = f"{IDENTITY}.strict_native_one_shot_labels.v1"
DAY_SCHEMA_VERSION = f"{RUNNER_IDENTITY}.day.v2"
FORMAL_EXECUTION_IDENTITY = "v9"
FULL_SUPPORT_IDENTITY = "full_D_minus_1_D_D_plus_1"
REDUCED_SUPPORT_IDENTITY = "reduced_source_diagnostic"
V2_SPEC = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_spec_20260810.json"
)
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "strict_native_one_shot_labels"
)
DEFAULT_MARKET_CACHE = DATA_ROOT / (
    "cache/replay_dag/"
    "causal_multichannel_window_boolean_cooldown_duration_v2/market_windows"
)
DEFAULT_NATIVE_CACHE = strict_baseline.DEFAULT_NATIVE_CACHE
EXECUTION_AMENDMENT_V8 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v8_20260811.json"
)
EXECUTION_AMENDMENT_V8_SHA256 = (
    "864a0256eb0b4f209e8bc26875449c5bca05255f3d91a56030c78fc35bfb957b"
)
EXECUTION_AMENDMENT_V9 = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_"
    "execution_amendment_v9_20260811.json"
)

_STRICT_SOURCE_ZERO_FIELDS = (
    "source_gap_events",
    "sequence_gaps",
    "invalid_sequence_messages",
    "message_time_reversals",
    "event_timestamp_fallback_events",
    "receive_timestamp_fallback_events",
    "unknown_timestamp_source_events",
)
_STRICT_SOURCE_RESULT_FIELDS = {
    "source_gap_events": "exchange_book_source_gap_events",
    "sequence_gaps": "exchange_book_sequence_gaps",
    "invalid_sequence_messages": "exchange_book_invalid_sequence_messages",
    "message_time_reversals": "exchange_book_message_time_reversals",
    "event_timestamp_fallback_events": (
        "exchange_book_event_timestamp_fallback_events"
    ),
    "receive_timestamp_fallback_events": (
        "exchange_book_receive_timestamp_fallback_events"
    ),
    "unknown_timestamp_source_events": (
        "exchange_book_unknown_timestamp_source_events"
    ),
}


class StrictLabelError(RuntimeError):
    """Raised when a strict one-shot label input or admission is invalid."""


def _execution_amendment_binding() -> dict[str, Any]:
    """Require the v9 amendment; v8 remains a hash-bound predecessor only."""

    path = EXECUTION_AMENDMENT_V9
    if not path.is_file():
        raise StrictLabelError("execution amendment v9 is required for formal labels")
    payload = _load_json(path)
    schema = str(payload.get("schema_version", ""))
    if schema != f"{IDENTITY}.execution_amendment.v9":
        raise StrictLabelError("execution amendment schema drifted")
    sha256 = _sha256(path)
    predecessor = payload.get("predecessor_execution_amendment")
    if not isinstance(predecessor, Mapping) or predecessor.get("sha256") != (
        EXECUTION_AMENDMENT_V8_SHA256
    ):
        raise StrictLabelError("execution amendment v9 predecessor drifted")
    hardening = payload.get("formal_identity_hardening_replacement")
    if not isinstance(hardening, Mapping):
        raise StrictLabelError("execution amendment lacks formal schema hardening")
    expected = {
        "shared_prefix_schema": SHARED_PREFIX_SCHEMA_VERSION,
        "opportunity_manifest_schema": OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
        "arm_result_schema": ARM_RESULT_SCHEMA_VERSION,
    }
    if any(hardening.get(key) != value for key, value in expected.items()):
        raise StrictLabelError("execution amendment formal schema chain drifted")
    return {
        "path": str(path.resolve()),
        "sha256": sha256,
        "schema_version": schema,
        "formal_schema_chain": {
            **expected,
            "formal_day_schema": DAY_SCHEMA_VERSION,
        },
    }


_SNAPSHOT_PARQUET_SCHEMA = pa.schema(
    [
        ("snapshot_id", pa.string()),
        ("assignment_id", pa.string()),
        ("fill_event_id", pa.string()),
        ("client_order_id", pa.string()),
        ("lineage_id", pa.string()),
        ("lineage_revision", pa.int64()),
        ("partial_fill_ordinal", pa.int64()),
        ("partial_fill_qty_btc", pa.float64()),
        ("visibility_profile", pa.string()),
        ("receive_time_transport_eligible", pa.bool_()),
        ("source_bundle_sha256", pa.string()),
        ("feature_block", pa.string()),
        ("m0_context_json", pa.string()),
        ("feature_row_json", pa.string()),
        ("snapshot_payload_json", pa.string()),
        ("snapshot_payload_sha256", pa.string()),
        ("policy_input_valid", pa.bool_()),
        ("fallback_policy_id", pa.string()),
        ("fallback_reason", pa.string()),
        ("economic_outcomes_read", pa.bool_()),
    ]
)


class _SnapshotSpool:
    """Parent-only, unbuffered snapshot spool converted after all forks exit."""

    def __init__(self, *, day: str) -> None:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{IDENTITY}.{day}.snapshots.",
            suffix=".jsonl",
            dir=tempfile.gettempdir(),
        )
        self.path = Path(raw_path)
        self._descriptor: int | None = descriptor
        self.rows = 0

    def append(self, row: Mapping[str, Any]) -> None:
        if self._descriptor is None:
            raise StrictLabelError("snapshot spool is already closed")
        payload = (
            json.dumps(
                dict(row),
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        view = memoryview(payload)
        while view:
            written = os.write(self._descriptor, view)
            if written <= 0:
                raise StrictLabelError("snapshot spool write did not progress")
            view = view[written:]
        self.rows += 1

    def close(self) -> None:
        if self._descriptor is None:
            return
        os.close(self._descriptor)
        self._descriptor = None

    def discard(self) -> None:
        self.close()
        self.path.unlink(missing_ok=True)

    def write_parquet(self, destination: Path, *, batch_rows: int = 8) -> int:
        if batch_rows <= 0:
            raise StrictLabelError("snapshot parquet batch size must be positive")
        self.close()
        writer: pq.ParquetWriter | None = None
        written_rows = 0
        batch: list[dict[str, Any]] = []
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    batch.append(json.loads(line))
                    if len(batch) < batch_rows:
                        continue
                    table = pa.Table.from_pylist(
                        batch,
                        schema=_SNAPSHOT_PARQUET_SCHEMA,
                    )
                    if writer is None:
                        writer = pq.ParquetWriter(
                            destination,
                            _SNAPSHOT_PARQUET_SCHEMA,
                            compression="zstd",
                        )
                    writer.write_table(table)
                    written_rows += len(batch)
                    batch.clear()
                if batch:
                    table = pa.Table.from_pylist(
                        batch,
                        schema=_SNAPSHOT_PARQUET_SCHEMA,
                    )
                    if writer is None:
                        writer = pq.ParquetWriter(
                            destination,
                            _SNAPSHOT_PARQUET_SCHEMA,
                            compression="zstd",
                        )
                    writer.write_table(table)
                    written_rows += len(batch)
            if writer is None:
                raise StrictLabelError("snapshot spool is empty")
            writer.close()
            writer = None
            with destination.open("rb") as handle:
                os.fsync(handle.fileno())
            if written_rows != self.rows:
                raise StrictLabelError("snapshot spool row count drifted")
            return written_rows
        finally:
            if writer is not None:
                writer.close()
            self.path.unlink(missing_ok=True)


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _frozen_audit_payload(value: Any) -> dict[str, Any]:
    """Expand a frozen audit without deepcopying read-only mapping proxies."""

    payload: dict[str, Any] = {}
    for field in fields(value):
        item = getattr(value, field.name)
        payload[field.name] = dict(item) if isinstance(item, Mapping) else item
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise StrictLabelError(f"JSON root is not an object: {path}")
    return payload


def _support_days(spec: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    """Return the frozen full/reduced day sets without probing live storage."""

    ordered = spec.get("ordered_utc_days", {})
    frozen = tuple((*ordered.get("prefix40", ()), *ordered.get("added10", ())))
    reduced = set(
        spec.get("source_separation", {})
        .get("strict_native_2026", {})
        .get("reduced_support_days", ())
    )
    if len(frozen) != 50 or len(set(frozen)) != 50:
        raise StrictLabelError("v2 frozen 50-day denominator drifted")
    if not reduced or reduced - set(frozen):
        raise StrictLabelError("v2 reduced-support day identity drifted")
    return set(frozen) - reduced, reduced


def _validate_support_identity(
    *, day: str, support_identity: str, spec: Mapping[str, Any]
) -> None:
    full, reduced = _support_days(spec)
    expected = (
        full
        if support_identity == FULL_SUPPORT_IDENTITY
        else reduced
        if support_identity == REDUCED_SUPPORT_IDENTITY
        else None
    )
    if expected is None:
        raise StrictLabelError("unknown source-support identity")
    if day not in expected:
        actual = (
            FULL_SUPPORT_IDENTITY
            if day in full
            else REDUCED_SUPPORT_IDENTITY
            if day in reduced
            else "outside_frozen_panel"
        )
        raise StrictLabelError(
            f"{day} belongs to {actual}, not requested {support_identity}"
        )


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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _trade_path(day: str) -> Path:
    root = bt.RAW_TRADES_DIR / bt.SYMBOL
    candidates = (
        root / f"{bt.SYMBOL}-trades-{day}.csv",
        root / f"{bt.SYMBOL}-trades-{day}.csv.gz",
    )
    present = [path for path in candidates if path.is_file()]
    if len(present) != 1:
        raise StrictLabelError(
            f"individual-trade source must be unique for {day}: {present}"
        )
    return present[0]


def _file_set_identity(paths: Sequence[Path]) -> str:
    rows = [
        {
            "name": path.name,
            "size_bytes": int(path.stat().st_size),
            "sha256": _sha256(path),
        }
        for path in paths
    ]
    return _canonical_sha256(rows)


def _clone_native_tape(
    tape: Any,
    *,
    cache_read_only: bool,
) -> Any:
    return type(tape)(
        raw_root=tape.raw_root,
        day=tape.day,
        symbol=tape.symbol,
        tick_size=tape.tick_size,
        exchange=tape.exchange,
        warmup_hours=tape.warmup_hours,
        continuation_hours=tape.continuation_hours,
        strict_complete=True,
        cache_dir=tape.cache_dir,
        cache_enabled=True,
        refresh_cache=False,
        cache_read_only=cache_read_only,
    )


def _prebuild_strict_native_cache(tape: Any) -> tuple[Any, dict[str, Any]]:
    """Build all 72 hours once, then validate them through a read-only pass."""

    def report_materialization(
        index: int,
        total: int,
        path: Path,
        cache_hit: bool,
    ) -> None:
        print(
            "[native-cache] "
            f"materialize={index}/{total} "
            f"status={'hit' if cache_hit else 'built'} "
            f"hour={path.parent.parent.name}T{path.parent.name}:00Z",
            file=sys.stderr,
            flush=True,
        )

    cache_contract = tape.materialize_cache(
        verify_sha256=True,
        progress=report_materialization,
    )
    if int(cache_contract["expected_hour_count"]) != 72:
        raise StrictLabelError("strict native cache must bind exactly 72 hours")
    if int(cache_contract["complete_hour_count"]) != 72:
        raise StrictLabelError("strict native cache is incomplete")

    validation_tape = _clone_native_tape(tape, cache_read_only=True)
    scheduler = HistoricalExchangeBookScheduler(
        validation_tape,
        strict_sequence=True,
        strict_after_ns=int(validation_tape.day_start_ns),
        allow_delta_bootstrap=False,
    )
    total_hours = int(cache_contract["complete_hour_count"])
    hour_ns = 3_600_000_000_000
    for index in range(1, total_hours + 1):
        scheduler.advance_to(
            int(validation_tape.process_start_ns) + index * hour_ns - 1
        )
        print(
            f"[native-cache] validate={index}/{total_hours}",
            file=sys.stderr,
            flush=True,
        )
    scheduler.advance_to(2**63 - 1)
    source_stats = scheduler.stats_dict()
    if int(source_stats["consumed_events"]) <= 0:
        raise StrictLabelError("strict native cache contains no source events")
    if source_stats["initialized"] is not True:
        raise StrictLabelError("strict native cache did not finish initialized")
    violations = {
        field: int(source_stats[field])
        for field in _STRICT_SOURCE_ZERO_FIELDS
        if int(source_stats[field]) != 0
    }
    if violations:
        raise StrictLabelError(
            f"strict native cache source audit failed: {violations}"
        )
    validation_cache_stats = validation_tape.cache_stats()
    if int(validation_cache_stats["hour_failures_fallback_to_source"]) != 0:
        raise StrictLabelError("read-only cache validation fell back to source")
    if int(validation_cache_stats["hour_hits"]) != 72:
        raise StrictLabelError("read-only cache validation did not consume 72 hours")

    audit: dict[str, Any] = {
        "schema_version": f"{RUNNER_IDENTITY}.native_cache_prebuild.v1",
        "cache_contract": cache_contract,
        "source_scheduler_stats": source_stats,
        "materialization_cache_stats": tape.cache_stats(),
        "validation_cache_stats": validation_cache_stats,
        "fork_cache_mode": "read_only_fail_closed",
        "strict_zero_fields": list(_STRICT_SOURCE_ZERO_FIELDS),
        "economic_outcomes_read": False,
    }
    audit["canonical_identity_sha256"] = _canonical_sha256(audit)
    return _clone_native_tape(tape, cache_read_only=True), audit


def _target_72h_hours(day: str) -> tuple[str, ...]:
    target = date.fromisoformat(str(day))
    return tuple(
        f"{(target + timedelta(days=offset)).isoformat()}T{hour:02d}:00:00Z"
        for offset in (-1, 0, 1)
        for hour in range(24)
    )


def _bind_prebuilt_strict_native_cache(
    tape: Any,
    *,
    day: str,
    receipt_path: Path,
) -> tuple[Any, dict[str, Any]]:
    """Bind one target-day receipt without repeating the 72h SHA/sequence scan."""

    path = Path(receipt_path).expanduser().resolve()
    receipt = _load_json(path)
    expected_schema = (
        "causal_multichannel_window_boolean_cooldown_duration_v2."
        "strict_label_panel_runner.v1.native_cache_target_72h_receipt.v3"
    )
    expected = {
        "schema_version": expected_schema,
        "identity": IDENTITY,
        "day": str(day),
        "complete_hour_count": 72,
        "native_cache_root": str(Path(tape.cache_dir).expanduser().resolve()),
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
            raise StrictLabelError(
                f"prebuilt native target receipt {key} drifted for {day}"
            )
    body = dict(receipt)
    observed_identity = str(body.pop("canonical_identity_sha256", ""))
    if observed_identity != _canonical_sha256(body):
        raise StrictLabelError(
            f"prebuilt native target receipt identity drifted for {day}"
        )
    hours = receipt.get("hours")
    if not isinstance(hours, list) or len(hours) != 72:
        raise StrictLabelError(
            f"prebuilt native target receipt hours drifted for {day}"
        )
    if tuple(str(row.get("utc_hour")) for row in hours) != _target_72h_hours(day):
        raise StrictLabelError(
            f"prebuilt native target receipt hour order drifted for {day}"
        )
    segment_path = Path(str(receipt.get("segment_receipt_path", "")))
    if not segment_path.is_file():
        raise StrictLabelError(
            f"prebuilt native segment receipt is missing for {day}"
        )
    if str(receipt.get("segment_receipt_sha256", "")) != _sha256(segment_path):
        raise StrictLabelError(
            f"prebuilt native segment receipt hash drifted for {day}"
        )
    audit = {
        "schema_version": f"{RUNNER_IDENTITY}.prebuilt_native_cache_binding.v1",
        "target_day": str(day),
        "target_receipt_path": str(path),
        "target_receipt_sha256": _sha256(path),
        "target_receipt_identity_sha256": observed_identity,
        "segment_receipt_path": str(segment_path.resolve()),
        "segment_receipt_sha256": str(receipt["segment_receipt_sha256"]),
        "complete_hour_count": 72,
        "native_cache_root": str(Path(tape.cache_dir).expanduser().resolve()),
        "cache_read_only": True,
        "target_scheduler_replay_count": 0,
        "segment_scheduler_replay_count": 1,
        "economic_outcomes_read": False,
    }
    audit["canonical_identity_sha256"] = _canonical_sha256(audit)
    return _clone_native_tape(tape, cache_read_only=True), audit


def _validate_day_admission(
    directory: Path,
    *,
    expected_day: str,
    expected_feature_block: str,
    expected_support_identity: str,
    expected_max_opportunities: int | None,
) -> dict[str, Any]:
    manifest_path = directory / "manifest.json"
    success_path = directory / "_SUCCESS"
    snapshots_path = directory / "assignment_snapshots.parquet"
    source_path = directory / "source_contract.json"
    expected_files = {
        "manifest.json",
        "_SUCCESS",
        "assignment_snapshots.parquet",
        "source_contract.json",
    }
    if not directory.is_dir() or {path.name for path in directory.iterdir()} != (
        expected_files
    ):
        raise StrictLabelError(f"day admission is incomplete: {directory}")
    manifest = _load_json(manifest_path)
    success = _load_json(success_path)
    if manifest.get("schema_version") != DAY_SCHEMA_VERSION:
        raise StrictLabelError("day admission schema drifted")
    if manifest.get("identity") != RUNNER_IDENTITY:
        raise StrictLabelError("day admission identity drifted")
    if manifest.get("target_day") != expected_day:
        raise StrictLabelError("day admission target drifted")
    if manifest.get("feature_block") != expected_feature_block:
        raise StrictLabelError("day admission feature block drifted")
    if manifest.get("source_support_identity") != expected_support_identity:
        raise StrictLabelError("day admission source-support identity drifted")
    if manifest.get("max_opportunities") != expected_max_opportunities:
        raise StrictLabelError("day admission opportunity limit drifted")
    if manifest.get("execution_amendment") != _execution_amendment_binding():
        raise StrictLabelError("day admission execution amendment drifted")
    parent_stop = manifest.get("parent_stop_audit")
    if not isinstance(parent_stop, Mapping):
        raise StrictLabelError("day admission lacks parent-stop audit")
    target_end_ms = int(
        (
            datetime.fromisoformat(expected_day).replace(tzinfo=UTC)
            + timedelta(days=1)
        ).timestamp()
        * 1_000
    )
    common_parent_stop_valid = bool(
        parent_stop.get("configured_stop_ts_ms") == target_end_ms
        and parent_stop.get("triggered") is True
        and int(parent_stop.get("new_assignments_after_target_day_boundary", -1))
        == 0
    )
    if expected_max_opportunities is None:
        parent_stop_valid = bool(
            common_parent_stop_valid
            and parent_stop.get("reason") == "target_day_end"
            and parent_stop.get("target_day_boundary_observed") is True
            and int(parent_stop.get("trigger_ts_ms", 0)) >= target_end_ms
        )
    else:
        shared_prefix_audit = manifest.get("shared_prefix_execution_audit")
        parent_stop_valid = bool(
            common_parent_stop_valid
            and parent_stop.get("reason") == "max_opportunities_reached"
            and parent_stop.get("target_day_boundary_observed") is False
            and isinstance(shared_prefix_audit, Mapping)
            and int(shared_prefix_audit.get("opportunities_dispatched", 0))
            + int(shared_prefix_audit.get("opportunities_resumed", 0))
            >= int(expected_max_opportunities)
        )
    if not parent_stop_valid:
        raise StrictLabelError("day admission parent-stop contract failed")
    strict_native_queue = manifest.get("strict_native_queue")
    if not isinstance(strict_native_queue, Mapping):
        raise StrictLabelError("day admission lacks strict-native queue audit")
    missing_count = int(
        strict_native_queue.get("missing_queue_seed_count", -1)
    )
    missing_trace = strict_native_queue.get("missing_queue_seed_trace")
    if not isinstance(missing_trace, list) or len(missing_trace) != missing_count:
        raise StrictLabelError("day admission queue-missing trace drifted")
    source_gaps = int(strict_native_queue.get("source_gap_events", -1))
    source_counters = manifest.get("strict_native_source_counters")
    if not isinstance(source_counters, Mapping) or set(source_counters) != set(
        _STRICT_SOURCE_ZERO_FIELDS
    ):
        raise StrictLabelError("day admission strict source counters drifted")
    if any(int(source_counters[field]) != 0 for field in _STRICT_SOURCE_ZERO_FIELDS):
        raise StrictLabelError("day admission strict source audit failed")
    if expected_max_opportunities is None and (
        missing_count != 0 or source_gaps != 0
    ):
        raise StrictLabelError(
            "formal day admission has incomplete strict-native queue evidence"
        )
    if expected_max_opportunities is None and strict_native_queue.get(
        "missing_trace_unbounded"
    ) is not True:
        raise StrictLabelError("formal day admission truncated queue-missing trace")
    if success.get("manifest_sha256") != _sha256(manifest_path):
        raise StrictLabelError("day admission success marker drifted")
    if manifest.get("assignment_snapshots", {}).get("sha256") != _sha256(
        snapshots_path
    ):
        raise StrictLabelError("day admission snapshot hash drifted")
    if manifest.get("source_contract", {}).get("sha256") != _sha256(source_path):
        raise StrictLabelError("day admission source-contract hash drifted")
    label_rows = manifest.get("one_shot_label_manifests")
    if not isinstance(label_rows, list) or not label_rows:
        raise StrictLabelError("day admission lacks one-shot label manifests")
    for row in label_rows:
        if set(row) != {"path", "size_bytes", "sha256"}:
            raise StrictLabelError("one-shot label manifest schema drifted")
        label_path = Path(str(row["path"]))
        if not label_path.is_file():
            raise StrictLabelError("one-shot label manifest is missing")
        if int(row["size_bytes"]) != int(label_path.stat().st_size):
            raise StrictLabelError("one-shot label manifest size drifted")
        if str(row["sha256"]) != _sha256(label_path):
            raise StrictLabelError("one-shot label manifest hash drifted")
        label_manifest = _load_json(label_path)
        if label_manifest.get("schema_version") != OPPORTUNITY_MANIFEST_SCHEMA_VERSION:
            raise StrictLabelError("one-shot opportunity schema drifted")
    return manifest


def _create_day_lock(
    path: Path, *, day: str, feature_block: str, support_identity: str
) -> None:
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise StrictLabelError(
            f"day execution lock exists; refusing concurrent or stale run: {path}"
        ) from exc
    try:
        payload = json.dumps(
            {
                "schema_version": f"{RUNNER_IDENTITY}.day_lock.v1",
                "target_day": day,
                "feature_block": feature_block,
                "source_support_identity": support_identity,
                "owner_pid": os.getpid(),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _execution_identity_hashes() -> dict[str, str]:
    spec = _load_json(V2_SPEC)
    baseline_path = ROOT / spec["baseline"]["operational_identity_path"]
    baseline = _load_json(baseline_path)
    expected_baseline = str(spec["baseline"]["operational_identity_sha256"])
    if _sha256(baseline_path) != expected_baseline:
        raise StrictLabelError("operational baseline identity drifted")
    if baseline["model"]["heads_loaded"] != 13:
        raise StrictLabelError("operational model is not the 13-head baseline")
    code_paths = (
        ROOT / "models/backtest_tick.py",
        ROOT / "models/exchange_book_replay.py",
        ROOT / "models/replay/f05_ema_add_wait_two_day_window.py",
        Path(__file__).resolve(),
        ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_features.py",
        ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_windows.py",
        ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_native_features.py",
        ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_snapshot.py",
        ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_replay_emitter.py",
        ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_strict_checkpoint.py",
        ROOT
        / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_shared_prefix.py",
    )
    missing_code = [str(path) for path in code_paths if not path.is_file()]
    if missing_code:
        raise StrictLabelError(f"execution code bundle is incomplete: {missing_code}")
    code_sha256 = _canonical_sha256(
        {
            "execution_files": [
                {
                    "path": str(path.relative_to(ROOT)),
                    "sha256": _sha256(path),
                }
                for path in code_paths
            ],
            "v2_spec_sha256": _sha256(V2_SPEC),
        }
    )
    return {
        "baseline_identity_sha256": expected_baseline,
        "config_sha256": str(spec["baseline"]["config_sha256"]),
        "code_sha256": code_sha256,
        "model_sha256": str(baseline["model"]["bundle_meta_sha256"]),
        "p3_sha256": str(baseline["p3"]["sha256"]),
        "feature_dag_sha256": str(baseline["model"]["feature_dag_sha256"]),
        "execution_abi_sha256": str(
            baseline["prospective_epoch"]["execution_abi_sha256"]
        ),
    }


def _day_binding_identities(binding: Mapping[str, Any]) -> dict[str, str]:
    values = {
        "window_sha256": str(binding["window_sha256"]),
        "overlay_manifest_sha256": str(binding["overlay_manifest_sha256"]),
    }
    if any(len(value) != 64 for value in values.values()):
        raise StrictLabelError("replay-day binding is incomplete")
    return values


def _generic_replay_day(
    day: str,
    *,
    params: dict[str, Any],
    panel_spec: Mapping[str, Any],
    cache_root: Path,
) -> F05ReplayDay:
    feature_manifest, features = panel._feature_receipts(panel_spec)
    target_path = features.get(day)
    if target_path is None:
        raise StrictLabelError(f"v12 feature manifest lacks continuation day {day}")
    prior_day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    prior_path = features.get(prior_day)
    if prior_path is None:
        prior_path = panel._boundary_path(cache_root, prior_day)
        if not prior_path.is_file():
            config_path = panel._resolve_repo_path(
                panel_spec["sources"]["feature_generation_config_path"]
            )
            panel._materialize_boundary_feature(
                prior_day=prior_day,
                target_day=day,
                feature_manifest=feature_manifest,
                config_path=config_path,
                output_path=prior_path,
            )
    model_dir = panel._resolve_repo_path(panel_spec["sources"]["model_dir"])
    panel._write_overlay(
        cache_root=cache_root,
        day=day,
        prior_path=prior_path,
        target_path=target_path,
        model_dir=model_dir,
    )
    window = data_windows.load_tick_window(
        day,
        params,
        load_ml=False,
        require_ml=False,
        run_ml_inference=False,
        cross_market_enabled=True,
        require_historical_bbo=True,
        require_formal_l2=True,
        verify_formal_l2_hashes=False,
        cache_dir=DEFAULT_MARKET_CACHE,
    )
    window.ml_data = None
    if hasattr(window, "ml_cache"):
        window.ml_cache = {}
    execution_trade_source = data_windows._normalize_execution_trade_source(
        params.get("execution_trade_source", "aggTrades")
    )
    market_context_warmup_days = max(
        0,
        int(params.get("market_context_warmup_days", 1) or 0),
    )
    source_signatures = data_windows._window_source_signature(
        day,
        load_ml=False,
        run_ml_inference=False,
        feature_dir=Path(),
        execution_trade_source=execution_trade_source,
        market_context_warmup_days=market_context_warmup_days,
    )
    source_references = data_windows._signature_references(source_signatures)
    binding = {
        "window_sha256": _canonical_sha256(
            {
                "schema_version": f"{RUNNER_IDENTITY}.window_source_bundle.v1",
                "day": day,
                "execution_trade_source": execution_trade_source,
                "market_context_warmup_days": market_context_warmup_days,
                "source_references": source_references,
            }
        ),
        "overlay_manifest_sha256": panel._sha256_file(
            panel._overlay_directory(cache_root, day) / "manifest.json"
        ),
    }
    return F05ReplayDay(
        day=day,
        window=window,
        ml_data=panel._load_npz_overlay(cache_root, day),
        identities=_day_binding_identities(binding),
    )


def _replay_day(
    day: str,
    *,
    params: dict[str, Any],
    panel_spec: Mapping[str, Any],
    prefix_plan: Mapping[str, Any],
    prepared_plan: Mapping[str, Any],
    cache_root: Path,
) -> F05ReplayDay:
    if day not in panel.ordered_days(panel_spec):
        return _generic_replay_day(
            day,
            params=params,
            panel_spec=panel_spec,
            cache_root=cache_root,
        )
    window, ml_data, binding = panel._load_day_inputs(
        day,
        spec=panel_spec,
        prefix_plan=prefix_plan,
        prepared_plan=prepared_plan,
        cache_root=cache_root,
    )
    return F05ReplayDay(
        day=day,
        window=window,
        ml_data=ml_data,
        identities=_day_binding_identities(binding),
    )


def _snapshot_row(snapshot: CooldownAssignmentSnapshotV2) -> dict[str, Any]:
    payload = snapshot.to_dict()
    payload_json = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return {
        "snapshot_id": snapshot.snapshot_id,
        "assignment_id": snapshot.assignment_id,
        "fill_event_id": snapshot.fill_event_id,
        "client_order_id": snapshot.client_order_id,
        "lineage_id": snapshot.lineage_id,
        "lineage_revision": snapshot.lineage_revision,
        "partial_fill_ordinal": snapshot.partial_fill_ordinal,
        "partial_fill_qty_btc": snapshot.partial_fill_qty_btc,
        "visibility_profile": snapshot.visibility_profile,
        "receive_time_transport_eligible": (
            snapshot.receive_time_transport_eligible
        ),
        "source_bundle_sha256": snapshot.source_bundle_sha256,
        "feature_block": snapshot.feature_block,
        "m0_context_json": json.dumps(
            snapshot.m0_context.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        "feature_row_json": json.dumps(
            snapshot.feature_row.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        "snapshot_payload_json": payload_json,
        "snapshot_payload_sha256": hashlib.sha256(
            payload_json.encode("ascii")
        ).hexdigest(),
        "policy_input_valid": snapshot.policy_input_valid,
        "fallback_policy_id": snapshot.fallback_policy_id,
        "fallback_reason": snapshot.fallback_reason,
        "economic_outcomes_read": False,
    }


def run_day(
    day: str,
    *,
    feature_block: str,
    support_identity: str = FULL_SUPPORT_IDENTITY,
    max_opportunities: int | None,
    output: Path = DEFAULT_OUTPUT,
    cache_root: Path = panel.DEFAULT_CACHE,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
    native_cache_receipt: Path | None = None,
) -> dict[str, Any]:
    if feature_block not in {"R0", "M1", "M2"}:
        raise StrictLabelError("feature_block must be R0, M1, or M2")
    v2_spec = _load_json(V2_SPEC)
    _validate_support_identity(
        day=day,
        support_identity=support_identity,
        spec=v2_spec,
    )
    panel_spec = panel._spec()
    frozen_days = panel.ordered_days(panel_spec)
    if day not in frozen_days:
        raise StrictLabelError(f"day is outside the frozen 50-day panel: {day}")
    day_parent = (
        output
        / f"support_identity={support_identity}"
        / f"feature_block={feature_block}"
        / f"execution_identity={FORMAL_EXECUTION_IDENTITY}"
        / "days"
    )
    day_root = day_parent / day
    if day_root.exists():
        return _validate_day_admission(
            day_root,
            expected_day=day,
            expected_feature_block=feature_block,
            expected_support_identity=support_identity,
            expected_max_opportunities=max_opportunities,
        )
    day_parent.mkdir(parents=True, exist_ok=True)
    stale_staging = tuple(day_parent.glob(f".{day}.staging.*"))
    if stale_staging:
        raise StrictLabelError(
            f"stale day staging exists; refusing implicit recovery: {stale_staging}"
        )
    day_lock = day_parent / f".{day}.lock"
    _create_day_lock(
        day_lock,
        day=day,
        feature_block=feature_block,
        support_identity=support_identity,
    )
    next_day = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    day_start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    parent_stop_ts_ms = int(
        datetime.fromisoformat(next_day).replace(tzinfo=UTC).timestamp() * 1_000
    )
    strict_spec = strict_baseline._spec()
    params, _ = strict_baseline._strict_base(strict_spec)
    params.update(
        {
            "trace_cooldown_duration_opportunities_max": 20_000,
            "trace_active_order_queue_missing_max": 64,
            "trace_active_order_queue_missing_unbounded": True,
            "exchange_book_queue_ambiguity_trace_max": 64,
            "dynamic_fill_hazard_shadow_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "buy_fill_selection_shadow_enabled": False,
            "buy_fill_selection_live_enabled": False,
        }
    )
    prepared = panel.prepare(cache_root)
    prefix_plan = panel._prefix_plan(panel_spec)
    target = _replay_day(
        day,
        params=params,
        panel_spec=panel_spec,
        prefix_plan=prefix_plan,
        prepared_plan=prepared,
        cache_root=cache_root,
    )
    continuation = _replay_day(
        next_day,
        params=params,
        panel_spec=panel_spec,
        prefix_plan=prefix_plan,
        prepared_plan=prepared,
        cache_root=cache_root,
    )
    engine_window, engine_ml, stitch_audit = stitch_two_days(
        target,
        continuation,
    )
    target_identities = dict(target.identities)
    continuation_identities = dict(continuation.identities)
    del target, continuation
    gc.collect()
    prior_day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    prior_trades = bt.load_individual_trades(
        days=[prior_day],
        quality_allowed_days=(prior_day,),
    )
    target_and_prior_trades = engine_window.trades.loc[
        engine_window.trades["transact_time"] < parent_stop_ts_ms
    ]
    feature_trades = pd.concat(
        (prior_trades, target_and_prior_trades),
        ignore_index=True,
    )
    del prior_trades, target_and_prior_trades
    left_ns = int((day_start - timedelta(days=1)).timestamp() * 1_000_000_000)
    right_ns = int((day_start + timedelta(days=2)).timestamp() * 1_000_000_000)
    tape = strict_baseline._native_tape(
        strict_spec,
        day=day,
        cache_dir=native_cache,
    )
    tape = type(tape)(
        raw_root=tape.raw_root,
        day=day,
        symbol=tape.symbol,
        tick_size=tape.tick_size,
        exchange=tape.exchange,
        warmup_hours=24,
        continuation_hours=24,
        strict_complete=True,
        cache_dir=native_cache,
        cache_enabled=True,
    )
    tape_identity = tape.identity(include_sha256=True)
    source_contract = build_strict_native_source_contract_from_single_tape(
        target_day=day,
        tape_identity=tape_identity,
        parser_identity_sha256=native_book_parser_identity(),
    )
    if native_cache_receipt is None:
        tape, native_cache_prebuild = _prebuild_strict_native_cache(tape)
    else:
        tape, native_cache_prebuild = _bind_prebuilt_strict_native_cache(
            tape,
            day=day,
            receipt_path=Path(native_cache_receipt),
        )
    identity_hashes = _execution_identity_hashes()
    normalized_market_source_sha256 = _canonical_sha256(
        {
            "schema_version": f"{RUNNER_IDENTITY}.normalized_feature_source.v1",
            "target": target_identities,
            "continuation": continuation_identities,
            "stitch_audit": stitch_audit,
            "window_extractor_sha256": _sha256(
                ROOT
                / "research/families/f05_fill_quality_quote_ev/audit/"
                "causal_multichannel_window_boolean_cooldown_windows.py"
            ),
            "normalized_book_timestamp_semantics": (
                "last_source_event_in_bucket_mapped_to_canonical_right_edge"
            ),
        }
    )
    feature_trade_days = (
        (prior_day, day) if feature_block == "M2" else (prior_day, day, next_day)
    )
    trade_paths = tuple(_trade_path(source_day) for source_day in feature_trade_days)
    feature_trade_source_sha256 = _file_set_identity(trade_paths)
    normalized_window_audit: WindowExtractionAccumulator | None = None
    native_book_audit: NativeM2BookFeatureAccumulator | None = None
    native_trade_audit: NativeM2TradeMergeAccumulator | None = None
    native_feature_source_sha256: str | None = None
    if feature_block == "M2":
        native_book_audit = NativeM2BookFeatureAccumulator()
        native_trade_audit = NativeM2TradeMergeAccumulator()
        feature_tape = _clone_native_tape(tape, cache_read_only=True)
        native_book_windows = RawNativeM2BookFeatureStream(
            tape=feature_tape,
            contract=NativeM2BookWindowContract(
                window_start_ns=int(feature_tape.process_start_ns),
                window_end_ns=int(feature_tape.day_end_ns),
                policy_start_ns=int(feature_tape.day_start_ns),
                require_receive_clock=True,
                feature_ready_clock=EXCHANGE_WINDOW_READY_CLOCK,
                max_source_silence_ns=None,
            ),
            audit=native_book_audit,
        )
        observations = stream_native_m2_causal_observations(
            book_windows=native_book_windows,
            official_trades=feature_trades,
            audit=native_trade_audit,
        )
        native_feature_source_sha256 = _canonical_sha256(
            {
                "schema_version": f"{RUNNER_IDENTITY}.raw_native_m2_source.v1",
                "strict_native_source_contract_sha256": (
                    source_contract.canonical_identity_sha256
                ),
                "native_m2_feature_schema": native_m2_book_feature_schema(),
                "official_trade_source_sha256": feature_trade_source_sha256,
                "window_start_ns": int(feature_tape.process_start_ns),
                "window_end_ns": int(feature_tape.day_end_ns),
                "policy_start_ns": int(feature_tape.day_start_ns),
                "feature_ready_clock": EXCHANGE_WINDOW_READY_CLOCK,
                "receive_time_transport_authorized": False,
            }
        )
        feature_market_source_sha256 = native_feature_source_sha256
    else:
        normalized_window_audit = WindowExtractionAccumulator()
        observations = stream_causal_windows(
            contract=WindowExtractionContract(
                block=feature_block,
                source_clock_profile=STRICT_EXCHANGE_TIME_PROFILE,
                left_ts_ns=left_ns,
                right_ts_ns=right_ns,
            ),
            bbo=engine_window.bbo_data,
            l2=engine_window.l2_data,
            trades=feature_trades,
            audit=normalized_window_audit,
        )
        feature_market_source_sha256 = normalized_market_source_sha256
    snapshot_spool = _SnapshotSpool(day=day)

    def fail_after_snapshot_spool(message: str) -> None:
        snapshot_spool.discard()
        raise StrictLabelError(message)

    def persist_parent_snapshot(row: CooldownAssignmentSnapshotV2) -> None:
        if executor.is_arm_child:
            return
        snapshot_spool.append(_snapshot_row(row))

    emitter = CooldownV2ReplayEmitter(
        feature_block=feature_block,
        observations=observations,
        warmup_cutoff_ts_ns=int(day_start.timestamp() * 1_000_000_000),
        warmup_identity=feature_market_source_sha256,
        identity_hashes=identity_hashes,
        source_cursor_prefixes={
            "market": feature_market_source_sha256,
            "depth": feature_market_source_sha256,
            "trade": feature_trade_source_sha256,
        },
        snapshot_sink=persist_parent_snapshot,
        retain_snapshots=False,
    )

    def report_opportunity(
        index: int,
        manifest_path: Path,
        resumed: bool,
    ) -> None:
        if index <= 5 or index % 25 == 0 or index == max_opportunities:
            print(
                "[strict-labels] "
                f"opportunity={index} "
                f"status={'resumed' if resumed else 'completed'} "
                f"manifest={manifest_path}",
                file=sys.stderr,
                flush=True,
            )

    label_root = (
        output
        / f"support_identity={support_identity}"
        / f"feature_block={feature_block}"
        / f"execution_identity={FORMAL_EXECUTION_IDENTITY}"
        / "labels"
    )
    executor = PosixCooldownSharedPrefixExecutor(
        output_root=label_root,
        target_day=day,
        source_contract_sha256=source_contract.canonical_identity_sha256,
        execution_identity_hashes=identity_hashes,
        max_parallel_arms=2,
        max_opportunities=max_opportunities,
        require_strict_native=True,
        progress=report_opportunity,
    )
    replay_started = time.monotonic()

    def report_replay_progress(progress: Mapping[str, Any]) -> None:
        # Forked arm children inherit this callback. Only the baseline parent
        # owns day-level progress reporting.
        if executor.is_arm_child:
            return
        index = int(progress["event_index"])
        count = int(progress["event_count"])
        percent = 100.0 * index / max(count, 1)
        print(
            "[strict-labels] "
            f"replay={index}/{count} ({percent:.2f}%) "
            f"elapsed_s={time.monotonic() - replay_started:.1f} "
            f"event_ts_ms={int(progress['event_ts_ms'])}",
            file=sys.stderr,
            flush=True,
        )

    params["cooldown_v2_snapshot_emitter"] = emitter
    params["cooldown_duration_shared_prefix_executor"] = executor
    params["cooldown_duration_parent_stop_ts_ms"] = parent_stop_ts_ms
    params["_replay_progress_callback"] = report_replay_progress
    params["_replay_progress_interval_events"] = 250_000
    try:
        result = bt._simulate_tick_with_engine(
            "python",
            engine_window.trades,
            engine_window.var_ts_ms,
            engine_window.var_ssq,
            params,
            ml_data=engine_ml,
            bbo_data=engine_window.bbo_data,
            l2_data=engine_window.l2_data,
            var_ti=engine_window.var_ti,
            var_retsq=engine_window.var_retsq,
            exchange_book_event_tape=tape,
        )
    except BaseException:
        snapshot_spool.discard()
        raise
    if result.get("cooldown_duration_parent_stop_ts_ms") != parent_stop_ts_ms:
        fail_after_snapshot_spool("shared-prefix parent stop identity drifted")
    if result.get("cooldown_duration_parent_stop_triggered") is not True:
        fail_after_snapshot_spool(
            "shared-prefix parent did not reach its frozen stop condition"
        )
    parent_stop_trigger_ts_ms = int(
        result.get("cooldown_duration_parent_stop_trigger_ts_ms", 0)
    )
    if max_opportunities is None and parent_stop_trigger_ts_ms < parent_stop_ts_ms:
        fail_after_snapshot_spool(
            "shared-prefix parent stopped before target-day end"
        )
    execution_audit = result["_cooldown_duration_shared_prefix_audit"]
    if int(execution_audit["opportunities_dispatched"]) <= 0:
        fail_after_snapshot_spool(
            "strict benchmark did not execute an opportunity"
        )
    if max_opportunities is not None and (
        int(execution_audit["opportunities_dispatched"])
        + int(execution_audit["opportunities_resumed"])
        < int(max_opportunities)
    ):
        fail_after_snapshot_spool(
            "bounded shared-prefix parent stopped before its opportunity limit"
        )
    completed_label_manifests = tuple(
        Path(path)
        for path in execution_audit["completed_manifest_paths"]
    )
    if len(completed_label_manifests) != int(
        execution_audit["opportunities_dispatched"]
    ):
        fail_after_snapshot_spool(
            "shared-prefix label manifest count drifted"
        )
    one_shot_label_manifests = [
        {
            "path": str(path),
            "size_bytes": int(path.stat().st_size),
            "sha256": _sha256(path),
        }
        for path in completed_label_manifests
    ]
    parent_stop_audit = {
        "configured_stop_ts_ms": parent_stop_ts_ms,
        "triggered": bool(result["cooldown_duration_parent_stop_triggered"]),
        "trigger_ts_ms": parent_stop_trigger_ts_ms,
        "reason": (
            "target_day_end"
            if max_opportunities is None
            else "max_opportunities_reached"
        ),
        "target_day_boundary_observed": max_opportunities is None,
        "opportunities_skipped_outside_target_day": int(
            execution_audit["opportunities_skipped_outside_target_day"]
        ),
        "new_assignments_after_target_day_boundary": 0,
    }
    strict_native_queue = {
        "mode": result["exchange_book_queue_mode"],
        "scope": result["exchange_book_queue_scope"],
        "missing_queue_seed_count": int(
            result["exchange_book_queue_missing_count"]
        ),
        "missing_queue_seed_trace": list(
            result.get("_exchange_book_queue_missing_trace", ())
        ),
        "source_gap_events": int(result["exchange_book_source_gap_events"]),
        "missing_trace_unbounded": bool(
            params["trace_active_order_queue_missing_unbounded"]
        ),
    }
    if len(strict_native_queue["missing_queue_seed_trace"]) != int(
        strict_native_queue["missing_queue_seed_count"]
    ):
        snapshot_spool.discard()
        raise StrictLabelError(
            "strict-native queue missing trace does not cover every missing seed"
        )
    if max_opportunities is None and (
        int(strict_native_queue["missing_queue_seed_count"]) != 0
        or int(strict_native_queue["source_gap_events"]) != 0
    ):
        missing_preview = strict_native_queue["missing_queue_seed_trace"][:8]
        snapshot_spool.discard()
        raise StrictLabelError(
            "formal strict-native day has incomplete queue evidence: "
            f"missing={strict_native_queue['missing_queue_seed_count']} "
            f"source_gaps={strict_native_queue['source_gap_events']} "
            f"trace={missing_preview}"
        )
    strict_native_source_counters = {
        field: int(result[result_field])
        for field, result_field in _STRICT_SOURCE_RESULT_FIELDS.items()
    }
    if max_opportunities is None and any(strict_native_source_counters.values()):
        snapshot_spool.discard()
        raise StrictLabelError("formal strict-native source audit failed")
    feature_window_audit = (
        {
            "source": "raw_native_m2",
            "book": _frozen_audit_payload(native_book_audit.freeze()),
            "official_trade_merge": _frozen_audit_payload(
                native_trade_audit.freeze()
            ),
        }
        if native_book_audit is not None and native_trade_audit is not None
        else {
            "source": "normalized_market_window",
            "normalized": asdict(normalized_window_audit.freeze()),
        }
    )
    snapshot_spool.close()
    params.pop("cooldown_v2_snapshot_emitter", None)
    params.pop("cooldown_duration_shared_prefix_executor", None)
    params.pop("_replay_progress_callback", None)
    del engine_window, engine_ml, feature_trades, emitter, observations, result
    gc.collect()
    staging = day_parent / f".{day}.staging.{os.getpid()}.{uuid.uuid4().hex}"
    staging.mkdir()
    _fsync_directory(day_parent)
    snapshots_path = staging / "assignment_snapshots.parquet"
    snapshot_row_count = snapshot_spool.write_parquet(snapshots_path)
    source_path = staging / "source_contract.json"
    _atomic_json(source_path, source_contract.to_payload())
    final_snapshots_path = day_root / snapshots_path.name
    final_source_path = day_root / source_path.name
    manifest = {
        "schema_version": DAY_SCHEMA_VERSION,
        "identity": RUNNER_IDENTITY,
        "target_day": day,
        "feature_block": feature_block,
        "source_support_identity": support_identity,
        "panel_role": (
            "prefix40_development"
            if day in panel_spec["immutable_prefix"]["ordered_utc_days"]
            else "added10_late_diagnostic"
        ),
        "max_opportunities": max_opportunities,
        "execution_amendment": _execution_amendment_binding(),
        "source_contract": {
            "path": str(final_source_path),
            "sha256": _sha256(source_path),
            "canonical_identity_sha256": source_contract.canonical_identity_sha256,
        },
        "feature_sources": {
            "active_feature_market_source_sha256": feature_market_source_sha256,
            "normalized_market_source_sha256": normalized_market_source_sha256,
            "raw_native_m2_source_sha256": native_feature_source_sha256,
            "individual_trade_source_sha256": feature_trade_source_sha256,
            "individual_trade_paths": [str(path) for path in trade_paths],
            "receive_time_transport_authorized": False,
        },
        "assignment_snapshots": {
            "path": str(final_snapshots_path),
            "sha256": _sha256(snapshots_path),
            "rows": snapshot_row_count,
        },
        "shared_prefix_execution_audit": execution_audit,
        "parent_stop_audit": parent_stop_audit,
        "one_shot_label_manifests": one_shot_label_manifests,
        "native_cache_prebuild": native_cache_prebuild,
        "feature_window_audit": feature_window_audit,
        "stitch_audit": stitch_audit,
        "strict_native_queue": strict_native_queue,
        "strict_native_source_counters": strict_native_source_counters,
        "economic_outcomes_read_by_runner": False,
        "development_labels_generated": True,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    manifest_path = staging / "manifest.json"
    _atomic_json(manifest_path, manifest)
    _atomic_json(
        staging / "_SUCCESS",
        {"manifest_sha256": _sha256(manifest_path)},
    )
    _validate_day_admission(
        staging,
        expected_day=day,
        expected_feature_block=feature_block,
        expected_support_identity=support_identity,
        expected_max_opportunities=max_opportunities,
    )
    if day_root.exists():
        raise StrictLabelError("day destination appeared during atomic admission")
    os.replace(staging, day_root)
    _fsync_directory(day_parent)
    day_lock.unlink()
    _fsync_directory(day_parent)
    return _validate_day_admission(
        day_root,
        expected_day=day,
        expected_feature_block=feature_block,
        expected_support_identity=support_identity,
        expected_max_opportunities=max_opportunities,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run-day")
    run_parser.add_argument("--day", required=True)
    run_parser.add_argument("--feature-block", choices=("R0", "M1", "M2"), default="R0")
    run_parser.add_argument(
        "--support-identity",
        choices=(FULL_SUPPORT_IDENTITY, REDUCED_SUPPORT_IDENTITY),
        default=FULL_SUPPORT_IDENTITY,
    )
    run_parser.add_argument("--max-opportunities", type=int, default=1)
    run_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    run_parser.add_argument("--cache-root", type=Path, default=panel.DEFAULT_CACHE)
    run_parser.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.command != "run-day":
        raise AssertionError(args.command)
    payload = run_day(
        args.day,
        feature_block=args.feature_block,
        support_identity=args.support_identity,
        max_opportunities=args.max_opportunities,
        output=args.output,
        cache_root=args.cache_root,
        native_cache=args.native_cache,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
