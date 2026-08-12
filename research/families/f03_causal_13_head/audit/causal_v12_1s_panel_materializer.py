#!/usr/bin/env python3
"""Atomic daily Parquet materializer for the causal-v12 1s feature panel."""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import shutil
import sys
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import causal_v12_1s_cpp_batch as cpp_batch
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as sources,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_full_schema as full
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

SCHEMA_VERSION = "causal_v12_1s_daily_panel_materializer.v4"
ARTIFACT_SCHEMA_VERSION = "causal_v12_1s_daily_feature_panel_artifact.v4"
PANEL_FILENAME = "panel.parquet"
MANIFEST_FILENAME = "manifest.json"
PROBE_FILENAME = "source_probe.json"
SUCCESS_FILENAME = "_SUCCESS"
MAX_LOCAL_HISTORY_BARS = 604_801
MAX_REFERENCE_HISTORY_BARS = 3_601
MAX_METRIC_HISTORY_ROWS = 80
PYTHON_ORACLE_ENGINE = "python_oracle"
CPP_BATCH_ENGINE = "cpp_batch"
SUPPORTED_ENGINES = (PYTHON_ORACLE_ENGINE, CPP_BATCH_ENGINE)


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _day_bounds_ms(day: str) -> tuple[int, int]:
    start = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000)
    return start, start + 86_400_000


def panel_arrow_schema() -> pa.Schema:
    metadata = [
        pa.field("cutoff_exclusive_ms", pa.int64(), nullable=False),
        pa.field("decision_ts_ms", pa.int64(), nullable=False),
        pa.field("feature_ready_ts_ms", pa.int64(), nullable=False),
        pa.field("unsupported_feature_count", pa.int16(), nullable=False),
        pa.field("feature_row_fingerprint_sha256", pa.string(), nullable=False),
        pa.field("local_bar_lag_state", pa.string(), nullable=False),
        pa.field("local_synthetic_seconds_24h", pa.int32(), nullable=False),
        pa.field("reference_bar_lag_state", pa.string(), nullable=False),
        pa.field("reference_synthetic_seconds_1h", pa.int32(), nullable=False),
    ]
    features = [
        pa.field(name, pa.float64(), nullable=True) for name in schema.TRAINABLE_FEATURE_ORDER
    ]
    return pa.schema([*metadata, *features])


def panel_schema_payload() -> dict[str, Any]:
    arrow_schema = panel_arrow_schema()
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "columns": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in arrow_schema
        ],
        "feature_order_sha256": schema.feature_order_sha256(),
        "feature_count": len(schema.TRAINABLE_FEATURE_ORDER),
        "target_day_decision_clock": {
            "interval": "[D 00:00:00, D+1 00:00:00)",
            "bar_support_rule": "completed_local_bar.start_ts_ms == decision_ts_ms - 1000",
            "first_decision_uses_previous_natural_day_warmup": True,
            "next_day_midnight_included": False,
        },
        "ten_second_feature_rows_accepted": False,
    }


def _load_cpp() -> Any:
    try:
        import narrowgate_cpp  # type: ignore[import-not-found]
    except ImportError as exc:
        raise sources.base.FeatureContractError(
            "engine=cpp_batch requires the repository narrowgate_cpp extension"
        ) from exc
    cpp_batch.validate_cpp_batch_module(narrowgate_cpp)
    return narrowgate_cpp


def _engine_identity(engine: str) -> dict[str, Any]:
    if engine == PYTHON_ORACLE_ENGINE:
        return {
            "engine": PYTHON_ORACLE_ENGINE,
            "engine_abi": full.SCHEMA_VERSION,
            "parity_oracle_only": True,
            "bulk_materialization_authorized": False,
            "row_fingerprint": "python_frozen_full_feature_row.v1",
        }
    if engine == CPP_BATCH_ENGINE:
        return cpp_batch.engine_identity(_load_cpp())
    raise sources.base.FeatureContractError(
        f"explicit engine must be one of {SUPPORTED_ENGINES}; got {engine!r}"
    )


def _code_identity(engine: str) -> dict[str, str]:
    reader_path = Path(sources.__file__).resolve()
    materializer_path = Path(__file__).resolve()
    full_schema_path = Path(full.__file__).resolve()
    schema_path = Path(schema.__file__).resolve()
    cpp_bridge_path = Path(cpp_batch.__file__).resolve()
    identity = {
        "daily_source_reader_sha256": sources.sha256_file(reader_path),
        "full_feature_generator_sha256": sources.sha256_file(full_schema_path),
        "materializer_sha256": sources.sha256_file(materializer_path),
        "trainable_schema_sha256": sources.sha256_file(schema_path),
    }
    if engine == CPP_BATCH_ENGINE:
        repository_root = Path(__file__).resolve().parents[4]
        cpp_source = (
            repository_root
            / "research/families/f03_causal_13_head/cpp/causal_v12_1s_features.cpp"
        )
        cpp_header = cpp_source.with_suffix(".hpp")
        identity.update(
            {
                "cpp_batch_bridge_sha256": sources.sha256_file(cpp_bridge_path),
                "cpp_feature_source_sha256": sources.sha256_file(cpp_source),
                "cpp_feature_header_sha256": sources.sha256_file(cpp_header),
            }
        )
    return identity


def cache_identity_payload(
    bundle: sources.DailySourceBundle,
    *,
    cutoffs_ms: Sequence[int] | None,
    engine: str,
    execution_component_identity_sha256: str | None = None,
    source_profile_id: str | None = None,
    source_permissions: Mapping[str, Any] | None = None,
    source_probe_identity_sha256: str | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_schema_sha256": _canonical_sha256(panel_schema_payload()),
        "bundle_identity_sha256": bundle.identity_sha256(),
        "code": _code_identity(engine),
        "engine": _engine_identity(engine),
        "cutoff_selection": {
            "mode": (
                "all_authoritative_target_day_decision_timestamps"
                if cutoffs_ms is None
                else "explicit_authoritative_target_day_decision_timestamps"
            ),
            "count": None if cutoffs_ms is None else len(cutoffs_ms),
            "sha256": None
            if cutoffs_ms is None
            else _canonical_sha256([int(value) for value in cutoffs_ms]),
            "interval": "[D 00:00:00, D+1 00:00:00)",
            "bar_support_rule": "cutoff_minus_1s_completed_local_bar",
        },
        "feature_contract_sha256": full.full_feature_contract_fingerprint(),
        "feature_order_sha256": schema.feature_order_sha256(),
        "source_manifest_sha256": schema.canonical_sha256(schema.source_manifest_payload()),
        "utc_day": bundle.utc_day,
    }
    if execution_component_identity_sha256 is not None:
        payload.update(
            {
                "execution_component_identity_sha256": str(
                    execution_component_identity_sha256
                ),
                "source_profile_id": str(source_profile_id),
                "source_permissions": dict(source_permissions or {}),
                "source_probe_identity_sha256": str(source_probe_identity_sha256),
            }
        )
    return payload


def cache_identity_sha256(
    bundle: sources.DailySourceBundle,
    *,
    cutoffs_ms: Sequence[int] | None,
    engine: str,
    execution_component_identity_sha256: str | None = None,
    source_profile_id: str | None = None,
    source_permissions: Mapping[str, Any] | None = None,
    source_probe_identity_sha256: str | None = None,
) -> str:
    return _canonical_sha256(
        cache_identity_payload(
            bundle,
            cutoffs_ms=cutoffs_ms,
            engine=engine,
            execution_component_identity_sha256=execution_component_identity_sha256,
            source_profile_id=source_profile_id,
            source_permissions=source_permissions,
            source_probe_identity_sha256=source_probe_identity_sha256,
        )
    )


@dataclass(frozen=True, slots=True)
class MaterializedPanel:
    output_dir: Path
    panel_path: Path
    manifest_path: Path
    cache_identity_sha256: str
    row_count: int
    reused: bool


@dataclass(frozen=True, slots=True)
class PanelFeatureRow:
    feature_row: full.FullFeatureRow
    local_bar_lag_state: str
    local_synthetic_seconds_24h: int
    reference_bar_lag_state: str
    reference_synthetic_seconds_1h: int


def _target_cutoffs(
    bundle: sources.DailySourceBundle,
    local_bars: Sequence[Any],
    explicit: Sequence[int] | None,
) -> tuple[int, ...]:
    day_start, day_end = _day_bounds_ms(bundle.utc_day)
    completed_starts = {int(bar.start_ts_ms) for bar in local_bars}
    authoritative = tuple(range(day_start, day_end, schema.CADENCE_MS))
    available = {
        cutoff for cutoff in authoritative if cutoff - schema.CADENCE_MS in completed_starts
    }
    if explicit is None:
        missing = [cutoff for cutoff in authoritative if cutoff not in available]
        if missing:
            raise ValueError(
                "authoritative target-day decision timestamps lack cutoff-1s local bars: "
                + ", ".join(str(value) for value in missing[:5])
            )
        selected = authoritative
    else:
        selected = tuple(int(value) for value in explicit)
        if selected != tuple(sorted(set(selected))):
            raise ValueError("explicit cutoffs must be unique and strictly increasing")
        outside = [value for value in selected if not day_start <= value < day_end]
        if outside:
            raise ValueError(
                "explicit cutoffs must lie in target-day decision interval [D,D+1): "
                + ", ".join(str(value) for value in outside[:5])
            )
        missing = sorted(set(selected) - available)
        if missing:
            raise ValueError(
                "explicit cutoffs lack an exact completed cutoff-1s local bar: "
                + ", ".join(str(value) for value in missing[:5])
            )
    if not selected:
        raise ValueError("no target-day 1s cutoffs are materializable")
    if any(value % schema.CADENCE_MS for value in selected):
        raise ValueError("target cutoffs must be canonical 1s boundaries")
    return selected


def _bounded_slice(
    values: Sequence[Any],
    timestamps: Sequence[int],
    *,
    cutoff: int,
    maximum_rows: int,
) -> Sequence[Any]:
    end = bisect.bisect_left(timestamps, cutoff)
    start = max(0, end - maximum_rows)
    return values[start:end]


def _iter_feature_rows(
    *,
    cutoffs: Sequence[int],
    local_bars: Sequence[Any],
    execution_l2: Sequence[full.ExecutionL2Observation],
    metrics: Sequence[full.MetricObservation],
    reference_bars: Sequence[Any],
    local_synthetic_starts: Sequence[int],
    reference_synthetic_starts: Sequence[int],
) -> Iterator[PanelFeatureRow]:
    local_times = [int(bar.start_ts_ms) for bar in local_bars]
    reference_times = [int(bar.start_ts_ms) for bar in reference_bars]
    metric_times = [int(item.source_ts_ms) for item in metrics]
    l2_by_start = {int(item.bucket_start_ts_ms): item for item in execution_l2}
    local_synthetic = tuple(sorted(int(value) for value in local_synthetic_starts))
    reference_synthetic = tuple(sorted(int(value) for value in reference_synthetic_starts))
    local_synthetic_set = set(local_synthetic)
    reference_synthetic_set = set(reference_synthetic)
    for cutoff in cutoffs:
        local_view = _bounded_slice(
            local_bars,
            local_times,
            cutoff=cutoff,
            maximum_rows=MAX_LOCAL_HISTORY_BARS,
        )
        reference_view = _bounded_slice(
            reference_bars,
            reference_times,
            cutoff=cutoff,
            maximum_rows=MAX_REFERENCE_HISTORY_BARS,
        )
        metric_end = bisect.bisect_left(metric_times, cutoff)
        metric_view = metrics[max(0, metric_end - MAX_METRIC_HISTORY_ROWS) : metric_end]
        l2_item = l2_by_start.get(cutoff - schema.CADENCE_MS)
        feature_row = full.generate_full_feature_row(
            local_view,
            cutoff_exclusive_ms=cutoff,
            execution_l2=() if l2_item is None else (l2_item,),
            metrics=metric_view,
            reference_bars=reference_view,
        )
        current_start = cutoff - schema.CADENCE_MS
        local_start = bisect.bisect_left(local_synthetic, cutoff - 86_400_000)
        local_end = bisect.bisect_left(local_synthetic, cutoff)
        reference_start = bisect.bisect_left(reference_synthetic, cutoff - 3_600_000)
        reference_end = bisect.bisect_left(reference_synthetic, cutoff)
        yield PanelFeatureRow(
            feature_row=feature_row,
            local_bar_lag_state=(
                sources.SYNTHETIC_BAR_LAG_STATE
                if current_start in local_synthetic_set
                else sources.OBSERVED_BAR_LAG_STATE
            ),
            local_synthetic_seconds_24h=local_end - local_start,
            reference_bar_lag_state=(
                "source_unavailable"
                if not reference_bars
                else sources.SYNTHETIC_BAR_LAG_STATE
                if current_start in reference_synthetic_set
                else sources.OBSERVED_BAR_LAG_STATE
            ),
            reference_synthetic_seconds_1h=reference_end - reference_start,
        )


def _panel_record(row: PanelFeatureRow) -> dict[str, Any]:
    feature_row = row.feature_row
    unsupported = sum(value.value is None for value in feature_row.values.values())
    return {
        "cutoff_exclusive_ms": feature_row.cutoff_exclusive_ms,
        "decision_ts_ms": feature_row.decision_ts_ms,
        "feature_ready_ts_ms": feature_row.feature_ready_ts_ms,
        "unsupported_feature_count": unsupported,
        "feature_row_fingerprint_sha256": feature_row.fingerprint_sha256,
        "local_bar_lag_state": row.local_bar_lag_state,
        "local_synthetic_seconds_24h": row.local_synthetic_seconds_24h,
        "reference_bar_lag_state": row.reference_bar_lag_state,
        "reference_synthetic_seconds_1h": row.reference_synthetic_seconds_1h,
        **{name: feature_row.values[name].value for name in schema.TRAINABLE_FEATURE_ORDER},
    }


def _write_panel(
    path: Path,
    rows: Iterator[PanelFeatureRow],
    *,
    batch_rows: int,
) -> tuple[int, int | None, int | None, Counter[str]]:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    writer = pq.ParquetWriter(
        path,
        panel_arrow_schema(),
        compression="zstd",
        write_statistics=True,
    )
    buffered: list[dict[str, Any]] = []
    count = 0
    first_cutoff: int | None = None
    last_cutoff: int | None = None
    unsupported_counts: Counter[str] = Counter()
    try:
        for row in rows:
            feature_row = row.feature_row
            if first_cutoff is None:
                first_cutoff = feature_row.cutoff_exclusive_ms
            last_cutoff = feature_row.cutoff_exclusive_ms
            count += 1
            unsupported_counts.update(
                name for name, value in feature_row.values.items() if value.value is None
            )
            buffered.append(_panel_record(row))
            if len(buffered) >= batch_rows:
                writer.write_table(pa.Table.from_pylist(buffered, schema=panel_arrow_schema()))
                buffered.clear()
        if buffered:
            writer.write_table(pa.Table.from_pylist(buffered, schema=panel_arrow_schema()))
    finally:
        writer.close()
    if count == 0:
        raise ValueError("materializer produced zero rows")
    return count, first_cutoff, last_cutoff, unsupported_counts


def _create_cpp_batch_engine(
    *,
    local_bars: Sequence[Any],
    execution_l2: Sequence[full.ExecutionL2Observation],
    metrics: Sequence[full.MetricObservation],
    reference_bars: Sequence[Any],
) -> tuple[Any, Any]:
    cpp = _load_cpp()
    engine = cpp_batch.create_engine(
        cpp,
        local_bars=local_bars,
        execution_l2=execution_l2,
        metrics=metrics,
        reference_bars=reference_bars,
    )
    return cpp, engine


def _iter_cpp_batch_records(
    *,
    cpp: Any,
    engine: Any,
    cutoffs: Sequence[int],
    local_synthetic_starts: Sequence[int],
    reference_synthetic_starts: Sequence[int],
    reference_available: bool,
    batch_rows: int,
) -> Iterator[tuple[dict[str, Any], tuple[str, ...]]]:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    local_synthetic = tuple(sorted(int(value) for value in local_synthetic_starts))
    reference_synthetic = tuple(sorted(int(value) for value in reference_synthetic_starts))
    local_synthetic_set = set(local_synthetic)
    reference_synthetic_set = set(reference_synthetic)
    lag_vocabulary = tuple(str(value) for value in cpp.F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY)
    feature_names = schema.TRAINABLE_FEATURE_ORDER
    for batch_start in range(0, len(cutoffs), batch_rows):
        selected = tuple(int(value) for value in cutoffs[batch_start : batch_start + batch_rows])
        output = cpp_batch.compute_batch(engine, selected)
        values = np.asarray(output["values"], dtype=np.float64)
        valid = np.asarray(output["valid"], dtype=np.uint8)
        source_ts = np.asarray(output["source_latest_ts_ms"], dtype=np.int64)
        ready_ts = np.asarray(
            output["feature_ready_ts_ms_by_feature"], dtype=np.int64
        )
        counts = np.asarray(output["observation_count"], dtype=np.int64)
        lag_codes = np.asarray(output["lag_state_code"], dtype=np.uint8)
        row_ready = np.asarray(output["feature_ready_ts_ms"], dtype=np.int64)
        decisions = np.asarray(output["decision_ts_ms"], dtype=np.int64)
        for row_index, cutoff in enumerate(selected):
            row_valid = valid[row_index]
            unsupported_indices = np.flatnonzero(row_valid == 0)
            unsupported_names = tuple(feature_names[int(index)] for index in unsupported_indices)
            current_start = cutoff - schema.CADENCE_MS
            local_start = bisect.bisect_left(local_synthetic, cutoff - 86_400_000)
            local_end = bisect.bisect_left(local_synthetic, cutoff)
            reference_start = bisect.bisect_left(
                reference_synthetic, cutoff - 3_600_000
            )
            reference_end = bisect.bisect_left(reference_synthetic, cutoff)
            record: dict[str, Any] = {
                "cutoff_exclusive_ms": cutoff,
                "decision_ts_ms": int(decisions[row_index]),
                "feature_ready_ts_ms": int(row_ready[row_index]),
                "unsupported_feature_count": len(unsupported_names),
                "feature_row_fingerprint_sha256": cpp_batch.feature_row_fingerprint(
                    cutoff_exclusive_ms=cutoff,
                    values=values[row_index],
                    valid=row_valid,
                    source_latest_ts_ms=source_ts[row_index],
                    feature_ready_ts_ms=ready_ts[row_index],
                    observation_count=counts[row_index],
                    lag_state_code=lag_codes[row_index],
                    lag_state_vocabulary=lag_vocabulary,
                ),
                "local_bar_lag_state": (
                    sources.SYNTHETIC_BAR_LAG_STATE
                    if current_start in local_synthetic_set
                    else sources.OBSERVED_BAR_LAG_STATE
                ),
                "local_synthetic_seconds_24h": local_end - local_start,
                "reference_bar_lag_state": (
                    "source_unavailable"
                    if not reference_available
                    else sources.SYNTHETIC_BAR_LAG_STATE
                    if current_start in reference_synthetic_set
                    else sources.OBSERVED_BAR_LAG_STATE
                ),
                "reference_synthetic_seconds_1h": reference_end - reference_start,
            }
            record.update(
                {
                    name: float(values[row_index, column])
                    if bool(row_valid[column])
                    else None
                    for column, name in enumerate(feature_names)
                }
            )
            yield record, unsupported_names


def _write_cpp_batch_panel(
    path: Path,
    *,
    cpp: Any,
    engine: Any,
    cutoffs: Sequence[int],
    local_synthetic_starts: Sequence[int],
    reference_synthetic_starts: Sequence[int],
    reference_available: bool,
    batch_rows: int,
) -> tuple[int, int | None, int | None, Counter[str]]:
    writer = pq.ParquetWriter(
        path,
        panel_arrow_schema(),
        compression="zstd",
        write_statistics=True,
    )
    buffered: list[dict[str, Any]] = []
    count = 0
    first_cutoff: int | None = None
    last_cutoff: int | None = None
    unsupported_counts: Counter[str] = Counter()
    try:
        for record, unsupported in _iter_cpp_batch_records(
            cpp=cpp,
            engine=engine,
            cutoffs=cutoffs,
            local_synthetic_starts=local_synthetic_starts,
            reference_synthetic_starts=reference_synthetic_starts,
            reference_available=reference_available,
            batch_rows=batch_rows,
        ):
            cutoff = int(record["cutoff_exclusive_ms"])
            first_cutoff = cutoff if first_cutoff is None else first_cutoff
            last_cutoff = cutoff
            count += 1
            unsupported_counts.update(unsupported)
            buffered.append(record)
            if len(buffered) >= batch_rows:
                writer.write_table(pa.Table.from_pylist(buffered, schema=panel_arrow_schema()))
                buffered.clear()
        if buffered:
            writer.write_table(pa.Table.from_pylist(buffered, schema=panel_arrow_schema()))
    finally:
        writer.close()
    if count == 0:
        raise ValueError("materializer produced zero rows")
    return count, first_cutoff, last_cutoff, unsupported_counts


def _reuse_existing(output_dir: Path, expected_identity: str) -> MaterializedPanel | None:
    if not output_dir.exists():
        return None
    manifest_path = output_dir / MANIFEST_FILENAME
    panel_path = output_dir / PANEL_FILENAME
    success_path = output_dir / SUCCESS_FILENAME
    if not (manifest_path.is_file() and panel_path.is_file() and success_path.is_file()):
        raise FileExistsError(f"incomplete existing panel artifact: {output_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("cache_identity_sha256") != expected_identity:
        raise FileExistsError(f"existing panel has a different cache identity: {output_dir}")
    if manifest.get("panel", {}).get("sha256") != sources.sha256_file(panel_path):
        raise ValueError(f"existing panel SHA256 mismatch: {panel_path}")
    if success_path.read_text(encoding="utf-8").strip() != sources.sha256_file(manifest_path):
        raise ValueError(f"existing panel admission marker mismatch: {success_path}")
    return MaterializedPanel(
        output_dir=output_dir,
        panel_path=panel_path,
        manifest_path=manifest_path,
        cache_identity_sha256=expected_identity,
        row_count=int(manifest["panel"]["rows"]),
        reused=True,
    )


def materialize_daily_panel(
    bundle: sources.DailySourceBundle,
    *,
    output_dir: Path,
    cutoffs_ms: Sequence[int] | None = None,
    batch_rows: int = 2_048,
    engine: str | None = None,
    source_probe_payload: Mapping[str, Any] | None = None,
    pipeline_execution_receipt_path: Path | None = None,
) -> MaterializedPanel:
    """Build and atomically admit one source/hash-bound daily feature panel."""

    if engine is None:
        raise sources.base.FeatureContractError(
            f"materialization engine must be explicit: {SUPPORTED_ENGINES}"
        )
    if engine == PYTHON_ORACLE_ENGINE and cutoffs_ms is None:
        raise sources.base.FeatureContractError(
            "engine=python_oracle cannot materialize a full daily panel"
        )
    engine_identity = _engine_identity(engine)
    execution_receipt: dict[str, Any] | None = None
    execution_component_sha: str | None = None
    source_profile_id: str | None = None
    source_permissions: Mapping[str, Any] | None = None
    source_probe_sha: str | None = None
    if pipeline_execution_receipt_path is not None:
        execution_receipt = execution_identity.validate_pipeline_execution_receipt(
            pipeline_execution_receipt_path
        )
        execution_component_sha = str(
            execution_receipt["f03_component_semantics"]["identity_sha256"]
        )
        source_profile_id = str(execution_receipt["profile_id"])
        source_permissions = execution_receipt["source_permissions"]
        if source_probe_payload is None:
            raise sources.base.FeatureContractError(
                "successor materialization requires an exact profile-bound source probe"
            )
        if source_probe_payload.get("profile_id") != source_profile_id:
            raise sources.base.FeatureContractError("source probe profile differs from receipt")
        if source_probe_payload.get("source_permissions") != source_permissions:
            raise sources.base.FeatureContractError(
                "source probe permission contract differs from receipt"
            )
        source_probe_sha = _canonical_sha256(source_probe_payload)
    output_dir = output_dir.expanduser().resolve()
    expected_identity = cache_identity_sha256(
        bundle,
        cutoffs_ms=cutoffs_ms,
        engine=engine,
        execution_component_identity_sha256=execution_component_sha,
        source_profile_id=source_profile_id,
        source_permissions=source_permissions,
        source_probe_identity_sha256=source_probe_sha,
    )
    reused = _reuse_existing(output_dir, expected_identity)
    if reused is not None:
        return reused

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = output_dir.parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex}"
    temporary_dir.mkdir()
    try:
        probe = (
            dict(source_probe_payload)
            if source_probe_payload is not None
            else sources.probe_source_bundle(bundle)
        )
        if probe.get("bundle_identity_sha256") != bundle.identity_sha256():
            raise sources.base.FeatureContractError(
                "source probe bundle identity differs from materialization input"
            )
        if not probe.get("physical_materialization_eligible"):
            reasons = probe.get("failure_reasons", ["unknown source authority failure"])
            raise sources.base.FeatureContractError(
                "source bundle is not physically materialization-eligible: "
                + "; ".join(str(reason) for reason in reasons)
            )
        local_audit = sources.read_local_trade_bars_with_audit(bundle.local_trade_tempo_paths)
        local_bars = local_audit.bars
        execution_l2 = sources.read_execution_l2(bundle.execution_l2_paths)
        metric_audit = sources.read_metrics_with_audit(bundle.metric_paths)
        metrics = metric_audit.observations
        reference_audit = sources.read_reference_bars_with_audit(bundle.reference_bar_paths)
        reference_bars = () if reference_audit is None else reference_audit.bars
        cutoffs = _target_cutoffs(bundle, local_bars, cutoffs_ms)

        probe_path = temporary_dir / PROBE_FILENAME
        _atomic_json(probe_path, probe)
        _fsync_file(probe_path)
        panel_path = temporary_dir / PANEL_FILENAME
        if engine == PYTHON_ORACLE_ENGINE:
            rows, first_cutoff, last_cutoff, unsupported_counts = _write_panel(
                panel_path,
                _iter_feature_rows(
                    cutoffs=cutoffs,
                    local_bars=local_bars,
                    execution_l2=execution_l2,
                    metrics=metrics,
                    reference_bars=reference_bars,
                    local_synthetic_starts=local_audit.synthesized_start_ts_ms,
                    reference_synthetic_starts=(
                        ()
                        if reference_audit is None
                        else reference_audit.synthesized_start_ts_ms
                    ),
                ),
                batch_rows=batch_rows,
            )
        elif engine == CPP_BATCH_ENGINE:
            cpp, native_engine = _create_cpp_batch_engine(
                local_bars=local_bars,
                execution_l2=execution_l2,
                metrics=metrics,
                reference_bars=reference_bars,
            )
            rows, first_cutoff, last_cutoff, unsupported_counts = _write_cpp_batch_panel(
                panel_path,
                cpp=cpp,
                engine=native_engine,
                cutoffs=cutoffs,
                local_synthetic_starts=local_audit.synthesized_start_ts_ms,
                reference_synthetic_starts=(
                    ()
                    if reference_audit is None
                    else reference_audit.synthesized_start_ts_ms
                ),
                reference_available=bool(reference_bars),
                batch_rows=batch_rows,
            )
        else:
            raise AssertionError(engine)
        _fsync_file(panel_path)
        if pipeline_execution_receipt_path is not None:
            # Detect source, Python, config, P3, ABI, or loaded-extension drift
            # after the expensive day computation but before publication.
            execution_identity.validate_pipeline_execution_receipt(
                pipeline_execution_receipt_path
            )
        manifest = {
            "schema_version": ARTIFACT_SCHEMA_VERSION,
            "identity": schema.IDENTITY,
            "status": "materialized_not_training_or_live_authorized",
            "created_at_utc": datetime.now(tz=UTC).isoformat(),
            "utc_day": bundle.utc_day,
            "cache_identity_sha256": expected_identity,
            "cache_identity_payload": cache_identity_payload(
                bundle,
                cutoffs_ms=cutoffs_ms,
                engine=engine,
                execution_component_identity_sha256=execution_component_sha,
                source_profile_id=source_profile_id,
                source_permissions=source_permissions,
                source_probe_identity_sha256=source_probe_sha,
            ),
            "engine": engine_identity,
            "execution_receipt": (
                None
                if execution_receipt is None or pipeline_execution_receipt_path is None
                else {
                    **execution_identity.file_identity(pipeline_execution_receipt_path),
                    "execution_identity_sha256": execution_receipt[
                        "execution_identity_sha256"
                    ],
                    "f03_component_semantics_sha256": execution_component_sha,
                }
            ),
            "source_profile_id": source_profile_id,
            "source_permissions": None if source_permissions is None else dict(source_permissions),
            "source_bundle": bundle.identity_payload(),
            "source_probe": {
                "path": PROBE_FILENAME,
                "sha256": sources.sha256_file(probe_path),
            },
            "source_runtime_audit": {
                "local_trade_tempo": local_audit.audit_payload(),
                "reference_bars": (
                    None if reference_audit is None else reference_audit.audit_payload()
                ),
                "metrics": [item.audit_payload() for item in metric_audit.files],
            },
            "panel_schema": panel_schema_payload(),
            "target_day_decision_clock": {
                "interval": "[D 00:00:00, D+1 00:00:00)",
                "bar_support_rule": "cutoff_minus_1s_completed_local_bar",
                "first_decision_uses_previous_natural_day_warmup": True,
                "next_day_midnight_included": False,
            },
            "panel": {
                "path": PANEL_FILENAME,
                "sha256": sources.sha256_file(panel_path),
                "size_bytes": panel_path.stat().st_size,
                "rows": rows,
                "first_cutoff_exclusive_ms": first_cutoff,
                "last_cutoff_exclusive_ms": last_cutoff,
            },
            "unsupported_feature_row_counts": dict(sorted(unsupported_counts.items())),
            "labels_read": False,
            "predictions_read": False,
            "economic_outcomes_read": False,
            "ten_second_feature_rows_accepted": False,
            "atomic_admission": True,
            "training_authorized": False,
            "live_authorized": False,
            "bulk_materialization_authorized": engine == CPP_BATCH_ENGINE,
        }
        manifest_path = temporary_dir / MANIFEST_FILENAME
        _atomic_json(manifest_path, manifest)
        _fsync_file(manifest_path)
        success_path = temporary_dir / SUCCESS_FILENAME
        with success_path.open("w", encoding="ascii") as handle:
            handle.write(sources.sha256_file(manifest_path) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(temporary_dir)
        os.replace(temporary_dir, output_dir)
        _fsync_directory(output_dir.parent)
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise

    return MaterializedPanel(
        output_dir=output_dir,
        panel_path=output_dir / PANEL_FILENAME,
        manifest_path=output_dir / MANIFEST_FILENAME,
        cache_identity_sha256=expected_identity,
        row_count=rows,
        reused=False,
    )


def _load_cutoffs(path: Path) -> tuple[int, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("cutoffs JSON must be a list of canonical epoch-ms values")
    return tuple(int(value) for value in payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--cutoffs-json", type=Path)
    parser.add_argument("--allow-full-day", action="store_true")
    parser.add_argument("--batch-rows", type=int, default=2_048)
    parser.add_argument("--engine", choices=SUPPORTED_ENGINES)
    parser.add_argument("--probe-only", action="store_true")
    args = parser.parse_args()
    bundle = sources.DailySourceBundle.from_json(args.source_spec.expanduser().resolve())
    if args.probe_only:
        print(json.dumps(sources.probe_source_bundle(bundle), indent=2, sort_keys=True))
        return 0
    if args.output_dir is None:
        parser.error("--output-dir is required unless --probe-only is used")
    if args.engine is None:
        parser.error(f"--engine is required; choose one of {SUPPORTED_ENGINES}")
    if args.cutoffs_json is None and not args.allow_full_day:
        parser.error("full-day materialization requires explicit --allow-full-day")
    cutoffs = None if args.cutoffs_json is None else _load_cutoffs(args.cutoffs_json)
    result = materialize_daily_panel(
        bundle,
        output_dir=args.output_dir,
        cutoffs_ms=cutoffs,
        batch_rows=args.batch_rows,
        engine=args.engine,
    )
    print(
        json.dumps(
            {
                "cache_identity_sha256": result.cache_identity_sha256,
                "manifest_path": str(result.manifest_path),
                "panel_path": str(result.panel_path),
                "reused": result.reused,
                "rows": result.row_count,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
