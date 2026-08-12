#!/usr/bin/env python3
"""Real-source C++ full-day and stratified Python parity for F03 1s panels.

The admitted panel is an expected-output artifact only.  C++ inputs are rebuilt
from the hash-bound D-1/target-day physical source bundle; panel feature values
are never fed back into either feature implementation.  Native comparison
covers every admitted cutoff.  The independent Python oracle covers a frozen,
boundary-heavy sample because the direct Python reference intentionally
recomputes long rolling windows and is not a production batch engine.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import os
import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_cpp_batch as cpp_batch,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as sources,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_full_schema as full,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_orico_source_spec as source_specs,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as materializer,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

SCHEMA_VERSION = "causal_v12_1s_real_day_cpp_fingerprint_parity.v6"
IDENTITY = "causal_v12_1s_real_day_cpp_parity_successor_v6"
CPP_ABI_VERSION = "causal_v12_1s_cpp_feature_parity.v1"
DEFAULT_RTOL = 2e-12
DEFAULT_ATOL = 2e-12
SIGNED_QUOTE_REDUCTION_ERROR_CONTRACT = (
    "ieee754_binary64_left_fold_vs_fsum_input_scale.v1"
)
SIGNED_QUOTE_REDUCTION_SAFETY_FACTOR = 2.0
SIGNED_QUOTE_REDUCTION_FEATURES = tuple(
    f"taker_signed_quote_sum_{window}s" for window in (5, 10, 30, 60)
)
FULL_DAY_ROWS = 86_400
PYTHON_ORACLE_EVEN_SAMPLE_ROWS = 1_024
PYTHON_ORACLE_EDGE_SECONDS = 120
PYTHON_ORACLE_FULL_DAY_SAMPLE_ROWS = (
    2 * PYTHON_ORACLE_EDGE_SECONDS + PYTHON_ORACLE_EVEN_SAMPLE_ROWS - 2
)
PYTHON_ORACLE_CHANNELS = (
    "value",
    "validity",
    "source_timestamp",
    "ready_timestamp",
    "observation_count",
    "lag_state",
)

_LEGACY_MANIFEST_KEYS = {
    "atomic_admission",
    "cache_identity_payload",
    "cache_identity_sha256",
    "created_at_utc",
    "economic_outcomes_read",
    "identity",
    "labels_read",
    "live_authorized",
    "panel",
    "panel_schema",
    "predictions_read",
    "schema_version",
    "source_bundle",
    "source_probe",
    "source_runtime_audit",
    "status",
    "target_day_decision_clock",
    "ten_second_feature_rows_accepted",
    "training_authorized",
    "unsupported_feature_row_counts",
    "utc_day",
    "bulk_materialization_authorized",
    "engine",
}
_SUCCESSOR_MANIFEST_KEYS = {
    *_LEGACY_MANIFEST_KEYS,
    "execution_receipt",
    "source_profile_id",
    "source_permissions",
}
_LEGACY_SOURCE_IDENTITY_KEYS = {
    "bar_clock_authority",
    "bundle_identity_sha256",
    "economic_outcomes_read",
    "execution_l2_clock_identity",
    "execution_l2_quality_authority",
    "failure_reasons",
    "files",
    "local_source_authority",
    "metrics_authority",
    "path_day_coverage",
    "physical_materialization_eligible",
    "reference_btcusdt_authority",
    "schema_version",
    "ten_second_feature_rows_accepted",
    "utc_day",
    "warmup_contract",
}
_PROFILE_SOURCE_IDENTITY_KEYS = {
    *_LEGACY_SOURCE_IDENTITY_KEYS,
    "aggregate_reference_bars_used",
    "fallback_discovery_used",
    "profile_id",
    "source_permissions",
    "substitute_warmup_used",
}
_SOURCE_GROUP_TO_BUNDLE_FIELD = {
    "local_trade_tempo": "local_trade_tempo_paths",
    "local_source_manifest": "local_source_manifest_paths",
    "execution_l2": "execution_l2_paths",
    "execution_l2_quality": "execution_l2_quality_paths",
    "metrics": "metric_paths",
    "reference_bars": "reference_bar_paths",
    "reference_bar_manifest": "reference_bar_manifest_paths",
}
_PANEL_METADATA_COLUMNS = (
    "cutoff_exclusive_ms",
    "decision_ts_ms",
    "feature_ready_ts_ms",
    "unsupported_feature_count",
    "feature_row_fingerprint_sha256",
    "local_bar_lag_state",
    "local_synthetic_seconds_24h",
    "reference_bar_lag_state",
    "reference_synthetic_seconds_1h",
)


class RealDayParityError(ValueError):
    """Fail-closed identity, source, cutoff, or feature parity violation."""


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def _canonical_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RealDayParityError(f"{label} is not readable canonical JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise RealDayParityError(f"{label} must be a JSON object: {path}")
    return payload


def _require_exact_keys(payload: Mapping[str, Any], expected: set[str], *, label: str) -> None:
    observed = set(payload)
    if observed != expected:
        raise RealDayParityError(
            f"{label} schema keys differ: missing={sorted(expected - observed)} "
            f"extra={sorted(observed - expected)}"
        )


def _artifact_child(root: Path, value: Any, *, label: str) -> Path:
    relative = Path(str(value))
    if relative.is_absolute():
        raise RealDayParityError(f"{label} must be relative to the admitted artifact")
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise RealDayParityError(f"{label} escapes the admitted artifact root") from exc
    return resolved


def _verify_file_identity(path: Path, identity: Mapping[str, Any], *, label: str) -> None:
    if not path.is_file():
        raise RealDayParityError(f"{label} is missing: {path}")
    expected_size = int(identity["size_bytes"])
    actual_size = path.stat().st_size
    if actual_size != expected_size:
        raise RealDayParityError(
            f"{label} size mismatch: expected={expected_size} actual={actual_size} path={path}"
        )
    expected_sha = str(identity["sha256"])
    actual_sha = sources.sha256_file(path)
    if actual_sha != expected_sha:
        raise RealDayParityError(
            f"{label} SHA256 mismatch: expected={expected_sha} actual={actual_sha} path={path}"
        )


def _current_python_code_identity() -> dict[str, str]:
    return {
        "daily_source_reader_sha256": sources.sha256_file(Path(sources.__file__).resolve()),
        "full_feature_generator_sha256": sources.sha256_file(Path(full.__file__).resolve()),
        "materializer_sha256": sources.sha256_file(Path(materializer.__file__).resolve()),
        "trainable_schema_sha256": sources.sha256_file(Path(schema.__file__).resolve()),
    }


@dataclass(frozen=True, slots=True)
class AdmittedPanel:
    root: Path
    manifest_path: Path
    manifest_sha256: str
    manifest: Mapping[str, Any]
    panel_path: Path
    source_identity_path: Path
    source_identity_sha256: str
    source_identity: Mapping[str, Any]
    bundle: sources.DailySourceBundle


def _bundle_from_bound_identity(
    manifest: Mapping[str, Any], source_identity: Mapping[str, Any]
) -> sources.DailySourceBundle:
    bound_inputs = manifest["source_bundle"]["inputs"]
    if not isinstance(bound_inputs, list) or not bound_inputs:
        raise RealDayParityError("source_bundle.inputs must be a non-empty list")
    probe_files = source_identity["files"]
    if not isinstance(probe_files, list) or not probe_files:
        raise RealDayParityError("source identity files must be a non-empty list")
    projected_probe = [
        {
            "path": str(row["path"]),
            "size_bytes": int(row["size_bytes"]),
            "sha256": str(row["sha256"]),
        }
        for row in probe_files
    ]
    if projected_probe != bound_inputs:
        raise RealDayParityError("source probe files differ from manifest source_bundle inputs")

    grouped: dict[str, list[Path]] = {
        field_name: [] for field_name in _SOURCE_GROUP_TO_BUNDLE_FIELD.values()
    }
    seen_paths: set[str] = set()
    for row in probe_files:
        group = str(row.get("group"))
        field_name = _SOURCE_GROUP_TO_BUNDLE_FIELD.get(group)
        if field_name is None:
            raise RealDayParityError(f"unsupported source identity group: {group}")
        path_text = str(row["path"])
        if path_text in seen_paths:
            raise RealDayParityError(f"duplicate physical source path: {path_text}")
        seen_paths.add(path_text)
        path = Path(path_text).expanduser().resolve()
        _verify_file_identity(path, row, label=f"physical source ({group})")
        grouped[field_name].append(path)

    source_ids = manifest["source_bundle"]["source_identities"]
    bundle = sources.DailySourceBundle(
        utc_day=str(manifest["utc_day"]),
        local_trade_tempo_paths=tuple(grouped["local_trade_tempo_paths"]),
        local_source_manifest_paths=tuple(grouped["local_source_manifest_paths"]),
        execution_l2_paths=tuple(grouped["execution_l2_paths"]),
        execution_l2_quality_paths=tuple(grouped["execution_l2_quality_paths"]),
        metric_paths=tuple(grouped["metric_paths"]),
        reference_bar_paths=tuple(grouped["reference_bar_paths"]),
        reference_bar_manifest_paths=tuple(grouped["reference_bar_manifest_paths"]),
        local_source_identity=str(source_ids["local"]),
        execution_l2_clock_identity=str(source_ids["execution_l2_clock"]),
        metric_source_identity=str(source_ids["metrics"]),
        reference_source_identity=str(source_ids["reference"]),
    )
    if bundle.identity_payload() != manifest["source_bundle"]:
        raise RealDayParityError("reconstructed physical source bundle identity differs")
    expected_bundle_sha = str(manifest["cache_identity_payload"]["bundle_identity_sha256"])
    if bundle.identity_sha256() != expected_bundle_sha:
        raise RealDayParityError("reconstructed source bundle canonical SHA256 differs")
    if str(source_identity["bundle_identity_sha256"]) != expected_bundle_sha:
        raise RealDayParityError("source identity bundle SHA256 differs from panel manifest")
    return bundle


def _validate_panel_schema_payload(payload: Mapping[str, Any]) -> None:
    expected = materializer.panel_schema_payload()
    for key in (
        "columns",
        "feature_order_sha256",
        "feature_count",
        "target_day_decision_clock",
        "ten_second_feature_rows_accepted",
    ):
        if payload.get(key) != expected.get(key):
            raise RealDayParityError(f"panel schema payload differs at {key}")


def load_admitted_panel(
    panel_manifest_path: Path,
    source_bundle_identity_path: Path,
) -> AdmittedPanel:
    """Load and fully rehash an admitted panel plus its source identity artifact."""

    manifest_path = panel_manifest_path.expanduser().resolve()
    root = manifest_path.parent
    manifest = _read_json_object(manifest_path, label="panel manifest")
    artifact_schema = str(manifest.get("schema_version", ""))
    if artifact_schema == "causal_v12_1s_daily_feature_panel_artifact.v3":
        _require_exact_keys(manifest, _LEGACY_MANIFEST_KEYS, label="panel manifest")
    elif artifact_schema == materializer.ARTIFACT_SCHEMA_VERSION:
        _require_exact_keys(manifest, _SUCCESSOR_MANIFEST_KEYS, label="panel manifest")
    else:
        raise RealDayParityError("unsupported admitted panel schema version")
    if manifest.get("identity") != schema.IDENTITY:
        raise RealDayParityError("admitted panel feature identity differs")
    if manifest.get("status") != "materialized_not_training_or_live_authorized":
        raise RealDayParityError("admitted panel status is not the frozen materialized state")
    if manifest.get("atomic_admission") is not True:
        raise RealDayParityError("panel was not atomically admitted")
    for key in ("labels_read", "predictions_read", "economic_outcomes_read"):
        if manifest.get(key) is not False:
            raise RealDayParityError(f"panel manifest must bind {key}=false")
    for key in ("training_authorized", "live_authorized"):
        if manifest.get(key) is not False:
            raise RealDayParityError(f"parity input may not carry {key}=true")
    if manifest.get("ten_second_feature_rows_accepted") is not False:
        raise RealDayParityError("10s feature rows are forbidden parity inputs")
    if manifest.get("bulk_materialization_authorized") is not True:
        raise RealDayParityError("panel lacks bulk materialization authority")
    engine = manifest.get("engine")
    if not isinstance(engine, Mapping) or engine.get("engine") != "cpp_batch":
        raise RealDayParityError("panel engine must be strongly typed cpp_batch")
    if engine != manifest.get("cache_identity_payload", {}).get("engine"):
        raise RealDayParityError("top-level and cache-bound engine identities differ")

    manifest_sha = sources.sha256_file(manifest_path)
    success_path = root / materializer.SUCCESS_FILENAME
    if not success_path.is_file():
        raise RealDayParityError("admission _SUCCESS marker is missing")
    if success_path.read_text(encoding="ascii").strip() != manifest_sha:
        raise RealDayParityError("admission _SUCCESS marker does not bind manifest SHA256")

    _validate_panel_schema_payload(manifest["panel_schema"])
    cache_payload = manifest["cache_identity_payload"]
    if _canonical_sha256(cache_payload) != str(manifest["cache_identity_sha256"]):
        raise RealDayParityError("cache identity canonical SHA256 mismatch")
    code = cache_payload.get("code")
    if not isinstance(code, Mapping):
        raise RealDayParityError("admitted panel lacks code identity")
    execution_identity.component_projection_from_legacy_panel_code(code)
    if cache_payload.get("artifact_schema_sha256") != _canonical_sha256(
        manifest["panel_schema"]
    ):
        raise RealDayParityError("artifact schema SHA256 differs")
    if cache_payload.get("feature_contract_sha256") != full.full_feature_contract_fingerprint():
        raise RealDayParityError("full feature contract SHA256 differs")
    if cache_payload.get("feature_order_sha256") != schema.feature_order_sha256():
        raise RealDayParityError("feature order SHA256 differs")
    expected_source_manifest_sha = schema.canonical_sha256(schema.source_manifest_payload())
    if cache_payload.get("source_manifest_sha256") != expected_source_manifest_sha:
        raise RealDayParityError("feature source-manifest SHA256 differs")

    panel_path = _artifact_child(root, manifest["panel"]["path"], label="panel.path")
    _verify_file_identity(panel_path, manifest["panel"], label="admitted panel")
    parquet = pq.ParquetFile(panel_path)
    expected_columns = [*_PANEL_METADATA_COLUMNS, *schema.TRAINABLE_FEATURE_ORDER]
    if parquet.schema_arrow.names != expected_columns:
        raise RealDayParityError("panel Parquet schema/order differs from the 173-field ABI")
    if parquet.metadata.num_rows != int(manifest["panel"]["rows"]):
        raise RealDayParityError("panel Parquet row count differs from manifest")

    source_identity_path = source_bundle_identity_path.expanduser().resolve()
    bound_source_path = _artifact_child(
        root, manifest["source_probe"]["path"], label="source_probe.path"
    )
    if source_identity_path != bound_source_path:
        raise RealDayParityError("provided source bundle identity is not the admitted source_probe")
    source_identity_sha = sources.sha256_file(source_identity_path)
    if source_identity_sha != str(manifest["source_probe"]["sha256"]):
        raise RealDayParityError("source bundle identity SHA256 differs from panel manifest")
    source_identity = _read_json_object(source_identity_path, label="source bundle identity")
    source_schema = str(source_identity.get("schema_version", ""))
    if source_schema == "causal_v12_1s_daily_source_probe.v2":
        _require_exact_keys(
            source_identity,
            _LEGACY_SOURCE_IDENTITY_KEYS,
            label="source bundle identity",
        )
    elif source_schema in {
        "causal_v12_1s_orico_exact_root_source_probe.v1",
        "causal_v12_1s_profile_bound_source_probe.v3",
    }:
        expected_keys = set(_PROFILE_SOURCE_IDENTITY_KEYS)
        if source_schema == "causal_v12_1s_orico_exact_root_source_probe.v1":
            expected_keys.remove("source_permissions")
        _require_exact_keys(source_identity, expected_keys, label="source bundle identity")
    else:
        raise RealDayParityError("unsupported source bundle identity schema")
    if source_identity.get("utc_day") != manifest.get("utc_day"):
        raise RealDayParityError("source identity UTC day differs from panel")
    if source_identity.get("physical_materialization_eligible") is not True:
        raise RealDayParityError("source identity is not physical-materialization eligible")
    if source_identity.get("failure_reasons") != []:
        raise RealDayParityError("source identity contains failure reasons")
    if source_identity.get("ten_second_feature_rows_accepted") is not False:
        raise RealDayParityError("source identity admits forbidden 10s feature rows")
    if source_identity.get("economic_outcomes_read") is not False:
        raise RealDayParityError("source identity must bind economic_outcomes_read=false")

    bundle = _bundle_from_bound_identity(manifest, source_identity)
    return AdmittedPanel(
        root=root,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        manifest=manifest,
        panel_path=panel_path,
        source_identity_path=source_identity_path,
        source_identity_sha256=source_identity_sha,
        source_identity=source_identity,
        bundle=bundle,
    )


@dataclass(frozen=True, slots=True)
class PhysicalSourceState:
    local_bars: tuple[base.OneSecondBar, ...]
    local_times: tuple[int, ...]
    local_synthetic: tuple[int, ...]
    local_synthetic_set: frozenset[int]
    execution_l2_by_start: Mapping[int, full.ExecutionL2Observation]
    metrics: tuple[full.MetricObservation, ...]
    metric_times: tuple[int, ...]
    reference_bars: tuple[base.OneSecondBar, ...]
    reference_times: tuple[int, ...]
    reference_synthetic: tuple[int, ...]
    reference_synthetic_set: frozenset[int]


def load_physical_source_state(bundle: sources.DailySourceBundle) -> PhysicalSourceState:
    """Read the hash-validated physical sources once; no panel values are consumed."""

    local = sources.read_local_trade_bars_with_audit(bundle.local_trade_tempo_paths)
    execution_l2 = sources.read_execution_l2(bundle.execution_l2_paths)
    metrics = sources.read_metrics_with_audit(bundle.metric_paths).observations
    reference_audit = sources.read_reference_bars_with_audit(bundle.reference_bar_paths)
    reference_bars = () if reference_audit is None else reference_audit.bars
    local_times = tuple(int(item.start_ts_ms) for item in local.bars)
    metric_times = tuple(int(item.source_ts_ms) for item in metrics)
    reference_times = tuple(int(item.start_ts_ms) for item in reference_bars)
    l2_by_start = {int(item.bucket_start_ts_ms): item for item in execution_l2}
    if len(l2_by_start) != len(execution_l2):
        raise RealDayParityError("duplicate reduced execution-L2 bucket")
    return PhysicalSourceState(
        local_bars=local.bars,
        local_times=local_times,
        local_synthetic=tuple(sorted(int(value) for value in local.synthesized_start_ts_ms)),
        local_synthetic_set=frozenset(int(value) for value in local.synthesized_start_ts_ms),
        execution_l2_by_start=l2_by_start,
        metrics=metrics,
        metric_times=metric_times,
        reference_bars=reference_bars,
        reference_times=reference_times,
        reference_synthetic=tuple(
            ()
            if reference_audit is None
            else sorted(int(value) for value in reference_audit.synthesized_start_ts_ms)
        ),
        reference_synthetic_set=frozenset(
            ()
            if reference_audit is None
            else (int(value) for value in reference_audit.synthesized_start_ts_ms)
        ),
    )


@dataclass(frozen=True, slots=True)
class CutoffSourceView:
    local_bars: tuple[base.OneSecondBar, ...]
    execution_l2: tuple[full.ExecutionL2Observation, ...]
    metrics: tuple[full.MetricObservation, ...]
    reference_bars: tuple[base.OneSecondBar, ...]
    local_bar_lag_state: str
    local_synthetic_seconds_24h: int
    reference_bar_lag_state: str
    reference_synthetic_seconds_1h: int


def _physical_lag_state_at_cutoff(
    state: PhysicalSourceState,
    cutoff: int,
) -> dict[str, str | int]:
    current_start = cutoff - schema.CADENCE_MS
    local_synth_start = bisect.bisect_left(state.local_synthetic, cutoff - 86_400_000)
    local_synth_end = bisect.bisect_left(state.local_synthetic, cutoff)
    reference_synth_start = bisect.bisect_left(
        state.reference_synthetic,
        cutoff - 3_600_000,
    )
    reference_synth_end = bisect.bisect_left(state.reference_synthetic, cutoff)
    return {
        "local_bar_lag_state": (
            sources.SYNTHETIC_BAR_LAG_STATE
            if current_start in state.local_synthetic_set
            else sources.OBSERVED_BAR_LAG_STATE
        ),
        "local_synthetic_seconds_24h": local_synth_end - local_synth_start,
        "reference_bar_lag_state": (
            "source_unavailable"
            if not state.reference_bars
            else sources.SYNTHETIC_BAR_LAG_STATE
            if current_start in state.reference_synthetic_set
            else sources.OBSERVED_BAR_LAG_STATE
        ),
        "reference_synthetic_seconds_1h": reference_synth_end - reference_synth_start,
    }


def source_view_at_cutoff(state: PhysicalSourceState, cutoff: int) -> CutoffSourceView:
    local_end = bisect.bisect_left(state.local_times, cutoff)
    local_start = max(0, local_end - materializer.MAX_LOCAL_HISTORY_BARS)
    reference_end = bisect.bisect_left(state.reference_times, cutoff)
    reference_start = max(0, reference_end - materializer.MAX_REFERENCE_HISTORY_BARS)
    metric_end = bisect.bisect_left(state.metric_times, cutoff)
    metric_start = max(0, metric_end - materializer.MAX_METRIC_HISTORY_ROWS)
    current_start = cutoff - schema.CADENCE_MS
    l2 = state.execution_l2_by_start.get(current_start)
    lag = _physical_lag_state_at_cutoff(state, cutoff)
    return CutoffSourceView(
        local_bars=state.local_bars[local_start:local_end],
        execution_l2=() if l2 is None else (l2,),
        metrics=state.metrics[metric_start:metric_end],
        reference_bars=state.reference_bars[reference_start:reference_end],
        local_bar_lag_state=str(lag["local_bar_lag_state"]),
        local_synthetic_seconds_24h=int(lag["local_synthetic_seconds_24h"]),
        reference_bar_lag_state=str(lag["reference_bar_lag_state"]),
        reference_synthetic_seconds_1h=int(lag["reference_synthetic_seconds_1h"]),
    )


def python_oracle_sample_indices(row_count: int) -> tuple[int, ...]:
    """Freeze a boundary-heavy, evenly distributed Python-oracle denominator."""

    if row_count <= 0:
        raise RealDayParityError("Python-oracle row count must be positive")
    if row_count <= PYTHON_ORACLE_EVEN_SAMPLE_ROWS + 2 * PYTHON_ORACLE_EDGE_SECONDS:
        return tuple(range(row_count))
    offsets = set(range(PYTHON_ORACLE_EDGE_SECONDS))
    offsets.update(range(row_count - PYTHON_ORACLE_EDGE_SECONDS, row_count))
    interior_count = PYTHON_ORACLE_EVEN_SAMPLE_ROWS - 2
    interior_start = PYTHON_ORACLE_EDGE_SECONDS
    interior_end = row_count - PYTHON_ORACLE_EDGE_SECONDS - 1
    interior_denominator = interior_count - 1
    even_offsets = {0, row_count - 1}
    even_offsets.update(
        interior_start
        + (index * (interior_end - interior_start)) // interior_denominator
        for index in range(interior_count)
    )
    if len(even_offsets) != PYTHON_ORACLE_EVEN_SAMPLE_ROWS:
        raise RealDayParityError("Python-oracle even sample denominator collapsed")
    offsets.update(even_offsets)
    result = tuple(sorted(offsets))
    if len(result) != PYTHON_ORACLE_FULL_DAY_SAMPLE_ROWS:
        raise RealDayParityError(
            "Python-oracle full-day sample denominator differs from the frozen contract"
        )
    return result


def python_oracle_sample_cutoffs(day_start_ms: int, row_count: int) -> tuple[int, ...]:
    return tuple(
        day_start_ms + index * schema.CADENCE_MS
        for index in python_oracle_sample_indices(row_count)
    )


def _load_cpp() -> Any:
    try:
        import narrowgate_cpp as cpp
    except ImportError as exc:
        raise RealDayParityError("narrowgate_cpp extension is unavailable") from exc
    if getattr(cpp, "F03_CAUSAL_V12_1S_FEATURE_ABI_VERSION", None) != CPP_ABI_VERSION:
        raise RealDayParityError("C++ F03 feature ABI version differs")
    if tuple(cpp.F03_CAUSAL_V12_1S_FEATURE_NAMES) != schema.TRAINABLE_FEATURE_ORDER:
        raise RealDayParityError("C++ F03 feature order differs from Python")
    if cpp.F03_CAUSAL_V12_1S_FEATURE_ORDER_SHA256 != schema.feature_order_sha256():
        raise RealDayParityError("C++ F03 feature-order SHA256 differs from Python")
    return cpp


def _implementation_identity(
    cpp: Any,
    *,
    native_build_receipt_path: Path | None,
) -> dict[str, Any]:
    repository_root = Path(__file__).resolve().parents[4]
    cpp_source = (
        repository_root / "research/families/f03_causal_13_head/cpp/causal_v12_1s_features.cpp"
    )
    cpp_header = (
        repository_root / "research/families/f03_causal_13_head/cpp/causal_v12_1s_features.hpp"
    )
    bindings = repository_root / "cpp/narrowgate_cpp/bindings.cpp"
    extension = Path(cpp.__file__).resolve()
    identity = {
        "runner_path": str(Path(__file__).resolve()),
        "runner_sha256": sources.sha256_file(Path(__file__).resolve()),
        "python_code": _current_python_code_identity(),
        "cpp_source_path": str(cpp_source),
        "cpp_source_sha256": sources.sha256_file(cpp_source),
        "cpp_header_path": str(cpp_header),
        "cpp_header_sha256": sources.sha256_file(cpp_header),
        "bindings_path": str(bindings),
        "bindings_sha256": sources.sha256_file(bindings),
        "loaded_extension_path": str(extension),
        "loaded_extension_sha256": sources.sha256_file(extension),
        "python_abi": execution_identity.python_abi_identity(),
        "f03_component_semantics": execution_identity.f03_component_semantics(cpp),
    }
    if native_build_receipt_path is not None:
        receipt_path = native_build_receipt_path.expanduser().resolve(strict=True)
        receipt = execution_identity.validate_native_build_receipt(receipt_path, cpp=cpp)
        identity["native_build_receipt"] = {
            **execution_identity.file_identity(receipt_path),
            "receipt_sha256": receipt["receipt_sha256"],
        }
    else:
        identity["native_build_receipt"] = None
    return identity


def _validate_profile_binding(
    admitted: AdmittedPanel,
    *,
    profile_id: str | None,
    market_data_root: Path | None,
) -> dict[str, Any] | None:
    if profile_id is None and market_data_root is None:
        return None
    if profile_id is None or market_data_root is None:
        raise RealDayParityError("profile_id and market_data_root must be provided together")
    try:
        built = source_specs.build_orico_daily_source_spec(
            target_day=str(admitted.manifest["utc_day"]),
            market_data_root=market_data_root,
            profile_id=profile_id,
        )
    except (OSError, ValueError, TypeError) as exc:
        raise RealDayParityError(f"profile-bound source validation failed: {exc}") from exc
    if built.bundle.identity_payload() != admitted.bundle.identity_payload():
        raise RealDayParityError("profile-resolved source bundle differs from admitted panel")
    permissions = built.probe.get("source_permissions")
    if not isinstance(permissions, Mapping):
        raise RealDayParityError("profile-bound source probe lacks permission contract")
    for key in ("queue_authority", "order_lifecycle_authority", "fill_path_authority", "pnl_authority"):
        if permissions.get(key) is not False:
            raise RealDayParityError(f"profile source permission must bind {key}=false")
    return {
        "profile_id": profile_id,
        "source_permissions": dict(permissions),
        "source_probe_identity_sha256": _canonical_sha256(built.probe),
        "bundle_identity_sha256": built.bundle.identity_sha256(),
    }


def _cpp_bar(cpp: Any, item: base.OneSecondBar) -> Any:
    result = cpp.F03CausalV12OneSecondBar()
    for name in (
        "start_ts_ms",
        "finalized_ts_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "buy_volume",
        "sell_volume",
        "trade_count",
        "buy_count",
        "sell_count",
        "buy_quote_qty",
        "sell_quote_qty",
        "max_same_side_run",
        "buy_price_high",
        "buy_price_low",
        "sell_price_high",
        "sell_price_low",
    ):
        setattr(result, name, getattr(item, name))
    return result


def _cpp_l2(cpp: Any, item: full.ExecutionL2Observation) -> Any:
    result = cpp.F03CausalV12ExecutionL2Observation()
    result.bucket_start_ts_ms = item.bucket_start_ts_ms
    result.feature_ready_ts_ms = item.feature_ready_ts_ms
    result.values = [item.values[name] for name in schema.EXECUTION_L2_FEATURES]
    return result


def _cpp_metric(cpp: Any, item: full.MetricObservation) -> Any:
    result = cpp.F03CausalV12MetricObservation()
    for name in (
        "source_ts_ms",
        "feature_ready_ts_ms",
        "sum_open_interest",
        "toptrader_ls_ratio",
        "crowd_ls_ratio",
        "taker_ls_ratio",
    ):
        setattr(result, name, getattr(item, name))
    return result


def compute_cpp_row(
    cpp: Any,
    view: CutoffSourceView,
    *,
    cutoff_exclusive_ms: int,
    decision_ts_ms: int,
) -> Mapping[str, Any]:
    return cpp.compute_f03_causal_v12_1s_features(
        [_cpp_bar(cpp, item) for item in view.local_bars],
        cutoff_exclusive_ms,
        decision_ts_ms,
        [_cpp_l2(cpp, item) for item in view.execution_l2],
        [_cpp_metric(cpp, item) for item in view.metrics],
        [_cpp_bar(cpp, item) for item in view.reference_bars],
    )


@dataclass(slots=True)
class FieldStats:
    supported_rows: int = 0
    unsupported_rows: int = 0
    exact_value_matches: int = 0
    max_abs_error: float = 0.0
    max_rel_error: float = 0.0
    max_allowed_abs_error: float = 0.0

    def observe(
        self,
        expected: float | None,
        actual: float,
        *,
        allowed_abs_error: float = 0.0,
    ) -> None:
        if expected is None:
            self.unsupported_rows += 1
            return
        self.supported_rows += 1
        if actual == expected:
            self.exact_value_matches += 1
        absolute = abs(actual - expected)
        relative = absolute / abs(expected) if expected != 0.0 else absolute
        self.max_abs_error = max(self.max_abs_error, absolute)
        self.max_rel_error = max(self.max_rel_error, relative)
        self.max_allowed_abs_error = max(
            self.max_allowed_abs_error,
            allowed_abs_error,
        )

    def payload(self) -> dict[str, int | float]:
        return {
            "supported_rows": self.supported_rows,
            "unsupported_rows": self.unsupported_rows,
            "exact_value_matches": self.exact_value_matches,
            "max_abs_error": self.max_abs_error,
            "max_rel_error": self.max_rel_error,
            "max_allowed_abs_error": self.max_allowed_abs_error,
        }


def _close(expected: float, actual: float, *, rtol: float, atol: float) -> bool:
    return math.isfinite(actual) and abs(actual - expected) <= atol + rtol * abs(expected)


def _signed_quote_reduction_error_bound(
    name: str,
    view: CutoffSourceView,
) -> float:
    """Bound binary64 left-fold cancellation against the Python fsum oracle."""

    cell = _signed_quote_reduction_cell(name, view)
    return 0.0 if cell is None else cell["computed_envelope"]


def _signed_quote_reduction_cell(
    name: str,
    view: CutoffSourceView,
) -> dict[str, int | float] | None:
    """Return the input-derived forward-error cell for an allowlisted reduction."""

    prefix = "taker_signed_quote_sum_"
    if name not in SIGNED_QUOTE_REDUCTION_FEATURES:
        return None
    window_text = name[len(prefix) : -1]
    window = int(window_text)
    bars = view.local_bars[-window:]
    if not bars:
        return None
    input_scale = math.fsum(
        abs(float(bar.buy_quote_qty)) + abs(float(bar.sell_quote_qty))
        for bar in bars
    )
    unit_roundoff = math.ulp(1.0) / 2.0
    operations = len(bars)
    gamma = (operations * unit_roundoff) / (1.0 - operations * unit_roundoff)
    return {
        "observation_count": operations,
        "input_l1_scale": input_scale,
        "gamma_n": gamma,
        "computed_envelope": SIGNED_QUOTE_REDUCTION_SAFETY_FACTOR
        * (gamma + 3.0 * unit_roundoff)
        * input_scale,
    }


def _feature_abs_tolerance(
    name: str,
    expected: float,
    view: CutoffSourceView,
    *,
    rtol: float,
    atol: float,
) -> float:
    base_tolerance = atol + rtol * abs(expected)
    reduction_cell = _signed_quote_reduction_cell(name, view)
    return (
        base_tolerance
        if reduction_cell is None
        else float(reduction_cell["computed_envelope"])
    )


def numeric_comparison_contract(*, rtol: float, atol: float) -> dict[str, Any]:
    return {
        "base": {
            "absolute_tolerance": atol,
            "relative_tolerance": rtol,
            "formula": "atol_plus_rtol_times_abs_python_oracle",
        },
        "signed_quote_reduction": {
            "contract_id": SIGNED_QUOTE_REDUCTION_ERROR_CONTRACT,
            "features": list(SIGNED_QUOTE_REDUCTION_FEATURES),
            "input_scale": "sum_abs_buy_quote_qty_plus_abs_sell_quote_qty",
            "unit_roundoff": math.ulp(1.0) / 2.0,
            "left_fold_operations": "visible_window_bar_count",
            "safety_factor": SIGNED_QUOTE_REDUCTION_SAFETY_FACTOR,
            "scope": "python_fsum_oracle_vs_cpp_binary64_left_fold_only",
            "reduction_graph": (
                "cpp_left_fold_buy_and_sell_then_binary64_subtract_vs_"
                "python_fsum_buy_and_sell_then_binary64_subtract"
            ),
            "rounding_mode": "round_to_nearest_ties_to_even",
            "fast_math": "forbidden_by_contract",
            "derived_features_remain_on_base_tolerance": True,
        },
    }


@dataclass(slots=True)
class ReductionEnvelopeStats:
    evaluated_cells: int = 0
    passed_cells: int = 0
    failed_cells: int = 0
    min_observation_count: int | None = None
    max_observation_count: int = 0
    max_input_l1_scale: float = 0.0
    max_computed_envelope: float = 0.0
    max_observed_abs_error: float = 0.0
    max_error_envelope_utilization: float = 0.0

    def observe(
        self,
        *,
        observation_count: int,
        input_l1_scale: float,
        computed_envelope: float,
        observed_abs_error: float,
        passed: bool,
    ) -> None:
        self.evaluated_cells += 1
        self.passed_cells += int(passed)
        self.failed_cells += int(not passed)
        self.min_observation_count = (
            observation_count
            if self.min_observation_count is None
            else min(self.min_observation_count, observation_count)
        )
        self.max_observation_count = max(self.max_observation_count, observation_count)
        self.max_input_l1_scale = max(self.max_input_l1_scale, input_l1_scale)
        self.max_computed_envelope = max(
            self.max_computed_envelope,
            computed_envelope,
        )
        self.max_observed_abs_error = max(
            self.max_observed_abs_error,
            observed_abs_error,
        )
        utilization = (
            observed_abs_error / computed_envelope
            if computed_envelope > 0.0
            else float(observed_abs_error > 0.0)
        )
        self.max_error_envelope_utilization = max(
            self.max_error_envelope_utilization,
            utilization,
        )

    def payload(self) -> dict[str, int | float | None]:
        return {
            "evaluated_cells": self.evaluated_cells,
            "passed_cells": self.passed_cells,
            "failed_cells": self.failed_cells,
            "min_observation_count": self.min_observation_count,
            "max_observation_count": self.max_observation_count,
            "max_input_l1_scale": self.max_input_l1_scale,
            "max_computed_envelope": self.max_computed_envelope,
            "max_observed_abs_error": self.max_observed_abs_error,
            "max_error_envelope_utilization": self.max_error_envelope_utilization,
        }


class ReductionEnvelopeAudit:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self.total = ReductionEnvelopeStats()
        self.by_feature = {
            name: ReductionEnvelopeStats() for name in SIGNED_QUOTE_REDUCTION_FEATURES
        }

    def observe(
        self,
        *,
        cutoff: int,
        name: str,
        expected: float,
        actual: float,
        cell: Mapping[str, int | float],
    ) -> None:
        observed_abs_error = abs(actual - expected)
        envelope = float(cell["computed_envelope"])
        passed = math.isfinite(actual) and observed_abs_error <= envelope
        values = {
            "observation_count": int(cell["observation_count"]),
            "input_l1_scale": float(cell["input_l1_scale"]),
            "computed_envelope": envelope,
            "observed_abs_error": observed_abs_error,
            "passed": passed,
        }
        self.total.observe(**values)
        self.by_feature[name].observe(**values)
        self._digest.update(
            _canonical_bytes(
                {
                    "cutoff_exclusive_ms": cutoff,
                    "feature": name,
                    "observation_count": values["observation_count"],
                    "input_l1_scale_hex": values["input_l1_scale"].hex(),
                    "computed_envelope_hex": values["computed_envelope"].hex(),
                    "python_value_hex": expected.hex(),
                    "cpp_value_hex": actual.hex(),
                    "observed_abs_error_hex": values["observed_abs_error"].hex(),
                    "passed": passed,
                }
            )
        )

    def payload(self, *, contract_sha256: str) -> dict[str, Any]:
        return {
            "contract_id": SIGNED_QUOTE_REDUCTION_ERROR_CONTRACT,
            "numeric_comparison_contract_sha256": contract_sha256,
            "feature_allowlist": list(SIGNED_QUOTE_REDUCTION_FEATURES),
            "total": self.total.payload(),
            "by_feature": {
                name: self.by_feature[name].payload()
                for name in SIGNED_QUOTE_REDUCTION_FEATURES
            },
            "comparison_stream_sha256": self._digest.hexdigest(),
        }


def _cpp_exact_fingerprint(cutoff: int, cpp_row: Mapping[str, Any]) -> str:
    values = []
    for index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        valid = bool(cpp_row["valid"][index])
        value = float(cpp_row["values"][index])
        values.append(
            {
                "name": name,
                "value_hex": value.hex() if valid else None,
                "source_latest_ts_ms": (
                    None
                    if int(cpp_row["source_latest_ts_ms"][index]) == -1
                    else int(cpp_row["source_latest_ts_ms"][index])
                ),
                "feature_ready_ts_ms": (
                    None
                    if int(cpp_row["feature_ready_ts_ms_by_feature"][index]) == -1
                    else int(cpp_row["feature_ready_ts_ms_by_feature"][index])
                ),
                "observation_count": int(cpp_row["observation_count"][index]),
                "lag_state": str(cpp_row["lag_state"][index]),
            }
        )
    return schema.canonical_sha256(
        {
            "cutoff_exclusive_ms": cutoff,
            "feature_contract_sha256": full.full_feature_contract_fingerprint(),
            "feature_order_sha256": schema.feature_order_sha256(),
            "source_manifest_sha256": schema.canonical_sha256(schema.source_manifest_payload()),
            "values": values,
        }
    )


def _compare_panel_cpp_full_day_row(
    panel_row: Mapping[str, Any],
    cpp_row: Mapping[str, Any],
    physical_lag: Mapping[str, str | int],
    field_stats: Mapping[str, FieldStats],
    *,
    rtol: float,
    atol: float,
) -> bool:
    cutoff = int(panel_row["cutoff_exclusive_ms"])
    if int(cpp_row["cutoff_exclusive_ms"]) != cutoff:
        raise RealDayParityError(f"panel/C++ cutoff mismatch at {cutoff}")
    if int(cpp_row["decision_ts_ms"]) != int(panel_row["decision_ts_ms"]):
        raise RealDayParityError(f"panel/C++ decision timestamp mismatch at {cutoff}")
    if int(cpp_row["feature_ready_ts_ms"]) != int(panel_row["feature_ready_ts_ms"]):
        raise RealDayParityError(f"panel/C++ row-ready mismatch at {cutoff}")
    for name, expected in physical_lag.items():
        if panel_row[name] != expected:
            raise RealDayParityError(
                f"panel physical lag-state mismatch at cutoff {cutoff}: {name}"
            )
    arrays = (
        "values",
        "valid",
        "source_latest_ts_ms",
        "feature_ready_ts_ms_by_feature",
        "observation_count",
        "lag_state",
    )
    if any(len(cpp_row[name]) != len(schema.TRAINABLE_FEATURE_ORDER) for name in arrays):
        raise RealDayParityError(f"C++ batch row ABI length mismatch at cutoff {cutoff}")
    unsupported = 0
    for index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        panel_value = panel_row[name]
        actual_valid = bool(cpp_row["valid"][index])
        actual_value = float(cpp_row["values"][index])
        expected_valid = panel_value is not None
        if actual_valid is not expected_valid:
            raise RealDayParityError(
                f"panel/C++ validity mismatch at cutoff {cutoff} feature {name}"
            )
        if panel_value is None:
            unsupported += 1
            if not math.isnan(actual_value):
                raise RealDayParityError(
                    f"C++ invalid feature has a value at cutoff {cutoff} feature {name}"
                )
        elif not _close(float(panel_value), actual_value, rtol=rtol, atol=atol):
            raise RealDayParityError(
                f"panel/C++ value mismatch at cutoff {cutoff} feature {name}: "
                f"panel={panel_value!r} actual={actual_value!r}"
            )
        field_stats[name].observe(
            None if panel_value is None else float(panel_value),
            actual_value,
        )
    if int(panel_row["unsupported_feature_count"]) != unsupported:
        raise RealDayParityError(f"panel unsupported-feature count mismatch at {cutoff}")
    panel_fingerprint = str(panel_row["feature_row_fingerprint_sha256"])
    if len(panel_fingerprint) != 64 or any(
        character not in "0123456789abcdef" for character in panel_fingerprint
    ):
        raise RealDayParityError(f"panel row fingerprint is malformed at cutoff {cutoff}")
    return panel_fingerprint == _cpp_exact_fingerprint(cutoff, cpp_row)


def _compare_one_row(
    panel_row: Mapping[str, Any],
    py_row: full.FullFeatureRow,
    cpp_row: Mapping[str, Any],
    view: CutoffSourceView,
    field_stats: Mapping[str, FieldStats],
    *,
    rtol: float,
    atol: float,
    reduction_envelope_audit: ReductionEnvelopeAudit | None = None,
) -> tuple[bool, bool, bool]:
    cutoff = int(panel_row["cutoff_exclusive_ms"])
    decision = int(panel_row["decision_ts_ms"])
    if py_row.cutoff_exclusive_ms != cutoff or int(cpp_row["cutoff_exclusive_ms"]) != cutoff:
        raise RealDayParityError(f"cutoff mismatch at {cutoff}")
    if py_row.decision_ts_ms != decision or int(cpp_row["decision_ts_ms"]) != decision:
        raise RealDayParityError(f"decision timestamp mismatch at cutoff {cutoff}")
    expected_ready = int(panel_row["feature_ready_ts_ms"])
    if py_row.feature_ready_ts_ms != expected_ready:
        raise RealDayParityError(f"panel/Python row-ready mismatch at cutoff {cutoff}")
    if int(cpp_row["feature_ready_ts_ms"]) != expected_ready:
        raise RealDayParityError(f"Python/C++ row-ready mismatch at cutoff {cutoff}")
    panel_fingerprint = str(panel_row["feature_row_fingerprint_sha256"])
    if len(panel_fingerprint) != 64 or any(
        character not in "0123456789abcdef" for character in panel_fingerprint
    ):
        raise RealDayParityError(f"panel row fingerprint is malformed at cutoff {cutoff}")

    physical_expected = {
        "local_bar_lag_state": view.local_bar_lag_state,
        "local_synthetic_seconds_24h": view.local_synthetic_seconds_24h,
        "reference_bar_lag_state": view.reference_bar_lag_state,
        "reference_synthetic_seconds_1h": view.reference_synthetic_seconds_1h,
    }
    for name, expected in physical_expected.items():
        if panel_row[name] != expected:
            raise RealDayParityError(
                f"panel physical lag-state mismatch at cutoff {cutoff}: {name}"
            )

    arrays = (
        "values",
        "valid",
        "source_latest_ts_ms",
        "feature_ready_ts_ms_by_feature",
        "observation_count",
        "lag_state",
    )
    if any(len(cpp_row[name]) != len(schema.TRAINABLE_FEATURE_ORDER) for name in arrays):
        raise RealDayParityError(f"C++ row ABI length mismatch at cutoff {cutoff}")

    unsupported = 0
    for index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        expected = py_row.values[name]
        panel_value = panel_row[name]
        actual_valid = bool(cpp_row["valid"][index])
        actual_value = float(cpp_row["values"][index])
        expected_valid = expected.value is not None
        if actual_valid is not expected_valid:
            raise RealDayParityError(
                f"Python/C++ validity mismatch at cutoff {cutoff} feature {name}"
            )
        if expected.value is None:
            unsupported += 1
            if panel_value is not None:
                raise RealDayParityError(
                    f"panel valid/value mismatch at cutoff {cutoff} feature {name}"
                )
            if not math.isnan(actual_value):
                raise RealDayParityError(
                    f"C++ valid/value mismatch at cutoff {cutoff} feature {name}"
                )
        else:
            expected_value = float(expected.value)
            allowed_abs_error = _feature_abs_tolerance(
                name,
                expected_value,
                view,
                rtol=rtol,
                atol=atol,
            )
            reduction_cell = _signed_quote_reduction_cell(name, view)
            if reduction_cell is not None and reduction_envelope_audit is not None:
                reduction_envelope_audit.observe(
                    cutoff=cutoff,
                    name=name,
                    expected=expected_value,
                    actual=actual_value,
                    cell=reduction_cell,
                )
            if panel_value is None or not (
                math.isfinite(float(panel_value))
                and abs(expected_value - float(panel_value)) <= allowed_abs_error
            ):
                raise RealDayParityError(
                    f"panel/Python value mismatch at cutoff {cutoff} feature {name}"
                )
            if not (
                math.isfinite(actual_value)
                and abs(expected_value - actual_value) <= allowed_abs_error
            ):
                raise RealDayParityError(
                    f"Python/C++ value mismatch at cutoff {cutoff} feature {name}: "
                    f"expected={expected_value!r} actual={actual_value!r}"
                )
            if not _close(float(panel_value), actual_value, rtol=rtol, atol=atol):
                raise RealDayParityError(
                    f"panel/C++ value mismatch at cutoff {cutoff} feature {name}: "
                    f"panel={panel_value!r} actual={actual_value!r}"
                )
        expected_source = (
            -1 if expected.source_latest_ts_ms is None else int(expected.source_latest_ts_ms)
        )
        expected_feature_ready = (
            -1 if expected.feature_ready_ts_ms is None else int(expected.feature_ready_ts_ms)
        )
        metadata_pairs = (
            (
                "source_latest_ts_ms",
                int(cpp_row["source_latest_ts_ms"][index]),
                expected_source,
            ),
            (
                "feature_ready_ts_ms",
                int(cpp_row["feature_ready_ts_ms_by_feature"][index]),
                expected_feature_ready,
            ),
            (
                "observation_count",
                int(cpp_row["observation_count"][index]),
                int(expected.observation_count),
            ),
            ("lag_state", str(cpp_row["lag_state"][index]), expected.lag_state),
        )
        for field_name, actual, expected_metadata in metadata_pairs:
            if actual != expected_metadata:
                raise RealDayParityError(
                    f"Python/C++ {field_name} mismatch at cutoff {cutoff} feature {name}: "
                    f"expected={expected_metadata!r} actual={actual!r}"
                )
        field_stats[name].observe(
            expected.value,
            actual_value,
            allowed_abs_error=(
                0.0
                if expected.value is None
                else _feature_abs_tolerance(
                    name,
                    float(expected.value),
                    view,
                    rtol=rtol,
                    atol=atol,
                )
            ),
        )

    if int(panel_row["unsupported_feature_count"]) != unsupported:
        raise RealDayParityError(f"panel unsupported-feature count mismatch at {cutoff}")
    cpp_fingerprint = _cpp_exact_fingerprint(cutoff, cpp_row)
    return (
        panel_fingerprint == py_row.fingerprint_sha256,
        cpp_fingerprint == py_row.fingerprint_sha256,
        panel_fingerprint == cpp_fingerprint,
    )


class _CanonicalIntListHasher:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._count = 0

    def update(self, value: int) -> None:
        if self._count:
            self._digest.update(b",")
        self._digest.update(str(int(value)).encode("ascii"))
        self._count += 1

    def hexdigest(self) -> str:
        clone = self._digest.copy()
        clone.update(b"]")
        return clone.hexdigest()


def _iter_panel_rows(panel_path: Path, *, batch_rows: int) -> Iterator[dict[str, Any]]:
    if batch_rows <= 0:
        raise RealDayParityError("batch_rows must be positive")
    parquet = pq.ParquetFile(panel_path)
    columns = [*_PANEL_METADATA_COLUMNS, *schema.TRAINABLE_FEATURE_ORDER]
    for batch in parquet.iter_batches(batch_size=batch_rows, columns=columns):
        yield from batch.to_pylist()


def _iter_panel_batches(
    panel_path: Path,
    *,
    batch_rows: int,
) -> Iterator[list[dict[str, Any]]]:
    if batch_rows <= 0:
        raise RealDayParityError("batch_rows must be positive")
    parquet = pq.ParquetFile(panel_path)
    columns = [*_PANEL_METADATA_COLUMNS, *schema.TRAINABLE_FEATURE_ORDER]
    for batch in parquet.iter_batches(batch_size=batch_rows, columns=columns):
        yield batch.to_pylist()


def _cpp_rows_for_panel_batch(
    cpp: Any,
    engine: Any,
    panel_rows: list[dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    cutoffs = [int(row["cutoff_exclusive_ms"]) for row in panel_rows]
    decisions = [int(row["decision_ts_ms"]) for row in panel_rows]
    output = cpp_batch.compute_batch(
        engine,
        cutoffs,
        decision_ts_ms=decisions,
    )
    values = np.asarray(output["values"], dtype=np.float64)
    valid = np.asarray(output["valid"], dtype=np.uint8)
    source_ts = np.asarray(output["source_latest_ts_ms"], dtype=np.int64)
    ready_ts = np.asarray(output["feature_ready_ts_ms_by_feature"], dtype=np.int64)
    counts = np.asarray(output["observation_count"], dtype=np.int64)
    lag_codes = np.asarray(output["lag_state_code"], dtype=np.uint8)
    row_ready = np.asarray(output["feature_ready_ts_ms"], dtype=np.int64)
    row_decisions = np.asarray(output["decision_ts_ms"], dtype=np.int64)
    lag_vocabulary = tuple(
        str(value) for value in cpp.F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY
    )
    for row_index, cutoff in enumerate(cutoffs):
        yield {
            "cutoff_exclusive_ms": cutoff,
            "decision_ts_ms": int(row_decisions[row_index]),
            "feature_ready_ts_ms": int(row_ready[row_index]),
            "values": values[row_index],
            "valid": valid[row_index],
            "source_latest_ts_ms": source_ts[row_index],
            "feature_ready_ts_ms_by_feature": ready_ts[row_index],
            "observation_count": counts[row_index],
            "lag_state": tuple(
                lag_vocabulary[int(code)] for code in lag_codes[row_index]
            ),
        }


def _validate_stream_cutoff(
    *, cutoff_mode: str, day_start: int, row_index: int, cutoff: int
) -> None:
    if cutoff_mode != "all_authoritative_target_day_decision_timestamps":
        return
    expected_cutoff = day_start + row_index * schema.CADENCE_MS
    if cutoff != expected_cutoff:
        raise RealDayParityError(
            "full-day cutoff stream is not the exact canonical target-day grid: "
            f"row={row_index} expected={expected_cutoff} actual={cutoff}"
        )


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
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
        temporary.unlink(missing_ok=True)


def audit_real_day_cpp_parity(
    *,
    panel_manifest_path: Path,
    source_bundle_identity_path: Path,
    report_path: Path | None = None,
    batch_rows: int = 256,
    rtol: float = DEFAULT_RTOL,
    atol: float = DEFAULT_ATOL,
    profile_id: str | None = None,
    market_data_root: Path | None = None,
    native_build_receipt_path: Path | None = None,
    require_complete_day: bool = False,
) -> dict[str, Any]:
    """Rebuild every cutoff in C++ and the frozen Python-oracle denominator."""

    if rtol < 0.0 or atol < 0.0 or not math.isfinite(rtol + atol):
        raise RealDayParityError("numeric tolerances must be finite and non-negative")
    admitted = load_admitted_panel(panel_manifest_path, source_bundle_identity_path)
    profile_binding = _validate_profile_binding(
        admitted,
        profile_id=profile_id,
        market_data_root=market_data_root,
    )
    state = load_physical_source_state(admitted.bundle)
    cpp = _load_cpp()
    if require_complete_day and native_build_receipt_path is None:
        raise RealDayParityError("complete-day authority requires a native build receipt")
    python_fields = {name: FieldStats() for name in schema.TRAINABLE_FEATURE_ORDER}
    full_day_cpp_fields = {name: FieldStats() for name in schema.TRAINABLE_FEATURE_ORDER}
    reduction_envelope_audit = ReductionEnvelopeAudit()
    cutoff_hasher = _CanonicalIntListHasher()
    python_sample_cutoff_hasher = _CanonicalIntListHasher()
    comparison_digest = hashlib.sha256()
    row_count = 0
    python_sample_count = 0
    first_cutoff: int | None = None
    last_cutoff: int | None = None
    prior_cutoff: int | None = None
    exact_panel_python_fingerprint_matches = 0
    exact_cpp_python_fingerprint_matches = 0
    exact_panel_cpp_fingerprint_matches = 0
    cutoff_identity = admitted.manifest["cache_identity_payload"]["cutoff_selection"]
    cutoff_mode = str(cutoff_identity["mode"])
    allowed_cutoff_modes = {
        "all_authoritative_target_day_decision_timestamps",
        "explicit_authoritative_target_day_decision_timestamps",
    }
    if cutoff_mode not in allowed_cutoff_modes:
        raise RealDayParityError(f"unsupported cutoff-selection mode: {cutoff_mode}")
    day_start = int(
        datetime.strptime(str(admitted.manifest["utc_day"]), "%Y-%m-%d")
        .replace(tzinfo=UTC)
        .timestamp()
        * 1_000
    )
    expected_rows = int(admitted.manifest["panel"]["rows"])
    python_sample_indices = python_oracle_sample_indices(expected_rows)
    python_sample_index_set = frozenset(python_sample_indices)
    l2 = tuple(
        state.execution_l2_by_start[key]
        for key in sorted(state.execution_l2_by_start)
    )
    cpp_engine = cpp_batch.create_engine(
        cpp,
        local_bars=state.local_bars,
        execution_l2=l2,
        metrics=state.metrics,
        reference_bars=state.reference_bars,
    )

    for panel_rows in _iter_panel_batches(admitted.panel_path, batch_rows=batch_rows):
        cpp_rows = list(_cpp_rows_for_panel_batch(cpp, cpp_engine, panel_rows))
        if len(cpp_rows) != len(panel_rows):
            raise RealDayParityError("C++ batch output denominator differs from panel batch")
        for panel_row, cpp_row in zip(panel_rows, cpp_rows, strict=True):
            cutoff = int(panel_row["cutoff_exclusive_ms"])
            decision = int(panel_row["decision_ts_ms"])
            if prior_cutoff is not None and cutoff <= prior_cutoff:
                raise RealDayParityError("panel cutoff clock is duplicate or reversed")
            if cutoff % schema.CADENCE_MS or decision != cutoff:
                raise RealDayParityError(f"non-canonical 1s decision cutoff: {cutoff}")
            _validate_stream_cutoff(
                cutoff_mode=cutoff_mode,
                day_start=day_start,
                row_index=row_count,
                cutoff=cutoff,
            )
            prior_cutoff = cutoff
            first_cutoff = cutoff if first_cutoff is None else first_cutoff
            last_cutoff = cutoff
            cutoff_hasher.update(cutoff)
            panel_cpp_exact = _compare_panel_cpp_full_day_row(
                panel_row,
                cpp_row,
                _physical_lag_state_at_cutoff(state, cutoff),
                full_day_cpp_fields,
                rtol=rtol,
                atol=atol,
            )
            exact_panel_cpp_fingerprint_matches += int(panel_cpp_exact)
            py_fingerprint: str | None = None
            if row_count in python_sample_index_set:
                view = source_view_at_cutoff(state, cutoff)
                py_row = full.generate_full_feature_row(
                    view.local_bars,
                    cutoff_exclusive_ms=cutoff,
                    decision_ts_ms=decision,
                    execution_l2=view.execution_l2,
                    metrics=view.metrics,
                    reference_bars=view.reference_bars,
                )
                panel_python_exact, cpp_python_exact, _ = _compare_one_row(
                    panel_row,
                    py_row,
                    cpp_row,
                    view,
                    python_fields,
                    rtol=rtol,
                    atol=atol,
                    reduction_envelope_audit=reduction_envelope_audit,
                )
                exact_panel_python_fingerprint_matches += int(panel_python_exact)
                exact_cpp_python_fingerprint_matches += int(cpp_python_exact)
                python_sample_count += 1
                python_sample_cutoff_hasher.update(cutoff)
                py_fingerprint = py_row.fingerprint_sha256
            comparison_digest.update(
                _canonical_bytes(
                    {
                        "cutoff_exclusive_ms": cutoff,
                        "panel_fingerprint_sha256": panel_row[
                            "feature_row_fingerprint_sha256"
                        ],
                        "cpp_exact_fingerprint_sha256": _cpp_exact_fingerprint(
                            cutoff,
                            cpp_row,
                        ),
                        "python_oracle_fingerprint_sha256": py_fingerprint,
                    }
                )
            )
            row_count += 1

    if python_sample_count != len(python_sample_indices):
        raise RealDayParityError(
            "Python-oracle sample denominator differs from the frozen index set"
        )

    panel_identity = admitted.manifest["panel"]
    checks = {
        "row_count": (row_count, int(panel_identity["rows"])),
        "first_cutoff": (first_cutoff, int(panel_identity["first_cutoff_exclusive_ms"])),
        "last_cutoff": (last_cutoff, int(panel_identity["last_cutoff_exclusive_ms"])),
    }
    if cutoff_mode == "explicit_authoritative_target_day_decision_timestamps":
        checks.update(
            {
                "cutoff_count": (row_count, int(cutoff_identity["count"])),
                "cutoff_sha256": (
                    cutoff_hasher.hexdigest(),
                    str(cutoff_identity["sha256"]),
                ),
            }
        )
    else:
        checks.update(
            {
                "full_day_cutoff_count": (row_count, FULL_DAY_ROWS),
                "full_day_first_cutoff": (first_cutoff, day_start),
                "full_day_last_cutoff": (
                    last_cutoff,
                    day_start + (FULL_DAY_ROWS - 1) * schema.CADENCE_MS,
                ),
            }
        )
    for label, (actual, expected) in checks.items():
        if actual != expected:
            raise RealDayParityError(
                f"panel {label} mismatch: expected={expected!r} actual={actual!r}"
            )
    if row_count == 0:
        raise RealDayParityError("admitted panel contains zero rows")
    full_day = cutoff_mode == "all_authoritative_target_day_decision_timestamps"
    if require_complete_day and not full_day:
        raise RealDayParityError("successor parity authority requires a complete UTC day")
    if require_complete_day and profile_binding is None:
        raise RealDayParityError("successor parity authority requires an exact source profile")

    feature_cell_comparisons = python_sample_count * len(schema.TRAINABLE_FEATURE_ORDER)
    numeric_contract = numeric_comparison_contract(rtol=rtol, atol=atol)
    numeric_contract_sha256 = _canonical_sha256(numeric_contract)
    report_unsigned = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": "passed_complete_day_cpp_and_stratified_python_173_field_parity",
        "utc_day": admitted.manifest["utc_day"],
        "complete_utc_day": full_day,
        "source_profile": profile_binding,
        "inputs": {
            "panel_manifest_path": str(admitted.manifest_path),
            "panel_manifest_sha256": admitted.manifest_sha256,
            "panel_path": str(admitted.panel_path),
            "panel_sha256": admitted.manifest["panel"]["sha256"],
            "source_bundle_identity_path": str(admitted.source_identity_path),
            "source_bundle_identity_sha256": admitted.source_identity_sha256,
            "bundle_identity_sha256": admitted.bundle.identity_sha256(),
            "cache_identity_sha256": admitted.manifest["cache_identity_sha256"],
        },
        "feature_contract": {
            "feature_count": len(schema.TRAINABLE_FEATURE_ORDER),
            "feature_names": list(schema.TRAINABLE_FEATURE_ORDER),
            "feature_order_sha256": schema.feature_order_sha256(),
            "feature_contract_sha256": full.full_feature_contract_fingerprint(),
            "source_manifest_sha256": schema.canonical_sha256(schema.source_manifest_payload()),
            "cpp_abi_version": CPP_ABI_VERSION,
        },
        "implementation_identity": _implementation_identity(
            cpp,
            native_build_receipt_path=native_build_receipt_path,
        ),
        "cutoffs": {
            "selection_mode": cutoff_mode,
            "rows": row_count,
            "first_cutoff_exclusive_ms": first_cutoff,
            "last_cutoff_exclusive_ms": last_cutoff,
            "canonical_list_sha256": cutoff_hasher.hexdigest(),
            "panel_batch_rows": batch_rows,
            "streaming_output_rows_retained": 0,
            "python_oracle_sample": {
                "method": (
                    "first_last_120_seconds_plus_1024_edge_exclusive_even_indices"
                    if row_count > PYTHON_ORACLE_EVEN_SAMPLE_ROWS
                    + 2 * PYTHON_ORACLE_EDGE_SECONDS
                    else "all_rows"
                ),
                "rows": python_sample_count,
                "minimum_even_rows": min(
                    row_count,
                    PYTHON_ORACLE_EVEN_SAMPLE_ROWS,
                ),
                "edge_seconds_each_side": min(
                    PYTHON_ORACLE_EDGE_SECONDS,
                    row_count,
                ),
                "index_list_sha256": _canonical_sha256(list(python_sample_indices)),
                "cutoff_list_sha256": python_sample_cutoff_hasher.hexdigest(),
            },
        },
        "parity": {
            "panel_python_row_fingerprint_matches": (
                exact_panel_python_fingerprint_matches
            ),
            "cpp_bitwise_exact_row_fingerprint_matches": (
                exact_cpp_python_fingerprint_matches
            ),
            "panel_cpp_bitwise_exact_row_fingerprint_matches": (
                exact_panel_cpp_fingerprint_matches
            ),
            "panel_cpp_bitwise_exact_required": True,
            "cpp_bitwise_exact_required": False,
            "cpp_authoritative_gate": (
                "full_day_panel_cpp_values_plus_stratified_python_cpp_six_channel_oracle"
            ),
            "panel_python_tolerance_parity_rows": python_sample_count,
            "panel_cpp_tolerance_parity_rows": row_count,
            "cpp_python_tolerance_parity_rows": python_sample_count,
            "python_oracle_tolerance_parity_rows": python_sample_count,
            "full_day_panel_cpp_tolerance_parity_rows": row_count,
            "value_rtol": rtol,
            "value_atol": atol,
            "numeric_comparison_contract": numeric_contract,
            "numeric_comparison_contract_sha256": numeric_contract_sha256,
            "signed_quote_reduction_envelope": reduction_envelope_audit.payload(
                contract_sha256=numeric_contract_sha256,
            ),
            "validity_mismatches": 0,
            "source_timestamp_mismatches": 0,
            "ready_timestamp_mismatches": 0,
            "observation_count_mismatches": 0,
            "lag_state_mismatches": 0,
            "cutoff_mismatches": 0,
            "python_oracle_feature_cell_comparisons": feature_cell_comparisons,
            "python_oracle_channel_comparisons": {
                channel: feature_cell_comparisons
                for channel in PYTHON_ORACLE_CHANNELS
            },
            "python_oracle_channel_mismatches": {
                channel: 0 for channel in PYTHON_ORACLE_CHANNELS
            },
            "comparison_stream_sha256": comparison_digest.hexdigest(),
        },
        "field_stats": {
            name: python_fields[name].payload()
            for name in schema.TRAINABLE_FEATURE_ORDER
        },
        "full_day_panel_cpp_field_stats": {
            name: full_day_cpp_fields[name].payload()
            for name in schema.TRAINABLE_FEATURE_ORDER
        },
        "permissions": {
            "labels_read": False,
            "predictions_read": False,
            "economic_outcomes_read": False,
            "training_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    report = {
        **report_unsigned,
        "report_identity_sha256": _canonical_sha256(report_unsigned),
    }
    if report_path is not None:
        _atomic_json(report_path.expanduser().resolve(), report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel-manifest", type=Path, required=True)
    parser.add_argument("--source-bundle-identity", type=Path, required=True)
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--batch-rows", type=int, default=256)
    parser.add_argument("--rtol", type=float, default=DEFAULT_RTOL)
    parser.add_argument("--atol", type=float, default=DEFAULT_ATOL)
    parser.add_argument("--profile", choices=tuple(sorted(source_specs.PROFILES)))
    parser.add_argument("--market-data-root", type=Path)
    parser.add_argument("--native-build-receipt", type=Path)
    parser.add_argument("--require-complete-day", action="store_true")
    args = parser.parse_args()
    report = audit_real_day_cpp_parity(
        panel_manifest_path=args.panel_manifest,
        source_bundle_identity_path=args.source_bundle_identity,
        report_path=args.report_json,
        batch_rows=args.batch_rows,
        rtol=args.rtol,
        atol=args.atol,
        profile_id=args.profile,
        market_data_root=args.market_data_root,
        native_build_receipt_path=args.native_build_receipt,
        require_complete_day=args.require_complete_day,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
