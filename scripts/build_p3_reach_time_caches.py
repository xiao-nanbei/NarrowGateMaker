#!/usr/bin/env python3
"""Build reusable, outcome-blind F02 reach-time DAG caches."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import data_paths  # noqa: E402
from research.families.f02_empirical_p3_touch.audit import (  # noqa: E402
    p3_reach_time_cache as label_node,
)
from research.families.f02_empirical_p3_touch.audit import (  # noqa: E402
    p3_reach_time_context as context_node,
)
from research.families.f02_empirical_p3_touch.audit import (  # noqa: E402
    p3_reach_time_source_manifest as source_manifest_node,
)
from research.families.f02_empirical_p3_touch.audit import (  # noqa: E402
    p3_reach_time_surface as surface_node,
)

SCHEMA_VERSION = "narrowgate.p3_reach_time_cache_build.v1"
DEFAULT_SOURCE_MANIFEST = (
    ROOT
    / "research/families/f02_empirical_p3_touch/docs"
    / "p3_reach_time_source_day_manifest_v1_20260804.json"
)
CONTEXT_CACHE_NODE = "p3_reach_time_context_day_v1"
LABEL_CACHE_NODE = "p3_reach_time_label_day_v1"
DEFAULT_SUMMARY_NAME = "p3_reach_time_cache_build_summary_v1.json"
MINIMUM_INTERNAL_FREE_BYTES = 50 * 1024**3
INTERNAL_SAFETY_RESERVE_BYTES = 60 * 1024**3
ATOMIC_BUILD_OVERLAP_MULTIPLIER = 2.5
ESTIMATED_CONTEXT_BYTES_PER_ORIGIN = 512
ESTIMATED_CACHE_METADATA_BYTES = 2 * 1024**2
ALLOWED_SOURCE_FILE_KINDS = frozenset({"quality", "bbo", "official_aggtrades"})
SOURCE_PROFILES = frozenset({"provider", "native"})


@dataclass(frozen=True)
class CacheBuildParameters:
    """Every parameter that can change a context or label cache byte."""

    tick_size: float = 0.1
    cadence_ms: int = 10_000
    past_warmup_s: int = 60
    administrative_censor_ms: int = 30_000
    max_bbo_age_ms: int = 5_000
    fast_window_s: int = 10
    slow_window_s: int = 60
    variance_floor: float = 1e-6
    time_step_ms: int = 100
    max_distance_ticks: int = 1_200

    def __post_init__(self) -> None:
        if self.tick_size <= 0.0:
            raise ValueError("tick_size must be positive")
        if self.cadence_ms <= 0 or 86_400_000 % self.cadence_ms:
            raise ValueError("cadence_ms must be positive and divide one UTC day")
        if self.past_warmup_s < self.slow_window_s:
            raise ValueError("past_warmup_s must cover slow_window_s")
        if self.past_warmup_s * 1_000 % self.cadence_ms:
            raise ValueError("past_warmup_s must align to cadence_ms")
        if self.administrative_censor_ms % self.cadence_ms:
            raise ValueError("administrative_censor_ms must align to cadence_ms")
        if self.max_bbo_age_ms < 0:
            raise ValueError("max_bbo_age_ms must be non-negative")
        if self.fast_window_s < 2 or self.slow_window_s < self.fast_window_s:
            raise ValueError("volatility windows are invalid")
        if self.variance_floor <= 0.0:
            raise ValueError("variance_floor must be positive")
        surface_node.ReachTimeGridSpec(
            time_step_ms=self.time_step_ms,
            max_horizon_ms=self.administrative_censor_ms,
            max_distance_ticks=self.max_distance_ticks,
        )

    @property
    def grid_spec(self) -> surface_node.ReachTimeGridSpec:
        return surface_node.ReachTimeGridSpec(
            time_step_ms=self.time_step_ms,
            max_horizon_ms=self.administrative_censor_ms,
            max_distance_ticks=self.max_distance_ticks,
        )


@dataclass(frozen=True)
class CacheJob:
    day: str
    source: str
    panel: str | None
    source_record_sha256: str
    weighted: bool
    selection_mode: str


def _canonical_sha256(payload: Any) -> str:
    return context_node.canonical_sha256(payload)


def _sha256_file(path: Path) -> str:
    return source_manifest_node.sha256_file(path)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    target = path.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, target)


def _emit(stream: TextIO | None, payload: Mapping[str, Any]) -> None:
    if stream is None:
        return
    stream.write(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")
    stream.flush()


def _module_hashes() -> dict[str, str]:
    paths = {
        "orchestrator": Path(__file__).resolve(),
        "context_node": Path(context_node.__file__).resolve(),
        "label_cache_node": Path(label_node.__file__).resolve(),
        "label_surface_node": Path(surface_node.__file__).resolve(),
        "source_manifest_node": Path(source_manifest_node.__file__).resolve(),
    }
    return {name: _sha256_file(path) for name, path in paths.items()}


def _validate_file_identity(
    identity: Mapping[str, Any],
    *,
    label: str,
    digest_cache: dict[Path, str],
) -> Path:
    path = Path(str(identity.get("path", ""))).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{label} missing: {path}")
    expected = str(identity.get("sha256", ""))
    if len(expected) != 64:
        raise ValueError(f"{label} lacks a valid SHA256")
    observed = digest_cache.get(path)
    if observed is None:
        observed = _sha256_file(path)
        digest_cache[path] = observed
    if observed != expected:
        raise ValueError(f"{label} hash mismatch: observed={observed} expected={expected}")
    if "size_bytes" in identity and path.stat().st_size != int(identity["size_bytes"]):
        raise ValueError(f"{label} size mismatch")
    return path


def _csv_rows_by_day(path: Path) -> dict[str, dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "day" not in reader.fieldnames:
            raise ValueError(f"quality CSV lacks day column: {path}")
        rows: dict[str, dict[str, str]] = {}
        for row in reader:
            day = str(row.get("day", ""))
            if not day or day in rows:
                raise ValueError(f"quality CSV contains blank or duplicate day: {day!r}")
            rows[day] = dict(row)
    return rows


def _validate_panel_request(manifest: Mapping[str, Any], digest_cache: dict[Path, str]) -> None:
    identity = manifest.get("panel_request_identity", {})
    if not identity:
        return
    path = _validate_file_identity(identity, label="panel request", digest_cache=digest_cache)
    payload = json.loads(path.read_text(encoding="utf-8"))
    observed = payload.pop("canonical_request_sha256", None)
    canonical = _canonical_sha256(payload)
    if observed != canonical or identity.get("canonical_sha256") != canonical:
        raise ValueError("panel request canonical hash mismatch")


def load_and_validate_source_manifest(path: Path) -> dict[str, Any]:
    """Load the frozen manifest and revalidate every referenced source byte."""

    target = path.expanduser().resolve()
    if not target.is_file():
        raise FileNotFoundError(f"F02 source manifest missing: {target}")
    manifest = json.loads(target.read_text(encoding="utf-8"))
    source_manifest_node.validate_source_day_manifest(manifest)
    digest_cache: dict[Path, str] = {}
    _validate_panel_request(manifest, digest_cache)

    native_quality_rows: dict[Path, dict[str, dict[str, str]]] = {}
    all_records = [
        *manifest.get("provider_records", ()),
        *manifest.get("native_records", ()),
    ]
    for record in all_records:
        source = str(record.get("source", ""))
        day = str(record.get("date", ""))
        files = record.get("files")
        if not isinstance(files, Mapping) or set(files) != ALLOWED_SOURCE_FILE_KINDS:
            raise ValueError(f"{source} {day} has unsupported source file kinds")
        for kind in sorted(ALLOWED_SOURCE_FILE_KINDS):
            identity = files[kind]
            if not isinstance(identity, Mapping):
                raise ValueError(f"{source} {day} {kind} identity is not a mapping")
            actual_path = _validate_file_identity(
                identity,
                label=f"{source} {day} {kind}",
                digest_cache=digest_cache,
            )
            if kind == "quality" and "row_sha256" in identity:
                rows = native_quality_rows.get(actual_path)
                if rows is None:
                    rows = _csv_rows_by_day(actual_path)
                    native_quality_rows[actual_path] = rows
                row = rows.get(day)
                if row is None:
                    raise ValueError(f"native quality row missing for {day}")
                if _canonical_sha256(row) != identity["row_sha256"]:
                    raise ValueError(f"native quality row hash mismatch for {day}")
    return manifest


def _record_indices(
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    provider = {str(row["date"]): row for row in manifest["provider_records"]}
    native = {str(row["date"]): row for row in manifest["native_records"]}
    return provider, native


def _requested_set(values: Sequence[str] | None) -> set[str]:
    return {str(value) for value in (values or ())}


def select_cache_jobs(
    manifest: Mapping[str, Any],
    *,
    days: Sequence[str] | None = None,
    panels: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    overlap_source_profiles: Sequence[str] | None = None,
) -> list[CacheJob]:
    """Select weighted primary days, or an explicit overlap source profile."""

    day_filter = _requested_set(days)
    panel_filter = _requested_set(panels)
    source_filter = _requested_set(sources)
    overlap_profiles = _requested_set(overlap_source_profiles)
    invalid_sources = (source_filter | overlap_profiles) - SOURCE_PROFILES
    if invalid_sources:
        raise ValueError(f"unsupported source filters: {sorted(invalid_sources)}")

    candidates: list[CacheJob] = []
    if overlap_profiles:
        for row in manifest["overlap_records"]:
            for source in sorted(overlap_profiles):
                candidates.append(
                    CacheJob(
                        day=str(row["date"]),
                        source=source,
                        panel=(
                            str(row["weighted_panel"])
                            if row.get("weighted_panel") is not None
                            else None
                        ),
                        source_record_sha256=str(row[f"{source}_record_sha256"]),
                        weighted=False,
                        selection_mode="overlap_source_comparison",
                    )
                )
    else:
        for row in manifest["weighted_day_records"]:
            candidates.append(
                CacheJob(
                    day=str(row["date"]),
                    source=str(row["primary_source"]),
                    panel=str(row["panel"]),
                    source_record_sha256=str(row["source_record_sha256"]),
                    weighted=True,
                    selection_mode="weighted_primary",
                )
            )

    available_days = {job.day for job in candidates}
    available_panels = {job.panel for job in candidates if job.panel is not None}
    available_sources = {job.source for job in candidates}
    if day_filter - available_days:
        raise ValueError(
            f"requested days are outside the selected manifest surface: "
            f"{sorted(day_filter - available_days)}"
        )
    if panel_filter - available_panels:
        raise ValueError(
            f"requested panels are outside the selected manifest surface: "
            f"{sorted(panel_filter - available_panels)}"
        )
    if source_filter - available_sources:
        raise ValueError(
            f"requested sources are outside the selected manifest surface: "
            f"{sorted(source_filter - available_sources)}"
        )

    selected = [
        job
        for job in candidates
        if (not day_filter or job.day in day_filter)
        and (not panel_filter or job.panel in panel_filter)
        and (not source_filter or job.source in source_filter)
    ]
    unique: dict[tuple[str, str], CacheJob] = {}
    for job in selected:
        key = (job.day, job.source)
        if key in unique:
            raise ValueError(f"duplicate cache job selected: {key}")
        unique[key] = job
    if not unique:
        raise ValueError("cache selection is empty")
    return sorted(unique.values(), key=lambda job: (job.day, job.source))


def _validated_cache_root(
    cache_root: Path,
    *,
    allow_external_cache_root: bool = False,
) -> tuple[Path, str]:
    root = cache_root.expanduser().resolve()
    project_data = data_paths.data_root().expanduser().resolve()
    external_cache = data_paths.external_cache_root().expanduser().resolve()
    if root == external_cache or root.is_relative_to(external_cache):
        if not allow_external_cache_root:
            raise ValueError(
                "P3 external cache root requires explicit allow_external_cache_root"
            )
        return root, "external_removable_cache"
    if root == project_data or root.is_relative_to(project_data):
        raise ValueError(f"P3 cache must stay inside the dedicated cache namespace: {root}")
    return root, "internal_cache"


def _nearest_existing_path(path: Path) -> Path:
    probe = path
    while not probe.exists():
        if probe == probe.parent:
            raise FileNotFoundError(f"no existing parent for cache root: {path}")
        probe = probe.parent
    return probe


def storage_preflight(
    cache_root: Path,
    *,
    minimum_free_bytes: int = MINIMUM_INTERNAL_FREE_BYTES,
    safety_reserve_bytes: int = INTERNAL_SAFETY_RESERVE_BYTES,
    estimated_new_final_bytes: int = 0,
    atomic_overlap_multiplier: float = ATOMIC_BUILD_OVERLAP_MULTIPLIER,
    enforce: bool = True,
    allow_external_cache_root: bool = False,
) -> dict[str, Any]:
    root, storage_tier = _validated_cache_root(
        cache_root,
        allow_external_cache_root=allow_external_cache_root,
    )
    usage = shutil.disk_usage(_nearest_existing_path(root))
    if minimum_free_bytes < 0 or safety_reserve_bytes < 0 or estimated_new_final_bytes < 0:
        raise ValueError("storage byte thresholds must be non-negative")
    if atomic_overlap_multiplier < 1.0:
        raise ValueError("atomic_overlap_multiplier must be at least 1.0")
    peak_new_bytes = int(
        math.ceil(float(estimated_new_final_bytes) * float(atomic_overlap_multiplier))
    )
    required_free_bytes = max(
        int(minimum_free_bytes),
        int(safety_reserve_bytes) + peak_new_bytes,
    )
    passed = usage.free >= required_free_bytes
    if enforce and not passed:
        raise RuntimeError(
            "P3 cache storage gate failed: "
            f"free={usage.free} required={required_free_bytes} "
            f"reserve={int(safety_reserve_bytes)} estimated_final={int(estimated_new_final_bytes)} "
            f"overlap_multiplier={float(atomic_overlap_multiplier)}"
        )
    return {
        "cache_root": str(root),
        "storage_tier": storage_tier,
        "free_bytes_before": int(usage.free),
        "minimum_free_bytes": int(minimum_free_bytes),
        "safety_reserve_bytes": int(safety_reserve_bytes),
        "estimated_new_final_bytes": int(estimated_new_final_bytes),
        "atomic_overlap_multiplier": float(atomic_overlap_multiplier),
        "estimated_peak_new_bytes": peak_new_bytes,
        "required_free_bytes": required_free_bytes,
        "passed": passed,
        "enforced": bool(enforce),
    }


def _estimated_cache_bytes(
    parameters: CacheBuildParameters,
    *,
    context_missing: bool,
    label_missing: bool,
) -> int:
    usable_ms = (
        86_400_000
        - parameters.past_warmup_s * 1_000
        - parameters.administrative_censor_ms
    )
    origins = max(0, usable_ms // parameters.cadence_ms + 1)
    estimated = 0
    if context_missing:
        estimated += (
            origins * ESTIMATED_CONTEXT_BYTES_PER_ORIGIN
            + ESTIMATED_CACHE_METADATA_BYTES
        )
    if label_missing:
        raw_surface_bytes = origins * parameters.grid_spec.n_time_bins * 2 * 2
        estimated += raw_surface_bytes + ESTIMATED_CACHE_METADATA_BYTES
    return int(estimated)


def _job_plan(
    *,
    cache_root: Path,
    job: CacheJob,
    source_record: Mapping[str, Any],
    parameters: CacheBuildParameters,
    module_hashes: Mapping[str, str],
) -> dict[str, Any]:
    parameter_identity = asdict(parameters)
    context_code_identity = _canonical_sha256(
        {
            "schema_version": context_node.SCHEMA_VERSION,
            "context_module_sha256": module_hashes["context_node"],
            "source_record_sha256": job.source_record_sha256,
            "parameters": parameter_identity,
        }
    )
    context_key = context_node.context_cache_key(
        day=job.day,
        source_profile=job.source,
        bbo_sha256=str(source_record["files"]["bbo"]["sha256"]),
        extractor_sha256=context_code_identity,
        tick_size=parameters.tick_size,
        cadence_ms=parameters.cadence_ms,
        administrative_censor_ms=parameters.administrative_censor_ms,
        max_bbo_age_ms=parameters.max_bbo_age_ms,
        fast_window_s=parameters.fast_window_s,
        slow_window_s=parameters.slow_window_s,
        variance_floor=parameters.variance_floor,
    )
    label_code_identity = _canonical_sha256(
        {
            "schema_version": label_node.SCHEMA_VERSION,
            "label_cache_module_sha256": module_hashes["label_cache_node"],
            "label_surface_module_sha256": module_hashes["label_surface_node"],
            "source_record_sha256": job.source_record_sha256,
            "parameters": parameter_identity,
        }
    )
    label_key = label_node.label_cache_key(
        day=job.day,
        context_cache_key=context_key,
        trade_sha256=str(source_record["files"]["official_aggtrades"]["sha256"]),
        label_kernel_sha256=label_code_identity,
        tick_size=parameters.tick_size,
        spec=parameters.grid_spec,
    )
    context_entry = cache_root / CONTEXT_CACHE_NODE / job.source / job.day / context_key
    label_entry = cache_root / LABEL_CACHE_NODE / job.source / job.day / label_key
    common_identity = {
        "day": job.day,
        "source_profile": job.source,
        "panel": job.panel,
        "weighted": job.weighted,
        "selection_mode": job.selection_mode,
        "source_record_sha256": job.source_record_sha256,
        "parameters": parameter_identity,
        "economic_outcomes_read": False,
        "queue_inputs_read": False,
        "order_lifecycle_inputs_read": False,
    }
    return {
        "job": job,
        "context_key": context_key,
        "label_key": label_key,
        "context_entry": context_entry,
        "context_path": context_entry / "context.parquet",
        "label_entry": label_entry,
        "label_path": label_entry / "labels.npz",
        "context_identity": {
            **common_identity,
            "node": CONTEXT_CACHE_NODE,
            "bbo_sha256": source_record["files"]["bbo"]["sha256"],
            "context_code_identity_sha256": context_code_identity,
        },
        "label_identity": {
            **common_identity,
            "node": LABEL_CACHE_NODE,
            "context_cache_key": context_key,
            "official_aggtrades_sha256": source_record["files"]["official_aggtrades"]["sha256"],
            "label_code_identity_sha256": label_code_identity,
        },
    }


def _load_context(plan: Mapping[str, Any]) -> tuple[Any, Mapping[str, Any]]:
    path = Path(plan["context_path"])
    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    if not path.is_file() or not manifest_path.is_file():
        raise ValueError(f"incomplete P3 context cache entry: {path.parent}")
    frame, manifest = context_node.load_context_cache(
        path, expected_cache_key=str(plan["context_key"])
    )
    if manifest.get("identity") != plan["context_identity"]:
        raise ValueError("P3 context cache identity mismatch")
    return frame, manifest


def _load_label(plan: Mapping[str, Any], context: Any) -> tuple[Any, Any, Mapping[str, Any]]:
    path = Path(plan["label_path"])
    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    if not path.is_file() or not manifest_path.is_file():
        raise ValueError(f"incomplete P3 label cache entry: {path.parent}")
    origins, surface, manifest = label_node.load_label_cache(
        path, expected_cache_key=str(plan["label_key"])
    )
    if manifest.get("identity") != plan["label_identity"]:
        raise ValueError("P3 label cache identity mismatch")
    expected_origins = context["origin_ts_ms"].to_numpy(dtype="int64")
    if origins.shape != expected_origins.shape or not (origins == expected_origins).all():
        raise ValueError("P3 label/context origin mismatch")
    return origins, surface, manifest


def _retarget_staged_manifest(staged_data: Path, final_data: Path) -> None:
    manifest_path = staged_data.with_suffix(staged_data.suffix + ".manifest.json")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["data_path"] = str(final_data)
    payload.pop("canonical_manifest_sha256", None)
    payload["canonical_manifest_sha256"] = _canonical_sha256(payload)
    _atomic_json(manifest_path, payload)


def _publish_entry(
    *,
    entry: Path,
    filename: str,
    build: Any,
) -> None:
    if entry.exists():
        raise FileExistsError(f"cache entry appeared during build: {entry}")
    entry.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{entry.name}.tmp-", dir=entry.parent))
    try:
        staged_data = temporary / filename
        build(staged_data)
        _retarget_staged_manifest(staged_data, entry / filename)
        os.replace(temporary, entry)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def _run_job(
    *,
    plan: Mapping[str, Any],
    source_record: Mapping[str, Any],
    parameters: CacheBuildParameters,
    dry_run: bool,
) -> dict[str, Any]:
    context_entry = Path(plan["context_entry"])
    label_entry = Path(plan["label_entry"])
    context_status = "planned"
    label_status = "planned"
    context = None
    context_manifest: Mapping[str, Any] | None = None
    label_manifest: Mapping[str, Any] | None = None

    if context_entry.exists():
        context, context_manifest = _load_context(plan)
        context_status = "loaded"
    elif not dry_run:
        bbo_path = Path(source_record["files"]["bbo"]["path"])

        def build_context(path: Path) -> None:
            frame = context_node.extract_reach_time_context(
                day=plan["job"].day,
                source_profile=plan["job"].source,
                bbo_path=bbo_path,
                tick_size=parameters.tick_size,
                cadence_ms=parameters.cadence_ms,
                past_warmup_s=parameters.past_warmup_s,
                administrative_censor_ms=parameters.administrative_censor_ms,
                max_bbo_age_ms=parameters.max_bbo_age_ms,
                fast_window_s=parameters.fast_window_s,
                slow_window_s=parameters.slow_window_s,
                variance_floor=parameters.variance_floor,
            )
            context_node.write_context_cache(
                path,
                frame=frame,
                cache_key=str(plan["context_key"]),
                identity=plan["context_identity"],
            )

        _publish_entry(entry=context_entry, filename="context.parquet", build=build_context)
        context, context_manifest = _load_context(plan)
        context_status = "built"

    if label_entry.exists():
        if context is None:
            raise ValueError("label cache exists without its context dependency")
        _, _, label_manifest = _load_label(plan, context)
        label_status = "loaded"
    elif not dry_run:
        if context is None:
            raise RuntimeError("context dependency was not materialized")
        trade_path = Path(source_record["files"]["official_aggtrades"]["path"])

        def build_label(path: Path) -> None:
            surface = label_node.build_reach_label_surface(
                context=context,
                trade_path=trade_path,
                tick_size=parameters.tick_size,
                spec=parameters.grid_spec,
            )
            label_node.write_label_cache(
                path,
                origins_ms=context["origin_ts_ms"].to_numpy(dtype="int64"),
                surface=surface,
                cache_key=str(plan["label_key"]),
                identity=plan["label_identity"],
            )

        _publish_entry(entry=label_entry, filename="labels.npz", build=build_label)
        _, _, label_manifest = _load_label(plan, context)
        label_status = "built"

    job: CacheJob = plan["job"]
    return {
        **asdict(job),
        "context_cache_key": plan["context_key"],
        "label_cache_key": plan["label_key"],
        "context_path": str(plan["context_path"]),
        "label_path": str(plan["label_path"]),
        "context_status": context_status,
        "label_status": label_status,
        "context_rows": (int(context_manifest["rows"]) if context_manifest is not None else None),
        "label_rows": (int(label_manifest["rows"]) if label_manifest is not None else None),
    }


def run_cache_build(
    *,
    manifest_path: Path = DEFAULT_SOURCE_MANIFEST,
    cache_root: Path | None = None,
    summary_path: Path | None = None,
    parameters: CacheBuildParameters | None = None,
    days: Sequence[str] | None = None,
    panels: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    overlap_source_profiles: Sequence[str] | None = None,
    dry_run: bool = False,
    progress_stream: TextIO | None = sys.stdout,
    minimum_free_bytes: int = MINIMUM_INTERNAL_FREE_BYTES,
    safety_reserve_bytes: int = INTERNAL_SAFETY_RESERVE_BYTES,
    atomic_overlap_multiplier: float = ATOMIC_BUILD_OVERLAP_MULTIPLIER,
    allow_external_cache_root: bool = False,
) -> dict[str, Any]:
    """Validate sources and build selected caches serially and fail-fast."""

    source_path = manifest_path.expanduser().resolve()
    manifest = load_and_validate_source_manifest(source_path)
    selected = select_cache_jobs(
        manifest,
        days=days,
        panels=panels,
        sources=sources,
        overlap_source_profiles=overlap_source_profiles,
    )
    actual_root, _ = _validated_cache_root(
        cache_root if cache_root is not None else data_paths.replay_dag_cache_root(),
        allow_external_cache_root=allow_external_cache_root,
    )
    params = parameters or CacheBuildParameters()
    hashes = _module_hashes()
    provider_by_day, native_by_day = _record_indices(manifest)
    record_indices = {"provider": provider_by_day, "native": native_by_day}
    plans: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    estimated_new_final_bytes = 0
    for job in selected:
        record = record_indices[job.source].get(job.day)
        if record is None or record.get("record_sha256") != job.source_record_sha256:
            raise ValueError(f"source record ownership mismatch for {job.day} {job.source}")
        plan = _job_plan(
            cache_root=actual_root,
            job=job,
            source_record=record,
            parameters=params,
            module_hashes=hashes,
        )
        plans.append((plan, record))
        estimated_new_final_bytes += _estimated_cache_bytes(
            params,
            context_missing=not Path(plan["context_entry"]).exists(),
            label_missing=not Path(plan["label_entry"]).exists(),
        )
    preflight = storage_preflight(
        actual_root,
        minimum_free_bytes=minimum_free_bytes,
        safety_reserve_bytes=safety_reserve_bytes,
        estimated_new_final_bytes=estimated_new_final_bytes,
        atomic_overlap_multiplier=atomic_overlap_multiplier,
        enforce=not dry_run,
        allow_external_cache_root=allow_external_cache_root,
    )
    output_summary = (
        summary_path.expanduser().resolve()
        if summary_path is not None
        else actual_root / DEFAULT_SUMMARY_NAME
    )
    _emit(
        progress_stream,
        {
            "schema_version": SCHEMA_VERSION,
            "event": "preflight",
            "dry_run": bool(dry_run),
            "selected_jobs": len(selected),
            "cache_root": str(actual_root),
            "free_bytes_before": preflight["free_bytes_before"],
        },
    )

    rows: list[dict[str, Any]] = []
    for index, (plan, record) in enumerate(plans, start=1):
        job: CacheJob = plan["job"]
        _emit(
            progress_stream,
            {
                "schema_version": SCHEMA_VERSION,
                "event": "day_start",
                "index": index,
                "total": len(selected),
                "day": job.day,
                "source": job.source,
                "panel": job.panel,
            },
        )
        row = _run_job(
            plan=plan,
            source_record=record,
            parameters=params,
            dry_run=bool(dry_run),
        )
        rows.append(row)
        _emit(
            progress_stream,
            {
                "schema_version": SCHEMA_VERSION,
                "event": "day_complete" if not dry_run else "day_planned",
                "index": index,
                "total": len(selected),
                "day": job.day,
                "source": job.source,
                "context_status": row["context_status"],
                "label_status": row["label_status"],
            },
        )

    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_manifest": {
            "path": str(source_path),
            "sha256": _sha256_file(source_path),
            "canonical_manifest_sha256": manifest["canonical_manifest_sha256"],
        },
        "cache_root": str(actual_root),
        "summary_path": str(output_summary),
        "dry_run": bool(dry_run),
        "selection": {
            "days": sorted(_requested_set(days)),
            "panels": sorted(_requested_set(panels)),
            "sources": sorted(_requested_set(sources)),
            "overlap_source_profiles": sorted(_requested_set(overlap_source_profiles)),
            "weighted_primary_by_default": not bool(overlap_source_profiles),
        },
        "parameters": asdict(params),
        "module_sha256": hashes,
        "storage_preflight": preflight,
        "job_count": len(rows),
        "counts": {
            "context_built": sum(row["context_status"] == "built" for row in rows),
            "context_loaded": sum(row["context_status"] == "loaded" for row in rows),
            "context_planned": sum(row["context_status"] == "planned" for row in rows),
            "label_built": sum(row["label_status"] == "built" for row in rows),
            "label_loaded": sum(row["label_status"] == "loaded" for row in rows),
            "label_planned": sum(row["label_status"] == "planned" for row in rows),
        },
        "jobs": rows,
        "economic_outcomes_read": False,
        "queue_inputs_read": False,
        "order_lifecycle_inputs_read": False,
        "cache_is_reproducible_and_disposable": True,
    }
    summary["canonical_summary_sha256"] = _canonical_sha256(summary)
    _atomic_json(output_summary, summary)
    _emit(
        progress_stream,
        {
            "schema_version": SCHEMA_VERSION,
            "event": "summary",
            "summary_path": str(output_summary),
            "canonical_summary_sha256": summary["canonical_summary_sha256"],
            "job_count": len(rows),
        },
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_SOURCE_MANIFEST)
    parser.add_argument("--cache-root", type=Path)
    parser.add_argument(
        "--allow-external-cache-root",
        action="store_true",
        help="Allow a cache root inside the project's removable cache namespace.",
    )
    parser.add_argument("--summary-output", type=Path)
    parser.add_argument("--day", action="append", default=[])
    parser.add_argument("--panel", action="append", default=[])
    parser.add_argument("--source", action="append", choices=sorted(SOURCE_PROFILES), default=[])
    parser.add_argument(
        "--overlap-source-profile",
        action="append",
        choices=sorted(SOURCE_PROFILES),
        default=[],
        help="Build only explicit overlap comparison rows for this source profile.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--cadence-ms", type=int, default=10_000)
    parser.add_argument("--past-warmup-s", type=int, default=60)
    parser.add_argument("--administrative-censor-ms", type=int, default=30_000)
    parser.add_argument("--max-bbo-age-ms", type=int, default=5_000)
    parser.add_argument("--fast-window-s", type=int, default=10)
    parser.add_argument("--slow-window-s", type=int, default=60)
    parser.add_argument("--variance-floor", type=float, default=1e-6)
    parser.add_argument("--time-step-ms", type=int, default=100)
    parser.add_argument("--max-distance-ticks", type=int, default=1_200)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    parameters = CacheBuildParameters(
        tick_size=args.tick_size,
        cadence_ms=args.cadence_ms,
        past_warmup_s=args.past_warmup_s,
        administrative_censor_ms=args.administrative_censor_ms,
        max_bbo_age_ms=args.max_bbo_age_ms,
        fast_window_s=args.fast_window_s,
        slow_window_s=args.slow_window_s,
        variance_floor=args.variance_floor,
        time_step_ms=args.time_step_ms,
        max_distance_ticks=args.max_distance_ticks,
    )
    run_cache_build(
        manifest_path=args.manifest,
        cache_root=args.cache_root,
        summary_path=args.summary_output,
        parameters=parameters,
        days=args.day,
        panels=args.panel,
        sources=args.source,
        overlap_source_profiles=args.overlap_source_profile,
        dry_run=args.dry_run,
        allow_external_cache_root=args.allow_external_cache_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
