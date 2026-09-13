#!/usr/bin/env python3
"""Benchmark small F03 1s physical panels without labels, PnL, or bulk output."""

from __future__ import annotations

import argparse
import gc
import json
import os
import resource
import shutil
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as daily,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as materializer,
)

SCHEMA_VERSION = "causal_v12_1s_small_materialization_benchmark.v1"
SECONDS_PER_DAY = 86_400


def canonical_cutoff_sample(utc_day: str, row_count: int) -> tuple[int, ...]:
    if not 1 <= row_count <= SECONDS_PER_DAY:
        raise ValueError("row_count must be in [1,86400]")
    day_start = int(datetime.strptime(utc_day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1_000)
    second_offsets = tuple((index * SECONDS_PER_DAY) // row_count for index in range(row_count))
    if len(set(second_offsets)) != row_count:
        raise AssertionError("canonical cutoff sampler produced duplicate seconds")
    return tuple(day_start + offset * 1_000 for offset in second_offsets)


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1_024


def extrapolated_full_day_wall_seconds(
    *,
    fixed_seconds: float,
    measured_row_seconds: float,
    row_count: int,
) -> float:
    if fixed_seconds < 0.0 or measured_row_seconds < 0.0 or row_count <= 0:
        raise ValueError("benchmark timing inputs must be non-negative with positive rows")
    return fixed_seconds + measured_row_seconds * SECONDS_PER_DAY / row_count


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def run_benchmark(
    *,
    source_spec: Path,
    output_dir: Path,
    row_count: int,
    batch_rows: int = 128,
    engine: str,
) -> dict[str, Any]:
    source_spec = source_spec.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    if output_dir.exists():
        raise FileExistsError(f"benchmark output already exists: {output_dir}")
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    bundle = daily.DailySourceBundle.from_json(source_spec)
    cutoffs = canonical_cutoff_sample(bundle.utc_day, row_count)

    temporary_dir = output_dir.parent / f".{output_dir.name}.tmp-{uuid.uuid4().hex}"
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir.mkdir()
    started = time.perf_counter()
    try:
        probe_started = time.perf_counter()
        probe = daily.probe_source_bundle(bundle)
        probe_seconds = time.perf_counter() - probe_started
        if not probe.get("physical_materialization_eligible"):
            reasons = probe.get("failure_reasons", ["unknown source authority failure"])
            raise base.FeatureContractError(
                "benchmark source spec is not physically materialization-eligible: "
                + "; ".join(str(reason) for reason in reasons)
            )

        load_started = time.perf_counter()
        local_audit = daily.read_local_trade_bars_with_audit(bundle.local_trade_tempo_paths)
        execution_l2 = daily.read_execution_l2(bundle.execution_l2_paths)
        metric_audit = daily.read_metrics_with_audit(bundle.metric_paths)
        reference_audit = daily.read_reference_bars_with_audit(bundle.reference_bar_paths)
        source_load_seconds = time.perf_counter() - load_started
        local_bars = local_audit.bars
        reference_bars = () if reference_audit is None else reference_audit.bars
        cutoffs = materializer._target_cutoffs(bundle, local_bars, cutoffs)

        engine_prepare_started = time.perf_counter()
        cpp = None
        native_engine = None
        if engine == materializer.CPP_BATCH_ENGINE:
            cpp, native_engine = materializer._create_cpp_batch_engine(
                local_bars=local_bars,
                execution_l2=execution_l2,
                metrics=metric_audit.observations,
                reference_bars=reference_bars,
            )
        elif engine != materializer.PYTHON_ORACLE_ENGINE:
            raise base.FeatureContractError(
                f"benchmark engine must be one of {materializer.SUPPORTED_ENGINES}"
            )
        engine_prepare_seconds = time.perf_counter() - engine_prepare_started

        panel_path = temporary_dir / materializer.PANEL_FILENAME
        writer = pq.ParquetWriter(
            panel_path,
            materializer.panel_arrow_schema(),
            compression="zstd",
            write_statistics=True,
        )
        if engine == materializer.CPP_BATCH_ENGINE:
            assert cpp is not None and native_engine is not None
            iterator = materializer._iter_cpp_batch_records(
                cpp=cpp,
                engine=native_engine,
                cutoffs=cutoffs,
                local_synthetic_starts=local_audit.synthesized_start_ts_ms,
                reference_synthetic_starts=(
                    () if reference_audit is None else reference_audit.synthesized_start_ts_ms
                ),
                reference_available=bool(reference_bars),
                batch_rows=batch_rows,
            )
        else:
            iterator = materializer._iter_feature_rows(
                cutoffs=cutoffs,
                local_bars=local_bars,
                execution_l2=execution_l2,
                metrics=metric_audit.observations,
                reference_bars=reference_bars,
                local_synthetic_starts=local_audit.synthesized_start_ts_ms,
                reference_synthetic_starts=(
                    () if reference_audit is None else reference_audit.synthesized_start_ts_ms
                ),
            )
        buffered: list[dict[str, Any]] = []
        feature_compute_seconds = 0.0
        panel_write_seconds = 0.0
        rows = 0
        try:
            for _ in cutoffs:
                compute_started = time.perf_counter()
                item = next(iterator)
                record = item[0] if engine == materializer.CPP_BATCH_ENGINE else materializer._panel_record(item)
                feature_compute_seconds += time.perf_counter() - compute_started
                buffered.append(record)
                rows += 1
                if len(buffered) >= batch_rows:
                    write_started = time.perf_counter()
                    writer.write_table(
                        pa.Table.from_pylist(
                            buffered,
                            schema=materializer.panel_arrow_schema(),
                        )
                    )
                    panel_write_seconds += time.perf_counter() - write_started
                    buffered.clear()
            if buffered:
                write_started = time.perf_counter()
                writer.write_table(
                    pa.Table.from_pylist(
                        buffered,
                        schema=materializer.panel_arrow_schema(),
                    )
                )
                panel_write_seconds += time.perf_counter() - write_started
        finally:
            close_started = time.perf_counter()
            writer.close()
            panel_write_seconds += time.perf_counter() - close_started
        if rows != row_count:
            raise AssertionError(f"benchmark row mismatch: {rows} != {row_count}")

        probe_path = temporary_dir / materializer.PROBE_FILENAME
        _atomic_json(probe_path, probe)
        measured_row_seconds = feature_compute_seconds + panel_write_seconds
        fixed_seconds = probe_seconds + source_load_seconds + engine_prepare_seconds
        report = {
            "schema_version": SCHEMA_VERSION,
            "status": "small_temporary_output_not_training_or_live_authorized",
            "created_at_utc": datetime.now(tz=UTC).isoformat(),
            "utc_day": bundle.utc_day,
            "engine": materializer._engine_identity(engine),
            "source_spec": {
                "path": str(source_spec),
                "sha256": daily.sha256_file(source_spec),
                "bundle_identity_sha256": bundle.identity_sha256(),
            },
            "cutoff_sample": {
                "method": "deterministic_even_coverage_of_canonical_target_day_seconds",
                "row_count": row_count,
                "first_cutoff_ms": cutoffs[0],
                "last_cutoff_ms": cutoffs[-1],
                "target_interval": "[D 00:00:00, D+1 00:00:00)",
                "predecessor_bar_required": True,
            },
            "timing": {
                "source_probe_seconds": probe_seconds,
                "source_load_seconds": source_load_seconds,
                "engine_prepare_seconds": engine_prepare_seconds,
                "feature_compute_seconds": feature_compute_seconds,
                "feature_compute_rows_per_second": rows / feature_compute_seconds,
                "panel_write_seconds": panel_write_seconds,
                "panel_write_rows_per_second": rows / panel_write_seconds,
                "measured_fixed_seconds": fixed_seconds,
                "measured_row_seconds": measured_row_seconds,
                "estimated_full_day_wall_seconds": extrapolated_full_day_wall_seconds(
                    fixed_seconds=fixed_seconds,
                    measured_row_seconds=measured_row_seconds,
                    row_count=rows,
                ),
                "full_day_estimate_method": (
                    "probe_plus_source_load_once_plus_linear_feature_compute_and_panel_write"
                ),
                "observed_process_wall_seconds_before_report": time.perf_counter() - started,
            },
            "memory": {
                "peak_rss_bytes": _peak_rss_bytes(),
                "peak_rss_measurement": "getrusage_process_maxrss",
                "platform": sys.platform,
            },
            "output": {
                "panel_path": materializer.PANEL_FILENAME,
                "panel_sha256": daily.sha256_file(panel_path),
                "panel_size_bytes": panel_path.stat().st_size,
                "source_probe_path": materializer.PROBE_FILENAME,
                "source_probe_sha256": daily.sha256_file(probe_path),
            },
            "source_runtime_audit": {
                "local_trade_tempo": local_audit.audit_payload(),
                "reference_bars": (
                    None if reference_audit is None else reference_audit.audit_payload()
                ),
                "metrics": [item.audit_payload() for item in metric_audit.files],
            },
            "labels_read": False,
            "predictions_read": False,
            "economic_outcomes_read": False,
            "full_day_materialization_started": False,
            "training_authorized": False,
            "live_authorized": False,
        }
        report_path = temporary_dir / "benchmark.json"
        _atomic_json(report_path, report)
        (temporary_dir / materializer.SUCCESS_FILENAME).write_text(
            daily.sha256_file(report_path) + "\n",
            encoding="ascii",
        )
        gc.collect()
        os.replace(temporary_dir, output_dir)
    except BaseException:
        shutil.rmtree(temporary_dir, ignore_errors=True)
        raise
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--row-count", type=int, required=True)
    parser.add_argument("--batch-rows", type=int, default=128)
    parser.add_argument("--engine", choices=materializer.SUPPORTED_ENGINES, required=True)
    args = parser.parse_args(argv)
    report = run_benchmark(
        source_spec=args.source_spec,
        output_dir=args.output_dir,
        row_count=args.row_count,
        batch_rows=args.batch_rows,
        engine=args.engine,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
