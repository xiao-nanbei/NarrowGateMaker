#!/usr/bin/env python3
"""Parallel, identity-frozen 66-day F03 1s successor materialization."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_overlay_materializer as overlays,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_legacy_v2_attestation as legacy_attestation,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_orico_source_spec as source_specs,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_parity_successor_gate as parity_gate,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training_admission_v3 as admissions,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training_contract as training_contract,
)

SCHEMA_VERSION = "causal_v12_1s_training_panel_pipeline.v3"
DAY_MANIFEST_SCHEMA_VERSION = admissions.TRAINING_DAY_MANIFEST_SCHEMA_VERSION
GIB = 1 << 30
SAFETY_RESERVE_BYTES = 60 * GIB
ESTIMATED_FINAL_BYTES = 8 * GIB
DEFAULT_WORKERS = 4
MAX_WORKERS = 4


class TrainingPanelPipelineError(ValueError):
    """Raised when batch identity, source authority, or storage gates drift."""


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
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


def _require_cache_namespace(market_data_root: Path, cache_root: Path) -> None:
    root = market_data_root.expanduser().resolve()
    cache = cache_root.expanduser().resolve()
    required_parent = root / "cache"
    try:
        cache.relative_to(required_parent)
    except ValueError as exc:
        raise TrainingPanelPipelineError(
            f"F03 materializations must stay below {required_parent}"
        ) from exc
    if cache.name != execution_identity.V3_CACHE_NAMESPACE:
        raise TrainingPanelPipelineError(
            f"successor cache root must be named {execution_identity.V3_CACHE_NAMESPACE}"
        )
    if not root.is_dir():
        raise TrainingPanelPipelineError(f"market-data root is unavailable: {root}")
    required_parent.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(required_parent).free
    required = SAFETY_RESERVE_BYTES + int(2.5 * ESTIMATED_FINAL_BYTES)
    if free < required:
        raise TrainingPanelPipelineError(
            f"storage gate failed: free={free}, required={required}"
        )


def _write_source_artifacts(
    built: source_specs.BuiltSourceSpec,
    *,
    source_dir: Path,
) -> tuple[Path, Path]:
    source_dir.mkdir(parents=True, exist_ok=True)
    spec_path = source_dir / "source-spec.json"
    probe_path = source_dir / "source-probe.json"
    spec_payload = source_specs.source_spec_payload(built.bundle)
    for path, payload in ((spec_path, spec_payload), (probe_path, built.probe)):
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing != payload:
                raise TrainingPanelPipelineError(
                    f"refusing to replace a different source artifact: {path}"
                )
        else:
            _atomic_json(path, payload)
    return spec_path, probe_path


def _worker_day(job: dict[str, Any]) -> dict[str, Any]:
    """Materialize or re-admit one day under the parent-owned receipt."""

    started = time.perf_counter()
    receipt_path = Path(job["pipeline_execution_receipt_path"])
    execution_identity.validate_pipeline_execution_receipt(
        receipt_path,
        require_materialization_workspace_stability=True,
    )
    utc_day = str(job["utc_day"])
    market_data_root = Path(job["market_data_root"])
    cache_root = Path(job["cache_root"])
    built = source_specs.build_orico_daily_source_spec(
        target_day=utc_day,
        market_data_root=market_data_root,
        profile_id=execution_identity.PROVIDER_PROFILE_ID,
    )
    source_dir = cache_root / "sources" / utc_day
    spec_path, probe_path = _write_source_artifacts(built, source_dir=source_dir)

    legacy_path = Path(job["legacy_v2_attestation_path"])
    legacy = legacy_attestation.validate_legacy_v2_attestation(legacy_path)
    legacy_row = legacy_attestation.candidate_by_day(legacy, utc_day)
    if legacy_row is None:
        feature_dir = cache_root / "features" / utc_day
        feature = panels.materialize_daily_panel(
            built.bundle,
            output_dir=feature_dir,
            cutoffs_ms=None,
            batch_rows=int(job["batch_rows"]),
            engine=panels.CPP_BATCH_ENGINE,
            source_probe_payload=built.probe,
            pipeline_execution_receipt_path=receipt_path,
        )
        label_dir = cache_root / "labels" / utc_day
        label = overlays.materialize_daily_label_overlay(
            built.bundle,
            feature_panel_dir=feature_dir,
            output_dir=label_dir,
            quote_config_path=Path(job["quote_config_path"]),
            p3_v2_artifact_path=Path(job["p3_v2_artifact_path"]),
            symbol="BTCUSDC",
        )
        feature_reused = feature.reused
        label_reused = label.reused
    else:
        feature_dir = Path(str(legacy_row["feature_dir"]))
        label_dir = Path(str(legacy_row["label_dir"]))
        feature_reused = True
        label_reused = True

    admission_path = cache_root / "admissions" / f"{utc_day}.json"
    admission = admissions.admit_daily_training_payload(
        admission_path,
        utc_day=utc_day,
        market_data_root=market_data_root,
        source_spec_path=spec_path,
        source_probe_path=probe_path,
        feature_dir=feature_dir,
        label_dir=label_dir,
        quote_config_path=Path(job["quote_config_path"]),
        p3_v2_artifact_path=Path(job["p3_v2_artifact_path"]),
        pipeline_execution_receipt_path=receipt_path,
        parity_successor_gate_path=Path(job["parity_successor_gate_path"]),
        legacy_v2_attestation_path=legacy_path,
    )
    execution_identity.validate_pipeline_execution_receipt(
        receipt_path,
        require_materialization_workspace_stability=True,
    )
    return {
        "ordinal": int(job["ordinal"]),
        "utc_day": utc_day,
        "source_spec_path": str(spec_path),
        "source_probe_path": str(probe_path),
        "feature_panel_dir": str(feature_dir),
        "label_overlay_dir": str(label_dir),
        "admission_receipt_path": str(admission_path),
        "admission_receipt_sha256": execution_identity.sha256_file(admission_path),
        "admission_identity_sha256": admission["admission_identity_sha256"],
        "producer_kind": admission["producer_kind"],
        "feature_rows": admission["feature_rows"],
        "label_rows": admission["label_rows"],
        "feature_reused": feature_reused,
        "label_reused": label_reused,
        "elapsed_seconds": time.perf_counter() - started,
    }


def run_pipeline(
    *,
    market_data_root: Path,
    cache_root: Path,
    quote_config_path: Path,
    p3_v2_artifact_path: Path,
    native_build_receipt_path: Path,
    parity_successor_gate_path: Path,
    legacy_v2_attestation_path: Path,
    training_design_path: Path = training_contract.DEFAULT_DESIGN_PATH,
    profile_id: str = source_specs.PROVIDER_NORMALIZED_PROFILE,
    batch_rows: int = 4_096,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, Any]:
    """Build/re-admit all 66 days; publish only an ordered all-day manifest."""

    if profile_id != execution_identity.PROVIDER_PROFILE_ID:
        raise TrainingPanelPipelineError("training requires provider_normalized_v1")
    if workers < 1 or workers > MAX_WORKERS:
        raise TrainingPanelPipelineError(f"workers must be in [1,{MAX_WORKERS}]")
    audit = training_contract.load_and_validate_training_design(training_design_path)
    days = tuple(audit["refit_days"])
    if len(days) != 66:
        raise TrainingPanelPipelineError("the frozen training panel must contain 66 days")
    market_data_root = market_data_root.expanduser().resolve()
    cache_root = cache_root.expanduser().resolve()
    quote_config_path = quote_config_path.expanduser().resolve(strict=True)
    p3_v2_artifact_path = p3_v2_artifact_path.expanduser().resolve(strict=True)
    native_build_receipt_path = native_build_receipt_path.expanduser().resolve(strict=True)
    parity_successor_gate_path = parity_successor_gate_path.expanduser().resolve(strict=True)
    legacy_v2_attestation_path = legacy_v2_attestation_path.expanduser().resolve(strict=True)
    training_design_path = training_design_path.expanduser().resolve(strict=True)
    _require_cache_namespace(market_data_root, cache_root)
    execution_identity.validate_explicit_p3_identity(
        quote_config_path,
        p3_v2_artifact_path,
    )
    build = execution_identity.validate_native_build_receipt(
        native_build_receipt_path,
        require_full_build_input_match=True,
    )
    gate = parity_gate.validate_training_parity_gate(parity_successor_gate_path)
    if gate["native_build_receipt"]["receipt_sha256"] != build["receipt_sha256"]:
        raise TrainingPanelPipelineError("parity gate differs from current native build receipt")
    legacy_attestation.validate_legacy_v2_attestation(legacy_v2_attestation_path)

    execution_dir = cache_root / "execution"
    receipt_path = execution_dir / "pipeline_execution_receipt.json"
    pipeline_receipt = execution_identity.freeze_pipeline_execution_receipt(
        receipt_path,
        native_build_receipt_path=native_build_receipt_path,
        quote_config_path=quote_config_path,
        explicit_p3_path=p3_v2_artifact_path,
        training_design_path=training_design_path,
        profile_id=profile_id,
        cache_root=cache_root,
        workers=workers,
        legacy_run_attestation_path=legacy_v2_attestation_path,
    )
    execution_identity.validate_pipeline_execution_receipt(
        receipt_path,
        require_materialization_workspace_stability=True,
    )

    started = time.perf_counter()
    progress_path = cache_root / "materialization_progress.json"
    jobs = [
        {
            "ordinal": ordinal,
            "utc_day": day,
            "market_data_root": str(market_data_root),
            "cache_root": str(cache_root),
            "quote_config_path": str(quote_config_path),
            "p3_v2_artifact_path": str(p3_v2_artifact_path),
            "pipeline_execution_receipt_path": str(receipt_path),
            "parity_successor_gate_path": str(parity_successor_gate_path),
            "legacy_v2_attestation_path": str(legacy_v2_attestation_path),
            "batch_rows": batch_rows,
        }
        for ordinal, day in enumerate(days, start=1)
    ]
    rows: list[dict[str, Any]] = []
    try:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for row in executor.map(_worker_day, jobs, chunksize=1):
                rows.append(row)
                _atomic_json(
                    progress_path,
                    {
                        "schema_version": SCHEMA_VERSION,
                        "status": "in_progress_not_training_authorized",
                        "completed_day_count": len(rows),
                        "total_day_count": len(days),
                        "last_completed_day": row["utc_day"],
                        "pipeline_execution_identity_sha256": pipeline_receipt[
                            "execution_identity_sha256"
                        ],
                        "days": rows,
                    },
                )
                print(
                    f"[{row['ordinal']:02d}/{len(days)}] {row['utc_day']} "
                    f"producer={row['producer_kind']} elapsed_s={row['elapsed_seconds']:.1f}",
                    flush=True,
                )
    except BaseException as exc:
        _atomic_json(
            progress_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "failed_closed_no_training_day_manifest",
                "completed_day_count": len(rows),
                "total_day_count": len(days),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "days": rows,
            },
        )
        raise

    execution_identity.validate_pipeline_execution_receipt(
        receipt_path,
        require_materialization_workspace_stability=True,
    )
    if tuple(row["utc_day"] for row in rows) != days:
        raise TrainingPanelPipelineError("worker aggregation changed the frozen ordinal order")
    for row in rows:
        admission = admissions.validate_daily_training_admission(
            Path(row["admission_receipt_path"])
        )
        if admission["admission_identity_sha256"] != row["admission_identity_sha256"]:
            raise TrainingPanelPipelineError("daily admission identity changed before aggregation")

    day_manifest = {
        "schema_version": DAY_MANIFEST_SCHEMA_VERSION,
        "identity": training_contract.IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "profile_id": profile_id,
        "source_permissions": execution_identity.SOURCE_PERMISSION_CONTRACT,
        "market_data_root": str(market_data_root),
        "cache_root": str(cache_root),
        "pipeline_execution_receipt": {
            **execution_identity.file_identity(receipt_path),
            "execution_identity_sha256": pipeline_receipt["execution_identity_sha256"],
        },
        "parity_successor_gate": {
            **execution_identity.file_identity(parity_successor_gate_path),
            "parity_gate_identity_sha256": gate["parity_gate_identity_sha256"],
        },
        "days": [
            {
                "ordinal": row["ordinal"],
                "utc_day": row["utc_day"],
                "feature_panel_dir": row["feature_panel_dir"],
                "label_overlay_dir": row["label_overlay_dir"],
                "admission_receipt_path": row["admission_receipt_path"],
                "admission_receipt_sha256": row["admission_receipt_sha256"],
                "admission_identity_sha256": row["admission_identity_sha256"],
                "producer_kind": row["producer_kind"],
            }
            for row in rows
        ],
        "day_count": len(rows),
        "training_input_authorized": True,
        "queue_authority": False,
        "order_lifecycle_authority": False,
        "fill_path_authority": False,
        "pnl_authority": False,
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
        "training_performed": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    manifest_path = cache_root / "training_day_manifest.v3.json"
    _atomic_json(manifest_path, day_manifest)
    final = {
        "schema_version": SCHEMA_VERSION,
        "status": "complete_66_day_successor_inputs_training_not_run",
        "training_day_manifest_path": str(manifest_path),
        "training_day_manifest_sha256": execution_identity.sha256_file(manifest_path),
        "completed_day_count": len(rows),
        "legacy_readmitted_day_count": sum(
            row["producer_kind"].startswith("legacy") for row in rows
        ),
        "newly_materialized_or_reused_v3_day_count": sum(
            row["producer_kind"].startswith("v3") for row in rows
        ),
        "workers": workers,
        "elapsed_seconds": time.perf_counter() - started,
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
        "training_performed": False,
    }
    _atomic_json(progress_path, final | {"days": rows})
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-data-root", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--quote-config", type=Path, required=True)
    parser.add_argument("--p3-v2-artifact", type=Path, required=True)
    parser.add_argument("--native-build-receipt", type=Path, required=True)
    parser.add_argument("--parity-successor-gate", type=Path, required=True)
    parser.add_argument("--legacy-v2-attestation", type=Path, required=True)
    parser.add_argument(
        "--training-design",
        type=Path,
        default=training_contract.DEFAULT_DESIGN_PATH,
    )
    parser.add_argument(
        "--profile",
        choices=(execution_identity.PROVIDER_PROFILE_ID,),
        default=execution_identity.PROVIDER_PROFILE_ID,
    )
    parser.add_argument("--batch-rows", type=int, default=4_096)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()
    result = run_pipeline(
        market_data_root=args.market_data_root,
        cache_root=args.cache_root,
        quote_config_path=args.quote_config,
        p3_v2_artifact_path=args.p3_v2_artifact,
        native_build_receipt_path=args.native_build_receipt,
        parity_successor_gate_path=args.parity_successor_gate,
        legacy_v2_attestation_path=args.legacy_v2_attestation,
        training_design_path=args.training_design,
        profile_id=args.profile,
        batch_rows=args.batch_rows,
        workers=args.workers,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
