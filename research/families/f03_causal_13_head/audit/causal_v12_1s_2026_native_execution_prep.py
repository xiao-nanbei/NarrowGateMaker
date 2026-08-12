#!/usr/bin/env python3
"""Prepare the frozen 2026 native 40-day F03 1s execution inputs."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import shutil
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_orico_source_spec as source_specs,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_panel_materializer as panels,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_prediction_overlay as predictions,
)
from research.governance.public_machine_projection import source_identity_sha256

SCHEMA_VERSION = "causal_v12_1s_2026_native_execution_prep.v1"
MANIFEST_SCHEMA_VERSION = "causal_v12_1s_2026_native_execution_prep_manifest.v1"
IDENTITY = "causal_v12_1s_2026_native_40day_execution_prep_v1"
EXPECTED_PROFILE = source_specs.NATIVE_HISTORICAL_MINIMAL141_PROFILE
EXPECTED_DAY_COUNT = 40
GIB = 1 << 30
SAFETY_RESERVE_BYTES = 60 * GIB
ESTIMATED_FINAL_BYTES = 5 * GIB

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_PRECOMMIT_PATH = (
    REPOSITORY_ROOT / "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_cadence_full_path_economic_precommit_v1_20260805.json"
)
DEFAULT_PROFILE_EVIDENCE_PATH = (
    REPOSITORY_ROOT / "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_native_historical_minimal141_source_profile_v1_implementation_20260805.json"
)
PROGRESS_FILENAME = "execution-prep-progress.json"
MANIFEST_FILENAME = "execution-prep-manifest.json"
SUCCESS_FILENAME = "_EXECUTION_PREP_SUCCESS"
_MATERIALIZER_PROBE_LOCK = threading.Lock()


class NativeExecutionPrepError(ValueError):
    """Raised when the frozen native preparation identity cannot be upheld."""


@dataclass(frozen=True, slots=True)
class FrozenNativePanel:
    days: tuple[str, ...]
    precommit_path: Path
    precommit_sha256: str
    profile_evidence_path: Path
    profile_evidence_sha256: str
    profile_id: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )
    return hashlib.sha256(encoded).hexdigest()


def _load_json_object(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise NativeExecutionPrepError(f"missing {role}: {resolved}")
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise NativeExecutionPrepError(f"{role} must be a JSON object")
    return payload


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


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="ascii") as handle:
            handle.write(value)
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


def _ordered_days(value: Any, *, role: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise NativeExecutionPrepError(f"{role} must be an ordered string list")
    days = tuple(value)
    if len(days) != EXPECTED_DAY_COUNT:
        raise NativeExecutionPrepError(f"{role} must contain exactly {EXPECTED_DAY_COUNT} days")
    if days != tuple(sorted(set(days))):
        raise NativeExecutionPrepError(f"{role} must be unique and chronological")
    return days


def load_frozen_native_panel(
    *,
    precommit_path: Path = DEFAULT_PRECOMMIT_PATH,
    profile_evidence_path: Path = DEFAULT_PROFILE_EVIDENCE_PATH,
) -> FrozenNativePanel:
    """Bind the precommitted 40-day denominator to the exact native profile."""

    precommit_path = precommit_path.expanduser().resolve()
    profile_evidence_path = profile_evidence_path.expanduser().resolve()
    precommit = _load_json_object(precommit_path, role="F03 1s economic precommit")
    evidence = _load_json_object(profile_evidence_path, role="native source profile evidence")
    if (
        precommit.get("status")
        != "frozen_before_candidate_training_predictions_or_economic_outcomes"
    ):
        raise NativeExecutionPrepError("F03 precommit is not in its frozen pre-outcome state")
    native = precommit.get("native_development_panel")
    if not isinstance(native, dict):
        raise NativeExecutionPrepError("F03 precommit lacks native_development_panel")
    if native.get("source_authority") != EXPECTED_PROFILE:
        raise NativeExecutionPrepError("F03 precommit source profile drift")
    if native.get("source_profile_sha256") != source_identity_sha256(
        profile_evidence_path
    ):
        raise NativeExecutionPrepError("F03 precommit no longer binds source profile evidence")
    if native.get("day_count") != EXPECTED_DAY_COUNT:
        raise NativeExecutionPrepError("F03 precommit day-count drift")
    precommit_days = _ordered_days(native.get("days"), role="precommitted native panel")

    profile = evidence.get("profile")
    development = evidence.get("development_40")
    if not isinstance(profile, dict) or not isinstance(development, dict):
        raise NativeExecutionPrepError("native profile evidence is structurally incomplete")
    if profile.get("profile_id") != EXPECTED_PROFILE:
        raise NativeExecutionPrepError("native profile evidence identity drift")
    if development.get("resolve_probe_accepted") != EXPECTED_DAY_COUNT:
        raise NativeExecutionPrepError("native profile did not accept all frozen days")
    if development.get("resolve_probe_rejected") != 0:
        raise NativeExecutionPrepError("native profile rejected a frozen day")
    evidence_days = _ordered_days(development.get("days"), role="profile evidence panel")
    if evidence_days != precommit_days:
        raise NativeExecutionPrepError("precommit and source-profile day denominators differ")
    return FrozenNativePanel(
        days=precommit_days,
        precommit_path=precommit_path,
        precommit_sha256=_sha256_file(precommit_path),
        profile_evidence_path=profile_evidence_path,
        profile_evidence_sha256=source_identity_sha256(profile_evidence_path),
        profile_id=EXPECTED_PROFILE,
    )


def _require_cache_root(market_data_root: Path, cache_root: Path) -> None:
    root = market_data_root.expanduser().resolve()
    cache = cache_root.expanduser().resolve()
    required_parent = root / "cache"
    if not root.is_dir():
        raise NativeExecutionPrepError(f"market-data root is unavailable: {root}")
    try:
        cache.relative_to(required_parent)
    except ValueError as exc:
        raise NativeExecutionPrepError(
            f"execution-prep cache must stay below {required_parent}"
        ) from exc
    required_parent.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(required_parent).free
    required = SAFETY_RESERVE_BYTES + int(2.5 * ESTIMATED_FINAL_BYTES)
    if free < required:
        raise NativeExecutionPrepError(f"storage gate failed: free={free}, required={required}")


def _write_immutable_json(path: Path, payload: Any) -> None:
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        if existing != payload:
            raise NativeExecutionPrepError(
                f"refusing to replace a different frozen artifact: {path}"
            )
        return
    _atomic_json(path, payload)


def _materialize_exact_native_panel(
    built: source_specs.BuiltSourceSpec,
    *,
    output_dir: Path,
    batch_rows: int,
) -> panels.MaterializedPanel:
    """Bridge the exact registry probe into the older daily materializer ABI.

    The exact profile deliberately gives D-1 warmup and target days different
    sequence requirements. The older generic probe cannot express that role
    distinction. The bridge is serialized, accepts only the already validated
    bundle identity, and restores the generic probe before returning.
    """

    if built.profile_id != EXPECTED_PROFILE:
        raise NativeExecutionPrepError("probe bridge only accepts the exact native profile")
    if built.probe.get("physical_materialization_eligible") is not True:
        raise NativeExecutionPrepError("cannot materialize a source profile that failed authority")
    authority = built.probe.get("execution_l2_quality_authority")
    if not isinstance(authority, dict):
        raise NativeExecutionPrepError("exact native probe lacks registry L2 authority")
    if authority.get("authority_mode") != source_specs.REGISTRY_MANIFEST_CSV_QUALITY_AUTHORITY:
        raise NativeExecutionPrepError("unexpected native L2 quality authority mode")
    if authority.get("valid") is not True or authority.get("errors") != []:
        raise NativeExecutionPrepError("registry L2 authority did not pass exactly")
    if _sha256_file(Path(str(authority["manifest_path"]))) != authority.get("manifest_sha256"):
        raise NativeExecutionPrepError("registry L2 manifest hash drift")
    if _sha256_file(Path(str(authority["daily_quality_path"]))) != authority.get(
        "daily_quality_sha256"
    ):
        raise NativeExecutionPrepError("registry daily-quality hash drift")
    expected_bundle_identity = built.bundle.identity_sha256()
    generic_probe = panels.sources.probe_source_bundle

    def exact_profile_probe(bundle: Any) -> dict[str, Any]:
        if bundle.identity_sha256() != expected_bundle_identity:
            raise NativeExecutionPrepError(
                "daily materializer requested a bundle outside the validated exact profile"
            )
        return built.probe

    with _MATERIALIZER_PROBE_LOCK:
        panels.sources.probe_source_bundle = exact_profile_probe
        try:
            return panels.materialize_daily_panel(
                built.bundle,
                output_dir=output_dir,
                cutoffs_ms=None,
                batch_rows=batch_rows,
                engine=panels.CPP_BATCH_ENGINE,
            )
        finally:
            panels.sources.probe_source_bundle = generic_probe


def _bundle_binding(bundle_dir: Path | None) -> dict[str, Any] | None:
    if bundle_dir is None:
        return None
    admitted = predictions.load_admitted_research_bundle(bundle_dir)
    return {
        "bundle_dir": str(admitted.output_dir),
        "bundle_meta_path": str(admitted.bundle_path),
        "bundle_meta_sha256": admitted.bundle_sha256,
        "training_identity_sha256": admitted.bundle["training_identity_sha256"],
        "head_count": len(admitted.heads),
    }


def _prepare_day(
    *,
    ordinal: int,
    day: str,
    profile_id: str,
    market_data_root: Path,
    cache_root: Path,
    batch_rows: int,
) -> dict[str, Any]:
    day_started = time.perf_counter()
    built = source_specs.build_orico_daily_source_spec(
        target_day=day,
        market_data_root=market_data_root,
        profile_id=profile_id,
    )
    if built.profile_id != profile_id or built.bundle.utc_day != day:
        raise NativeExecutionPrepError("resolved daily source identity drift")
    source_dir = cache_root / "source-specs" / day
    source_spec_path = source_dir / "source-spec.json"
    source_probe_path = source_dir / "source-probe.json"
    _write_immutable_json(source_spec_path, source_specs.source_spec_payload(built.bundle))
    _write_immutable_json(source_probe_path, built.probe)
    feature = _materialize_exact_native_panel(
        built,
        output_dir=cache_root / "feature-panels" / day,
        batch_rows=batch_rows,
    )
    if feature.row_count != 86_400:
        raise NativeExecutionPrepError(
            f"{day} feature panel rows={feature.row_count}, expected=86400"
        )
    return {
        "ordinal": ordinal,
        "utc_day": day,
        "source_spec_path": str(source_spec_path),
        "source_spec_sha256": _sha256_file(source_spec_path),
        "source_probe_path": str(source_probe_path),
        "source_probe_sha256": _sha256_file(source_probe_path),
        "materializer_source_bundle_identity_sha256": built.bundle.identity_sha256(),
        "materializer_probe_adapter": "exact_profile_role_aware_probe.v1",
        "feature_panel_dir": str(feature.output_dir),
        "feature_panel_path": str(feature.panel_path),
        "feature_panel_sha256": _sha256_file(feature.panel_path),
        "feature_manifest_path": str(feature.manifest_path),
        "feature_manifest_sha256": _sha256_file(feature.manifest_path),
        "feature_cache_identity_sha256": feature.cache_identity_sha256,
        "feature_rows": feature.row_count,
        "feature_reused": feature.reused,
        "elapsed_seconds": time.perf_counter() - day_started,
    }


def prepare_native_execution_inputs(
    *,
    market_data_root: Path,
    cache_root: Path,
    model_bundle_dir: Path | None = None,
    require_bound_model_bundle: bool = False,
    precommit_path: Path = DEFAULT_PRECOMMIT_PATH,
    profile_evidence_path: Path = DEFAULT_PROFILE_EVIDENCE_PATH,
    batch_rows: int = 4_096,
    workers: int = 1,
) -> dict[str, Any]:
    """Materialize only feature panels; never train or read predictions/PnL."""

    if batch_rows <= 0:
        raise NativeExecutionPrepError("batch_rows must be positive")
    if not 1 <= workers <= 8:
        raise NativeExecutionPrepError("workers must be in [1,8]")
    if require_bound_model_bundle and model_bundle_dir is None:
        raise NativeExecutionPrepError(
            "model bundle is unknown; execution-ready preparation fails closed before materialization"
        )
    frozen = load_frozen_native_panel(
        precommit_path=precommit_path,
        profile_evidence_path=profile_evidence_path,
    )
    market_data_root = market_data_root.expanduser().resolve()
    cache_root = cache_root.expanduser().resolve()
    _require_cache_root(market_data_root, cache_root)
    bundle = _bundle_binding(model_bundle_dir)

    started = time.perf_counter()
    progress_path = cache_root / PROGRESS_FILENAME
    rows: list[dict[str, Any]] = []
    jobs = [
        {
            "ordinal": ordinal,
            "day": day,
            "profile_id": frozen.profile_id,
            "market_data_root": market_data_root,
            "cache_root": cache_root,
            "batch_rows": batch_rows,
        }
        for ordinal, day in enumerate(frozen.days, start=1)
    ]
    if workers == 1:
        results = (_prepare_day(**job) for job in jobs)
    else:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=workers)
        futures = [executor.submit(_prepare_day, **job) for job in jobs]
        results = (future.result() for future in concurrent.futures.as_completed(futures))
    try:
        for row in results:
            rows.append(row)
            ordered_rows = sorted(rows, key=lambda item: int(item["ordinal"]))
            _atomic_json(
                progress_path,
                {
                    "schema_version": SCHEMA_VERSION,
                    "identity": IDENTITY,
                    "status": "materializing_feature_panels",
                    "completed_day_count": len(rows),
                    "total_day_count": len(frozen.days),
                    "last_completed_day": row["utc_day"],
                    "profile_id": frozen.profile_id,
                    "workers": workers,
                    "days": ordered_rows,
                    "predictions_read": False,
                    "economic_outcomes_read": False,
                    "training_performed": False,
                },
            )
            print(
                f"[{len(rows):02d}/{len(frozen.days)}] ordinal={row['ordinal']:02d} "
                f"{row['utc_day']} feature_reused={row['feature_reused']} "
                f"elapsed_s={row['elapsed_seconds']:.1f}",
                flush=True,
            )
    finally:
        if workers > 1:
            executor.shutdown(wait=True, cancel_futures=True)
    rows.sort(key=lambda item: int(item["ordinal"]))

    stable_rows = [
        {
            key: value
            for key, value in row.items()
            if key not in {"elapsed_seconds", "feature_reused"}
        }
        for row in rows
    ]
    identity_payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "profile_id": frozen.profile_id,
        "precommit_path": str(frozen.precommit_path),
        "precommit_sha256": frozen.precommit_sha256,
        "profile_evidence_path": str(frozen.profile_evidence_path),
        "profile_evidence_sha256": frozen.profile_evidence_sha256,
        "market_data_root": str(market_data_root),
        "cache_root": str(cache_root),
        "days": list(frozen.days),
        "feature_panels": stable_rows,
        "model_bundle": bundle,
    }
    prep_identity_sha256 = _canonical_sha256(identity_payload)
    status = (
        "feature_panels_complete_model_bundle_bound"
        if bundle is not None
        else "feature_panels_complete_model_bundle_unbound"
    )
    blockers = [] if bundle is not None else ["model_bundle_identity_unknown"]
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "execution_prep_identity_sha256": prep_identity_sha256,
        "identity_payload": identity_payload,
        "completed_day_count": len(rows),
        "model_bundle_bound": bundle is not None,
        "execution_input_eligible": bundle is not None,
        "blockers": blockers,
        "labels_read": False,
        "predictions_read": False,
        "economic_outcomes_read": False,
        "training_performed": False,
        "training_authorized": False,
        "prediction_execution_started": False,
        "economic_replay_started": False,
        "action_authorized": False,
        "live_authorized": False,
        "atomic_admission": True,
    }
    manifest_path = cache_root / MANIFEST_FILENAME
    success_path = cache_root / SUCCESS_FILENAME
    if manifest_path.exists() or success_path.exists():
        if not (manifest_path.is_file() and success_path.is_file()):
            raise NativeExecutionPrepError("incomplete prior execution-prep admission")
        existing = _load_json_object(manifest_path, role="execution-prep manifest")
        if existing.get("execution_prep_identity_sha256") != prep_identity_sha256:
            raise NativeExecutionPrepError(
                "existing execution-prep admission has a different identity"
            )
        if success_path.read_text(encoding="ascii").strip() != _sha256_file(manifest_path):
            raise NativeExecutionPrepError("execution-prep admission marker mismatch")
        manifest = existing
    else:
        _atomic_json(manifest_path, manifest)
        _atomic_text(success_path, _sha256_file(manifest_path) + "\n")
    _atomic_json(
        progress_path,
        {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "status": manifest["status"],
            "completed_day_count": len(rows),
            "total_day_count": len(rows),
            "execution_prep_manifest_path": str(manifest_path),
            "execution_prep_manifest_sha256": _sha256_file(manifest_path),
            "elapsed_seconds": time.perf_counter() - started,
            "model_bundle_bound": bundle is not None,
            "execution_input_eligible": bundle is not None,
            "blockers": blockers,
            "predictions_read": False,
            "economic_outcomes_read": False,
            "training_performed": False,
        },
    )
    return {
        "status": manifest["status"],
        "completed_day_count": len(rows),
        "execution_prep_manifest_path": str(manifest_path),
        "execution_prep_manifest_sha256": _sha256_file(manifest_path),
        "execution_prep_identity_sha256": prep_identity_sha256,
        "model_bundle_bound": bundle is not None,
        "execution_input_eligible": bundle is not None,
        "blockers": blockers,
        "training_performed": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-data-root", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--model-bundle-dir", type=Path)
    parser.add_argument("--require-bound-model-bundle", action="store_true")
    parser.add_argument("--precommit", type=Path, default=DEFAULT_PRECOMMIT_PATH)
    parser.add_argument("--profile-evidence", type=Path, default=DEFAULT_PROFILE_EVIDENCE_PATH)
    parser.add_argument("--batch-rows", type=int, default=4_096)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args(argv)
    result = prepare_native_execution_inputs(
        market_data_root=args.market_data_root,
        cache_root=args.cache_root,
        model_bundle_dir=args.model_bundle_dir,
        require_bound_model_bundle=args.require_bound_model_bundle,
        precommit_path=args.precommit,
        profile_evidence_path=args.profile_evidence,
        batch_rows=args.batch_rows,
        workers=args.workers,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
