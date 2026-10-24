#!/usr/bin/env python3
"""Freeze provenance for reusable, complete days from the failed F03 v2 run.

The attestation never authorizes training.  It only proves which immutable v2
payloads may be considered by the v3 successor admission gate.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training_contract as training_contract,
)

SCHEMA_VERSION = "causal_v12_1s_legacy_v2_run_attestation.v1"
STATUS = "mixed_code_v2_superseded_reuse_candidates_only"
LEGACY_ROOT_NAME = "f03_causal_v12_1s_metrics_source_ready_v2"
LEGACY_FEATURE_SCHEMA = "causal_v12_1s_daily_feature_panel_artifact.v3"
LEGACY_LABEL_SCHEMA = "causal_v12_1s_daily_label_overlay_artifact.v2"
EXPECTED_COMPLETE_DAYS = 56
INCOMPLETE_TRANSIENT_DAY = "2025-11-30"
EXPECTED_COMPLETED_BINDINGS_SHA256 = (
    "b62cea5dfa1f5fe2be73bd3f79efdae90d15b27db9a5a227ddc2a7d7743b6d9f"
)
EXPECTED_TRANSIENT_BINDINGS_SHA256 = (
    "33564219b090833c26d2b40492115efb336c7c0003164edd68ed7df9e5f8dc24"
)
OBSERVED_LOADED_EXTENSION_SHA256 = (
    "80dcb3581a9d0020a1437388df8384c581484e93ed88410aa856592f4580a72e"
)


class LegacyV2AttestationError(ValueError):
    """Raised when the failed v2 run cannot be described without ambiguity."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LegacyV2AttestationError(f"invalid {role}: {path}") from exc
    if not isinstance(payload, dict):
        raise LegacyV2AttestationError(f"{role} must be a JSON object")
    return payload


def _artifact_binding(path: Path) -> dict[str, Any]:
    return execution_identity.file_identity(path)


def _validate_success(directory: Path, manifest_path: Path) -> None:
    success = directory / "_SUCCESS"
    if not success.is_file():
        raise LegacyV2AttestationError(f"missing legacy admission marker: {success}")
    if success.read_text(encoding="ascii").strip() != execution_identity.sha256_file(
        manifest_path
    ):
        raise LegacyV2AttestationError(f"legacy admission marker mismatch: {success}")


def _complete_day_entry(root: Path, day: str) -> dict[str, Any]:
    feature_dir = root / "features" / day
    label_dir = root / "labels" / day
    feature_manifest_path = feature_dir / "manifest.json"
    feature_panel_path = feature_dir / "panel.parquet"
    label_manifest_path = label_dir / "manifest.json"
    label_overlay_path = label_dir / "label_overlay.parquet"
    for path in (
        feature_manifest_path,
        feature_panel_path,
        label_manifest_path,
        label_overlay_path,
    ):
        if not path.is_file():
            raise LegacyV2AttestationError(f"legacy day is incomplete: {path}")
    _validate_success(feature_dir, feature_manifest_path)
    _validate_success(label_dir, label_manifest_path)
    feature = _load_json(feature_manifest_path, role="legacy feature manifest")
    label = _load_json(label_manifest_path, role="legacy label manifest")
    if feature.get("schema_version") != LEGACY_FEATURE_SCHEMA:
        raise LegacyV2AttestationError(f"legacy feature schema differs for {day}")
    if label.get("schema_version") != LEGACY_LABEL_SCHEMA:
        raise LegacyV2AttestationError(f"legacy label schema differs for {day}")
    if feature.get("utc_day") != day or label.get("utc_day") != day:
        raise LegacyV2AttestationError(f"legacy UTC identity differs for {day}")
    if feature.get("bulk_materialization_authorized") is not True:
        raise LegacyV2AttestationError(f"legacy feature was not bulk-authorized for {day}")
    engine = feature.get("engine")
    cache_engine = feature.get("cache_identity_payload", {}).get("engine")
    if not isinstance(engine, Mapping) or engine.get("engine") != "cpp_batch":
        raise LegacyV2AttestationError(f"legacy feature engine differs for {day}")
    if engine != cache_engine:
        raise LegacyV2AttestationError(f"legacy feature engine binding differs for {day}")
    code = feature.get("cache_identity_payload", {}).get("code")
    if not isinstance(code, Mapping):
        raise LegacyV2AttestationError(f"legacy feature code identity is missing for {day}")
    if code.get("cpp_bindings_sha256") != EXPECTED_COMPLETED_BINDINGS_SHA256:
        raise LegacyV2AttestationError(f"legacy complete day has unexpected bindings for {day}")
    panel = feature.get("panel")
    overlay = label.get("overlay")
    if not isinstance(panel, Mapping) or not isinstance(overlay, Mapping):
        raise LegacyV2AttestationError(f"legacy payload identity is missing for {day}")
    if panel.get("sha256") != execution_identity.sha256_file(feature_panel_path):
        raise LegacyV2AttestationError(f"legacy feature payload hash differs for {day}")
    if overlay.get("sha256") != execution_identity.sha256_file(label_overlay_path):
        raise LegacyV2AttestationError(f"legacy label payload hash differs for {day}")
    if pq.ParquetFile(feature_panel_path).metadata.num_rows != 86_400:
        raise LegacyV2AttestationError(f"legacy feature denominator differs for {day}")
    if pq.ParquetFile(label_overlay_path).metadata.num_rows != 86_400:
        raise LegacyV2AttestationError(f"legacy label denominator differs for {day}")
    feature_manifest_sha = execution_identity.sha256_file(feature_manifest_path)
    if label.get("feature_panel_manifest_sha256") != feature_manifest_sha:
        raise LegacyV2AttestationError(f"legacy label/feature binding differs for {day}")
    if label.get("feature_panel_sha256") != panel.get("sha256"):
        raise LegacyV2AttestationError(f"legacy label/panel binding differs for {day}")
    return {
        "utc_day": day,
        "feature_dir": str(feature_dir),
        "label_dir": str(label_dir),
        "feature_manifest": _artifact_binding(feature_manifest_path),
        "feature_panel": _artifact_binding(feature_panel_path),
        "feature_success": _artifact_binding(feature_dir / "_SUCCESS"),
        "label_manifest": _artifact_binding(label_manifest_path),
        "label_overlay": _artifact_binding(label_overlay_path),
        "label_success": _artifact_binding(label_dir / "_SUCCESS"),
        "source_bundle_identity_sha256": feature["cache_identity_payload"][
            "bundle_identity_sha256"
        ],
        "feature_payload_component_projection": (
            execution_identity.component_projection_from_legacy_panel_code(code)
        ),
        "wrapper_provenance": {
            "cpp_bindings_sha256": code["cpp_bindings_sha256"],
            "materializer_sha256": code["materializer_sha256"],
        },
        "label_generator_sha256": label["label_generator_sha256"],
        "label_quote_config_sha256": label["label_quote_config_sha256"],
        "p3_v2_artifact_sha256": label["p3_v2_artifact_sha256"],
    }


def freeze_legacy_v2_attestation(
    output_path: Path,
    *,
    legacy_root: Path,
    loaded_extension_path: Path,
    training_design_path: Path = training_contract.DEFAULT_DESIGN_PATH,
    producer_pid: int = 82_240,
    producer_started_at_local: str = "2026-08-05 08:46:38 Asia/Shanghai",
) -> dict[str, Any]:
    """Freeze the 56 complete v2 days without granting successor authority."""

    root = legacy_root.expanduser().resolve(strict=True)
    if root.name != LEGACY_ROOT_NAME:
        raise LegacyV2AttestationError(f"legacy root must be named {LEGACY_ROOT_NAME}")
    audit = training_contract.load_and_validate_training_design(training_design_path)
    refit_days = tuple(audit["refit_days"])
    complete_days = tuple(
        day
        for day in refit_days
        if (root / "features" / day / "_SUCCESS").is_file()
        and (root / "labels" / day / "_SUCCESS").is_file()
    )
    if len(complete_days) != EXPECTED_COMPLETE_DAYS:
        raise LegacyV2AttestationError(
            f"expected {EXPECTED_COMPLETE_DAYS} complete v2 days; observed {len(complete_days)}"
        )
    if INCOMPLETE_TRANSIENT_DAY in complete_days:
        raise LegacyV2AttestationError("transient 2025-11-30 must not be reusable")
    entries = [_complete_day_entry(root, day) for day in complete_days]
    projections = {
        execution_identity.canonical_sha256(row["feature_payload_component_projection"])
        for row in entries
    }
    if len(projections) != 1:
        raise LegacyV2AttestationError("complete v2 days do not share one F03 payload component")
    transient_manifest_path = root / "features" / INCOMPLETE_TRANSIENT_DAY / "manifest.json"
    transient = _load_json(transient_manifest_path, role="transient feature manifest")
    transient_code = transient.get("cache_identity_payload", {}).get("code", {})
    if transient_code.get("cpp_bindings_sha256") != EXPECTED_TRANSIENT_BINDINGS_SHA256:
        raise LegacyV2AttestationError("2025-11-30 transient bindings provenance differs")
    missing_days = tuple(day for day in refit_days if day not in complete_days)
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "legacy_root": str(root),
        "producer_process_attestation": {
            "pid": producer_pid,
            "started_at_local": producer_started_at_local,
            "loaded_extension_path": str(
                loaded_extension_path.expanduser().resolve(strict=False)
            ),
            "loaded_extension_sha256": OBSERVED_LOADED_EXTENSION_SHA256,
            "evidence_scope": "owner_confirmed_process_mapping_not_build_receipt",
        },
        "complete_reuse_candidates": entries,
        "complete_reuse_candidate_days": list(complete_days),
        "complete_reuse_candidate_count": len(entries),
        "required_rebuild_days": list(missing_days),
        "required_rebuild_day_count": len(missing_days),
        "transient_incomplete_day": {
            "utc_day": INCOMPLETE_TRANSIENT_DAY,
            "feature_manifest": _artifact_binding(transient_manifest_path),
            "cpp_bindings_sha256": transient_code["cpp_bindings_sha256"],
            "reason": "feature_only_mixed_code_no_label_overlay",
        },
        "full_bindings_source_role": "provenance_not_f03_payload_semantics",
        "legacy_v2_training_authorized": False,
        "successor_admission_required": True,
        "provider_and_native_complete_day_parity_required": True,
        "predictions_read": False,
        "economic_outcomes_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    payload = {
        **unsigned,
        "attestation_identity_sha256": execution_identity.canonical_sha256(unsigned),
    }
    output = output_path.expanduser().resolve()
    if output.exists():
        return validate_legacy_v2_attestation(output)
    execution_identity.write_json_fsync(output, payload)
    return payload


def validate_legacy_v2_attestation(path: Path) -> dict[str, Any]:
    attestation_path = path.expanduser().resolve(strict=True)
    payload = _load_json(attestation_path, role="legacy v2 run attestation")
    if payload.get("schema_version") != SCHEMA_VERSION or payload.get("status") != STATUS:
        raise LegacyV2AttestationError("unsupported legacy v2 run attestation")
    unsigned = dict(payload)
    identity = unsigned.pop("attestation_identity_sha256", None)
    if identity != execution_identity.canonical_sha256(unsigned):
        raise LegacyV2AttestationError("legacy v2 attestation identity mismatch")
    if payload.get("legacy_v2_training_authorized") is not False:
        raise LegacyV2AttestationError("legacy v2 attestation must not authorize training")
    rows = payload.get("complete_reuse_candidates")
    if not isinstance(rows, list) or len(rows) != EXPECTED_COMPLETE_DAYS:
        raise LegacyV2AttestationError("legacy v2 reuse candidate denominator differs")
    observed_days = tuple(str(row.get("utc_day", "")) for row in rows)
    if observed_days != tuple(payload.get("complete_reuse_candidate_days", ())):
        raise LegacyV2AttestationError("legacy v2 reuse candidate order differs")
    if observed_days != tuple(sorted(set(observed_days))):
        raise LegacyV2AttestationError("legacy v2 reuse candidates are not ordered and unique")
    if INCOMPLETE_TRANSIENT_DAY in observed_days:
        raise LegacyV2AttestationError("transient 2025-11-30 was admitted as reusable")
    for row in rows:
        for key in (
            "feature_manifest",
            "feature_panel",
            "feature_success",
            "label_manifest",
            "label_overlay",
            "label_success",
        ):
            binding = row.get(key)
            if not isinstance(binding, Mapping):
                raise LegacyV2AttestationError(f"legacy binding is missing: {key}")
            artifact_path = Path(str(binding.get("path", ""))).resolve(strict=True)
            if execution_identity.file_identity(artifact_path) != dict(binding):
                raise LegacyV2AttestationError(f"legacy artifact drifted: {artifact_path}")
    return payload


def candidate_by_day(payload: Mapping[str, Any], utc_day: str) -> dict[str, Any] | None:
    rows = payload.get("complete_reuse_candidates")
    if not isinstance(rows, Sequence):
        raise LegacyV2AttestationError("legacy v2 reuse candidate list is missing")
    for row in rows:
        if isinstance(row, Mapping) and row.get("utc_day") == utc_day:
            return dict(row)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-root", type=Path, required=True)
    parser.add_argument("--loaded-extension-path", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--training-design",
        type=Path,
        default=training_contract.DEFAULT_DESIGN_PATH,
    )
    args = parser.parse_args()
    payload = freeze_legacy_v2_attestation(
        args.output,
        legacy_root=args.legacy_root,
        loaded_extension_path=args.loaded_extension_path,
        training_design_path=args.training_design,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
