#!/usr/bin/env python3
"""Successor admission for exact F03 1s feature/label training payloads."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as daily_sources,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_generator as label_generator,
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

SCHEMA_VERSION = "causal_v12_1s_daily_training_admission.v3"
STATUS = "successor_daily_payload_admitted_for_training_input"
TRAINING_DAY_MANIFEST_SCHEMA_VERSION = "causal_v12_1s_training_day_manifest.v3"
ROWS_PER_DAY = 86_400


class TrainingAdmissionV3Error(ValueError):
    """Raised when a daily payload cannot enter the successor training panel."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TrainingAdmissionV3Error(f"invalid {role}: {path}") from exc
    if not isinstance(payload, dict):
        raise TrainingAdmissionV3Error(f"{role} must be a JSON object")
    return payload


def _require_false(payload: Mapping[str, Any], keys: tuple[str, ...], *, role: str) -> None:
    for key in keys:
        if payload.get(key) is not False:
            raise TrainingAdmissionV3Error(f"{role} must bind {key}=false")


def _validate_success(directory: Path, manifest_path: Path) -> Path:
    success = directory / "_SUCCESS"
    if not success.is_file():
        raise TrainingAdmissionV3Error(f"missing atomic admission marker: {success}")
    if success.read_text(encoding="ascii").strip() != execution_identity.sha256_file(
        manifest_path
    ):
        raise TrainingAdmissionV3Error(f"atomic admission marker mismatch: {success}")
    return success


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _profile_bound_source(
    *,
    utc_day: str,
    market_data_root: Path,
    source_spec_path: Path,
    source_probe_path: Path,
) -> source_specs.BuiltSourceSpec:
    built = source_specs.build_orico_daily_source_spec(
        target_day=utc_day,
        market_data_root=market_data_root,
        profile_id=execution_identity.PROVIDER_PROFILE_ID,
    )
    spec = _load_json(source_spec_path, role="profile-bound source spec")
    probe = _load_json(source_probe_path, role="profile-bound source probe")
    if spec != source_specs.source_spec_payload(built.bundle):
        raise TrainingAdmissionV3Error(f"source spec differs from exact provider paths: {utc_day}")
    if probe != built.probe:
        raise TrainingAdmissionV3Error(f"source probe differs from exact provider authority: {utc_day}")
    if probe.get("profile_id") != execution_identity.PROVIDER_PROFILE_ID:
        raise TrainingAdmissionV3Error(f"source profile differs for {utc_day}")
    if probe.get("source_permissions") != execution_identity.SOURCE_PERMISSION_CONTRACT:
        raise TrainingAdmissionV3Error(f"source permissions differ for {utc_day}")
    return built


def _validate_engine(feature: Mapping[str, Any], *, utc_day: str) -> None:
    if feature.get("bulk_materialization_authorized") is not True:
        raise TrainingAdmissionV3Error(f"feature panel is not bulk-authorized: {utc_day}")
    engine = feature.get("engine")
    cache_engine = feature.get("cache_identity_payload", {}).get("engine")
    if not isinstance(engine, Mapping) or set(engine) != {
        "engine",
        "engine_abi",
        "feature_order_sha256",
        "lag_state_vocabulary",
        "python_bitwise_row_fingerprint_claimed",
        "python_precomputed_feature_values_accepted",
        "raw_inputs_only",
        "row_fingerprint",
    }:
        raise TrainingAdmissionV3Error(f"feature engine has an unexpected shape: {utc_day}")
    if engine.get("engine") != "cpp_batch" or engine != cache_engine:
        raise TrainingAdmissionV3Error(f"feature engine binding differs: {utc_day}")
    if engine.get("raw_inputs_only") is not True:
        raise TrainingAdmissionV3Error(f"feature engine did not consume raw inputs: {utc_day}")
    if engine.get("python_precomputed_feature_values_accepted") is not False:
        raise TrainingAdmissionV3Error(f"feature engine accepted Python values: {utc_day}")


def _validate_feature(
    feature_dir: Path,
    *,
    utc_day: str,
    built: source_specs.BuiltSourceSpec,
    pipeline_receipt: Mapping[str, Any],
    legacy_row: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], Path, Path, Path, str]:
    manifest_path = feature_dir / panels.MANIFEST_FILENAME
    panel_path = feature_dir / panels.PANEL_FILENAME
    if not manifest_path.is_file() or not panel_path.is_file():
        raise TrainingAdmissionV3Error(f"feature payload is incomplete: {feature_dir}")
    success = _validate_success(feature_dir, manifest_path)
    feature = _load_json(manifest_path, role="feature manifest")
    if feature.get("utc_day") != utc_day or feature.get("atomic_admission") is not True:
        raise TrainingAdmissionV3Error(f"feature day/admission identity differs: {utc_day}")
    _require_false(
        feature,
        ("labels_read", "predictions_read", "economic_outcomes_read", "training_authorized", "live_authorized"),
        role=f"feature panel {utc_day}",
    )
    _validate_engine(feature, utc_day=utc_day)
    panel_entry = feature.get("panel")
    if not isinstance(panel_entry, Mapping):
        raise TrainingAdmissionV3Error(f"feature panel identity is missing: {utc_day}")
    panel_sha = execution_identity.sha256_file(panel_path)
    if panel_entry.get("sha256") != panel_sha or panel_entry.get("rows") != ROWS_PER_DAY:
        raise TrainingAdmissionV3Error(f"feature panel denominator/hash differs: {utc_day}")
    parquet = pq.ParquetFile(panel_path)
    if parquet.metadata.num_rows != ROWS_PER_DAY:
        raise TrainingAdmissionV3Error(f"feature Parquet denominator differs: {utc_day}")
    if not parquet.schema_arrow.equals(panels.panel_arrow_schema(), check_metadata=False):
        raise TrainingAdmissionV3Error(f"feature Parquet schema differs: {utc_day}")
    panel_schema = feature.get("panel_schema")
    if not isinstance(panel_schema, Mapping):
        raise TrainingAdmissionV3Error(f"feature panel schema identity is missing: {utc_day}")
    if panel_schema.get("feature_count") != 173:
        raise TrainingAdmissionV3Error(f"feature count differs: {utc_day}")
    cache_payload = feature.get("cache_identity_payload")
    if not isinstance(cache_payload, Mapping):
        raise TrainingAdmissionV3Error(f"feature cache identity is missing: {utc_day}")
    if feature.get("cache_identity_sha256") != execution_identity.canonical_sha256(
        cache_payload
    ):
        raise TrainingAdmissionV3Error(f"feature cache identity differs: {utc_day}")
    if feature.get("source_bundle") != built.bundle.identity_payload():
        raise TrainingAdmissionV3Error(f"feature source bundle differs: {utc_day}")
    if cache_payload.get("bundle_identity_sha256") != built.bundle.identity_sha256():
        raise TrainingAdmissionV3Error(f"feature source identity differs: {utc_day}")
    source_probe_path = feature_dir / str(feature.get("source_probe", {}).get("path", ""))
    if not source_probe_path.is_file():
        raise TrainingAdmissionV3Error(f"feature source probe is missing: {utc_day}")
    source_probe = _load_json(source_probe_path, role="feature source probe")
    if feature.get("source_probe", {}).get("sha256") != execution_identity.sha256_file(
        source_probe_path
    ):
        raise TrainingAdmissionV3Error(f"feature source probe hash differs: {utc_day}")

    schema_version = feature.get("schema_version")
    if schema_version == panels.ARTIFACT_SCHEMA_VERSION:
        if legacy_row is not None:
            raise TrainingAdmissionV3Error(f"new v3 namespace day was marked legacy: {utc_day}")
        if feature.get("source_profile_id") != execution_identity.PROVIDER_PROFILE_ID:
            raise TrainingAdmissionV3Error(f"feature source profile differs: {utc_day}")
        if feature.get("source_permissions") != execution_identity.SOURCE_PERMISSION_CONTRACT:
            raise TrainingAdmissionV3Error(f"feature source permissions differ: {utc_day}")
        if source_probe != built.probe:
            raise TrainingAdmissionV3Error(f"feature source probe payload differs: {utc_day}")
        receipt_binding = feature.get("execution_receipt")
        if not isinstance(receipt_binding, Mapping):
            raise TrainingAdmissionV3Error(f"feature execution receipt is missing: {utc_day}")
        if receipt_binding.get("execution_identity_sha256") != pipeline_receipt.get(
            "execution_identity_sha256"
        ):
            raise TrainingAdmissionV3Error(f"feature execution receipt differs: {utc_day}")
        producer_kind = "v3_native_materialization"
    elif schema_version == legacy_attestation.LEGACY_FEATURE_SCHEMA:
        if legacy_row is None:
            raise TrainingAdmissionV3Error(f"legacy v2 day lacks successor attestation: {utc_day}")
        if utc_day == legacy_attestation.INCOMPLETE_TRANSIENT_DAY:
            raise TrainingAdmissionV3Error("transient 2025-11-30 cannot be re-admitted")
        expected_manifest = legacy_row.get("feature_manifest")
        expected_panel = legacy_row.get("feature_panel")
        if execution_identity.file_identity(manifest_path) != expected_manifest:
            raise TrainingAdmissionV3Error(f"legacy feature manifest differs: {utc_day}")
        if execution_identity.file_identity(panel_path) != expected_panel:
            raise TrainingAdmissionV3Error(f"legacy feature panel differs: {utc_day}")
        code = cache_payload.get("code")
        if not isinstance(code, Mapping):
            raise TrainingAdmissionV3Error(f"legacy feature code is missing: {utc_day}")
        projection = execution_identity.component_projection_from_legacy_panel_code(code)
        if projection != legacy_row.get("feature_payload_component_projection"):
            raise TrainingAdmissionV3Error(f"legacy F03 payload component differs: {utc_day}")
        if projection != execution_identity.current_legacy_panel_code_projection():
            raise TrainingAdmissionV3Error(
                f"legacy F03 payload component differs from current implementation: {utc_day}"
            )
        if source_probe != daily_sources.probe_source_bundle(built.bundle):
            raise TrainingAdmissionV3Error(f"legacy source probe payload differs: {utc_day}")
        producer_kind = "legacy_v2_successor_readmission"
    else:
        raise TrainingAdmissionV3Error(f"unsupported feature artifact schema: {utc_day}")
    return feature, source_probe, manifest_path, panel_path, success, producer_kind


def _validate_label(
    label_dir: Path,
    *,
    utc_day: str,
    built: source_specs.BuiltSourceSpec,
    feature_manifest_path: Path,
    feature_panel_path: Path,
    quote_config_path: Path,
    p3_v2_artifact_path: Path,
    legacy_row: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], Path, Path, Path]:
    manifest_path = label_dir / overlays.MANIFEST_FILENAME
    overlay_path = label_dir / overlays.OVERLAY_FILENAME
    if not manifest_path.is_file() or not overlay_path.is_file():
        raise TrainingAdmissionV3Error(f"label payload is incomplete: {label_dir}")
    success = _validate_success(label_dir, manifest_path)
    label = _load_json(manifest_path, role="label manifest")
    if label.get("utc_day") != utc_day or label.get("atomic_admission") is not True:
        raise TrainingAdmissionV3Error(f"label day/admission identity differs: {utc_day}")
    _require_false(
        label,
        (
            "predictions_read",
            "economic_outcomes_read",
            "training_performed",
            "training_authorized",
            "action_authorized",
            "live_authorized",
        ),
        role=f"label overlay {utc_day}",
    )
    schema_version = label.get("schema_version")
    allowed = {overlays.ARTIFACT_SCHEMA_VERSION, legacy_attestation.LEGACY_LABEL_SCHEMA}
    if schema_version not in allowed:
        raise TrainingAdmissionV3Error(f"unsupported label artifact schema: {utc_day}")
    if (legacy_row is None) != (schema_version == overlays.ARTIFACT_SCHEMA_VERSION):
        raise TrainingAdmissionV3Error(f"feature/label producer generation differs: {utc_day}")
    overlay_entry = label.get("overlay")
    if not isinstance(overlay_entry, Mapping):
        raise TrainingAdmissionV3Error(f"label overlay identity is missing: {utc_day}")
    overlay_sha = execution_identity.sha256_file(overlay_path)
    if overlay_entry.get("sha256") != overlay_sha or overlay_entry.get("rows") != ROWS_PER_DAY:
        raise TrainingAdmissionV3Error(f"label denominator/hash differs: {utc_day}")
    parquet = pq.ParquetFile(overlay_path)
    if parquet.metadata.num_rows != ROWS_PER_DAY:
        raise TrainingAdmissionV3Error(f"label Parquet denominator differs: {utc_day}")
    if not parquet.schema_arrow.equals(overlays.overlay_arrow_schema(), check_metadata=False):
        raise TrainingAdmissionV3Error(f"label Parquet schema differs: {utc_day}")
    feature_manifest_sha = execution_identity.sha256_file(feature_manifest_path)
    feature_panel_sha = execution_identity.sha256_file(feature_panel_path)
    if label.get("feature_panel_manifest_sha256") != feature_manifest_sha:
        raise TrainingAdmissionV3Error(f"label/feature manifest binding differs: {utc_day}")
    if label.get("feature_panel_sha256") != feature_panel_sha:
        raise TrainingAdmissionV3Error(f"label/feature payload binding differs: {utc_day}")
    if label.get("source_bundle_identity_sha256") != built.bundle.identity_sha256():
        raise TrainingAdmissionV3Error(f"label source identity differs: {utc_day}")
    if label.get("label_generator_sha256") != execution_identity.sha256_file(
        Path(label_generator.__file__).resolve()
    ):
        raise TrainingAdmissionV3Error(f"label generator identity differs: {utc_day}")
    config_identity = execution_identity.file_identity(quote_config_path)
    p3_identity = execution_identity.validate_explicit_p3_identity(
        quote_config_path,
        p3_v2_artifact_path,
    )
    if label.get("label_quote_config_sha256") != config_identity["sha256"]:
        raise TrainingAdmissionV3Error(f"label config identity differs: {utc_day}")
    if label.get("p3_v2_artifact_sha256") != p3_identity["sha256"]:
        raise TrainingAdmissionV3Error(f"label P3 identity differs: {utc_day}")
    if legacy_row is not None:
        if execution_identity.file_identity(manifest_path) != legacy_row.get("label_manifest"):
            raise TrainingAdmissionV3Error(f"legacy label manifest differs: {utc_day}")
        if execution_identity.file_identity(overlay_path) != legacy_row.get("label_overlay"):
            raise TrainingAdmissionV3Error(f"legacy label overlay differs: {utc_day}")
    elif label.get("config_resolved_p3") != p3_identity:
        raise TrainingAdmissionV3Error(f"label config-resolved P3 differs: {utc_day}")
    feature_join = pq.read_table(
        feature_panel_path,
        columns=list(overlays.JOIN_COLUMNS),
    )
    overlay_join = pq.read_table(overlay_path, columns=list(overlays.JOIN_COLUMNS))
    if not feature_join.equals(overlay_join):
        raise TrainingAdmissionV3Error(f"feature/label row order differs: {utc_day}")
    return label, manifest_path, overlay_path, success


def _durability_revalidation(paths: tuple[Path, ...], directories: tuple[Path, ...]) -> dict[str, Any]:
    for path in paths:
        _fsync_file(path)
    for directory in directories:
        _fsync_dir(directory)
    return {
        "fsync_revalidated": True,
        "file_count": len(paths),
        "directory_count": len(directories),
        "legacy_initial_materialization_durability_proven": False,
        "limitation": (
            "successor fsync proves current durable admission state; it does not reconstruct "
            "whether legacy v2 called fsync before its original rename"
        ),
    }


def admit_daily_training_payload(
    output_path: Path,
    *,
    utc_day: str,
    market_data_root: Path,
    source_spec_path: Path,
    source_probe_path: Path,
    feature_dir: Path,
    label_dir: Path,
    quote_config_path: Path,
    p3_v2_artifact_path: Path,
    pipeline_execution_receipt_path: Path,
    parity_successor_gate_path: Path,
    legacy_v2_attestation_path: Path | None = None,
) -> dict[str, Any]:
    """Validate, fsync, and bind one day into the v3 training namespace."""

    output = output_path.expanduser().resolve()
    if output.exists():
        return validate_daily_training_admission(output)
    pipeline_path = pipeline_execution_receipt_path.expanduser().resolve(strict=True)
    pipeline = execution_identity.validate_pipeline_execution_receipt(
        pipeline_path,
        require_materialization_workspace_stability=True,
    )
    gate_path = parity_successor_gate_path.expanduser().resolve(strict=True)
    gate = parity_gate.validate_training_parity_gate(gate_path)
    if gate.get("f03_component_semantics_sha256") != pipeline.get(
        "f03_component_semantics", {}
    ).get("identity_sha256"):
        raise TrainingAdmissionV3Error("pipeline and parity gate F03 components differ")
    if gate.get("native_build_receipt", {}).get("receipt_sha256") != pipeline.get(
        "native_build_receipt", {}
    ).get("receipt_sha256"):
        raise TrainingAdmissionV3Error("pipeline and parity gate build receipts differ")
    source_spec_path = source_spec_path.expanduser().resolve(strict=True)
    source_probe_path = source_probe_path.expanduser().resolve(strict=True)
    built = _profile_bound_source(
        utc_day=utc_day,
        market_data_root=market_data_root,
        source_spec_path=source_spec_path,
        source_probe_path=source_probe_path,
    )
    legacy_payload = None
    legacy_row = None
    legacy_path = None
    if legacy_v2_attestation_path is not None:
        legacy_path = legacy_v2_attestation_path.expanduser().resolve(strict=True)
        legacy_payload = legacy_attestation.validate_legacy_v2_attestation(legacy_path)
        legacy_row = legacy_attestation.candidate_by_day(legacy_payload, utc_day)
    feature_dir = feature_dir.expanduser().resolve(strict=True)
    label_dir = label_dir.expanduser().resolve(strict=True)
    feature, source_probe, feature_manifest, feature_panel, feature_success, producer_kind = (
        _validate_feature(
            feature_dir,
            utc_day=utc_day,
            built=built,
            pipeline_receipt=pipeline,
            legacy_row=legacy_row,
        )
    )
    if producer_kind.startswith("legacy") and legacy_row is None:
        raise TrainingAdmissionV3Error(f"legacy day is not an attested candidate: {utc_day}")
    label, label_manifest, label_overlay, label_success = _validate_label(
        label_dir,
        utc_day=utc_day,
        built=built,
        feature_manifest_path=feature_manifest,
        feature_panel_path=feature_panel,
        quote_config_path=quote_config_path.expanduser().resolve(strict=True),
        p3_v2_artifact_path=p3_v2_artifact_path.expanduser().resolve(strict=True),
        legacy_row=legacy_row,
    )
    durability = _durability_revalidation(
        (
            source_spec_path,
            source_probe_path,
            feature_manifest,
            feature_panel,
            feature_success,
            feature_dir / str(feature.get("source_probe", {}).get("path", "")),
            label_manifest,
            label_overlay,
            label_success,
        ),
        (source_spec_path.parent, feature_dir, label_dir),
    )
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "utc_day": utc_day,
        "producer_kind": producer_kind,
        "source_profile_id": execution_identity.PROVIDER_PROFILE_ID,
        "source_permissions": execution_identity.SOURCE_PERMISSION_CONTRACT,
        "source_bundle_identity_sha256": built.bundle.identity_sha256(),
        "source_spec": execution_identity.file_identity(source_spec_path),
        "source_probe": execution_identity.file_identity(source_probe_path),
        "source_probe_identity_sha256": execution_identity.canonical_sha256(source_probe),
        "feature_manifest": execution_identity.file_identity(feature_manifest),
        "feature_panel": execution_identity.file_identity(feature_panel),
        "feature_success": execution_identity.file_identity(feature_success),
        "label_manifest": execution_identity.file_identity(label_manifest),
        "label_overlay": execution_identity.file_identity(label_overlay),
        "label_success": execution_identity.file_identity(label_success),
        "feature_rows": ROWS_PER_DAY,
        "label_rows": ROWS_PER_DAY,
        "feature_count": 173,
        "head_count": 13,
        "quote_config": execution_identity.file_identity(
            quote_config_path.expanduser().resolve(strict=True)
        ),
        "p3_v2_artifact": execution_identity.validate_explicit_p3_identity(
            quote_config_path,
            p3_v2_artifact_path,
        ),
        "pipeline_execution_receipt": {
            **execution_identity.file_identity(pipeline_path),
            "execution_identity_sha256": pipeline["execution_identity_sha256"],
        },
        "parity_successor_gate": {
            **execution_identity.file_identity(gate_path),
            "parity_gate_identity_sha256": gate["parity_gate_identity_sha256"],
        },
        "f03_component_semantics_sha256": gate["f03_component_semantics_sha256"],
        "legacy_v2_attestation": (
            None
            if legacy_path is None or legacy_row is None or legacy_payload is None
            else {
                **execution_identity.file_identity(legacy_path),
                "attestation_identity_sha256": legacy_payload[
                    "attestation_identity_sha256"
                ],
            }
        ),
        "durability": durability,
        "training_input_authorized": True,
        "model_training_executed": False,
        "predictions_read": False,
        "economic_outcomes_read": False,
        "queue_authority": False,
        "order_lifecycle_authority": False,
        "fill_path_authority": False,
        "pnl_authority": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    payload = {
        **unsigned,
        "admission_identity_sha256": execution_identity.canonical_sha256(unsigned),
    }
    execution_identity.write_json_fsync(output, payload)
    return validate_daily_training_admission(output)


def _validate_binding(binding: Any, *, role: str) -> Path:
    if not isinstance(binding, Mapping):
        raise TrainingAdmissionV3Error(f"admission lacks {role} binding")
    path = Path(str(binding.get("path", ""))).expanduser().resolve(strict=True)
    expected_file_identity = {
        key: binding.get(key) for key in ("path", "sha256", "size_bytes")
    }
    if execution_identity.file_identity(path) != expected_file_identity:
        raise TrainingAdmissionV3Error(f"admitted {role} drifted: {path}")
    return path


def validate_daily_training_admission(path: Path) -> dict[str, Any]:
    """Revalidate a v3 day without trusting legacy manifests directly."""

    admission_path = path.expanduser().resolve(strict=True)
    payload = _load_json(admission_path, role="daily training admission")
    if payload.get("schema_version") != SCHEMA_VERSION or payload.get("status") != STATUS:
        raise TrainingAdmissionV3Error("unsupported daily training admission")
    unsigned = dict(payload)
    identity = unsigned.pop("admission_identity_sha256", None)
    if identity != execution_identity.canonical_sha256(unsigned):
        raise TrainingAdmissionV3Error("daily training admission identity mismatch")
    if payload.get("training_input_authorized") is not True:
        raise TrainingAdmissionV3Error("daily training input is not authorized")
    if payload.get("source_profile_id") != execution_identity.PROVIDER_PROFILE_ID:
        raise TrainingAdmissionV3Error("daily training source profile differs")
    if payload.get("source_permissions") != execution_identity.SOURCE_PERMISSION_CONTRACT:
        raise TrainingAdmissionV3Error("daily training source permissions differ")
    _require_false(
        payload,
        (
            "model_training_executed",
            "predictions_read",
            "economic_outcomes_read",
            "queue_authority",
            "order_lifecycle_authority",
            "fill_path_authority",
            "pnl_authority",
            "action_authorized",
            "live_authorized",
        ),
        role="daily training admission",
    )
    for role in (
        "source_spec",
        "source_probe",
        "feature_manifest",
        "feature_panel",
        "feature_success",
        "label_manifest",
        "label_overlay",
        "label_success",
        "quote_config",
        "p3_v2_artifact",
    ):
        _validate_binding(payload.get(role), role=role)
    pipeline_binding = payload.get("pipeline_execution_receipt")
    gate_binding = payload.get("parity_successor_gate")
    pipeline_path = _validate_binding(pipeline_binding, role="pipeline execution receipt")
    gate_path = _validate_binding(gate_binding, role="parity successor gate")
    pipeline = execution_identity.validate_pipeline_execution_receipt(
        pipeline_path,
        require_materialization_workspace_stability=False,
    )
    gate = parity_gate.validate_training_parity_gate(gate_path)
    if pipeline_binding.get("execution_identity_sha256") != pipeline.get(
        "execution_identity_sha256"
    ):
        raise TrainingAdmissionV3Error("pipeline receipt identity drifted")
    if gate_binding.get("parity_gate_identity_sha256") != gate.get(
        "parity_gate_identity_sha256"
    ):
        raise TrainingAdmissionV3Error("parity successor gate identity drifted")
    if payload.get("f03_component_semantics_sha256") != gate.get(
        "f03_component_semantics_sha256"
    ):
        raise TrainingAdmissionV3Error("daily F03 component differs from parity gate")
    config_path = Path(str(payload["quote_config"]["path"]))
    p3_path = Path(str(payload["p3_v2_artifact"]["path"]))
    if execution_identity.validate_explicit_p3_identity(config_path, p3_path) != payload.get(
        "p3_v2_artifact"
    ):
        raise TrainingAdmissionV3Error("daily config/P3 binding drifted")
    legacy_binding = payload.get("legacy_v2_attestation")
    if legacy_binding is not None:
        legacy_path = _validate_binding(legacy_binding, role="legacy v2 attestation")
        legacy = legacy_attestation.validate_legacy_v2_attestation(legacy_path)
        if legacy_binding.get("attestation_identity_sha256") != legacy.get(
            "attestation_identity_sha256"
        ):
            raise TrainingAdmissionV3Error("legacy v2 attestation identity drifted")
        if legacy_attestation.candidate_by_day(legacy, str(payload["utc_day"])) is None:
            raise TrainingAdmissionV3Error("legacy day is absent from the reuse candidate set")
    return payload
