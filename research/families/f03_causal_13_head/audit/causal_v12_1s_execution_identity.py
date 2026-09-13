#!/usr/bin/env python3
"""Immutable native/runtime identities for the F03 canonical-1s successor."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
import sysconfig
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "causal_v12_1s_execution_identity.v3"
NATIVE_BUILD_RECEIPT_SCHEMA_VERSION = "causal_v12_1s_native_build_receipt.v1"
PIPELINE_RECEIPT_SCHEMA_VERSION = "causal_v12_1s_pipeline_execution_receipt.v3"
PROVIDER_PROFILE_ID = "provider_normalized_v1"
V3_CACHE_NAMESPACE = "f03_causal_v12_1s_metrics_source_ready_v3"
ORDER_LIFECYCLE_JOURNAL_ABI_ATTR = "ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION"
EXPECTED_ORDER_LIFECYCLE_JOURNAL_ABI = (
    "order_lifecycle_journal_v2_cpp_event_stream_mirror.v2"
)

REPO_ROOT = Path(__file__).resolve().parents[4]
F03_CPP_SOURCE = (
    REPO_ROOT / "research/families/f03_causal_13_head/cpp/causal_v12_1s_features.cpp"
)
F03_CPP_HEADER = F03_CPP_SOURCE.with_suffix(".hpp")
BINDINGS_MODULE_SOURCE = REPO_ROOT / "cpp/narrowgate_cpp/bindings_module.cpp"

SOURCE_PERMISSION_CONTRACT = {
    "feature_prediction_training_authority": True,
    "queue_authority": False,
    "order_lifecycle_authority": False,
    "fill_path_authority": False,
    "pnl_authority": False,
    "economic_outcomes_read": False,
}

_F03_SEMANTIC_PYTHON_PATHS = (
    "research/families/f03_causal_13_head/audit/causal_v12_1s_cpp_batch.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_daily_sources.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_feature_generator.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_full_schema.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_panel_materializer.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_schema.py",
)

_PIPELINE_PYTHON_PATHS = (
    *_F03_SEMANTIC_PYTHON_PATHS,
    "research/families/f03_causal_13_head/audit/causal_v12_1s_execution_identity.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_legacy_v2_attestation.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_label_generator.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_label_overlay_materializer.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_orico_source_spec.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_parity_successor_gate.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_real_day_cpp_parity.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_training_admission_v3.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_training_contract.py",
    "research/families/f03_causal_13_head/audit/causal_v12_1s_training_panel_pipeline.py",
)


class ExecutionIdentityError(ValueError):
    """Raised when native, configuration, profile, or process identity drifts."""


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_identity(path: Path, *, relative_to_repo: bool = False) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    display = str(resolved)
    if relative_to_repo:
        try:
            display = str(resolved.relative_to(REPO_ROOT))
        except ValueError as exc:
            raise ExecutionIdentityError(f"identity path escapes repository: {resolved}") from exc
    return {
        "path": display,
        "sha256": sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _python_file_identities(paths: Sequence[str]) -> list[dict[str, Any]]:
    return [file_identity(REPO_ROOT / value, relative_to_repo=True) for value in paths]


def python_abi_identity() -> dict[str, Any]:
    return {
        "executable": str(Path(sys.executable).resolve()),
        "version": platform.python_version(),
        "implementation": sys.implementation.name,
        "cache_tag": sys.implementation.cache_tag,
        "soabi": sysconfig.get_config_var("SOABI"),
        "extension_suffix": sysconfig.get_config_var("EXT_SUFFIX"),
        "platform": sysconfig.get_platform(),
        "machine": platform.machine(),
    }


def _load_cpp() -> Any:
    try:
        import narrowgate_cpp as cpp
    except ImportError as exc:
        raise ExecutionIdentityError("narrowgate_cpp extension is unavailable") from exc
    return cpp


def loaded_extension_identity(cpp: Any | None = None) -> dict[str, Any]:
    module = _load_cpp() if cpp is None else cpp
    path = Path(str(module.__file__)).expanduser().resolve(strict=True)
    return {
        **file_identity(path),
        "module": "narrowgate_cpp",
        "python_abi": python_abi_identity(),
    }


def reported_extension_abis(cpp: Any | None = None) -> dict[str, str]:
    """Capture cross-family ABI markers from the exact loaded native module.

    These markers are build provenance, not F03 feature-cache semantics.  The
    loaded extension SHA remains the decisive binary identity for F03.
    """

    module = _load_cpp() if cpp is None else cpp
    lifecycle_abi = str(getattr(module, ORDER_LIFECYCLE_JOURNAL_ABI_ATTR, ""))
    if lifecycle_abi != EXPECTED_ORDER_LIFECYCLE_JOURNAL_ABI:
        raise ExecutionIdentityError(
            "loaded extension reports an unsupported order-lifecycle journal ABI"
        )
    return {"order_lifecycle_journal_v2": lifecycle_abi}


def f03_exported_abi(cpp: Any | None = None) -> dict[str, Any]:
    module = _load_cpp() if cpp is None else cpp
    required = (
        "F03_CAUSAL_V12_1S_FEATURE_ABI_VERSION",
        "F03_CAUSAL_V12_1S_FEATURE_NAMES",
        "F03_CAUSAL_V12_1S_FEATURE_ORDER_SHA256",
        "F03_CAUSAL_V12_1S_BATCH_ABI_VERSION",
        "F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY",
    )
    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise ExecutionIdentityError(f"loaded extension lacks F03 ABI exports: {missing}")
    names = tuple(str(value) for value in module.F03_CAUSAL_V12_1S_FEATURE_NAMES)
    return {
        "feature_abi": str(module.F03_CAUSAL_V12_1S_FEATURE_ABI_VERSION),
        "batch_abi": str(module.F03_CAUSAL_V12_1S_BATCH_ABI_VERSION),
        "feature_names_sha256": canonical_sha256(list(names)),
        "feature_order_sha256": str(module.F03_CAUSAL_V12_1S_FEATURE_ORDER_SHA256),
        "feature_count": len(names),
        "lag_state_vocabulary": [
            str(value) for value in module.F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY
        ],
    }


def _native_build_input_paths() -> tuple[Path, ...]:
    paths = [REPO_ROOT / "cpp/CMakeLists.txt", REPO_ROOT / "cpp/pyproject.toml"]
    for relative in (
        "cpp/narrowgate_cpp",
        "research/families/f03_causal_13_head/cpp",
        "research/families/f06_placement_fill_cif/cpp",
        "research/families/f07_active_order_continuation/cpp",
    ):
        directory = REPO_ROOT / relative
        paths.extend(directory.glob("*.cpp"))
        paths.extend(directory.glob("*.hpp"))
    return tuple(sorted({path.resolve(strict=True) for path in paths}, key=str))


def full_native_build_provenance(cpp: Any | None = None) -> dict[str, Any]:
    inputs = [file_identity(path, relative_to_repo=True) for path in _native_build_input_paths()]
    payload = {
        "build_inputs": inputs,
        "loaded_extension": loaded_extension_identity(cpp),
        "reported_extension_abis": reported_extension_abis(cpp),
        "python_abi": python_abi_identity(),
        "capture_role": "post_build_identity_attestation",
    }
    return {**payload, "identity_sha256": canonical_sha256(payload)}


def f03_component_semantics(cpp: Any | None = None) -> dict[str, Any]:
    module = _load_cpp() if cpp is None else cpp
    payload = {
        "loaded_extension": loaded_extension_identity(module),
        "f03_cpp_source": file_identity(F03_CPP_SOURCE, relative_to_repo=True),
        "f03_cpp_header": file_identity(F03_CPP_HEADER, relative_to_repo=True),
        "f03_exported_abi": f03_exported_abi(module),
        "f03_python_components": _python_file_identities(_F03_SEMANTIC_PYTHON_PATHS),
    }
    return {**payload, "identity_sha256": canonical_sha256(payload)}


def current_native_build_receipt_payload(cpp: Any | None = None) -> dict[str, Any]:
    module = _load_cpp() if cpp is None else cpp
    component = f03_component_semantics(module)
    provenance = full_native_build_provenance(module)
    return {
        "schema_version": NATIVE_BUILD_RECEIPT_SCHEMA_VERSION,
        "status": "post_build_identity_captured_not_training_authority",
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "f03_component_semantics": component,
        "full_build_provenance": provenance,
        "full_bindings_source_role": "provenance_not_f03_cache_semantics",
        "full_bindings_source": file_identity(
            BINDINGS_MODULE_SOURCE, relative_to_repo=True
        ),
        "training_authorized": False,
        "economic_outcomes_read": False,
    }


def write_json_fsync(path: Path, payload: Mapping[str, Any]) -> None:
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


def freeze_native_build_receipt(path: Path, cpp: Any | None = None) -> dict[str, Any]:
    output = path.expanduser().resolve()
    if output.exists():
        return validate_native_build_receipt(
            output,
            cpp=cpp,
            require_full_build_input_match=True,
        )
    payload = current_native_build_receipt_payload(cpp)
    payload["receipt_sha256"] = canonical_sha256(payload)
    write_json_fsync(output, payload)
    return payload


def validate_native_build_receipt(
    path: Path,
    cpp: Any | None = None,
    *,
    require_full_build_input_match: bool = False,
) -> dict[str, Any]:
    receipt_path = path.expanduser().resolve(strict=True)
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != NATIVE_BUILD_RECEIPT_SCHEMA_VERSION:
        raise ExecutionIdentityError("unsupported native build receipt")
    receipt_sha = payload.get("receipt_sha256")
    unsigned = dict(payload)
    unsigned.pop("receipt_sha256", None)
    if receipt_sha != canonical_sha256(unsigned):
        raise ExecutionIdentityError("native build receipt canonical SHA256 mismatch")
    current_component = f03_component_semantics(cpp)
    if payload.get("f03_component_semantics") != current_component:
        raise ExecutionIdentityError("F03 component semantics drifted from build receipt")
    provenance = payload.get("full_build_provenance")
    if not isinstance(provenance, Mapping):
        raise ExecutionIdentityError("native build receipt lacks full build provenance")
    provenance_unsigned = dict(provenance)
    provenance_identity = provenance_unsigned.pop("identity_sha256", None)
    if provenance_identity != canonical_sha256(provenance_unsigned):
        raise ExecutionIdentityError("native build provenance identity is invalid")
    if require_full_build_input_match:
        current_provenance = full_native_build_provenance(cpp)
        if provenance != current_provenance:
            raise ExecutionIdentityError("native build inputs drifted from receipt")
        if payload.get("full_bindings_source") != file_identity(
            BINDINGS_MODULE_SOURCE, relative_to_repo=True
        ):
            raise ExecutionIdentityError(
                "bindings module provenance drifted from build receipt"
            )
    return payload


def resolve_config_p3(quote_config_path: Path) -> dict[str, Any]:
    config_path = quote_config_path.expanduser().resolve(strict=True)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    model_dir_raw = str((raw.get("ml") or {}).get("model_dir") or "").strip()
    if not model_dir_raw:
        raise ExecutionIdentityError("quote config must explicitly set ml.model_dir")
    model_dir = Path(model_dir_raw).expanduser()
    if not model_dir.is_absolute():
        model_dir = REPO_ROOT / model_dir
    p3_path = (model_dir / "fill_prob_params.json").resolve(strict=True)
    return file_identity(p3_path)


def validate_explicit_p3_identity(
    quote_config_path: Path,
    explicit_p3_path: Path,
) -> dict[str, Any]:
    explicit = file_identity(explicit_p3_path)
    resolved = resolve_config_p3(quote_config_path)
    if explicit != resolved:
        raise ExecutionIdentityError(
            "explicit P3 artifact differs from config-resolved ml.model_dir/fill_prob_params.json"
        )
    return explicit


def pipeline_code_identity() -> dict[str, Any]:
    payload = {"files": _python_file_identities(_PIPELINE_PYTHON_PATHS)}
    return {**payload, "identity_sha256": canonical_sha256(payload)}


def freeze_pipeline_execution_receipt(
    path: Path,
    *,
    native_build_receipt_path: Path,
    quote_config_path: Path,
    explicit_p3_path: Path,
    training_design_path: Path,
    profile_id: str,
    cache_root: Path,
    workers: int,
    legacy_run_attestation_path: Path | None = None,
) -> dict[str, Any]:
    output = path.expanduser().resolve()
    if output.exists():
        existing = validate_pipeline_execution_receipt(output)
        expected_static = {
            "profile_id": profile_id,
            "cache_namespace": cache_root.expanduser().resolve().name,
            "workers": workers,
        }
        if any(existing.get(key) != value for key, value in expected_static.items()):
            raise FileExistsError(f"pipeline receipt invocation differs: {output}")
        return existing
    if profile_id != PROVIDER_PROFILE_ID:
        raise ExecutionIdentityError("F03 training panels require provider_normalized_v1")
    if cache_root.expanduser().resolve().name != V3_CACHE_NAMESPACE:
        raise ExecutionIdentityError(f"successor cache root must be named {V3_CACHE_NAMESPACE}")
    if workers < 1 or workers > 4:
        raise ExecutionIdentityError("workers must be fixed in [1,4]")
    build_path = native_build_receipt_path.expanduser().resolve(strict=True)
    build = validate_native_build_receipt(
        build_path,
        require_full_build_input_match=True,
    )
    config = file_identity(quote_config_path)
    p3 = validate_explicit_p3_identity(quote_config_path, explicit_p3_path)
    design = file_identity(training_design_path)
    legacy = None
    if legacy_run_attestation_path is not None:
        legacy_path = legacy_run_attestation_path.expanduser().resolve(strict=True)
        legacy = file_identity(legacy_path)
    unsigned = {
        "schema_version": PIPELINE_RECEIPT_SCHEMA_VERSION,
        "status": "immutable_parent_owned_materialization_identity",
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "owner_pid": os.getpid(),
        "profile_id": profile_id,
        "source_permissions": SOURCE_PERMISSION_CONTRACT,
        "cache_namespace": V3_CACHE_NAMESPACE,
        "workers": workers,
        "pipeline_code": pipeline_code_identity(),
        "native_build_receipt": {
            **file_identity(build_path),
            "receipt_sha256": build["receipt_sha256"],
            "f03_component_semantics_sha256": build["f03_component_semantics"][
                "identity_sha256"
            ],
        },
        "f03_component_semantics": build["f03_component_semantics"],
        "quote_config": config,
        "p3_v2_artifact": p3,
        "training_design": design,
        "legacy_v2_run_attestation": legacy,
        "queue_authority": False,
        "order_lifecycle_authority": False,
        "fill_path_authority": False,
        "pnl_authority": False,
        "predictions_read": False,
        "economic_outcomes_read": False,
        "training_authorized": False,
    }
    payload = {**unsigned, "execution_identity_sha256": canonical_sha256(unsigned)}
    write_json_fsync(output, payload)
    return payload


def validate_pipeline_execution_receipt(
    path: Path,
    *,
    require_owner_pid: int | None = None,
    require_materialization_workspace_stability: bool = True,
) -> dict[str, Any]:
    receipt_path = path.expanduser().resolve(strict=True)
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != PIPELINE_RECEIPT_SCHEMA_VERSION:
        raise ExecutionIdentityError("unsupported pipeline execution receipt")
    identity = payload.get("execution_identity_sha256")
    unsigned = dict(payload)
    unsigned.pop("execution_identity_sha256", None)
    if identity != canonical_sha256(unsigned):
        raise ExecutionIdentityError("pipeline execution receipt SHA256 mismatch")
    if require_owner_pid is not None and payload.get("owner_pid") != require_owner_pid:
        raise ExecutionIdentityError("pipeline execution receipt owner PID differs")
    if payload.get("profile_id") != PROVIDER_PROFILE_ID:
        raise ExecutionIdentityError("pipeline receipt does not bind provider_normalized_v1")
    if payload.get("source_permissions") != SOURCE_PERMISSION_CONTRACT:
        raise ExecutionIdentityError("pipeline source permission contract drifted")
    if payload.get("cache_namespace") != V3_CACHE_NAMESPACE:
        raise ExecutionIdentityError("pipeline cache namespace drifted")
    if payload.get("pipeline_code") != pipeline_code_identity():
        raise ExecutionIdentityError("pipeline Python code drifted during materialization")
    build_binding = payload.get("native_build_receipt")
    if not isinstance(build_binding, Mapping):
        raise ExecutionIdentityError("pipeline receipt lacks native build receipt")
    build_path = Path(str(build_binding.get("path", ""))).expanduser().resolve(strict=True)
    if file_identity(build_path) != {
        key: build_binding[key] for key in ("path", "sha256", "size_bytes")
    }:
        raise ExecutionIdentityError("native build receipt file identity drifted")
    build = validate_native_build_receipt(
        build_path,
        require_full_build_input_match=require_materialization_workspace_stability,
    )
    if payload.get("f03_component_semantics") != build.get("f03_component_semantics"):
        raise ExecutionIdentityError("pipeline F03 component identity drifted")
    if build_binding.get("receipt_sha256") != build.get("receipt_sha256"):
        raise ExecutionIdentityError("pipeline native receipt identity drifted")
    config = payload.get("quote_config")
    p3 = payload.get("p3_v2_artifact")
    if not isinstance(config, Mapping) or not isinstance(p3, Mapping):
        raise ExecutionIdentityError("pipeline config/P3 identity is missing")
    config_path = Path(str(config.get("path", ""))).expanduser().resolve(strict=True)
    p3_path = Path(str(p3.get("path", ""))).expanduser().resolve(strict=True)
    if file_identity(config_path) != dict(config):
        raise ExecutionIdentityError("quote config drifted during materialization")
    if validate_explicit_p3_identity(config_path, p3_path) != dict(p3):
        raise ExecutionIdentityError("P3 identity drifted during materialization")
    return payload


def component_projection_from_legacy_panel_code(code: Mapping[str, Any]) -> dict[str, Any]:
    """Project old code identity onto payload-producing F03 semantic components.

    The wrapper materializer and registered binding source set remain provenance.
    Their hashes are intentionally excluded because successor admission proves
    the stored payload against the current component with complete-day parity.
    """

    required = {
        "cpp_batch_bridge_sha256",
        "cpp_feature_header_sha256",
        "cpp_feature_source_sha256",
        "daily_source_reader_sha256",
        "full_feature_generator_sha256",
        "trainable_schema_sha256",
    }
    missing = sorted(required - set(code))
    if missing:
        raise ExecutionIdentityError(f"legacy panel lacks F03 component hashes: {missing}")
    return {key: str(code[key]) for key in sorted(required)}


def current_legacy_panel_code_projection() -> dict[str, Any]:
    paths = {
        "cpp_batch_bridge_sha256": REPO_ROOT
        / "research/families/f03_causal_13_head/audit/causal_v12_1s_cpp_batch.py",
        "cpp_feature_header_sha256": F03_CPP_HEADER,
        "cpp_feature_source_sha256": F03_CPP_SOURCE,
        "daily_source_reader_sha256": REPO_ROOT
        / "research/families/f03_causal_13_head/audit/causal_v12_1s_daily_sources.py",
        "full_feature_generator_sha256": REPO_ROOT
        / "research/families/f03_causal_13_head/audit/causal_v12_1s_full_schema.py",
        "trainable_schema_sha256": REPO_ROOT
        / "research/families/f03_causal_13_head/audit/causal_v12_1s_schema.py",
    }
    return {name: sha256_file(path) for name, path in sorted(paths.items())}
