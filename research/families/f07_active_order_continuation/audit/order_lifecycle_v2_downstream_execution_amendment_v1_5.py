#!/usr/bin/env python3
"""Freeze and validate the outcome-blind F07 downstream execution chain."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_replay_emitter as emitter,
)

ROOT = Path(__file__).resolve().parents[4]
IDENTITY = "f07_order_lifecycle_v2_downstream_execution_amendment_v1_5"
SCHEMA_VERSION = "f07_order_lifecycle_v2_downstream_execution_amendment.v1_5"
STATUS = "frozen_mechanics_only_execution_chain_no_economic_or_live_authority"

LOCKSTEP_IDENTITY = (
    "f07_order_lifecycle_v2_40day_cpp_event_lockstep_v1_5_provenance_v1"
)
LOCKSTEP_SCHEMA_VERSION = (
    "f07_order_lifecycle_v2_40day_cpp_event_lockstep_report.v1_5_provenance_v1"
)
TRAINING_IDENTITY = (
    "active_order_lifecycle_competing_risk_cif_100ms_v1_5_provenance_v1"
)
TRAINING_SCHEMA_VERSION = (
    "active_order_lifecycle_competing_risk_cif_artifact.v1_5_provenance_v1"
)
TRAINING_REPORT_SCHEMA_VERSION = (
    "active_order_lifecycle_competing_risk_cif_training_report.v1_5_provenance_v1"
)
PARITY_IDENTITY = "active_order_lifecycle_cif_cpp_inference_parity_v1_5_provenance_v1"
PARITY_SCHEMA_VERSION = (
    "active_order_lifecycle_cif_cpp_inference_parity_report.v1_5_provenance_v1"
)

AMENDMENT_SCOPE = {
    "mechanics_only": True,
    "economic_outcomes_read": False,
    "markout_read": False,
    "q90_adverse_score_available": False,
}
DENIED_PERMISSIONS = {
    "economic_evaluation": False,
    "q90_action": False,
    "action": False,
    "live_transport": False,
    "live_deployment": False,
}
LOCKSTEP_SCOPE = {
    "mechanics_only": True,
    "economic_outcomes_read": False,
    "q90_action_authorized": False,
    "live_transport_executed": False,
}
LOCKSTEP_PERMISSIONS = {"cif_training": True, **DENIED_PERMISSIONS}
TRAINING_SCOPE = {
    "mechanics_only": True,
    "economic_outcomes_read": False,
    "markout_read": False,
    "calendar_and_risk_time_preserved": True,
}
TRAINING_PERMISSIONS = {"cif_cpp_parity": True, **DENIED_PERMISSIONS}
PARITY_SCOPE = {
    "mechanics_only": True,
    "economic_outcomes_read": False,
    "q90_adverse_score_available": False,
    "live_transport_executed": False,
}
PARITY_PERMISSIONS = dict(DENIED_PERMISSIONS)

_IMPLEMENTATION_PATHS = {
    "amendment_builder_validator": (
        "research/families/f07_active_order_continuation/audit/"
        "order_lifecycle_v2_downstream_execution_amendment_v1_5.py"
    ),
    "lockstep_wrapper": (
        "research/families/f07_active_order_continuation/audit/"
        "order_lifecycle_v2_40day_cpp_lockstep.py"
    ),
    "training_implementation": (
        "research/families/f07_active_order_continuation/audit/"
        "active_order_lifecycle_cif_100ms_training_v1_5.py"
    ),
    "parity_implementation": (
        "research/families/f07_active_order_continuation/audit/"
        "active_order_lifecycle_cif_cpp_parity_v1_5.py"
    ),
    "python_cif_inference": (
        "research/families/f07_active_order_continuation/audit/"
        "active_order_competing_risk_cif_inference_v1_1.py"
    ),
    "python_cif_base": (
        "research/families/f07_active_order_continuation/audit/"
        "active_order_competing_risk_cif.py"
    ),
    "cpp_cif_source": (
        "research/families/f07_active_order_continuation/cpp/"
        "active_order_competing_risk_cif.cpp"
    ),
    "cpp_cif_header": (
        "research/families/f07_active_order_continuation/cpp/"
        "active_order_competing_risk_cif.hpp"
    ),
    "cpp_pybind_source": "cpp/narrowgate_cpp/bindings.cpp",
    "cpp_build_contract": "cpp/CMakeLists.txt",
}
_PANEL_KEYS = {
    "schema_version",
    "identity",
    "status",
    "generated_at_utc",
    "plan_sha256",
    "ordered_utc_days",
    "day_manifests",
    "mechanics_totals",
    "scope",
    "permissions",
    "canonical_manifest_sha256",
}
_PANEL_COUNTER_KEYS = {
    "lifecycle_count",
    "event_count",
    "terminal_observation_count",
    "cancel_reject_count",
    "cancel_reject_to_active_count",
    "cancel_reject_to_partially_filled_count",
    "sub_lot_partial_remaining_count",
    "terminal_positive_remainder_count",
    "exact_native_lifecycle_count",
    "native_queue_censored_lifecycle_count",
}
_LOCKSTEP_KEYS = {
    "schema_version",
    "identity",
    "status",
    "generated_at_utc",
    "downstream_execution_amendment",
    "plan",
    "panel_manifest",
    "plan_sha256",
    "global_execution_identity_sha256",
    "cpp_abi_version",
    "cpp_module",
    "counts",
    "mismatch_counts",
    "post_terminal_violation_counts",
    "days",
    "gates",
    "formal_40day_lockstep_passed",
    "scope",
    "permissions",
    "canonical_report_sha256",
}
_LOCKSTEP_GATE_KEYS = {
    "forty_days_present",
    "all_day_event_lockstep",
    "zero_python_cpp_mismatch",
    "zero_post_terminal_risk_or_queue_reuse",
    "cpp_module_hash_bound",
    "exact_native_spells_present",
    "panel_canonical_hash_bound",
    "runtime_identity_hash_bound",
    "downstream_implementation_hashes_bound",
}
_LOCKSTEP_DAY_KEYS = {
    "day",
    "day_manifest_sha256",
    "journal_row_count",
    "lockstep_report_sha256",
    "mechanics_lockstep_passed",
    "mismatch_counts",
    "post_terminal_safety",
}
_TRAINING_ARTIFACT_KEYS = {
    "schema_version",
    "identity",
    "status",
    "trained_at_utc",
    "downstream_execution_amendment",
    "plan_sha256",
    "input_artifacts",
    "grid",
    "cause_contract",
    "conditioning",
    "training_counts",
    "parent_rates",
    "cells",
    "scope",
    "permissions",
    "canonical_artifact_sha256",
}
_TRAINING_REPORT_KEYS = {
    "schema_version",
    "identity",
    "status",
    "generated_at_utc",
    "downstream_execution_amendment",
    "model_artifact",
    "input_artifacts",
    "plan_sha256",
    "training_counts",
    "day_support",
    "gates",
    "scope",
    "permissions",
    "canonical_report_sha256",
}
_TRAINING_GATE_KEYS = {
    "forty_admitted_days",
    "python_cpp_event_lockstep_passed",
    "exact_native_risk_spells_present",
    "exact_native_denominator_parity",
    "native_queue_censor_denominator_parity",
    "terminal_causes_do_not_exceed_eligible_spells",
    "positive_risk_exposure",
    "finite_nonnegative_rates",
    "economic_outcomes_not_read",
    "downstream_implementation_hashes_bound",
    "lockstep_report_canonical_hash_bound",
}


class DownstreamProvenanceError(RuntimeError):
    """Fail-closed downstream provenance or permission error."""


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_document_sha256(
    payload: Mapping[str, object],
    identity_field: str,
) -> str:
    body = dict(payload)
    body.pop(identity_field, None)
    return canonical_sha256(body)


def read_json(path: Path, *, label: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DownstreamProvenanceError(f"invalid {label}: {resolved}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DownstreamProvenanceError(f"{label} must be a JSON object")
    return payload


def artifact_identity(path: Path) -> dict[str, object]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise DownstreamProvenanceError(f"bound artifact is missing: {resolved}")
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": file_sha256(resolved),
    }


def _resolve_artifact(
    value: Mapping[str, object],
    *,
    label: str,
    base: Path = ROOT,
    expected_path: Path | None = None,
) -> Path:
    required = {"path", "size_bytes", "sha256"}
    if not required.issubset(value):
        raise DownstreamProvenanceError(f"{label} lacks path/size/SHA256")
    path = Path(str(value["path"])).expanduser()
    if not path.is_absolute():
        path = base / path
    path = path.resolve()
    if expected_path is not None and path != expected_path.expanduser().resolve():
        raise DownstreamProvenanceError(f"{label} path differs")
    if not path.is_file():
        raise DownstreamProvenanceError(f"{label} is missing: {path}")
    if path.stat().st_size != int(value["size_bytes"]):
        raise DownstreamProvenanceError(f"{label} size differs")
    if file_sha256(path) != str(value["sha256"]):
        raise DownstreamProvenanceError(f"{label} SHA256 differs")
    return path


def amendment_reference(path: Path, amendment: Mapping[str, object]) -> dict[str, object]:
    return {
        **artifact_identity(path),
        "canonical_amendment_sha256": amendment["canonical_amendment_sha256"],
    }


def _validate_amendment_reference(
    value: Mapping[str, object],
    *,
    amendment_path: Path,
    amendment: Mapping[str, object],
) -> None:
    if set(value) != {
        "path",
        "size_bytes",
        "sha256",
        "canonical_amendment_sha256",
    }:
        raise DownstreamProvenanceError("downstream amendment reference schema differs")
    _resolve_artifact(
        value,
        label="downstream execution amendment",
        expected_path=amendment_path,
    )
    if value["canonical_amendment_sha256"] != amendment["canonical_amendment_sha256"]:
        raise DownstreamProvenanceError("downstream amendment canonical identity differs")


def _runtime_versions() -> dict[str, object]:
    import numpy as np
    import pyarrow as pa

    return {
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "version_info": list(sys.version_info[:3]),
            "cache_tag": str(sys.implementation.cache_tag),
        },
        "numpy": {"version": str(np.__version__)},
        "pyarrow": {"version": str(pa.__version__)},
    }


def _implementation_artifacts() -> list[dict[str, object]]:
    rows = []
    for role, relative in sorted(_IMPLEMENTATION_PATHS.items()):
        rows.append({"role": role, **artifact_identity(ROOT / relative)})
    return rows


def _validate_global_runtime_artifacts(plan: Mapping[str, object]) -> None:
    global_identity = plan["global_execution_identity"]
    if plan["global_execution_identity_sha256"] != canonical_sha256(global_identity):
        raise DownstreamProvenanceError("plan global runtime identity SHA256 differs")
    if bool(global_identity.get("economic_outcomes_read")):
        raise DownstreamProvenanceError("runtime identity permits economic outcome access")
    if bool(global_identity.get("q90_action_enabled")):
        raise DownstreamProvenanceError("runtime identity enables q90 action")

    for key in (
        "source_contract",
        "operational_config",
        "model_bundle",
        "p3_artifact",
        "latency_profile",
        "queue_calibration",
    ):
        _resolve_artifact(global_identity[key], label=f"runtime artifact {key}")
    for index, value in enumerate(global_identity["frozen_source_artifacts"]):
        _resolve_artifact(value, label=f"frozen source artifact {index}")
    feature_dag = global_identity["feature_dag"]
    _resolve_artifact(feature_dag["implementation"], label="Feature DAG implementation")

    runtime_rows = global_identity["runtime_code_artifacts"]
    if not isinstance(runtime_rows, Sequence) or isinstance(runtime_rows, (str, bytes)):
        raise DownstreamProvenanceError("runtime code artifact list is missing")
    for value in runtime_rows:
        logical_path = str(value["logical_path"])
        expected = (ROOT / logical_path).resolve()
        _resolve_artifact(
            value,
            label=f"runtime code artifact {logical_path}",
            expected_path=expected,
        )

    import narrowgate_cpp

    cpp = global_identity["cpp_event_stream"]
    cpp_path = _resolve_artifact(
        cpp["module_artifact"],
        label="compiled C++ module",
        expected_path=Path(narrowgate_cpp.__file__),
    )
    if cpp_path != Path(narrowgate_cpp.__file__).resolve():
        raise DownstreamProvenanceError("loaded C++ module path differs")
    observed_abi = str(
        getattr(narrowgate_cpp, "ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION", "")
    )
    if observed_abi != str(cpp["abi_version"]):
        raise DownstreamProvenanceError("loaded C++ event-stream ABI differs")


def validate_plan_runtime_identity(plan_path: Path) -> dict[str, Any]:
    resolved = plan_path.expanduser().resolve()
    plan = read_json(resolved, label="execution plan")
    try:
        emitter.validate_execution_plan(plan)
    except Exception as exc:
        raise DownstreamProvenanceError("execution plan validation failed") from exc
    if Path(str(plan["cache_root"])).expanduser().resolve() != resolved.parent:
        raise DownstreamProvenanceError("execution plan is outside its cache root")
    _validate_global_runtime_artifacts(plan)
    return plan


def _amendment_payload(plan_path: Path, plan: Mapping[str, object]) -> dict[str, object]:
    import narrowgate_cpp

    cpp_path = Path(narrowgate_cpp.__file__).resolve()
    plan_cpp = plan["global_execution_identity"]["cpp_event_stream"]["module_artifact"]
    _resolve_artifact(
        plan_cpp,
        label="plan compiled C++ module",
        expected_path=cpp_path,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": STATUS,
        "execution_plan": {
            **artifact_identity(plan_path),
            "canonical_plan_sha256": plan["canonical_plan_sha256"],
            "global_execution_identity_sha256": plan[
                "global_execution_identity_sha256"
            ],
        },
        "implementation_artifacts": _implementation_artifacts(),
        "compiled_cpp_module": artifact_identity(cpp_path),
        "runtime_versions": _runtime_versions(),
        "stage_contract": {
            "ordered_stages": [
                "formal_40day_cpp_event_lockstep",
                "mechanics_only_cif_training",
                "trained_artifact_cpp_parity",
            ],
            "complete_canonical_panel_required": True,
            "lockstep_report_required_for_training": True,
            "training_report_required_for_parity": True,
            "report_artifact_plan_closure_required": True,
        },
        "output_contracts": {
            "lockstep": {
                "identity": LOCKSTEP_IDENTITY,
                "schema_version": LOCKSTEP_SCHEMA_VERSION,
            },
            "training_artifact": {
                "identity": TRAINING_IDENTITY,
                "schema_version": TRAINING_SCHEMA_VERSION,
            },
            "training_report": {
                "identity": f"{TRAINING_IDENTITY}_training",
                "schema_version": TRAINING_REPORT_SCHEMA_VERSION,
            },
            "cpp_parity": {
                "identity": PARITY_IDENTITY,
                "schema_version": PARITY_SCHEMA_VERSION,
            },
        },
        "scope": dict(AMENDMENT_SCOPE),
        "permissions": dict(DENIED_PERMISSIONS),
    }


def build_downstream_execution_amendment(
    *,
    plan_path: Path,
    output_path: Path,
) -> dict[str, object]:
    """Freeze the exact downstream mechanics implementation before execution."""

    resolved_plan = plan_path.expanduser().resolve()
    plan = validate_plan_runtime_identity(resolved_plan)
    payload = _amendment_payload(resolved_plan, plan)
    payload["canonical_amendment_sha256"] = canonical_sha256(payload)
    resolved_output = output_path.expanduser().resolve()
    if resolved_output.exists():
        existing = read_json(resolved_output, label="downstream execution amendment")
        if existing != payload:
            raise FileExistsError(
                f"refusing to replace a different downstream amendment: {resolved_output}"
            )
        return existing
    resolved_output.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved_output.with_name(
        f".{resolved_output.name}.partial-{os.getpid()}-{uuid.uuid4().hex}"
    )
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved_output)
        descriptor = os.open(resolved_output.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)
    return payload


def validate_downstream_execution_amendment(
    amendment_path: Path,
    *,
    plan_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_amendment = amendment_path.expanduser().resolve()
    resolved_plan = plan_path.expanduser().resolve()
    observed = read_json(resolved_amendment, label="downstream execution amendment")
    if observed.get("schema_version") != SCHEMA_VERSION or observed.get("identity") != IDENTITY:
        raise DownstreamProvenanceError("downstream amendment identity differs")
    if observed.get("status") != STATUS:
        raise DownstreamProvenanceError("downstream amendment status differs")
    claimed = str(observed.get("canonical_amendment_sha256", ""))
    if claimed != canonical_document_sha256(observed, "canonical_amendment_sha256"):
        raise DownstreamProvenanceError("downstream amendment canonical SHA256 differs")
    plan = validate_plan_runtime_identity(resolved_plan)
    expected = _amendment_payload(resolved_plan, plan)
    expected["canonical_amendment_sha256"] = canonical_sha256(expected)
    if observed != expected:
        raise DownstreamProvenanceError(
            "downstream implementation, runtime, plan, scope, or permissions drifted"
        )
    return observed, plan


def validate_panel_manifest_strict(
    panel_path: Path,
    *,
    plan: Mapping[str, object],
) -> dict[str, Any]:
    resolved = panel_path.expanduser().resolve()
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    if resolved != cache_root / "panel_manifest.json":
        raise DownstreamProvenanceError("panel manifest path differs from execution plan")
    panel = read_json(resolved, label="panel manifest")
    if set(panel) != _PANEL_KEYS:
        raise DownstreamProvenanceError("panel manifest schema keys differ")
    if (
        panel["schema_version"] != emitter.PANEL_MANIFEST_SCHEMA_VERSION
        or panel["identity"] != emitter.IDENTITY
        or panel["status"] != "journal_emission_complete_lockstep_not_executed"
    ):
        raise DownstreamProvenanceError("panel manifest identity or status differs")
    if panel["canonical_manifest_sha256"] != canonical_document_sha256(
        panel, "canonical_manifest_sha256"
    ):
        raise DownstreamProvenanceError("panel manifest canonical SHA256 differs")
    days = list(map(str, plan["ordered_utc_days"]))
    if panel["plan_sha256"] != plan["canonical_plan_sha256"]:
        raise DownstreamProvenanceError("panel and execution plan identities differ")
    if list(map(str, panel["ordered_utc_days"])) != days or len(days) != 40:
        raise DownstreamProvenanceError("panel ordered 40-day denominator differs")
    expected_scope = {
        "mechanics_only": True,
        "economic_outcomes_read": False,
        "formal_40day_journal_emission_complete": True,
        "formal_40day_lockstep_executed": False,
    }
    expected_permissions = {
        "cif_training": False,
        "economic_evaluation": False,
        "q90_action": False,
        "live_transport": False,
        "live_deployment": False,
    }
    if panel["scope"] != expected_scope or panel["permissions"] != expected_permissions:
        raise DownstreamProvenanceError("panel scope or permissions drifted")

    references = panel["day_manifests"]
    if not isinstance(references, list) or len(references) != 40:
        raise DownstreamProvenanceError("panel day-manifest references differ")
    by_day = emitter.validate_execution_plan(plan)
    totals: Counter[str] = Counter()
    for expected_day, reference in zip(days, references, strict=True):
        if set(reference) != {"day", "artifact", "journal_rows"}:
            raise DownstreamProvenanceError("panel day reference schema differs")
        if str(reference["day"]) != expected_day:
            raise DownstreamProvenanceError("panel day reference order differs")
        expected_path = cache_root / "days" / expected_day / "day_manifest.json"
        _resolve_artifact(
            reference["artifact"],
            label=f"{expected_day} day manifest",
            base=cache_root,
            expected_path=expected_path,
        )
        manifest = emitter._validate_day_manifest(
            expected_path,
            plan=plan,
            day_row=by_day[expected_day],
        )
        if int(reference["journal_rows"]) != int(manifest["journal_v2"]["row_count"]):
            raise DownstreamProvenanceError(f"{expected_day}: panel journal row count differs")
        counters = manifest["journal_v2"]["counters"]
        for key in _PANEL_COUNTER_KEYS:
            totals[key] += int(counters[key])
    if set(panel["mechanics_totals"]) != _PANEL_COUNTER_KEYS:
        raise DownstreamProvenanceError("panel mechanics total schema differs")
    if dict(sorted(totals.items())) != panel["mechanics_totals"]:
        raise DownstreamProvenanceError("panel mechanics totals differ from daily manifests")
    return panel


def _validate_report_artifact(
    value: Mapping[str, object],
    *,
    path: Path,
    label: str,
) -> None:
    if set(value) != {"path", "size_bytes", "sha256"}:
        raise DownstreamProvenanceError(f"{label} artifact schema differs")
    _resolve_artifact(value, label=label, expected_path=path)


def validate_lockstep_report_for_training(
    report_path: Path,
    *,
    plan_path: Path,
    panel_path: Path,
    amendment_path: Path,
    amendment: Mapping[str, object],
    plan: Mapping[str, object],
) -> dict[str, Any]:
    report = read_json(report_path, label="40-day lockstep report")
    if set(report) != _LOCKSTEP_KEYS:
        raise DownstreamProvenanceError("lockstep report schema keys differ")
    if (
        report["schema_version"] != LOCKSTEP_SCHEMA_VERSION
        or report["identity"] != LOCKSTEP_IDENTITY
        or report["status"] != "passed"
    ):
        raise DownstreamProvenanceError("lockstep report identity or status differs")
    if report["canonical_report_sha256"] != canonical_document_sha256(
        report, "canonical_report_sha256"
    ):
        raise DownstreamProvenanceError("lockstep report canonical SHA256 differs")
    _validate_amendment_reference(
        report["downstream_execution_amendment"],
        amendment_path=amendment_path,
        amendment=amendment,
    )
    _validate_report_artifact(report["plan"], path=plan_path, label="execution plan")
    _validate_report_artifact(
        report["panel_manifest"], path=panel_path, label="panel manifest"
    )
    plan_cpp = plan["global_execution_identity"]["cpp_event_stream"]
    cpp_path = Path(str(plan_cpp["module_artifact"]["path"])).expanduser().resolve()
    _validate_report_artifact(report["cpp_module"], path=cpp_path, label="C++ module")
    if (
        report["plan_sha256"] != plan["canonical_plan_sha256"]
        or report["global_execution_identity_sha256"]
        != plan["global_execution_identity_sha256"]
        or report["cpp_abi_version"] != plan_cpp["abi_version"]
    ):
        raise DownstreamProvenanceError("lockstep report runtime identity differs")
    if set(report["gates"]) != _LOCKSTEP_GATE_KEYS or not all(report["gates"].values()):
        raise DownstreamProvenanceError("lockstep report gates did not all pass")
    if not bool(report["formal_40day_lockstep_passed"]):
        raise DownstreamProvenanceError("formal 40-day lockstep did not pass")
    if report["mismatch_counts"] or report["post_terminal_violation_counts"]:
        raise DownstreamProvenanceError("lockstep mismatch or terminal violation is nonzero")
    if int(report["counts"].get("day_count", -1)) != 40 or len(report["days"]) != 40:
        raise DownstreamProvenanceError("lockstep report day denominator differs")
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    for expected_day, day_report in zip(
        map(str, plan["ordered_utc_days"]),
        report["days"],
        strict=True,
    ):
        if set(day_report) != _LOCKSTEP_DAY_KEYS or str(day_report["day"]) != expected_day:
            raise DownstreamProvenanceError("lockstep daily report schema or order differs")
        if (
            not bool(day_report["mechanics_lockstep_passed"])
            or day_report["mismatch_counts"]
            or not bool(day_report["post_terminal_safety"].get("passed"))
            or day_report["post_terminal_safety"].get("violation_counts")
        ):
            raise DownstreamProvenanceError(f"{expected_day}: daily lockstep did not pass")
        manifest_path = cache_root / "days" / expected_day / "day_manifest.json"
        if file_sha256(manifest_path) != str(day_report["day_manifest_sha256"]):
            raise DownstreamProvenanceError(
                f"{expected_day}: lockstep day-manifest SHA256 differs"
            )
    if report["scope"] != LOCKSTEP_SCOPE or report["permissions"] != LOCKSTEP_PERMISSIONS:
        raise DownstreamProvenanceError("lockstep report scope or permissions drifted")
    return report


def validate_training_artifact_for_parity(
    artifact_path: Path,
    *,
    plan_path: Path,
    panel_path: Path,
    lockstep_report_path: Path,
    amendment_path: Path,
    amendment: Mapping[str, object],
    plan: Mapping[str, object],
) -> dict[str, Any]:
    artifact = read_json(artifact_path, label="CIF training artifact")
    if set(artifact) != _TRAINING_ARTIFACT_KEYS:
        raise DownstreamProvenanceError("CIF training artifact schema keys differ")
    if (
        artifact["schema_version"] != TRAINING_SCHEMA_VERSION
        or artifact["identity"] != TRAINING_IDENTITY
        or artifact["status"] != "trained_mechanics_only"
    ):
        raise DownstreamProvenanceError("CIF training artifact identity differs")
    if artifact["canonical_artifact_sha256"] != canonical_document_sha256(
        artifact, "canonical_artifact_sha256"
    ):
        raise DownstreamProvenanceError("CIF training artifact canonical SHA256 differs")
    _validate_amendment_reference(
        artifact["downstream_execution_amendment"],
        amendment_path=amendment_path,
        amendment=amendment,
    )
    if artifact["plan_sha256"] != plan["canonical_plan_sha256"]:
        raise DownstreamProvenanceError("CIF artifact plan identity differs")
    inputs = artifact["input_artifacts"]
    if set(inputs) != {"execution_plan", "panel_manifest", "python_cpp_lockstep"}:
        raise DownstreamProvenanceError("CIF artifact input schema differs")
    _validate_report_artifact(inputs["execution_plan"], path=plan_path, label="execution plan")
    _validate_report_artifact(inputs["panel_manifest"], path=panel_path, label="panel manifest")
    _validate_report_artifact(
        inputs["python_cpp_lockstep"],
        path=lockstep_report_path,
        label="lockstep report",
    )
    if artifact["scope"] != TRAINING_SCOPE or artifact["permissions"] != TRAINING_PERMISSIONS:
        raise DownstreamProvenanceError("CIF artifact scope or permissions drifted")
    if int(artifact["training_counts"].get("day_count", -1)) != 40:
        raise DownstreamProvenanceError("CIF artifact day denominator differs")
    cause_contract = artifact["cause_contract"]
    if (
        bool(cause_contract.get("realized_fill_quality_read"))
        or bool(cause_contract.get("q90_adverse_score_available"))
        or cause_contract.get("kernel_rate_mapping", {}).get("adverse_fill")
        != "fixed_zero_unclassified_channel"
    ):
        raise DownstreamProvenanceError("CIF artifact adverse-score scope drifted")
    return artifact


def validate_training_report_for_parity(
    report_path: Path,
    *,
    artifact_path: Path,
    artifact: Mapping[str, object],
    plan_path: Path,
    panel_path: Path,
    lockstep_report_path: Path,
    amendment_path: Path,
    amendment: Mapping[str, object],
    plan: Mapping[str, object],
) -> dict[str, Any]:
    report = read_json(report_path, label="CIF training report")
    if set(report) != _TRAINING_REPORT_KEYS:
        raise DownstreamProvenanceError("CIF training report schema keys differ")
    if (
        report["schema_version"] != TRAINING_REPORT_SCHEMA_VERSION
        or report["identity"] != f"{TRAINING_IDENTITY}_training"
        or report["status"] != "passed"
    ):
        raise DownstreamProvenanceError("CIF training report identity differs")
    if report["canonical_report_sha256"] != canonical_document_sha256(
        report, "canonical_report_sha256"
    ):
        raise DownstreamProvenanceError("CIF training report canonical SHA256 differs")
    _validate_amendment_reference(
        report["downstream_execution_amendment"],
        amendment_path=amendment_path,
        amendment=amendment,
    )
    _validate_report_artifact(report["model_artifact"], path=artifact_path, label="model artifact")
    inputs = report["input_artifacts"]
    if set(inputs) != {"execution_plan", "panel_manifest", "python_cpp_lockstep"}:
        raise DownstreamProvenanceError("CIF training report input schema differs")
    _validate_report_artifact(inputs["execution_plan"], path=plan_path, label="execution plan")
    _validate_report_artifact(inputs["panel_manifest"], path=panel_path, label="panel manifest")
    _validate_report_artifact(
        inputs["python_cpp_lockstep"],
        path=lockstep_report_path,
        label="lockstep report",
    )
    if report["plan_sha256"] != plan["canonical_plan_sha256"]:
        raise DownstreamProvenanceError("CIF training report plan identity differs")
    if report["training_counts"] != artifact["training_counts"]:
        raise DownstreamProvenanceError("training report and artifact counts differ")
    if len(report["day_support"]) != 40 or not all(
        bool(row.get("denominator_parity")) for row in report["day_support"]
    ):
        raise DownstreamProvenanceError("training report day denominator parity differs")
    if set(report["gates"]) != _TRAINING_GATE_KEYS or not all(
        report["gates"].values()
    ):
        raise DownstreamProvenanceError("CIF training report gates did not all pass")
    if report["scope"] != TRAINING_SCOPE or report["permissions"] != TRAINING_PERMISSIONS:
        raise DownstreamProvenanceError("CIF training report scope or permissions drifted")
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--plan", type=Path, required=True)
    build.add_argument("--out", type=Path, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--plan", type=Path, required=True)
    validate.add_argument("--amendment", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "build":
        payload = build_downstream_execution_amendment(
            plan_path=args.plan,
            output_path=args.out,
        )
    else:
        payload, _ = validate_downstream_execution_amendment(
            args.amendment,
            plan_path=args.plan,
        )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
