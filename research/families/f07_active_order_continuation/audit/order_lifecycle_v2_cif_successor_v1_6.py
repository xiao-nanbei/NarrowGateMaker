#!/usr/bin/env python3
"""Validate the F07 v1.6 successor chain for mechanics-only CIF work."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_downstream_execution_amendment_v1_5 as frozen_v1_5,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_runtime_compatibility_v1_6 as successor,
)

TRAINING_IDENTITY = successor.TRAINING_IDENTITY
TRAINING_SCHEMA_VERSION = successor.TRAINING_SCHEMA_VERSION
TRAINING_REPORT_SCHEMA_VERSION = successor.TRAINING_REPORT_SCHEMA_VERSION
PARITY_IDENTITY = successor.PARITY_IDENTITY
PARITY_SCHEMA_VERSION = successor.PARITY_SCHEMA_VERSION

TRAINING_SCOPE = dict(frozen_v1_5.TRAINING_SCOPE)
TRAINING_PERMISSIONS = dict(frozen_v1_5.TRAINING_PERMISSIONS)
PARITY_SCOPE = dict(frozen_v1_5.PARITY_SCOPE)
PARITY_PERMISSIONS = dict(frozen_v1_5.PARITY_PERMISSIONS)

_LOCKSTEP_KEYS = {
    "schema_version",
    "identity",
    "status",
    "generated_at_utc",
    "successor_amendment",
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
    "cancel_reject_branch_present",
    "cancel_reject_routes_complete",
    "homogeneous_successor_amendment_bound",
    "full_40day_fingerprint_equivalence_bound",
    "economic_outcomes_not_read",
}
_LOCKSTEP_COUNT_KEYS = {
    "day_count",
    "event_count",
    "lifecycle_count",
    "exact_native_lifecycle_count",
    "cancel_reject_count",
    "cancel_reject_route_count",
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


def amendment_reference(path: Path, amendment: Mapping[str, object]) -> dict[str, object]:
    return {
        **successor.artifact_identity(path),
        "canonical_amendment_sha256": amendment["canonical_amendment_sha256"],
    }


def validate_downstream_execution_amendment(
    amendment_path: Path,
    *,
    plan_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    return successor.validate_successor_amendment(
        amendment_path,
        successor_plan_path=plan_path,
    )


def validate_panel_manifest_strict(
    panel_path: Path,
    *,
    plan: Mapping[str, object],
) -> dict[str, Any]:
    return frozen_v1_5.validate_panel_manifest_strict(panel_path, plan=plan)


def _validate_artifact_reference(
    value: Mapping[str, object],
    *,
    path: Path,
    label: str,
) -> None:
    if set(value) != {"path", "size_bytes", "sha256"}:
        raise successor.RuntimeCompatibilityError(f"{label} artifact schema differs")
    observed = successor._validate_artifact(value, label=label)
    if observed != path.expanduser().resolve():
        raise successor.RuntimeCompatibilityError(f"{label} artifact path differs")


def _validate_amendment_reference(
    value: Mapping[str, object],
    *,
    amendment_path: Path,
    amendment: Mapping[str, object],
) -> None:
    if set(value) != {"path", "size_bytes", "sha256", "canonical_amendment_sha256"}:
        raise successor.RuntimeCompatibilityError("successor amendment reference schema differs")
    artifact = {key: value[key] for key in ("path", "size_bytes", "sha256")}
    _validate_artifact_reference(
        artifact,
        path=amendment_path,
        label="successor amendment",
    )
    if value["canonical_amendment_sha256"] != amendment["canonical_amendment_sha256"]:
        raise successor.RuntimeCompatibilityError("successor amendment identity differs")


def validate_lockstep_report_for_training(
    report_path: Path,
    *,
    plan_path: Path,
    panel_path: Path,
    amendment_path: Path,
    amendment: Mapping[str, object],
    plan: Mapping[str, object],
) -> dict[str, Any]:
    report = successor._read_json(report_path, label="v1.6 40-day lockstep report")
    if set(report) != _LOCKSTEP_KEYS:
        raise successor.RuntimeCompatibilityError("v1.6 lockstep report schema keys differ")
    if (
        report["schema_version"] != successor.LOCKSTEP_SCHEMA_VERSION
        or report["identity"] != successor.LOCKSTEP_IDENTITY
        or report["status"] != "passed"
    ):
        raise successor.RuntimeCompatibilityError("v1.6 lockstep identity or status differs")
    if report["canonical_report_sha256"] != successor.canonical_document_sha256(
        report, "canonical_report_sha256"
    ):
        raise successor.RuntimeCompatibilityError("v1.6 lockstep canonical SHA256 differs")
    _validate_amendment_reference(
        report["successor_amendment"],
        amendment_path=amendment_path,
        amendment=amendment,
    )
    _validate_artifact_reference(report["plan"], path=plan_path, label="execution plan")
    _validate_artifact_reference(
        report["panel_manifest"], path=panel_path, label="panel manifest"
    )
    cpp = plan["global_execution_identity"]["cpp_event_stream"]
    cpp_path = Path(str(cpp["module_artifact"]["path"])).expanduser().resolve()
    _validate_artifact_reference(report["cpp_module"], path=cpp_path, label="C++ module")
    if (
        report["plan_sha256"] != plan["canonical_plan_sha256"]
        or report["global_execution_identity_sha256"]
        != plan["global_execution_identity_sha256"]
        or report["cpp_abi_version"] != cpp["abi_version"]
    ):
        raise successor.RuntimeCompatibilityError("v1.6 lockstep runtime identity differs")
    if set(report["gates"]) != _LOCKSTEP_GATE_KEYS or not all(report["gates"].values()):
        raise successor.RuntimeCompatibilityError("v1.6 lockstep gates did not all pass")
    if (
        not bool(report["formal_40day_lockstep_passed"])
        or report["mismatch_counts"]
        or report["post_terminal_violation_counts"]
    ):
        raise successor.RuntimeCompatibilityError("v1.6 lockstep mismatches are nonzero")
    days = list(map(str, plan["ordered_utc_days"]))
    if set(report["counts"]) != _LOCKSTEP_COUNT_KEYS:
        raise successor.RuntimeCompatibilityError("v1.6 lockstep count schema differs")
    if len(days) != 40 or int(report["counts"].get("day_count", -1)) != 40:
        raise successor.RuntimeCompatibilityError("v1.6 lockstep day denominator differs")
    if len(report["days"]) != 40:
        raise successor.RuntimeCompatibilityError("v1.6 lockstep daily reports are incomplete")
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    for expected_day, row in zip(days, report["days"], strict=True):
        if set(row) != _LOCKSTEP_DAY_KEYS or str(row.get("day")) != expected_day:
            raise successor.RuntimeCompatibilityError("v1.6 lockstep day schema or order differs")
        if (
            not bool(row.get("mechanics_lockstep_passed"))
            or row.get("mismatch_counts")
            or not bool(row.get("post_terminal_safety", {}).get("passed"))
            or row.get("post_terminal_safety", {}).get("violation_counts")
        ):
            raise successor.RuntimeCompatibilityError(f"{expected_day}: lockstep failed")
        manifest = cache_root / "days" / expected_day / "day_manifest.json"
        if successor.file_sha256(manifest) != str(row.get("day_manifest_sha256")):
            raise successor.RuntimeCompatibilityError(
                f"{expected_day}: lockstep day-manifest SHA256 differs"
            )
    if report["scope"] != {"mechanics_only": True, "economic_outcomes_read": False}:
        raise successor.RuntimeCompatibilityError("v1.6 lockstep scope differs")
    if report["permissions"] != successor.LOCKSTEP_PERMISSIONS:
        raise successor.RuntimeCompatibilityError("v1.6 lockstep permissions differ")
    return report


@contextmanager
def _patched_frozen_training_contracts() -> Iterator[None]:
    names = {
        "TRAINING_IDENTITY": TRAINING_IDENTITY,
        "TRAINING_SCHEMA_VERSION": TRAINING_SCHEMA_VERSION,
        "TRAINING_REPORT_SCHEMA_VERSION": TRAINING_REPORT_SCHEMA_VERSION,
        "TRAINING_SCOPE": TRAINING_SCOPE,
        "TRAINING_PERMISSIONS": TRAINING_PERMISSIONS,
    }
    previous = {name: getattr(frozen_v1_5, name) for name in names}
    for name, value in names.items():
        setattr(frozen_v1_5, name, value)
    try:
        yield
    finally:
        for name, value in previous.items():
            setattr(frozen_v1_5, name, value)


def validate_training_artifact_for_parity(*args: Any, **kwargs: Any) -> dict[str, Any]:
    with _patched_frozen_training_contracts():
        return frozen_v1_5.validate_training_artifact_for_parity(*args, **kwargs)


def validate_training_report_for_parity(*args: Any, **kwargs: Any) -> dict[str, Any]:
    with _patched_frozen_training_contracts():
        return frozen_v1_5.validate_training_report_for_parity(*args, **kwargs)
