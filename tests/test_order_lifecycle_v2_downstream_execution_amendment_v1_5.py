from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import narrowgate_cpp
import pytest

from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_100ms_training_v1_5 as training,
)
from research.families.f07_active_order_continuation.audit import (
    active_order_lifecycle_cif_cpp_parity_v1_5 as parity,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_cpp_lockstep as lockstep,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_downstream_execution_amendment_v1_5 as subject,
)

FROZEN_AMENDMENT = (
    Path(__file__).resolve().parents[1]
    / "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_downstream_execution_amendment_v1_5_20260805.json"
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_frozen_amendment_is_canonical_and_denies_policy_permissions() -> None:
    payload = json.loads(FROZEN_AMENDMENT.read_text(encoding="utf-8"))
    assert payload["canonical_amendment_sha256"] == subject.canonical_document_sha256(
        payload,
        "canonical_amendment_sha256",
    )
    assert payload["permissions"] == subject.DENIED_PERMISSIONS
    assert {row["role"] for row in payload["implementation_artifacts"]} == set(
        subject._IMPLEMENTATION_PATHS
    )
    assert payload["runtime_versions"]["python"]["version_info"] == [3, 12, 13]


def test_downstream_clis_require_the_frozen_amendment_and_training_report() -> None:
    lockstep_args = lockstep._build_parser().parse_args(
        [
            "--plan",
            "plan.json",
            "--execution-amendment",
            "amendment.json",
            "--out",
            "lockstep.json",
        ]
    )
    assert str(lockstep_args.execution_amendment) == "amendment.json"
    training_args = training._build_parser().parse_args(
        [
            "--plan",
            "plan.json",
            "--execution-amendment",
            "amendment.json",
            "--lockstep-report",
            "lockstep.json",
            "--artifact",
            "artifact.json",
            "--report",
            "training.json",
        ]
    )
    assert str(training_args.execution_amendment) == "amendment.json"
    parity_args = parity._build_parser().parse_args(
        [
            "--artifact",
            "artifact.json",
            "--training-report",
            "training.json",
            "--execution-amendment",
            "amendment.json",
            "--out",
            "parity.json",
        ]
    )
    assert str(parity_args.training_report) == "training.json"


def _fake_runtime_plan(plan_path: Path) -> dict[str, Any]:
    cpp_path = Path(narrowgate_cpp.__file__).resolve()
    return {
        "canonical_plan_sha256": "1" * 64,
        "global_execution_identity_sha256": "2" * 64,
        "global_execution_identity": {
            "cpp_event_stream": {
                "abi_version": str(
                    narrowgate_cpp.ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION
                ),
                "module_artifact": subject.artifact_identity(cpp_path),
            }
        },
        "cache_root": str(plan_path.parent),
        "ordered_utc_days": [f"2026-01-{index:02d}" for index in range(1, 41)],
    }


def test_amendment_rejects_rehashed_permission_and_runtime_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_path = tmp_path / "execution_plan.json"
    _write_json(plan_path, {"fixture": "plan"})
    plan = _fake_runtime_plan(plan_path)
    monkeypatch.setattr(subject, "validate_plan_runtime_identity", lambda _path: plan)
    amendment_path = tmp_path / "amendment.json"
    frozen = subject.build_downstream_execution_amendment(
        plan_path=plan_path,
        output_path=amendment_path,
    )
    observed, observed_plan = subject.validate_downstream_execution_amendment(
        amendment_path,
        plan_path=plan_path,
    )
    assert observed == frozen
    assert observed_plan == plan

    drifted = json.loads(json.dumps(frozen))
    drifted["permissions"]["action"] = True
    drifted["canonical_amendment_sha256"] = subject.canonical_document_sha256(
        drifted,
        "canonical_amendment_sha256",
    )
    _write_json(amendment_path, drifted)
    with pytest.raises(subject.DownstreamProvenanceError, match="drifted"):
        subject.validate_downstream_execution_amendment(
            amendment_path,
            plan_path=plan_path,
        )

    runtime_drift = json.loads(json.dumps(frozen))
    runtime_drift["runtime_versions"]["numpy"]["version"] = "0.0.invalid"
    runtime_drift["canonical_amendment_sha256"] = subject.canonical_document_sha256(
        runtime_drift,
        "canonical_amendment_sha256",
    )
    _write_json(amendment_path, runtime_drift)
    with pytest.raises(subject.DownstreamProvenanceError, match="drifted"):
        subject.validate_downstream_execution_amendment(
            amendment_path,
            plan_path=plan_path,
        )


def _panel_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict[str, Any], Path, list[Path]]:
    days = [f"2026-01-{index:02d}" for index in range(1, 41)]
    manifests: list[Path] = []
    references = []
    for day in days:
        path = tmp_path / "days" / day / "day_manifest.json"
        _write_json(path, {"day": day})
        manifests.append(path)
        references.append(
            {
                "day": day,
                "artifact": subject.artifact_identity(path),
                "journal_rows": 1,
            }
        )
    plan = {
        "cache_root": str(tmp_path),
        "canonical_plan_sha256": "3" * 64,
        "ordered_utc_days": days,
    }
    by_day = {day: {"day": day} for day in days}
    monkeypatch.setattr(subject.emitter, "validate_execution_plan", lambda _plan: by_day)

    def validate_day(path: Path, **_kwargs: Any) -> dict[str, Any]:
        return {
            "journal_v2": {
                "row_count": 1,
                "counters": {key: 1 for key in subject._PANEL_COUNTER_KEYS},
            }
        }

    monkeypatch.setattr(subject.emitter, "_validate_day_manifest", validate_day)
    panel: dict[str, Any] = {
        "schema_version": subject.emitter.PANEL_MANIFEST_SCHEMA_VERSION,
        "identity": subject.emitter.IDENTITY,
        "status": "journal_emission_complete_lockstep_not_executed",
        "generated_at_utc": "2026-08-05T00:00:00+00:00",
        "plan_sha256": plan["canonical_plan_sha256"],
        "ordered_utc_days": days,
        "day_manifests": references,
        "mechanics_totals": {
            key: len(days) for key in subject._PANEL_COUNTER_KEYS
        },
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_journal_emission_complete": True,
            "formal_40day_lockstep_executed": False,
        },
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "live_transport": False,
            "live_deployment": False,
        },
    }
    panel["canonical_manifest_sha256"] = subject.canonical_sha256(panel)
    panel_path = tmp_path / "panel_manifest.json"
    _write_json(panel_path, panel)
    return plan, panel_path, manifests


def test_panel_validator_binds_canonical_permissions_and_daily_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, panel_path, manifests = _panel_fixture(tmp_path, monkeypatch)
    panel = subject.validate_panel_manifest_strict(panel_path, plan=plan)
    assert len(panel["ordered_utc_days"]) == 40

    drifted = json.loads(json.dumps(panel))
    drifted["permissions"]["live_transport"] = True
    drifted["canonical_manifest_sha256"] = subject.canonical_document_sha256(
        drifted,
        "canonical_manifest_sha256",
    )
    _write_json(panel_path, drifted)
    with pytest.raises(subject.DownstreamProvenanceError, match="scope or permissions"):
        subject.validate_panel_manifest_strict(panel_path, plan=plan)

    clean = json.loads(json.dumps(panel))
    clean["day_manifests"][0]["artifact"]["sha256"] = "0" * 64
    clean["canonical_manifest_sha256"] = subject.canonical_document_sha256(
        clean,
        "canonical_manifest_sha256",
    )
    _write_json(panel_path, clean)
    with pytest.raises(subject.DownstreamProvenanceError, match="SHA256 differs"):
        subject.validate_panel_manifest_strict(panel_path, plan=plan)
    assert manifests[0].is_file()


def _amendment_fixture(tmp_path: Path) -> tuple[dict[str, Any], Path]:
    payload = {"canonical_amendment_sha256": "4" * 64}
    path = tmp_path / "downstream_amendment.json"
    _write_json(path, payload)
    return payload, path


def _lockstep_report(
    *,
    plan: dict[str, Any],
    plan_path: Path,
    panel_path: Path,
    manifest_paths: list[Path],
    amendment: dict[str, Any],
    amendment_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    cpp_path = Path(narrowgate_cpp.__file__).resolve()
    plan["global_execution_identity_sha256"] = "5" * 64
    plan["global_execution_identity"] = {
        "cpp_event_stream": {
            "abi_version": str(
                narrowgate_cpp.ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION
            ),
            "module_artifact": subject.artifact_identity(cpp_path),
        }
    }
    _write_json(plan_path, plan)
    report: dict[str, Any] = {
        "schema_version": subject.LOCKSTEP_SCHEMA_VERSION,
        "identity": subject.LOCKSTEP_IDENTITY,
        "status": "passed",
        "generated_at_utc": "2026-08-05T00:00:00+00:00",
        "downstream_execution_amendment": subject.amendment_reference(
            amendment_path,
            amendment,
        ),
        "plan": subject.artifact_identity(plan_path),
        "panel_manifest": subject.artifact_identity(panel_path),
        "plan_sha256": plan["canonical_plan_sha256"],
        "global_execution_identity_sha256": plan[
            "global_execution_identity_sha256"
        ],
        "cpp_abi_version": plan["global_execution_identity"]["cpp_event_stream"][
            "abi_version"
        ],
        "cpp_module": subject.artifact_identity(cpp_path),
        "counts": {
            "day_count": 40,
            "lifecycle_count": 40,
            "event_count": 40,
            "exact_native_lifecycle_count": 40,
            "native_queue_censored_lifecycle_count": 0,
        },
        "mismatch_counts": {},
        "post_terminal_violation_counts": {},
        "days": [
            {
                "day": day,
                "day_manifest_sha256": subject.file_sha256(path),
                "journal_row_count": 1,
                "lockstep_report_sha256": "6" * 64,
                "mechanics_lockstep_passed": True,
                "mismatch_counts": {},
                "post_terminal_safety": {
                    "passed": True,
                    "violation_counts": {},
                },
            }
            for day, path in zip(plan["ordered_utc_days"], manifest_paths, strict=True)
        ],
        "gates": {key: True for key in subject._LOCKSTEP_GATE_KEYS},
        "formal_40day_lockstep_passed": True,
        "scope": dict(subject.LOCKSTEP_SCOPE),
        "permissions": dict(subject.LOCKSTEP_PERMISSIONS),
    }
    report["canonical_report_sha256"] = subject.canonical_sha256(report)
    _write_json(output_path, report)
    return report


def test_lockstep_report_requires_canonical_identity_and_exact_permissions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, panel_path, manifests = _panel_fixture(tmp_path, monkeypatch)
    amendment, amendment_path = _amendment_fixture(tmp_path)
    plan_path = tmp_path / "execution_plan.json"
    lockstep_path = tmp_path / "lockstep.json"
    report = _lockstep_report(
        plan=plan,
        plan_path=plan_path,
        panel_path=panel_path,
        manifest_paths=manifests,
        amendment=amendment,
        amendment_path=amendment_path,
        output_path=lockstep_path,
    )
    validated = subject.validate_lockstep_report_for_training(
        lockstep_path,
        plan_path=plan_path,
        panel_path=panel_path,
        amendment_path=amendment_path,
        amendment=amendment,
        plan=plan,
    )
    assert validated["permissions"]["cif_training"] is True

    drifted = json.loads(json.dumps(report))
    drifted["permissions"]["action"] = True
    drifted["canonical_report_sha256"] = subject.canonical_document_sha256(
        drifted,
        "canonical_report_sha256",
    )
    _write_json(lockstep_path, drifted)
    with pytest.raises(subject.DownstreamProvenanceError, match="permissions"):
        subject.validate_lockstep_report_for_training(
            lockstep_path,
            plan_path=plan_path,
            panel_path=panel_path,
            amendment_path=amendment_path,
            amendment=amendment,
            plan=plan,
        )


def test_training_report_closes_artifact_and_plan_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, panel_path, manifests = _panel_fixture(tmp_path, monkeypatch)
    amendment, amendment_path = _amendment_fixture(tmp_path)
    plan_path = tmp_path / "execution_plan.json"
    lockstep_path = tmp_path / "lockstep.json"
    _lockstep_report(
        plan=plan,
        plan_path=plan_path,
        panel_path=panel_path,
        manifest_paths=manifests,
        amendment=amendment,
        amendment_path=amendment_path,
        output_path=lockstep_path,
    )
    inputs = {
        "execution_plan": subject.artifact_identity(plan_path),
        "panel_manifest": subject.artifact_identity(panel_path),
        "python_cpp_lockstep": subject.artifact_identity(lockstep_path),
    }
    training_counts = {"day_count": 40, "eligible_lifecycle_count": 1}
    artifact: dict[str, Any] = {
        "schema_version": subject.TRAINING_SCHEMA_VERSION,
        "identity": subject.TRAINING_IDENTITY,
        "status": "trained_mechanics_only",
        "trained_at_utc": "2026-08-05T00:00:00+00:00",
        "downstream_execution_amendment": subject.amendment_reference(
            amendment_path,
            amendment,
        ),
        "plan_sha256": plan["canonical_plan_sha256"],
        "input_artifacts": inputs,
        "grid": {"interval_ms": 100},
        "cause_contract": {
            "realized_fill_quality_read": False,
            "q90_adverse_score_available": False,
            "kernel_rate_mapping": {
                "adverse_fill": "fixed_zero_unclassified_channel"
            },
        },
        "conditioning": {},
        "training_counts": training_counts,
        "parent_rates": [],
        "cells": [],
        "scope": dict(subject.TRAINING_SCOPE),
        "permissions": dict(subject.TRAINING_PERMISSIONS),
    }
    artifact["canonical_artifact_sha256"] = subject.canonical_sha256(artifact)
    artifact_path = tmp_path / "cif_artifact.json"
    _write_json(artifact_path, artifact)
    validated_artifact = subject.validate_training_artifact_for_parity(
        artifact_path,
        plan_path=plan_path,
        panel_path=panel_path,
        lockstep_report_path=lockstep_path,
        amendment_path=amendment_path,
        amendment=amendment,
        plan=plan,
    )

    report: dict[str, Any] = {
        "schema_version": subject.TRAINING_REPORT_SCHEMA_VERSION,
        "identity": f"{subject.TRAINING_IDENTITY}_training",
        "status": "passed",
        "generated_at_utc": "2026-08-05T00:00:00+00:00",
        "downstream_execution_amendment": subject.amendment_reference(
            amendment_path,
            amendment,
        ),
        "model_artifact": subject.artifact_identity(artifact_path),
        "input_artifacts": inputs,
        "plan_sha256": plan["canonical_plan_sha256"],
        "training_counts": training_counts,
        "day_support": [
            {"day": day, "denominator_parity": True}
            for day in plan["ordered_utc_days"]
        ],
        "gates": {key: True for key in subject._TRAINING_GATE_KEYS},
        "scope": dict(subject.TRAINING_SCOPE),
        "permissions": dict(subject.TRAINING_PERMISSIONS),
    }
    report["canonical_report_sha256"] = subject.canonical_sha256(report)
    report_path = tmp_path / "training_report.json"
    _write_json(report_path, report)
    subject.validate_training_report_for_parity(
        report_path,
        artifact_path=artifact_path,
        artifact=validated_artifact,
        plan_path=plan_path,
        panel_path=panel_path,
        lockstep_report_path=lockstep_path,
        amendment_path=amendment_path,
        amendment=amendment,
        plan=plan,
    )

    drifted = json.loads(json.dumps(report))
    drifted["model_artifact"]["sha256"] = "0" * 64
    drifted["canonical_report_sha256"] = subject.canonical_document_sha256(
        drifted,
        "canonical_report_sha256",
    )
    _write_json(report_path, drifted)
    with pytest.raises(subject.DownstreamProvenanceError, match="SHA256 differs"):
        subject.validate_training_report_for_parity(
            report_path,
            artifact_path=artifact_path,
            artifact=validated_artifact,
            plan_path=plan_path,
            panel_path=panel_path,
            lockstep_report_path=lockstep_path,
            amendment_path=amendment_path,
            amendment=amendment,
            plan=plan,
        )
