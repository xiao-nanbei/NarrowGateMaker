from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    sell_first_fill_conditional_value_artifact as artifact,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _implementation_identity() -> dict[str, str]:
    return {
        relative: artifact.sha256_file(artifact.ROOT / relative)
        for relative in artifact.REQUIRED_IMPLEMENTATION_PATHS
    }


def _fit_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    days = ["2026-04-20", "2026-04-22"]
    f10_path = tmp_path / "f10.json"
    _write_json(
        f10_path,
        {
            "panels": {
                "development_primary_grade_a_days": [days[0]],
                "development_sensitivity_grade_b_days": [days[1]],
            }
        },
    )
    producer_spec_path = tmp_path / "producer-spec.json"
    _write_json(
        producer_spec_path,
        {
            "f10_spec_identity": {
                "path": str(f10_path),
                "sha256": artifact.sha256_file(f10_path),
            }
        },
    )
    audit_path = tmp_path / "producer-audit.parquet"
    pd.DataFrame({"day": days}).to_parquet(audit_path, index=False)
    trace_path = tmp_path / "trace.parquet"
    pd.DataFrame({"placeholder": [1]}).to_parquet(trace_path, index=False)
    day_audits = []
    for day in days:
        checkpoint_path = tmp_path / f"{day}.parquet"
        pd.DataFrame({"day": [day]}).to_parquet(checkpoint_path, index=False)
        day_audits.append(
            {
                "day": day,
                "run_mode": "formal_development_native_production",
                "producer_spec_sha256": artifact.sha256_file(
                    producer_spec_path
                ),
                "trace_path": str(checkpoint_path),
                "trace_sha256": artifact.sha256_file(checkpoint_path),
                "runtime_audit": {
                    "source_payload_audit": {
                        "day": day,
                        "payload_count": 1,
                        "payload_identity_sha256": "a" * 64,
                    }
                },
            }
        )
    producer_path = tmp_path / "producer-manifest.json"
    producer = {
        "schema_version": artifact.producer_runner.RUN_SCHEMA_VERSION,
        "identity": artifact.producer_runner.IDENTITY,
        "mode": "formal_development_native_production",
        "complete_development": True,
        "producer_identity": {
            "native_producer_spec": {
                "path": str(producer_spec_path),
                "sha256": artifact.sha256_file(producer_spec_path),
            }
        },
        "expected_days": days,
        "requested_days": days,
        "producer_audit_path": str(audit_path),
        "producer_audit_sha256": artifact.sha256_file(audit_path),
        "day_audits": day_audits,
        "decision_feature_support": {
            "passed": True,
            "model_features": list(artifact.lifecycle_contract.MODEL_FEATURES),
        },
        "true_opener_support": {
            "passed": True,
            "coverage": 1.0,
            "minimum_coverage": 0.99,
        },
        "trace_path": str(trace_path),
        "trace_sha256": artifact.sha256_file(trace_path),
    }
    _write_json(producer_path, producer)
    monkeypatch.setattr(
        artifact.lifecycle_contract,
        "validate_spec",
        lambda payload: None,
    )
    monkeypatch.setattr(
        artifact.producer_runner,
        "validate_producer_spec",
        lambda payload: None,
    )
    fit = {
        "schema_version": artifact.FIT_SCHEMA_VERSION,
        "identity": artifact.IDENTITY,
        "status": "frozen_after_complete_native_development_artifact",
        "permissions": {
            "development_outcome_read": True,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
        "f10_spec_identity": {
            "path": str(f10_path),
            "sha256": artifact.sha256_file(f10_path),
        },
        "producer_manifest_identity": {
            "path": str(producer_path),
            "sha256": artifact.sha256_file(producer_path),
        },
        "trace_identity": {
            "path": str(trace_path),
            "sha256": artifact.sha256_file(trace_path),
        },
        "implementation_identity": _implementation_identity(),
    }
    fit["canonical_fit_sha256"] = artifact.canonical_fit_sha256(fit)
    return fit


def test_fit_admission_checks_transitive_producer_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fit = _fit_identity(tmp_path, monkeypatch)
    artifact.validate_fit_identity(fit)

    producer_path = Path(fit["producer_manifest_identity"]["path"])
    producer = json.loads(producer_path.read_text(encoding="utf-8"))
    producer["requested_days"] = producer["requested_days"][:-1]
    _write_json(producer_path, producer)
    fit["producer_manifest_identity"]["sha256"] = artifact.sha256_file(
        producer_path
    )
    fit["canonical_fit_sha256"] = artifact.canonical_fit_sha256(fit)
    with pytest.raises(ValueError, match="denominator drifted"):
        artifact.validate_fit_identity(fit)


def test_fit_admission_rejects_empty_implementation_or_later_permission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fit = _fit_identity(tmp_path, monkeypatch)
    fit["implementation_identity"] = {}
    fit["canonical_fit_sha256"] = artifact.canonical_fit_sha256(fit)
    with pytest.raises(ValueError, match="implementation identity is incomplete"):
        artifact.validate_fit_identity(fit)

    fit = _fit_identity(tmp_path, monkeypatch)
    fit["permissions"]["validation_read"] = True
    fit["canonical_fit_sha256"] = artifact.canonical_fit_sha256(fit)
    with pytest.raises(ValueError, match="forbidden authority"):
        artifact.validate_fit_identity(fit)


def test_fit_admission_rejects_diagnostic_producer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fit = _fit_identity(tmp_path, monkeypatch)
    producer_path = Path(fit["producer_manifest_identity"]["path"])
    producer = json.loads(producer_path.read_text(encoding="utf-8"))
    producer["mode"] = "partial_diagnostic_only"
    _write_json(producer_path, producer)
    fit["producer_manifest_identity"]["sha256"] = artifact.sha256_file(
        producer_path
    )
    fit["canonical_fit_sha256"] = artifact.canonical_fit_sha256(fit)

    with pytest.raises(ValueError, match="diagnostic checkpoints"):
        artifact.validate_fit_identity(fit)
