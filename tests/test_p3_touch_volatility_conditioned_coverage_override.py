from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from research.families.f02_empirical_p3_touch.audit.p3_touch_volatility_conditioned_coverage_override import (
    canonical_sha256,
    evaluate,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def test_coverage_override_changes_only_threshold_and_passes(tmp_path: Path) -> None:
    original_root = tmp_path / "original"
    original_root.mkdir()
    original_spec = original_root / "spec.json"
    original_report = original_root / "report.json"
    original_manifest = original_root / "manifest.json"
    cache_usage = original_root / "cache_usage.csv"
    development_report = original_root / "development.md"
    current_v2 = original_root / "v2.json"
    model = original_root / "model.txt"

    _write_json(
        original_spec,
        {
            "identity": "p3_touch_volatility_conditioned_v4",
            "evaluation": {"context_coverage_gate": {"minimum_fraction": 0.98}},
        },
    )
    model.write_text("frozen model\n", encoding="utf-8")
    original_report_payload = {
        "decision": "conditional_v4_prediction_gate_failed_development",
        "gates": {
            "proper_score_passed": True,
            "calibration_passed": True,
            "source_transport_passed": True,
            "monotonicity_contract_valid": True,
            "context_coverage_passed": False,
            "historical_prediction_gate_passed": False,
        },
        "context_cache": {"minimum_coverage_fraction": 0.9685},
        "final_development_artifact": {"model": {"path": str(model)}},
        "proper_score": {"passed": True},
        "calibration_gate": {"passed": True},
        "source_prediction_transport": [{"passed": True}],
        "monotonicity_contract": {"violations": 0},
    }
    _write_json(original_report, original_report_payload)
    _write_json(
        original_manifest,
        {
            "files": {
                "model.txt": {
                    "sha256": _sha(model),
                    "size_bytes": model.stat().st_size,
                }
            }
        },
    )
    pd.DataFrame(
        [
            {
                "source": "native",
                "panel": "historical",
                "day": "2026-01-01",
                "windows": 8362,
                "cache_hit": False,
                "cache_path": "/cache/a.npz",
                "cache_key": "a",
                "coverage_fraction": 0.9685,
            },
            {
                "source": "provider",
                "panel": "fit",
                "day": "2025-08-01",
                "windows": 8634,
                "cache_hit": False,
                "cache_path": "/cache/b.npz",
                "cache_key": "b",
                "coverage_fraction": 1.0,
            },
        ]
    ).to_csv(cache_usage, index=False)
    development_report.write_text("frozen report\n", encoding="utf-8")
    _write_json(current_v2, {"identity": "v2"})

    implementation = (
        Path(__file__).resolve().parents[1]
        / "research/families/f02_empirical_p3_touch/audit/"
        "p3_touch_volatility_conditioned_coverage_override.py"
    )
    paths = {
        "coverage_override_implementation": implementation,
        "current_v2_artifact": current_v2,
        "original_v4_cache_usage": cache_usage,
        "original_v4_development_report": development_report,
        "original_v4_manifest": original_manifest,
        "original_v4_report": original_report,
        "original_v4_spec": original_spec,
    }
    spec = {
        "canonical_spec_identity_sha256": "pending",
        "coverage_threshold_adjustment": {
            "changed_fields": [
                "evaluation.context_coverage_gate.minimum_fraction"
            ],
            "original_identity_immutable": True,
            "original_minimum_fraction": 0.98,
            "outcome_informed": True,
            "successor_minimum_fraction": 0.95,
        },
        "identity": "p3_touch_volatility_conditioned_v4_1",
        "identities": {
            name: {"path": str(path), "sha256": _sha(path)}
            for name, path in paths.items()
        },
        "permissions": {
            "action_authority": False,
            "historical_development_prediction_support": True,
            "independent_confirmation": False,
            "live_authority": False,
            "operational_prediction_authority": False,
            "overwrite_current_v2_artifact": False,
            "quote_mapping_authority": False,
        },
        "schema_version": (
            "narrowgate_p3_touch_volatility_conditioned.v4_1."
            "coverage_override.spec"
        ),
    }
    normalized = dict(spec)
    normalized.pop("canonical_spec_identity_sha256")
    spec["canonical_spec_identity_sha256"] = canonical_sha256(normalized)
    spec_path = tmp_path / "successor_spec.json"
    _write_json(spec_path, spec)

    result = evaluate(spec_path, tmp_path / "result")
    assert result["gates"]["historical_prediction_gate_passed"] is True
    assert result["retraining_performed"] is False
    assert result["predecessor"]["immutable"] is True
    assert result["context_cache"]["minimum_coverage_fraction"] == 0.9685
    assert result["permissions"]["quote_mapping_authority"] is False
