from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f06_placement_fill_cif.audit import (
    evaluate_nested_role_competing_curve_cif as evaluator,
)
from research.families.f06_placement_fill_cif.audit.placement_fill_spec import load_placement_fill_spec


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_v6_evaluator_reuses_v4_gate_and_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = load_placement_fill_spec(evaluator.V6_SPEC)
    base = (
        Path(__file__).resolve().parents[1]
        / "research"
        / "families"
        / "f06_placement_fill_cif"
        / "audit"
        / "evaluate_competing_curve_cif.py"
    )
    spec["lineage"]["base_evaluator"] = str(base)
    spec["lineage"]["base_evaluator_sha256"] = _sha256(base)
    migrated_spec = tmp_path / "v6_migrated_spec.json"
    migrated_spec.write_text(json.dumps(spec), encoding="utf-8")
    monkeypatch.setattr(evaluator, "V6_SPEC", migrated_spec)
    rows = []
    for horizon in (5010, 5816, 7900):
        for action, fill in {
            "closer_1tick": 0.03,
            "current": 0.02,
            "farther_1tick": 0.01,
        }.items():
            rows.append(
                {
                    "action_lifecycle_id": f"one:{action}",
                    "cohort_id": "one",
                    "day": "2026-01-01",
                    "side": "BUY",
                    "inventory_role": "opener",
                    "action": action,
                    "horizon_ms": horizon,
                    "activation_probability": 1.0,
                    "fill_probability": fill,
                    "cancel_ack_probability": 0.40,
                    "no_event_probability": 0.60 - fill,
                    "fill_target": 0,
                    "cancel_ack_target": 0,
                    "no_event_target": 1,
                    "baseline_fill_probability": 0.01,
                    "baseline_cancel_ack_probability": 0.30,
                    "baseline_no_event_probability": 0.69,
                    "fold": 0,
                }
            )
    oof_path = tmp_path / "oof.parquet"
    pd.DataFrame(rows).to_parquet(oof_path, index=False)
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps(
            {
                "family_id": "placement_fill_nested_role_competing_cif_v6",
                "outputs": {
                    "oof_nested_role_predictions": {
                        "path": str(oof_path),
                        "sha256": _sha256(oof_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = evaluator.evaluate(report_path, bootstrap_samples=25, bootstrap_seed=7)

    assert result["development_curve_gate_passed"] is False
    assert result["validation_read"] is False
    assert result["action_or_live_authorization"] is False
    assert result["gate_inherited_without_change_from"]["path"].endswith(
        "placement_fill_full_curve_competing_cif_v4_spec_20260727.json"
    )
