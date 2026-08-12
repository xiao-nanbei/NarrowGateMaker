from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from research.families.f06_placement_fill_cif.audit.evaluate_policy_clock_fill_cif import evaluate


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_policy_clock_evaluator_keeps_three_gates_separate(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    family = root / "research" / "families" / "f06_placement_fill_cif" / "audit"
    implementation = family / "policy_clock_fill_cif.py"
    evaluator = family / "evaluate_policy_clock_fill_cif.py"
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(
        json.dumps(
            {
                "schema_version": (
                    "narrowgate_placement_fill_policy_clock_race_fit_spec.v1"
                ),
                "family_id": "placement_fill_policy_clock_race_v1",
                "lineage": {
                    "implementation": "research/families/f06_placement_fill_cif/audit/policy_clock_fill_cif.py",
                    "implementation_sha256": _sha256(implementation),
                    "evaluator": "research/families/f06_placement_fill_cif/audit/evaluate_policy_clock_fill_cif.py",
                    "evaluator_sha256": _sha256(evaluator),
                },
                "reporting": {
                    "frozen_empirical_horizons_ms": {"p25": 100, "p75": 200},
                    "curve_level_gate": {
                        "curve_count": 1,
                        "bootstrap_samples": 25,
                        "bootstrap_seed": 7,
                        "required_oof_days": 1,
                        "minimum_events_per_cause": 0,
                        "probability_simplex_tolerance": 1e-6,
                        "monotonicity_tolerance": 1e-9,
                        "latency_parity": {
                            "required_oof_days": 1,
                            "minimum_ack_rows_per_side_threshold": 1,
                            "monotonicity_tolerance": 1e-9,
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    fill_rows = []
    outcomes = {
        100: [(1, 0, 0), (0, 0, 1)],
        200: [(1, 0, 0), (0, 1, 0)],
    }
    for horizon, rows in outcomes.items():
        for index, (fill, cancel, no_event) in enumerate(rows):
            fill_rows.append(
                {
                    "action_lifecycle_id": f"life-{index}",
                    "cohort_id": f"cohort-{index}",
                    "day": "2026-01-01",
                    "side": "BUY",
                    "inventory_role": "opener",
                    "action": "current",
                    "horizon_ms": horizon,
                    "fill_probability": float(fill),
                    "cancel_ack_probability": float(cancel),
                    "no_event_probability": float(no_event),
                    "fill_target": fill,
                    "cancel_ack_target": cancel,
                    "no_event_target": no_event,
                    "baseline_fill_probability": 0.2,
                    "baseline_cancel_ack_probability": 0.2,
                    "baseline_no_event_probability": 0.6,
                }
            )
    fill_path = tmp_path / "fill.parquet"
    pd.DataFrame(fill_rows).to_parquet(fill_path, index=False)

    latency_rows = []
    for threshold, targets in ((5, (1, 0)), (10, (1, 1))):
        for index, target in enumerate(targets):
            latency_rows.append(
                {
                    "action_lifecycle_id": f"life-{index}",
                    "cohort_id": f"cohort-{index}",
                    "day": "2026-01-01",
                    "side": "BUY",
                    "inventory_role": "opener",
                    "action": "current",
                    "cancel_request_reason": "requote_replace",
                    "latency_threshold_ms": threshold,
                    "observed_ack_latency_ms": 5.0 + 5.0 * index,
                    "ack_latency_target": target,
                    "ack_latency_probability": float(target),
                    "baseline_ack_latency_probability": 0.5,
                }
            )
    latency_path = tmp_path / "latency.parquet"
    pd.DataFrame(latency_rows).to_parquet(latency_path, index=False)

    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps(
            {
                "family_id": "placement_fill_policy_clock_race_v1",
                "spec": {"path": str(spec_path), "sha256": _sha256(spec_path)},
                "policy_request_parity": {"passed": True},
                "outputs": {
                    "oof_predictions": {
                        "path": str(fill_path),
                        "sha256": _sha256(fill_path),
                    },
                    "oof_ack_latency_predictions": {
                        "path": str(latency_path),
                        "sha256": _sha256(latency_path),
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    result = evaluate(report_path, bootstrap_samples=25, bootstrap_seed=7)

    assert result["policy_request_gate"]["passed"] is True
    assert result["ack_latency_gate"]["passed"] is True
    assert result["fill_cif_gate"]["passed"] is True
    assert result["development_curve_gate_passed"] is True
    assert result["validation_access_allowed"] is False
    assert result["action_or_live_authorization"] is False
