from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from research.families.f06_placement_fill_cif.audit.evaluate_competing_curve_cif import evaluate


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_competing_curve_evaluator_fails_closed_without_support(
    tmp_path: Path,
) -> None:
    rows = []
    actions = {
        "closer_1tick": 0.03,
        "current": 0.02,
        "farther_1tick": 0.01,
    }
    for horizon in (5010, 5816, 7900):
        for action, fill in actions.items():
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
                "family_id": "placement_fill_full_curve_competing_cif_v4",
                "outputs": {
                    "oof_competing_predictions": {
                        "path": str(oof_path),
                        "sha256": _sha256(oof_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = evaluate(report_path, bootstrap_samples=25, bootstrap_seed=7)

    assert result["identity"]["identity_pass"] is True
    assert result["development_curve_gate_passed"] is False
    assert result["validation_read"] is False
    assert result["sealed_holdout_read"] is False
    assert result["action_or_live_authorization"] is False
